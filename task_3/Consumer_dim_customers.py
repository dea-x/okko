from pyspark.shell import spark, sc, sqlContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
import json
import time

PARTITION = 0
TOPIC = "dim_customers"
BROKER_LIST = 'cdh631.itfbgroup.local:9092'
HDFS_OUTPUT_PATH = "hdfs://cdh631.itfbgroup.local:8020/user/usertest/okko/dim_customers"

HOST_IP = "192.168.88.95"
PORT = "1521"
SID = "orcl"

TARGET_DB_TABLE_NAME = "DIM_CUSTOMERS"
OFFSET_TABLE_NAME = "OFFSET_DIM_CUSTOMERS" 
TARGET_DB_USER_NAME = "test_user"
TARGET_DB_USER_PASSWORD = "test_user"


def parse(line):
    """ Parsing JSON messages from Kafka Producer """
    data = json.loads(line)
    return data['CUSTOMER_ID'], data['COUNTRY'], data['CITY'], data['PHONE'], data['FIRST_NAME'], data['LAST_NAME'], data['MAIL'], data['LAST_UPDATE_DATE']
    
def deserializer():
    """ Deserializer messages from Kafka Producer """
    return bytes.decode

def save_data(rdd):
    global flag
    flag = False
    """
    Parsing JSON value in each RDDs
    Creating Spark SQL DataFrame from RDD
    Writing DataFrame to HDFS and Oracle DB
    """
    if not rdd.isEmpty():
        # Create df for duplicate handling
        df_max_id = spark.read \
            .format("jdbc") \
            .option("driver", 'oracle.jdbc.OracleDriver') \
            .option("url", "jdbc:oracle:thin:@{0}:{1}:{2}".format(HOST_IP, PORT, SID)) \
            .option("dbtable", TARGET_DB_TABLE_NAME) \
            .option("user", TARGET_DB_USER_NAME) \
            .option("password", TARGET_DB_USER_PASSWORD) \
            .load()
        
        max_id = df_max_id.agg({'product_id': 'max'}).collect()[0][0]
        if max_id == None:
            max_id = 0 
            
        rdd = rdd.map(lambda m: parse(m[1]))
        df = sqlContext.createDataFrame(rdd)
        df.createOrReplaceTempView("t")
        result = spark.sql('''select customer_id, country,city, phone, first_name, last_name, mail, last_update_date 
            from (select row_number() over (partition by _1 order by _8) as RN,_1 as customer_id,_2 as country,_3 as city,
            _4 as phone,_5 as first_name,_6 as last_name,_7 as mail,to_timestamp(_8) as last_update_date 
            from t where _1 > ''' + str(max_id) + ''')
            where RN = 1''')

        
        try:
            # Writing to HDFS
            result.write \
                .format("csv") \
                .mode("append") \
                .option("header", "true") \
                .save(HDFS_OUTPUT_PATH)
        
            # Writing to Oracle DB
            result.write \
                .format("jdbc") \
                .mode("append") \
                .option("driver", 'oracle.jdbc.OracleDriver') \
                .option("url", "jdbc:oracle:thin:@{0}:{1}:{2}".format(HOST_IP, PORT, SID)) \
                .option("dbtable", TARGET_DB_TABLE_NAME) \
                .option("user", TARGET_DB_USER_NAME) \
                .option("password", TARGET_DB_USER_PASSWORD) \
                .save()
        except Exception as e:
            print('--> It seems an Error occurred: {}'.format(e))
            flag = True
    else:
        ssc.stop()
    return rdd
    
def store_offset_ranges(rdd):
    """ 
    Storing offsets
    OffsetRanges represents a range of offsets from a single Kafka TopicAndPartition
    """
    global offsetRanges
    offsetRanges = rdd.offsetRanges()
    return rdd
  
def write_offset_ranges(rdd):
    """
    Writing value of untilOffset to *.txt file
    :param untilOffset: Exclusive ending offset.
    """
    if flag != True:
        for o in offsetRanges:
            currentOffset = int(o.untilOffset)
            df1 = sqlContext.createDataFrame([{"OFFSET": currentOffset}])
            df1.write \
                .format("jdbc") \
                .mode("overwrite") \
                .option("driver", 'oracle.jdbc.OracleDriver') \
                .option("url", "jdbc:oracle:thin:@{0}:{1}:{2}".format(HOST_IP, PORT, SID)) \
                .option("dbtable", OFFSET_TABLE_NAME) \
                .option("user", TARGET_DB_USER_NAME) \
                .option("password", TARGET_DB_USER_PASSWORD) \
                .save()

if __name__ == "__main__":
    ssc = StreamingContext(sc, 5)
    
    df1 = spark.read \
        .format("jdbc") \
        .option("driver", 'oracle.jdbc.OracleDriver') \
        .option("url", "jdbc:oracle:thin:@{0}:{1}:{2}".format(HOST_IP, PORT, SID)) \
        .option("dbtable", OFFSET_TABLE_NAME) \
        .option("user", TARGET_DB_USER_NAME) \
        .option("password", TARGET_DB_USER_PASSWORD) \
        .load()
        
    maxOffset = df1.agg({'OFFSET': 'max'}).collect()[0][0]
    if maxOffset == None:
        maxOffset = 0
    
    topicPartion = TopicAndPartition(TOPIC, PARTITION)

    fromOffset = {topicPartion: maxOffset}
    kafkaParams = {"metadata.broker.list": BROKER_LIST}
    kafkaParams["enable.auto.commit"] = "false"

    directKafkaStream = KafkaUtils.createDirectStream(ssc, [TOPIC], kafkaParams, fromOffsets=fromOffset,
                                                      keyDecoder=deserializer(), valueDecoder=deserializer())

    time.sleep(5)
    directKafkaStream.foreachRDD(lambda x: save_data(x))
    directKafkaStream.transform(store_offset_ranges) \
        .foreachRDD(write_offset_ranges)

    ssc.start()
    ssc.awaitTermination()