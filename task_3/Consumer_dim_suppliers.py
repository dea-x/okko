from pyspark.shell import spark, sc, sqlContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
from collections import namedtuple
import datetime
import json
import time

PARTITION = 0
TOPIC = "dim_suppliers"
BROKER_LIST = 'cdh631.itfbgroup.local:9092'
HDFS_OUTPUT_PATH = "hdfs://cdh631.itfbgroup.local:8020/user/usertest/okko/dim_suppliers"

HOST_IP = "192.168.88.95"
PORT = "1521"
SID = "orcl"

TARGET_DB_TABLE_NAME = "DIM_SUPPLIERS"
OFFSET_TABLE_NAME = "OFFSET_DIM_SUPPLIERS" 
TARGET_DB_USER_NAME = "test_user"
TARGET_DB_USER_PASSWORD = "test_user"

URL_LOG_TABLE = "jdbc:oracle:thin:@192.168.88.252:1521:oradb"
LOG_TABLE_NAME = "log_table"

def parse(line):
    """ Parsing JSON messages from Kafka Producer """
    data = json.loads(line)
    return data['SUPPLIERS_ID'], data['CATEGORY_ID'], data['NAME'], data['COUNTRY'], data['CITY'], data['LAST_UPDATE_DATE']
    
def deserializer():
    """ Deserializer messages from Kafka Producer """
    return bytes.decode

def write_log(level_log, program_name, procedure_name, message):
    log_row = namedtuple('log_row', 'TIME_LOG LEVEL_LOG PROGRAM_NAME PROCEDURE_NAME MESSAGE'.split())
    data = log_row(datetime.datetime.today(), level_log, program_name, procedure_name, message)
    result = spark.createDataFrame([data])
    result.write \
        .format('jdbc') \
        .mode('append') \
        .option('driver', 'oracle.jdbc.OracleDriver') \
        .option('url', URL_LOG_TABLE) \
        .option('dbtable', LOG_TABLE_NAME) \
        .option('user', TARGET_DB_USER_NAME) \
        .option('password', TARGET_DB_USER_PASSWORD) \
        .save()

def save_data(rdd):
    """
    Parsing JSON value in each RDDs
    Creating Spark SQL DataFrame from RDD
    Writing DataFrame to HDFS and Oracle DB
    """
    global flag
    flag = False
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
        
        max_id = df_max_id.agg({'suppliers_id': 'max'}).collect()[0][0]
        if max_id == None:
            max_id = 0      
            
        rdd = rdd.map(lambda m: parse(m[1]))
        df = sqlContext.createDataFrame(rdd)
        df.createOrReplaceTempView("t")
        result = spark.sql('''select suppliers_id, category_id, name, country, city, last_update_date 
            from (select row_number() over (partition by _1 order by _6) as RN,_1 as suppliers_id,_2 as category_id,_3 as name,
            _4 as country,_5 as city,to_timestamp(_6) as last_update_date from t where _1 > ''' + str(max_id) + ''')
			where RN = 1''')
            
        count = result.count()
        
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
                
            write_log('INFO', 'Consumer_dim_suppliers.py', 'main', '{} rows inserted successfully'.format(count))

        except Exception as e:
            print('--> It seems an Error occurred: {}'.format(e))
            write_log('ERROR', 'Consumer_dim_suppliers.py', 'main', str(e)[:1000])
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