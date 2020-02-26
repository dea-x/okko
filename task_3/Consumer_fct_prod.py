from pyspark.shell import spark, sc, sqlContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
from collections import namedtuple
import datetime
import json
import time

PARTITION = 0
TOPIC = "fct_prod"
BROKER_LIST = 'cdh631.itfbgroup.local:9092'
HDFS_OUTPUT_PATH = "hdfs://cdh631.itfbgroup.local:8020/user/usertest/okko/fct_prod"

HOST_IP = "192.168.88.95"
PORT = "1521"
SID = "orcl"

TARGET_DB_TABLE_NAME = "FCT_PROD"
OFFSET_TABLE_NAME = "OFFSET_FCT_PROD"
LOG_TABLE_NAME = "LOG_TABLE_TARGET"
TARGET_DB_USER_NAME = "test_user"
TARGET_DB_USER_PASSWORD = "test_user"


def parse(line):
    """ Parsing JSON messages from Kafka Producer """
    data = json.loads(line)
    return data['ID'], data['EVENT_ID'], data['EVENT_TIME'], data['PRODUCT_ID'], data['CUSTOMER_ID']



def deserializer():
    """ Deserializer messages from Kafka Producer """
    return bytes.decode

def write_log(level_log, program_name, message):
    log_row = namedtuple('log_row', 'TIME_LOG LEVEL_LOG PROGRAM_NAME MESSAGE'.split())
    data = log_row(datetime.datetime.today(), level_log, program_name, message)
    result = spark.createDataFrame([data])
    result.write \
        .format('jdbc') \
        .mode('append') \
        .option('driver', 'oracle.jdbc.OracleDriver') \
        .option('url', "jdbc:oracle:thin:@{0}:{1}:{2}".format(HOST_IP, PORT, SID)) \
        .option('dbtable', LOG_TABLE_NAME) \
        .option('user', TARGET_DB_USER_NAME) \
        .option('password', TARGET_DB_USER_PASSWORD) \
        .save()


def save_data(rdd):
    global flag
    flag = False
    """
    Parsing JSON value in each RDDs
    Creating Spark SQL dataFrame from RDD
    Writing dataFrame to HDFS and Oracle DB
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
        
        max_id = df_max_id.agg({'id': 'max'}).collect()[0][0]
        if max_id == None:
            max_id = 0        
    
        rdd = rdd.map(lambda m: parse(m[1]))
        df_fct_events = sqlContext.createDataFrame(rdd)
        df.createOrReplaceTempView("t")
        result = spark.sql(
            '''select id, event_id, event_time, product_id, customer_id
        from (select row_number() over (partition by _1 order by _3) as RN, _1 as id,_2 as event_id,
        to_timestamp(_3) as event_time,_4 as product_id,_5 as customer_id
                    from t where _1 > ''' + str(max_id) + ''')
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
            write_log('INFO', TARGET_DB_TABLE_NAME, '{} rows inserted successfully'.format(count))

        except Exception as e:
            print('--> It seems an Error occurred: {}'.format(e))
            write_log('ERROR', TARGET_DB_TABLE_NAME, str(e)[:1000])
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
    Writing value of untilOffset to DB table
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

