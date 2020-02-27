from pyspark.shell import spark, sc, sqlContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
from collections import namedtuple
import datetime
import json
import time

PARTITION = 0
TOPIC = "dim_event_type"
BROKER_LIST = 'cdh631.itfbgroup.local:9092'
HDFS_OUTPUT_PATH = "hdfs://cdh631.itfbgroup.local:8020/user/usertest/okko/dim_event_type"

TARGET_DB_TABLE_NAME = "DIM_EVENT_TYPE"
OFFSET_TABLE_NAME = "OFFSET_DIM_EVENT_TYPE"
TARGET_DB_USER_NAME = "test_user"
TARGET_DB_USER_PASSWORD = "test_user"
SOURCE_DB_USER_NAME = "test_user"
SOURCE_DB_USER_PASSWORD = "1234"

URL_SOURCE_DB = "jdbc:oracle:thin:@192.168.88.102:1521:orcl"
URL_TARGET_DB = "jdbc:oracle:thin:@192.168.88.95:1521:orcl"
DRIVER = 'oracle.jdbc.OracleDriver'
LOG_TABLE_NAME = "log_table"


def parse(line):
    """ Parsing JSON messages from Kafka Producer """
    data = json.loads(line)
    return data['EVENT_ID'], data['EVENT_TYPE']


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
        .option('driver', DRIVER) \
        .option('url', URL_SOURCE_DB) \
        .option('dbtable', LOG_TABLE_NAME) \
        .option('user', SOURCE_DB_USER_NAME) \
        .option('password', SOURCE_DB_USER_PASSWORD) \
        .save()


def save_data(rdd):
    global flag
    flag = False
    """
    Parsing JSON value in each RDDs
    Creating Spark SQL DataFrame from RDD
    Writing DataFrame to HDFS and Oracle DB
    """
    if not rdd.isEmpty():
        rdd = rdd.map(lambda m: parse(m[1]))
        df = sqlContext.createDataFrame(rdd)
        df.createOrReplaceTempView("t")
        result = spark.sql("select _1 as event_id,_2 as event_type from t")

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
                .option("driver", DRIVER) \
                .option("url", URL_TARGET_DB) \
                .option("dbtable", TARGET_DB_TABLE_NAME) \
                .option("user", TARGET_DB_USER_NAME) \
                .option("password", TARGET_DB_USER_PASSWORD) \
                .save()

            write_log('INFO', 'Consumer_dim_event_type.py', 'main', '{} rows inserted successfully'.format(count))

        except Exception as e:
            print('--> It seems an Error occurred: {}'.format(e))
            write_log('ERROR', 'Consumer_dim_event_type.py', 'main', str(e)[:1000])
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
    Writing value of untilOffset to DB for offsets
    :param untilOffset: Exclusive ending offset.
    """
    if flag != True:
        for o in offsetRanges:
            currentOffset = int(o.untilOffset)
            df_write_offsets = sqlContext.createDataFrame([{"OFFSET": currentOffset}])
            df_write_offsets.write \
                .format("jdbc") \
                .mode("overwrite") \
                .option("driver", DRIVER) \
                .option("url", URL_TARGET_DB) \
                .option("dbtable", OFFSET_TABLE_NAME) \
                .option("user", TARGET_DB_USER_NAME) \
                .option("password", TARGET_DB_USER_PASSWORD) \
                .save()


if __name__ == "__main__":
    ssc = StreamingContext(sc, 5)

    df_read_offsets = spark.read \
        .format("jdbc") \
        .option("driver", DRIVER) \
        .option("url", URL_TARGET_DB) \
        .option("dbtable", OFFSET_TABLE_NAME) \
        .option("user", TARGET_DB_USER_NAME) \
        .option("password", TARGET_DB_USER_PASSWORD) \
        .load()

    maxOffset = df_read_offsets.agg({'OFFSET': 'max'}).collect()[0][0]
    if maxOffset == None:
        maxOffset = 0

    topicPartion = TopicAndPartition(TOPIC, PARTITION)
    fromOffset = {topicPartion: maxOffset}
    kafkaParams = {"metadata.broker.list": BROKER_LIST, "enable.auto.commit": "false"}

    directKafkaStream = KafkaUtils.createDirectStream(ssc, [TOPIC], kafkaParams, fromOffsets=fromOffset,
                                                      keyDecoder=deserializer(), valueDecoder=deserializer())

    time.sleep(5)
    directKafkaStream.foreachRDD(lambda x: save_data(x))
    directKafkaStream.transform(store_offset_ranges) \
        .foreachRDD(write_offset_ranges)

    ssc.start()
    ssc.awaitTermination()
