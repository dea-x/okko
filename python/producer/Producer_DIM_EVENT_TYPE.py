import json
from collections import namedtuple
import datetime
import os

from pyspark.shell import spark
import pyspark.sql.functions as sf
from kafka import KafkaProducer, KafkaConsumer, TopicPartition

# CONSTANTS
# Topic name
TOPIC = 'dim_event_type'
# Parameters of database source
DATABASE_SOURCE = {"url": "jdbc:oracle:thin:@192.168.88.102:1521:orcl",
                   'user': 'test_user',
                   'password': '1234',
                   'table': 'dim_event_type'}
# Parameters of database destination
DATABASE_TARGET = {'url': 'jdbc:oracle:thin:@192.168.88.95:1521:orcl',
                   'user': 'test_user',
                   'password': 'test_user',
                   'table': 'dim_event_type'}
LOG_TABLE_NAME = 'log_table'
SERVER_ADDRESS = "cdh631.itfbgroup.local:9092"
# program name
SCRIPT_NAME = os.path.basename(__file__)


def serializer():
    """ Function for serialization """
    return str.encode


def send_to_Kafka(rows):
    """ Function for sending data in Kafka """
    producer = KafkaProducer(bootstrap_servers=[SERVER_ADDRESS], value_serializer=serializer())
    for row in rows:
        try:
            producer.send(TOPIC, key=TOPIC, value=json.dumps(row.asDict(), default=str))
        except Exception:
            # print('--> It seems an Error occurred: {}'.format(e))
            # write_log("ERROR", SCRIPT_NAME, "send_to_Kafka", str(err))
            pass
    producer.flush()


def connection_to_bases():
    """ Getting DataFrame from DB """
    # creating a dataframe for the source table
    df_source = spark.read \
        .format('jdbc') \
        .option('driver', 'oracle.jdbc.OracleDriver') \
        .option('url', DATABASE_SOURCE['url']) \
        .option('dbtable', DATABASE_SOURCE['table']) \
        .option('user', DATABASE_SOURCE['user']) \
        .option('password', DATABASE_SOURCE['password']) \
        .load()

    # creating a dataframe for the target table
    df_target = spark.read \
        .format('jdbc') \
        .option('driver', 'oracle.jdbc.OracleDriver') \
        .option('url', DATABASE_TARGET['url']) \
        .option('dbtable', DATABASE_TARGET['table']) \
        .option('user', DATABASE_TARGET['user']) \
        .option('password', DATABASE_TARGET['password']) \
        .load()
    return df_source, df_target


def write_log(level_log, program_name, procedure_name, message):
    """ Function for writing log

    :param level_log: level of logging, can be one of ["INFO", "WARN", "ERROR"];
    :param program_name: script's name;
    :param procedure_name: function's name;
    :param message: description of the recording.
    """
    log_row = namedtuple('log_row', 'TIME_LOG LEVEL_LOG PROGRAM_NAME PROCEDURE_NAME MESSAGE'.split())
    data = log_row(datetime.datetime.today(), level_log, program_name, procedure_name, message)
    result = spark.createDataFrame([data])
    result.write \
        .format("jdbc") \
        .mode("append") \
        .option("driver", 'oracle.jdbc.OracleDriver') \
        .option("url", DATABASE_SOURCE['url']) \
        .option("dbtable", LOG_TABLE_NAME) \
        .option("user", DATABASE_SOURCE['user']) \
        .option("password", DATABASE_SOURCE['password']) \
        .save()


def get_offset():
    """ Function to receive current offset """
    consumer = KafkaConsumer(TOPIC, bootstrap_servers=[SERVER_ADDRESS])
    # get partition
    # part = consumer.partitions_for_topic(TOPIC)
    # part = part.pop()
    tp = TopicPartition(TOPIC, 0)
    consumer.topics()
    # consumer.seek_to_end(tp)
    return consumer.position(tp)


def main():
    try:
        start_offset = get_offset()
        df_source, df_target = connection_to_bases()
        max_id = next(df_target.agg({"EVENT_ID": "max"}).toLocalIterator())[0]
        max_id = 0 if max_id is None else max_id
        df_result = df_source.where(sf.col("EVENT_ID") > max_id)
        # Sending dataframe to Kafka
        df_result.foreachPartition(send_to_Kafka)
        # Writing log
        end_offset = get_offset()
        count = end_offset - start_offset  # = df_result.count()
        write_log("INFO", SCRIPT_NAME, "main", "Successful sending of {0} lines".format(count))
    except Exception as err:
        write_log("ERROR", SCRIPT_NAME, "main", str(err)[:1000])


main()
