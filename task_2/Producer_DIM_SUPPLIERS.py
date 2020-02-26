import json
from collections import namedtuple
import datetime
import os

from pyspark.shell import spark
import pyspark.sql.functions as sf
from kafka import KafkaProducer, KafkaConsumer, TopicPartition

# CONSTANTS
# Topic name
TOPIC = 'dim_suppliers_kozyar'
# Parameters of database source
DATABASE_SOURCE = {"url": "jdbc:oracle:thin:@192.168.88.252:1521:oradb",
                   'user': 'test_user',
                   'password': 'test_user'}
# Parameters of database destination
DATABASE_TARGET = {'url': 'jdbc:oracle:thin:@192.168.88.95:1521:orcl',
                   'user': 'test_user',
                   'password': 'test_user'}
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
            raise Exception("My Exception")
        except Exception:
            # print('--> It seems an Error occurred: {}'.format(err))
            # log_process = Process(target=write_log, args=("ERROR", "Producer_DIM_CUSTOMERS.py",
            #                                               "send_to_Kafka", str(err)[:1000]))
            # log_process.daemon = True
            # log_process.start()
            # ERRORS.put(["ERROR", "Producer_DIM_CUSTOMERS.py", "send_to_Kafka", str(err)])
            # ERRORS.append(["ERROR", "Producer_DIM_CUSTOMERS.py", "send_to_Kafka", str(err)])
            # write_log("ERROR", "Producer_DIM_CUSTOMERS.py", "send_to_Kafka", str(err))
            pass
    producer.flush()


def connection_to_bases():
    """ Getting DataFrame from DB """
    # creating a dataframe for the source table
    df_source = spark.read \
        .format('jdbc') \
        .option('driver', 'oracle.jdbc.OracleDriver') \
        .option('url', DATABASE_SOURCE['url']) \
        .option('dbtable', "dim_suppliers") \
        .option('user', DATABASE_SOURCE['user']) \
        .option('password', DATABASE_SOURCE['password']) \
        .load()

    # creating a dataframe for the target table
    df_target = spark.read \
        .format('jdbc') \
        .option('driver', 'oracle.jdbc.OracleDriver') \
        .option('url', DATABASE_TARGET['url']) \
        .option('dbtable', "dim_suppliers".upper()) \
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
        .option('driver', 'oracle.jdbc.OracleDriver') \
        .option("url", DATABASE_SOURCE['url']) \
        .option("dbtable", 'log_table') \
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
    return consumer.position(tp)


def main():
    try:
        start_offset = get_offset()
        df_source, df_target = connection_to_bases()
        last_date = next(df_target.agg({"last_update_date": "max"}).toLocalIterator())[0]
        # last_date = datetime.datetime(2000, 1, 1, 0, 0, 0) if last_date is None else last_date
        if last_date is None:
            df_result = df_source
        else:
            df_result = df_source.where(sf.col("last_update_date") > last_date)
        # Sending dataframe to Kafka
        df_result.foreachPartition(send_to_Kafka)
        # Writing log
        end_offset = get_offset()
        count = end_offset - start_offset  # = df_result.count()
        write_log("INFO", SCRIPT_NAME, "main", "Successful sending of {0} lines".format(count))
    except Exception as err:
        write_log("ERROR", SCRIPT_NAME, "main", str(err)[:1000])


main()
