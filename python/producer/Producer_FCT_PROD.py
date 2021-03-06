from pyspark.shell import spark
import pyspark.sql.functions as sf
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from collections import namedtuple
import datetime
import json

# Topic name
TOPIC = 'fct_prod'
# Parameters of database source
DATABASE_SOURCE = {'url': 'jdbc:oracle:thin:@192.168.88.102:1521:orcl',
                   'user': 'test_user',
                   'password': '1234',
                   'table': 'FCT_PROD'}
# Parameters of database destination
DATABASE_TARGET = {'url': 'jdbc:oracle:thin:@192.168.88.95:1521:orcl',
                   'user': 'test_user',
                   'password': 'test_user',
                   'table': 'FCT_PROD'}
SERVER_ADDRESS = 'cdh631.itfbgroup.local:9092'


def serializer():
    return str.encode


# The implementation of the producer
def send_to_Kafka(rows):
    producer = KafkaProducer(bootstrap_servers=[SERVER_ADDRESS], value_serializer=serializer())
    for row in rows:
        try:
            producer.send(TOPIC, key=str('fct_prod'), value=json.dumps(row.asDict(), default=str))
        except Exception as e:
            # write_log('ERROR', 'Producer_FCT_PROD', 'send_to_Kafka', str(e)[:1000])
            pass
    producer.flush()


def connection_to_bases():
    # creating a dataframe for the target table
    max_id = None
    try:
        df_target = spark.read \
            .format('jdbc') \
            .option('driver', 'oracle.jdbc.OracleDriver') \
            .option('url', DATABASE_TARGET['url']) \
            .option('dbtable', '(select max(id) as id from {0})'.format(DATABASE_TARGET['table'])) \
            .option('user', DATABASE_TARGET['user']) \
            .option('password', DATABASE_TARGET['password']) \
            .load()
        max_id = next(df_target.agg({'id': 'max'}).toLocalIterator())[0]

    except Exception as e:
        write_log('ERROR', 'Producer_FCT_PROD.py', 'connection_to_bases', str(e)[:1000])

    max_id = 0 if max_id is None else max_id

    # creating a dataframe for the source table
    df_source = spark.read \
        .format('jdbc') \
        .option('driver', 'oracle.jdbc.OracleDriver') \
        .option('url', DATABASE_SOURCE['url']) \
        .option('dbtable', '(select * from {0} where id > {1} and id < {2})'.format(DATABASE_SOURCE['table'], max_id,
                                                                                    max_id + 1000000)) \
        .option('user', DATABASE_SOURCE['user']) \
        .option('password', DATABASE_SOURCE['password']) \
        .load()
    return df_source


def write_log(level_log, program_name, procedure_name, message):
    log_row = namedtuple('log_row', 'TIME_LOG LEVEL_LOG PROGRAM_NAME PROCEDURE_NAME MESSAGE'.split())
    data = log_row(datetime.datetime.today(), level_log, program_name, procedure_name, message)
    result = spark.createDataFrame([data])
    result.write \
        .format('jdbc') \
        .mode('append') \
        .option('driver', 'oracle.jdbc.OracleDriver') \
        .option('url', DATABASE_SOURCE['url']) \
        .option('dbtable', 'log_table') \
        .option('user', DATABASE_SOURCE['user']) \
        .option('password', DATABASE_SOURCE['password']) \
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
        # Connection to the Bases
        df_source = connection_to_bases()
        # Finding the max increment value
        # maxID = next(df_target.agg({'id': 'max'}).toLocalIterator())[0]
        # maxID = 0 if maxID is None else maxID
        # Creation of final dataframe
        # dfResult = df_source.where((sf.col('id') > maxID) & (sf.col('id') < maxID + 10000000))
        # Sending dataframe to Kafka
        df_source.foreachPartition(send_to_Kafka)
        # Count offset
        end_offset = get_offset()
        count = end_offset - start_offset  # = df_result.count()
        # Write to logs
        write_log('INFO', 'Producer_FCT_PROD.py', 'main', 'Successful sending of {0} lines'.format(count))
    except Exception as e:
        write_log('ERROR', 'Producer_FCT_PROD.py', 'main', str(e)[:1000])


main()
