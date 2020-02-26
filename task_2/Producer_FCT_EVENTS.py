from pyspark.shell import spark
import pyspark.sql.functions as sf
from kafka import KafkaProducer
from collections import namedtuple
import datetime
import json

# Topic name
TOPIC = 'fct_events'
# Parameters of database source
DATABASE_SOURCE = {"url": "jdbc:oracle:thin:@192.168.88.252:1521:oradb",
                   'user': 'test_user',
                   'password': 'test_user',
                   'table': 'fct_events'}
# Parameters of database destination
DATABASE_TARGET = {'url': 'jdbc:oracle:thin:@192.168.88.95:1521:orcl',
                   'user': 'test_user',
                   'password': 'test_user',
                   'table': 'fct_events'}


def serializer():
    return str.encode


# The implementation of the producer
def send_to_Kafka(rows):
    producer = KafkaProducer(bootstrap_servers=['cdh631.itfbgroup.local:9092'],
                             value_serializer=serializer())
    for row in rows:
        try:
            producer.send(TOPIC, key=str('fct_events'), value=json.dumps(row.asDict(), default=str))
        except Exception as e:
            write_log('ERROR', 'Producer_FCT_EVENTS', 'send_to_Kafka', str(e))
    producer.flush()


def connection_to_bases():
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
        .option('dbtable', DATABASE_TARGET['table'].upper()) \
        .option('user', DATABASE_TARGET['user']) \
        .option('password', DATABASE_TARGET['password']) \
        .load()
    return df_source, df_target


def write_log(level_log, program_name, procedure_name, message):
    log_row = namedtuple('log_row', 'TIME_LOG LEVEL_LOG PROGRAM_NAME PROCEDURE_NAME MESSAGE'.split())
    data = log_row(datetime.datetime.today(), level_log, program_name, procedure_name, message)
    result = spark.createDataFrame([data])
    result.write \
        .format("jdbc") \
        .mode("append") \
        .option("driver", 'oracle.jdbc.OracleDriver') \
        .option("url", DATABASE_SOURCE['url']) \
        .option("dbtable", 'log_table') \
        .option("user", DATABASE_SOURCE['user']) \
        .option("password", DATABASE_SOURCE['password']) \
        .save()


def main():
    try:
        # Connection to the Bases
        df_source, df_target = connection_to_bases()
        # Finding the max increment value
        maxID = next(df_target.agg({'event_id': 'max'}).toLocalIterator())[0]
        maxID = 0 if maxID is None else maxID
        # Creation of final dataframe
        dfResult = df_source.where((sf.col('event_id') > maxID) & (sf.col('event_id') < maxID + 1000000))
        # Sending dataframe to Kafka
        dfResult.foreachPartition(send_to_Kafka)
        # Write to logs
        write_log('INFO', 'Producer_FCT_EVENTS', 'main', str(e))
    except Exception as e:
        write_log('ERROR', 'Producer_FCT_EVENTS', 'main', str(e))


main()
