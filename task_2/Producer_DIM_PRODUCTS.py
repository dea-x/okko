from pyspark.shell import spark
import pyspark.sql.functions as sf
from kafka import KafkaProducer
from collections import namedtuple
import datetime
import json

# Topic name
TOPIC = 'dim_products'
# Parameters of database source
DATABASE_SOURCE = {"url": "jdbc:oracle:thin:@192.168.88.252:1521:oradb",
                   'user': 'test_user',
                   'password': 'test_user',
                   'table': 'dim_products'}
# Parameters of database destination
DATABASE_TARGET = {'url': 'jdbc:oracle:thin:@192.168.88.95:1521:orcl',
                   'user': 'test_user',
                   'password': 'test_user',
                   'table': 'DIM_PRODUCTS'}


def serializer():
    return str.encode


# The implementation of the producer
def send_to_Kafka(rows):
    producer = KafkaProducer(bootstrap_servers=['cdh631.itfbgroup.local:9092'],
                             value_serializer=serializer())
    for row in rows:
        try:
            producer.send(TOPIC, key=str('dim_products'), value=json.dumps(row.asDict(), default=str))
        except Exception as e:
            write_log('ERROR', 'Producer_DIM_PRODUCTS', 'send_to_Kafka', str(e)[:1000])
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
        # Connection to the bases
        df_source, df_target = connection_to_bases()
        # Finding the max increment value
        maxID = next(df_target.agg({'last_update_date': 'max'}).toLocalIterator())[0]
        # Creation of final dataframe
        if maxID is None:
            dfResult = df_source
        else:
            dfResult = df_source.where(sf.col('last_update_date') > maxID)
        # Sending dataframe to Kafka
        dfResult.foreachPartition(send_to_Kafka)
        count = dfResult.count()
        # Write to logs
        write_log('INFO', 'Producer_DIM_PRODUCTS', 'main', "Successful sending of {0} lines".format(count))
    except Exception as e:
        write_log('ERROR', 'Producer_DIM_PRODUCTS', 'main', str(e)[:1000])


main()
