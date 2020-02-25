from pyspark.shell import spark
import pyspark.sql.functions as sf
from kafka import KafkaProducer
import json

# Topic name
TOPIC = 'vshagFirstTopic'
# Parameters of database source
DATABASE_SOURCE = {"url": "jdbc:oracle:thin:@192.168.88.252:1521:oradb",
                   'user': 'test_user',
                   'password': 'test_user',
                   'table': 'fct_events'}
# Parameters of database destination
DATABASE_DESTINATION = {'url': 'jdbc:oracle:thin:@192.168.88.95:1521:orcl',
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
            print('--> It seems an Error occurred: {}'.format(e))
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
        .option('url', DATABASE_DESTINATION['url']) \
        .option('dbtable', DATABASE_DESTINATION['table'].upper()) \
        .option('user', DATABASE_DESTINATION['user']) \
        .option('password', DATABASE_DESTINATION['password']) \
        .load()
    return df_source, df_target


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

    except Exception as e:
        print('--> It seems an Error occurred: {}'.format(e))


main()
