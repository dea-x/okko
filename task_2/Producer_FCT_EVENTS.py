from pyspark.shell import spark
import pyspark.sql.functions as sf
from kafka import KafkaProducer
import json


def serializer():
    return str.encode


# The implementation of the producer
def producer_to_Kafka(dfResult):
    producer = KafkaProducer(bootstrap_servers=['cdh631.itfbgroup.local:9092'],
                             value_serializer=serializer())

    for row in dfResult:
        try:
            producer.send('fct_events', json.dumps(row.asDict(), default=str))
        except Exception as e:
            print('--> It seems an Error occurred: {}'.format(e))
    producer.flush()


def connection_to_bases(database_source, database_destination):
    # creating a dataframe for the source table
    dfSource = spark.read \
        .format('jdbc') \
        .option('driver', 'oracle.jdbc.oracledriver') \
        .option('url', database_source['url']) \
        .option('dbtable', database_source['table']) \
        .option('user', database_source['user']) \
        .option('password', database_source['password']) \
        .load()

    # creating a dataframe for the target table
    dfTarget = spark.read \
        .format('jdbc') \
        .option('driver', 'oracle.jdbc.oracledriver') \
        .option('url', database_destination['url']) \
        .option('dbtable', database_source['table'].upper()) \
        .option('user', database_destination['user']) \
        .option('password', database_destination['password']) \
        .load()

    return (dfSource, dfTarget)


if __name__ == '__main__':
    # Topic name
    TOPIC = 'vshagFirstTopic'
    # Parameters of database source
    DATABASE_SOURCE = {'url': 'jdbc:oracle:thin:@192.168.88.252:1521:oradb', 'user': 'test_user',
                       'password': 'test_user', 'table': 'fct_events'}
    # Parameters of database destination
    DATABASE_DESTINATION = {'url': 'jdbc:oracle:thin:@192.168.88.95:1521:orcl', 'user': 'test_user',
                            'password': 'test_user', 'table': 'fct_events'}

    try:
        # Connection to the Bases
        dfSource, dfTarget = connection_to_bases(DATABASE_SOURCE, DATABASE_DESTINATION)
    except Exception as e:
        print('Incorrect connection')

    # Finding the max increment value
    maxID = next(dfTarget.agg({'event_id': 'max'}).toLocalIterator())[0]
    # Creation of final dataframe
    if maxID == None:
        dfResult = dfSource.toLocalIterator()
    else:
        dfResult = dfSource.where(
            (sf.col('event_id') > maxID) & (sf.col('event_id') < maxID + 10)).toLocalIterator()
    # Sending dataframe to Kafka
    producer_to_Kafka(dfResult)
