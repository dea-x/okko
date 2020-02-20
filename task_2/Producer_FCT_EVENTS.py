import time
start_time_global = time.time()
from pyspark.shell import spark
import pyspark.sql.functions as sf
from kafka import KafkaProducer
import json


def serializer():
    return str.encode


# Serialization in JSON
def build_JSON(event_time, event_type, event_id, product_id, category_id, category_code, brand, price, customer_id):
    data = dict(event_time=event_time, event_type=event_type, event_id=event_id, product_id=product_id,
                category_id=category_id, category_code=category_code, brand=brand, price=price, customer_id=customer_id)
    return json.dumps(data)


# The implementation of the producer
def producer_to_Kafka(dfResult):
    producer = KafkaProducer(bootstrap_servers=['cdh631.itfbgroup.local:9092'],
                             value_serializer=serializer())

    for row in dfResult:
        try:
            event_time = str(row['EVENT_TIME'])
            event_type = row['EVENT_TYPE']
            event_id = int(row['EVENT_ID'])
            product_id = int(row['PRODUCT_ID'])
            category_id = int(row['CATEGORY_ID'])
            category_code = row['CATEGORY_CODE']
            brand = row['BRAND']
            price = int(row['PRICE'])
            customer_id = int(row['CUSTOMER_ID'])

            values = build_JSON(event_time, event_type, event_id, product_id, category_id, category_code, brand, price,
                                customer_id)
            future = producer.send(TOPIC, key=str('fct_events'), value=values)

        except Exception as e:
            print('--> It seems an Error occurred: {}'.format(e))

    producer.flush()


if __name__ == '__main__':
    start_main_time = time.time()
    TOPIC = 'fct_events'
    start_time = time.time()

    # Creating a dataframe for the source table
    df0 = spark.read \
        .format("jdbc") \
        .option("driver", 'oracle.jdbc.OracleDriver') \
        .option("url", "jdbc:oracle:thin:@192.168.88.252:1521:oradb") \
        .option("dbtable", "fct_events") \
        .option("user", "test_user") \
        .option("password", "test_user") \
        .option("batchsize", '10000') \
        .load()

    print("---first connect %s seconds ---" % (time.time() - start_time))
    start_time = time.time()

    # Creating a dataframe for the recipient table
    df1 = spark.read \
        .format("jdbc") \
        .option("driver", 'oracle.jdbc.OracleDriver') \
        .option("url", "jdbc:oracle:thin:@192.168.88.95:1521:orcl") \
        .option("dbtable", "FCT_EVENTS") \
        .option("user", "test_user") \
        .option("password", "test_user") \
        .load()

    print("---second connect %s seconds ---" % (time.time() - start_time))
    start_time = time.time()

    # maxID = df1.agg({'event_id': 'max'}).collect()[0][0]
    maxID = next(df1.agg({'event_id': 'max'}).toLocalIterator())[0]
    
    print("---maxID %s seconds ---" % (time.time() - start_time))
    start_time = time.time()

    if maxID == None:
        maxID = 0
    dfResult = df0.where((sf.col('event_id') > maxID) & (sf.col('event_id') < maxID + 1000000)).toLocalIterator()
    # dfResult = df0.where((sf.col('event_id') > maxID) & (sf.col('event_id') < maxID + 1000000)).collect()

    print("---dfResult %s seconds ---" % (time.time() - start_time))
    start_time = time.time()

    producer_to_Kafka(dfResult)
    print("---Producer %s seconds ---" % (time.time() - start_time))
    print("---Itog %s seconds ---" % (time.time() - start_main_time))
    print("---global %s seconds ---" % (time.time() - start_time_global))
