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

    for i in range(len(dfResult)):
        try:
            event_time = dfResult[i]['EVENT_TIME']
            event_type = dfResult[i]['EVENT_TYPE']
            event_id = int(dfResult[i]['EVENT_ID'])
            product_id = int(dfResult[i]['PRODUCT_ID'])
            category_id = int(dfResult[i]['CATEGORY_ID'])
            category_code = dfResult[i]['CATEGORY_CODE']
            brand = dfResult[i]['BRAND']
            price = int(dfResult[i]['PRICE'])
            customer_id = int(dfResult[i]['CUSTOMER_ID'])

            values = build_JSON(event_time, event_type, event_id, product_id, category_id, category_code, brand, price,
                                customer_id)
            print(values)
            future = producer.send(TOPIC, key=str('fct_events'), value=values)

        except Exception as e:
            print('--> It seems an Error occurred: {}'.format(e))

    producer.flush()


if __name__ == '__main__':
    TOPIC = 'fct_events'
    df0 = spark.read \
        .format("jdbc") \
        .option("driver", 'oracle.jdbc.OracleDriver') \
        .option("url", "jdbc:oracle:thin:@192.168.88.252:1521:orcl") \
        .option("dbtable", "fct_events") \
        .option("user", "test_user") \
        .option("password", "test_user") \
        .load()

    df1 = spark.read \
        .format("jdbc") \
        .option("driver", 'oracle.jdbc.OracleDriver') \
        .option("url", "jdbc:oracle:thin:@192.168.88.95:1521:orcl") \
        .option("dbtable", "fct_events") \
        .option("user", "test_user") \
        .option("password", "test_user") \
        .load()

    maxID = df0.agg({'event_id': 'max'}).collect()[0][0]
    res = df1.where(sf.col('event_id') > maxID).collect()
    producer_to_Kafka(res)
