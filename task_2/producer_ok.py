from pyspark.shell import spark
import pyspark.sql.functions as sf
from kafka import KafkaProducer
import json


def serializer():
    return str.encode


# Serialization in JSON
def build_JSON(event_time, event_id, product_id, category_id, category_code, brand, price, customer_id,
               customer_session):
    data = dict(event_time=event_time, event_id=event_id, product_id=product_id, category_id=category_id,
                category_code=category_code, brand=brand, price=price, customer_id=customer_id,
                customer_session=customer_session)
    return json.dumps(data)


# The implementation of the producer
def producer_to_Kafka(dfResult):
    producer = KafkaProducer(bootstrap_servers=['cdh631.itfbgroup.local:9092'],
                             key_serializer=serializer(), value_serializer=serializer())

    for i in range(len(dfResult)):
        try:
            event_time = dfResult[i]['event_time']
            event_id = int(dfResult[i]['event_id'])
            product_id = int(dfResult[i]['product_id'])
            category_id = int(dfResult[i]['category_id'])
            category_code = dfResult[i]['category_code']
            brand = dfResult[i]['brand']
            price = int(dfResult[i]['price'])
            customer_id = int(dfResult[i]['customer_id'])
            customer_session = dfResult[i]['customer_session']

            values = build_JSON(event_time, event_id, product_id, category_id, category_code, brand, price, customer_id,
                                customer_session)
            print(values)
            future = producer.send(TOPIC, key=str('fact_table'), value=values)

        except Exception as e:
            print('--> It seems an Error occurred: {}'.format(e))

    producer.flush()


if __name__ == "__main__":
    # Topic for sending data
    TOPIC = 'vshagFirstTopic'

    # Creating a dataframe for the source table
    df0 = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:oracle:thin:@192.168.88.252:1521/oradb") \
        .option("dbtable", "test_events") \
        .option("user", "test_user") \
        .option("password", "test_user") \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .load()

    # Creating a dataframe for the recipient table
    df1 = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:oracle:thin:@192.168.88.98:1521:orcl") \
        .option("dbtable", "test_events_target") \
        .option("user", "test_user") \
        .option("password", "test_user") \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .load()

    # Finding the last event in the recipient table
    row1 = df1.agg({"event_id": "max"}).collect()[0]
    maxID = int(row1["max(event_id)"])

    # Creating a dataframe for transmission
    dfResult = df0.where(sf.col("event_id") > maxID).collect()
    producer_to_Kafka(dfResult)
