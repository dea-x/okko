from pyspark.shell import spark
import pyspark.sql.functions as sf
from kafka import KafkaProducer
import json


def serializer():
    return str.encode


# Serialization in JSON
def build_JSON(product_id, category_id, category_code, brand, description, name, price, last_update_date):
    data = dict(product_id=product_id, category_id=category_id, category_code=category_code, brand=brand,
                description=description, name=name, price=price, last_update_date=last_update_date)
    return json.dumps(data)


# The implementation of the producer
def producer_to_Kafka(dfResult):
    producer = KafkaProducer(bootstrap_servers=['cdh631.itfbgroup.local:9092'],
                             value_serializer=serializer())

    for i in range(len(dfResult)):
        try:
            product_id = int(dfResult[i]['PRODUCT_ID'])
            category_id = int(dfResult[i]['CATEGORY_ID'])
            category_code = dfResult[i]['CATEGORY_CODE']
            brand = dfResult[i]['BRAND']
            description = dfResult[i]['DESCRIPTION']
            name = dfResult[i]['NAME']
            price = int(dfResult[i]['PRICE'])
            last_update_date = str(dfResult[i]['LAST_UPDATE_DATE'])

            values = build_JSON(product_id, category_id, category_code, brand, description, name, price,
                                last_update_date)
            print(values)
            future = producer.send(TOPIC, key=str('dim_products'), value=values)

        except Exception as e:
            print('--> It seems an Error occurred: {}'.format(e))

    producer.flush()


if __name__ == '__main__':
    TOPIC = 'dim_products'

    # Creating a dataframe for the source table
    df0 = spark.read \
        .format("jdbc") \
        .option("driver", 'oracle.jdbc.OracleDriver') \
        .option("url", "jdbc:oracle:thin:@192.168.88.252:1521:oradb") \
        .option("dbtable", "dim_products") \
        .option("user", "test_user") \
        .option("password", "test_user") \
        .load()

    # Creating a dataframe for the recipient table
    df1 = spark.read \
        .format("jdbc") \
        .option("driver", 'oracle.jdbc.OracleDriver') \
        .option("url", "jdbc:oracle:thin:@192.168.88.95:1521:orcl") \
        .option("dbtable", "DIM_PRODUCTS") \
        .option("user", "test_user") \
        .option("password", "test_user") \
        .load()

    maxID = df1.agg({'last_update_date': 'max'}).collect()[0][0]
    if maxID == None:
        maxID = 0
    dfResult = df0.where(sf.col('last_update_date') > maxID).collect()
    producer_to_Kafka(dfResult)
