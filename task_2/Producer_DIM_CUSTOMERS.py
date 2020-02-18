from pyspark.shell import spark
import pyspark.sql.functions as sf
from kafka import KafkaProducer
import json


def serializer():
    return str.encode


# Serialization in JSON
def build_JSON(customer_id, country, city, phone, first_name, last_name, mail, last_update_date):
    data = dict(customer_id=customer_id, country=country, city=city, phone=phone, first_name=first_name,
                last_name=last_name, mail=mail, last_update_date=last_update_date)
    return json.dumps(data)


# The implementation of the producer
def producer_to_Kafka(dfResult):
    producer = KafkaProducer(bootstrap_servers=['cdh631.itfbgroup.local:9092'],
                             value_serializer=serializer())

    for i in range(len(dfResult)):
        try:
            customer_id = int(dfResult[i]['CUSTOMER_ID'])
            country = dfResult[i]['COUNTRY']
            city = dfResult[i]['CITY']
            phone = dfResult[i]['PHONE']
            first_name = dfResult[i]['FIRST_NAME']
            last_name = dfResult[i]['LAST_NAME']
            mail = dfResult[i]['MAIL']
            last_update_date = str(dfResult[i]['LAST_UPDATE_DATE'])

            values = build_JSON(customer_id, country, city, phone, first_name, last_name, mail, last_update_date)
            print(values)
            future = producer.send(TOPIC, key=str('dim_customers'), value=values)

        except Exception as e:
            print('--> It seems an Error occurred: {}'.format(e))

    producer.flush()


if __name__ == '__main__':
    TOPIC = 'dim_customers'

    # Creating a dataframe for the source table
    df0 = spark.read \
        .format("jdbc") \
        .option("driver", 'oracle.jdbc.OracleDriver') \
        .option("url", "jdbc:oracle:thin:@192.168.88.252:1521:oradb") \
        .option("dbtable", "dim_customers") \
        .option("user", "test_user") \
        .option("password", "test_user") \
        .load()

    # Creating a dataframe for the recipient table
    df1 = spark.read \
        .format("jdbc") \
        .option("driver", 'oracle.jdbc.OracleDriver') \
        .option("url", "jdbc:oracle:thin:@192.168.88.95:1521:orcl") \
        .option("dbtable", "DIM_CUSTOMERS") \
        .option("user", "test_user") \
        .option("password", "test_user") \
        .load()

    maxID = df1.agg({'last_update_date': 'max'}).collect()[0][0]
    if maxID == None:
        maxID = 0
    dfResult = df0.where(sf.col('last_update_date') > maxID).collect()
    producer_to_Kafka(dfResult)
