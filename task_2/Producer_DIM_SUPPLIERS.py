from pyspark.shell import spark
import pyspark.sql.functions as sf
from kafka import KafkaProducer
import json


def serializer():
    return str.encode


# Serialization in JSON
def build_JSON(suppliers_id, category, name, country, city, last_update_date):
    data = dict(suppliers_id=suppliers_id, category=category, name=name, country=country, city=city,
                last_update_date=last_update_date)
    return json.dumps(data)


# The implementation of the producer
def producer_to_Kafka(dfResult):
    producer = KafkaProducer(bootstrap_servers=['cdh631.itfbgroup.local:9092'],
                             value_serializer=serializer())

    for i in range(len(dfResult)):
        try:
            suppliers_id = int(dfResult[i]['SUPPLIERS_ID'])
            category = dfResult[i]['CATEGORY']
            name = dfResult[i]['NAME']
            country = dfResult[i]['COUNTRY']
            city = dfResult[i]['CITY']
            last_update_date = str(dfResult[i]['LAST_UPDATE_DATE'])

            values = build_JSON(suppliers_id, category, name, country, city, last_update_date)
            print(values)
            future = producer.send(TOPIC, key=str('dim_suppliers'), value=values)

        except Exception as e:
            print('--> It seems an Error occurred: {}'.format(e))

    producer.flush()


if __name__ == '__main__':
    TOPIC = 'dim_suppliers'

    # Creating a dataframe for the source table
    df0 = spark.read \
        .format("jdbc") \
        .option("driver", 'oracle.jdbc.OracleDriver') \
        .option("url", "jdbc:oracle:thin:@192.168.88.252:1521:oradb") \
        .option("dbtable", "dim_suppliers") \
        .option("user", "test_user") \
        .option("password", "test_user") \
        .load()

    # Creating a dataframe for the recipient table
    df1 = spark.read \
        .format("jdbc") \
        .option("driver", 'oracle.jdbc.OracleDriver') \
        .option("url", "jdbc:oracle:thin:@192.168.88.95:1521:orcl") \
        .option("dbtable", "DIM_SUPPLIERS") \
        .option("user", "test_user") \
        .option("password", "test_user") \
        .load()

    maxID = df1.agg({'last_update_date': 'max'}).collect()[0][0]
    if maxID == None:
        maxID = 0
    dfResult = df0.where(sf.col('last_update_date') > maxID).collect()
    producer_to_Kafka(dfResult)
