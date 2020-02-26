from pyspark.shell import spark
import pyspark.sql.functions as f
from kafka import KafkaProducer
import json
from pyspark.sql.types import IntegerType


def serializer():
    return str.encode


def send_to_kafka(rows):
    producer = KafkaProducer(bootstrap_servers=['cdh631.itfbgroup.local:9092'],
                             value_serializer=serializer())
    for row in rows:
        producer.send('four_part', json.dumps(row.asDict(), indent=4, sort_keys=True, default=str))
        producer.flush()


if __name__ == '__main__':
    df0 = spark.read \
        .format("jdbc") \
        .option("driver", 'oracle.jdbc.OracleDriver') \
        .option("url", "jdbc:oracle:thin:@192.168.88.102:1521:orcl") \
        .option("dbtable", "fct_events") \
        .option("user", "test_user") \
        .option("password", "1234") \
        .load()

    df1 = spark.read \
        .format("jdbc") \
        .option("driver", 'oracle.jdbc.OracleDriver') \
        .option("url", "jdbc:oracle:thin:@192.168.88.102:1521:orcl") \
        .option("dbtable", "fct_events_test") \
        .option("user", "test_user") \
        .option("password", "1234") \
        .load()

    res = df0
    # res=res.where(f.col('event_id')>1000000)
    res = res.withColumn('EVENT_ID', df0['EVENT_ID'].cast(IntegerType()))
    res = res.withColumn('PRODUCT_ID', df0['PRODUCT_ID'].cast(IntegerType()))
    res = res.withColumn('CUSTOMER_ID', df0['CUSTOMER_ID'].cast(IntegerType()))
    res = res.withColumn('CATEGORY_ID', df0['CATEGORY_ID'].cast(IntegerType()))
    res = res.withColumn('PRICE', df0['PRICE'].cast(IntegerType()))
    res.foreachPartition(send_to_kafka)
    res.printSchema()
