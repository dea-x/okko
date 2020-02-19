from pyspark.shell import spark
import pyspark.sql.functions as sf
from kafka import KafkaProducer
import json
import decimal
import datetime

def serializer():
    return str.encode


def build_JSON(x):
    return json.dumps(x)

#
ARR_TOPIC = ['dim_customers', 'dim_products', 'dim_suppliers']
#
DATABASE_SOURCE = {'url': 'jdbc:oracle:thin:@192.168.88.252:1521:oradb', 'user': 'test_user', 'password': 'test_user'}
#
DATABASE_DESTINATION = {'url': 'jdbc:oracle:thin:@192.168.88.95:1521:orcl', 'user': 'test_user',
                        'password': 'test_user'}
#
DICT_INCREMENTAL_FIELD = {'fct_events': 'event_id', 'dim_customers': 'last_update_date',
                          'dim_products': 'last_update_date', 'dim_suppliers': 'last_update_date'}
#
DICT_FIELDS = {
    'fct_events': {'event_time': None, 'event_type': None, 'event_id': None, 'product_id': None, 'category_id': None,
                   'category_code': None, 'brand': None, 'price': None, 'customer_id': None},
    'dim_customers': {'customer_id': None, 'country': None, 'city': None, 'phone': None, 'first_name': None,
                      'last_name': None, 'mail': None, 'last_update_date': None},
    'dim_products': {'product_id': None, 'category_id': None, 'category_code': None, 'brand': None, 'description': None,
                     'name': None, 'price': None, 'last_update_date': None},
    'dim_suppliers': {'suppliers_id': None, 'category': None, 'name': None, 'country': None, 'city': None,
                      'last_update_date': None}
}


# The implementation of the producer
def sending_to_Kafka(dfResult, topic):
    producer = KafkaProducer(bootstrap_servers=['cdh631.itfbgroup.local:9092'],
                             value_serializer=serializer())

    for i in range(len(dfResult)):
        try:
            for field in DICT_FIELDS[topic].keys():
                tmp = dfResult[i][field.upper()]
                if isinstance(tmp, decimal.Decimal):
                    DICT_FIELDS[topic][field] = int(tmp)
                elif isinstance(tmp, datetime.datetime):
                    DICT_FIELDS[topic][field] = str(tmp)
                else:
                    DICT_FIELDS[topic][field] = tmp

            values = build_JSON(DICT_FIELDS[topic])
            print(values)
            future = producer.send(topic, key=topic, value=values)

        except Exception as e:
            print('--> It seems an Error occurred: {}'.format(e))

    producer.flush()


if __name__ == '__main__':
    for topic in ARR_TOPIC:
        # Creating a dataframe for the source table
        df0 = spark.read \
            .format('jdbc') \
            .option('driver', 'oracle.jdbc.OracleDriver') \
            .option('url', DATABASE_SOURCE['url']) \
            .option('dbtable', topic) \
            .option('user', DATABASE_SOURCE['user']) \
            .option('password', DATABASE_SOURCE['password']) \
            .load()

        # Creating a dataframe for the destination table
        df1 = spark.read \
            .format('jdbc') \
            .option('driver', 'oracle.jdbc.OracleDriver') \
            .option('url', DATABASE_DESTINATION['url']) \
            .option('dbtable', topic.upper()) \
            .option('user', DATABASE_DESTINATION['user']) \
            .option('password', DATABASE_DESTINATION['password']) \
            .load()

        incr_field = DICT_INCREMENTAL_FIELD[topic]

        max_value = df1.agg({incr_field: 'max'}).collect()[0][0]
        print(max_value)
        print(type(max_value))
        if max_value is None:
            if isinstance(max_value, int):
                max_value = 0
            else:
                pass
        dfResult = df0.where(sf.col(incr_field) > max_value).collect()
        sending_to_Kafka(dfResult, topic)

