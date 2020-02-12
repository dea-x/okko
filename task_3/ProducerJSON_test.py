from kafka import KafkaProducer
from numpy.random import choice, randint
import json

arrFams = ['Petrov', 'Perumov', 'Vadikov', 'Prizhiy', 'Grigoryev', 'Galinin', 'Ivanov', 'Mikhailov', 'Krasnov']
arrNames = ['Ivan', 'Mikhail', 'Pericles', 'Ivan', 'Matvey', 'Perum', 'Ruslan', 'Nikita', 'Maxim']
arrCategory = ['car', 'beer', 'game', 'electronics', 'goods', 'cheese'] 
arrDate = ['2019-10-12', '2017-15-05', '2014-10-14', '2015-5-20', '2019-12-19']
TOPIC = 'test_topic'
NUMBER_OF_ITER = 1000


def build_JSON(event_time, event_id, product_id, category_id, category_code, brand, price, customer_id, customer_session):
    data = dict(event_time=event_time, event_id=event_id, product_id=product_id, category_id=category_id, category_code=category_code, brand=brand, price=price, customer_id=customer_id, customer_session=customer_session)
    return json.dumps(data)


def serializer():
    return str.encode


def producer_stream():
    producer = KafkaProducer(bootstrap_servers=['cdh631.itfbgroup.local:9092'],
                             key_serializer=serializer(), value_serializer=serializer())

    for i in range(NUMBER_OF_ITER):
        try:
            event_time = choice(arrDate)
            event_id = i
            product_id = randint(1, 500)
            category_id = randint(1, 500)
            category_code = choice(arrCategory)
            brand = choice(arrFams)
            price = randint(15, 5000)
            customer_id = randint(1, 500)
            customer_session = choice(arrNames)

            values = build_JSON(event_time, event_id, product_id, category_id, category_code, brand, price, customer_id, customer_session)
            print(values)
            future = producer.send(TOPIC, key=str(i), value=values)
            record_metadata = future.get(timeout=10)

        except Exception as e:
            print('--> It seems an Error occurred: {}'.format(e))

    producer.flush()


if __name__ == "__main__":
    producer_stream()
    
# for test user
