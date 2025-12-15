import os
import sys

from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError

load_dotenv("env.conf")


if __name__ == '__main__':
    conf = {
        'bootstrap.servers': os.getenv('LAB02_KAFKA_ADDR'),
        'group.id': 'foo',
        'auto.offset.reset': 'smallest',
    }

    consumer = Consumer(conf)
    consumer.subscribe([os.getenv('LAB02_KAFKA_TOPIC')])

    try:
        counter = 0
        while True:
            msg = consumer.poll(timeout=1)
            if msg is None:
                continue
            if not msg.error():
               print(
                  f'{msg.offset()=}',
                  f'{msg.key()=}',
                  f'{msg.value().decode()=}',
                )


               counter += 1
               if counter >= 100:
                   break

            elif msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                info = f'%% {msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}\n'
                sys.stderr.write(info)
    finally:
        consumer.close()