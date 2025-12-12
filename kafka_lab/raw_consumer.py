import os
import sys

from confluent_kafka import Consumer, KafkaError


if __name__ == '__main__':
    conf = {
        'bootstrap.servers': os.getenv('BROKER'),
        'group.id': 'foo',
        'auto.offset.reset': 'smallest',
    }

    consumer = Consumer(conf)
    consumer.subscribe([os.getenv('TOPIC')])

    try:
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
            elif msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                info = f'%% {msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}\n'
                sys.stderr.write(info)
    finally:
        consumer.close()