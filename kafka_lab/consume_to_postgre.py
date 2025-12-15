import os
import sys
import json
from datetime import datetime

from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError
import psycopg2
from psycopg2.extras import execute_batch

load_dotenv("env.conf")


class KafkaToPostgresConsumer:
    def __init__(self):
        # PostgreSQL connection
        self.pg_conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT'),
            database=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD')
        )
        self.pg_cursor = self.pg_conn.cursor()

        # Kafka consumer config
        kafka_conf = {
            'bootstrap.servers': os.getenv('LAB02_KAFKA_ADDR'),
            'group.id': 'postgres-consumer-group',
            'auto.offset.reset': 'smallest',
            'enable.auto.commit': False  # Мы будем коммитить вручную
        }

        self.consumer = Consumer(kafka_conf)

    def parse_event(self, event_data):
        """Parse Kafka event and extract relevant data for raw_events table"""
        try:
            data = json.loads(event_data)

            # Фильтруем только like и repost события
            if data.get('event_type') not in ['like', 'repost']:
                return None

            # Преобразуем timestamp в datetime объект
            timestamp_str = data.get('timestamp')
            if timestamp_str:
                # Удаляем 'Z' и преобразуем в datetime
                if timestamp_str.endswith('Z'):
                    timestamp_str = timestamp_str[:-1]
                created_at = datetime.fromisoformat(timestamp_str)
            else:
                # Если нет timestamp, используем текущее время
                created_at = datetime.now()

            return {
                'event_type': data['event_type'],
                'user_id': data['user_id'],
                'post_id': data['post_id'],
                'created_at': created_at
            }

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            print(f"Error parsing event: {e}")
            return None

    def insert_to_postgres(self, events):
        """Insert events into PostgreSQL"""
        if not events:
            return

        insert_query = """
            INSERT INTO raw_events (event_type, user_id, post_id, created_at)
            VALUES (%s, %s, %s, %s)
        """

        try:
            # Преобразуем события в список кортежей
            values = [
                (event['event_type'], event['user_id'], event['post_id'], event['created_at'])
                for event in events
            ]

            # Используем execute_batch для эффективной вставки
            execute_batch(self.pg_cursor, insert_query, values)
            self.pg_conn.commit()
            print(f"Inserted {len(events)} events into PostgreSQL")

        except Exception as e:
            self.pg_conn.rollback()
            print(f"Error inserting to PostgreSQL: {e}")
            raise

    def consume_and_store(self, batch_size=100):
        """Consume messages from Kafka and store in PostgreSQL"""
        topic = os.getenv('LAB02_KAFKA_TOPIC')
        self.consumer.subscribe([topic])

        print(f"Started consumer for topic: {topic}")
        events_batch = []
        counter = 0
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        info = f'%% {msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}\n'
                        sys.stderr.write(info)
                    else:
                        print(f"Error: {msg.error()}")
                    continue

                # Парсим сообщение
                event_data = msg.value().decode('utf-8')
                parsed_event = self.parse_event(event_data)

                if parsed_event:
                    events_batch.append(parsed_event)
                    counter += 1

                # Вставляем батчем для эффективности
                if len(events_batch) >= batch_size:
                    self.insert_to_postgres(events_batch)
                    # Коммитим offset Kafka
                    self.consumer.commit(asynchronous=False)
                    events_batch = []

                # Для отладки выводим каждое N-ое сообщение
                if counter % 10 == 0:
                    print(f"Processed {counter} events")

        except KeyboardInterrupt:
            print("Shutting down consumer...")
        finally:
            # Вставляем оставшиеся события
            if events_batch:
                self.insert_to_postgres(events_batch)

            # Закрываем соединения
            self.consumer.commit(asynchronous=False)
            self.consumer.close()
            self.pg_cursor.close()
            self.pg_conn.close()
            print("Consumer stopped. Total processed:", counter)

if __name__ == '__main__':
    consumer = KafkaToPostgresConsumer()
    consumer.consume_and_store(batch_size=100)