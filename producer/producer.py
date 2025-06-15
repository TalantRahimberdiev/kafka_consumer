from kafka import KafkaProducer
import json
import time
import random

# Подключение к Kafka
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'test-topic'
counter = 0

print("Producer запущен. Отправка сообщений...")

try:
    while True:
        message = {
            'number': counter,
            'text': f'Message {counter}'
        }
        producer.send(topic, value=message)
        print(f"Отправлено сообщение: {message}")
        counter += 1
        time.sleep(1)  # Пауза в 1 секунду
except KeyboardInterrupt:
    print("Остановка producer'а.")
finally:
    producer.flush()
    producer.close()
