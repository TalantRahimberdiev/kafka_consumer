from kafka import KafkaProducer
import json
import time

# Подключаемся к Kafka через внешний адрес и порт из docker-compose (PLAINTEXT_HOST)
producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],  # Порт 29092 из твоего compose
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'test-topic'

for i in range(7,10):
    message = {'number': i, 'text': f'Message {i}'}
    producer.send(topic, value=message)
    print(f"Отправлено сообщение: {message}")
    time.sleep(1)

producer.flush()
producer.close()
