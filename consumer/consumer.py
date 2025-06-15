from kafka import KafkaConsumer
from clickhouse_driver import Client
import json

# Настройки
KAFKA_TOPIC = 'test-topic'
KAFKA_SERVER = 'kafka:9092'
CLICKHOUSE_HOST = 'clickhouse'
CLICKHOUSE_TABLE = 'kafka_messages'

# Подключение к ClickHouse
ch = Client(host=CLICKHOUSE_HOST)

# Создаём таблицу, если нет
ch.execute(f'''
    CREATE TABLE IF NOT EXISTS {CLICKHOUSE_TABLE} (
        number UInt64,
        text String
    ) ENGINE = MergeTree()
    ORDER BY number
''')

# Подключение к Kafka
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers='kafka:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='python-clickhouse-consumer',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Consumer запущен. Ожидание сообщений...")

try:
    for msg in consumer:
        data = msg.value
        print(f"Получено сообщение: {data}")
        try:
            number = int(data.get('number'))
            text = str(data.get('text'))
            ch.execute(f'INSERT INTO {CLICKHOUSE_TABLE} (number, text) VALUES', [(number, text)])
            print(f"Записано в ClickHouse: number={number}, text={text}")
        except Exception as e:
            print(f"Ошибка вставки в ClickHouse: {e}")
except KeyboardInterrupt:
    print("Остановка consumer'а.")
finally:
    consumer.close()
