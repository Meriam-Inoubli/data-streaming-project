from kafka import KafkaConsumer
import json

# Configuration Kafka
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'stock-data'

def consume_data():
    consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers=KAFKA_BROKER, value_deserializer=lambda v: json.loads(v.decode('utf-8')))
    for message in consumer:
        print(f"Received data for {message.value['symbol']}: {message.value}")

if __name__ == "__main__":
    consume_data()