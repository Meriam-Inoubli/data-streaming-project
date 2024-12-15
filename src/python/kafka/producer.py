from kafka import KafkaProducer
import json
import time
from src.python.api.fetch_data import fetch_data

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'  # Kafka broker address
TOPIC_NAME = 'stock-data'        # Kafka topic to send data to

def produce_data():
    # Create a Kafka producer with JSON serialization for message values
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER, 
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize Python dictionary to JSON and encode to UTF-8
    )
    
    while True:
        # Fetch the data to be sent to Kafka
        data = fetch_data()
        
        # Send price data to Kafka
        for record in data['price_data']:
            producer.send(TOPIC_NAME, record)  # Send each record to the Kafka topic
            print(f"Sent price data for {record['symbol']}: {record}")  # Log the sent record

        # Send klines (candlestick data) to Kafka
        for record in data['klines_data']:
            producer.send(TOPIC_NAME, record)
            print(f"Sent klines data for {record['symbol']}: {record}")

        # Send order book data to Kafka
        for record in data['order_book_data']:
            producer.send(TOPIC_NAME, record)
            print(f"Sent order book data for {record['symbol']}: {record}")

        # Send trades data to Kafka
        for record in data['trades_data']:
            producer.send(TOPIC_NAME, record)
            print(f"Sent trades data for {record['symbol']}: {record}")

        # Send market info data to Kafka
        for record in data['market_info_data']:
            producer.send(TOPIC_NAME, record)
            print(f"Sent market info data for {record['symbol']}: {record}")
        
        # Wait for 1 minute before fetching and sending the next batch of data
        time.sleep(60)

if __name__ == "__main__":
    # Start the Kafka producer to send data
    produce_data()
