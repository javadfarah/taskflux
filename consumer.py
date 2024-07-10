import json
import argparse
from confluent_kafka import Consumer, KafkaError


def process_message(message):
    # Define your processing logic here
    data = json.loads(message.value().decode('utf-8'))
    print(f"Processing message with key: {message.key().decode('utf-8')}, value: {data}")


def create_consumer(config, topic):
    # Create Kafka consumer
    consumer = Consumer(config)
    consumer.subscribe([topic])

    print(f"Consumer subscribed to topic: {topic}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print(f"Reached end of partition: {msg.partition()}")
                elif msg.error():
                    # Error event
                    print(f"Error: {msg.error()}")
            else:
                # Valid message received
                process_message(msg)

    except KeyboardInterrupt:
        print("Consumer interrupted by user")
    finally:
        # Close the consumer to commit final offsets
        consumer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Consumer")
    parser.add_argument("--queue", type=str, required=True, help="The Kafka topic to consume from")
    parser.add_argument("--bootstrap-servers", type=str, required=True, help="Kafka bootstrap servers")

    args = parser.parse_args()

    conf = {
        'bootstrap.servers': args.bootstrap_servers,
        'group.id': 'task_consumer_group',
        'auto.offset.reset': 'earliest'
    }

    create_consumer(config=conf, topic=args.topic)
