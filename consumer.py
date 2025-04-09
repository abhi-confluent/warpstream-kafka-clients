import argparse
import json

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

from models.customer import dict_to_customer


class KafkaConsumer:
    def __init__(self, config_path):
        # Load configuration
        with open(config_path, 'r') as f:
            self.config = json.load(f)

        # Set up Schema Registry client
        schema_registry_conf = {
            'url': self.config['schema_registry']['url'],
            'basic.auth.user.info': f"{self.config['schema_registry']['api_key']}:{self.config['schema_registry']['api_secret']}"
        }
        self.schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        # Load schema
        with open(self.config['schema_registry']['schema_path']) as f:
            self.schema_str = f.read()

        # Initialize deserializer
        self.avro_deserializer = AvroDeserializer(
            self.schema_registry_client,
            self.schema_str,
            dict_to_customer
        )

        # Create consumer instance
        self.consumer = Consumer(self.config['kafka_consumer'])

    def consume_messages(self):
        """Consume messages from Kafka"""
        # Subscribe to topic
        topic = self.config.get('topic', 'customer-data-topic')
        self.consumer.subscribe([topic])
        print(f"Consuming messages from topic: {topic}")

        # Counter for consumed messages
        message_count = 0

        # Poll messages in a loop
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)  # Wait for messages
                if msg is None:
                    continue  # No message received, retry

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print("Reached end of partition")
                    else:
                        print(f"Kafka error: {msg.error()}")
                    continue

                # Increment message counter
                message_count += 1

                # Deserialize and process the received message
                customer = self.avro_deserializer(
                    msg.value(),
                    SerializationContext(msg.topic(), MessageField.VALUE)
                )

                if customer is not None:
                    print("User record {}: customer_id: {}\n"
                          "\temail: {}\n"
                          "\tfirst_name: {}\n"
                          "\tlast_name: {}\n"
                          "\tphone_number: {}\n"
                          .format(msg.key(), customer.customer_id,
                                  customer.email,
                                  customer.first_name, customer.last_name, customer.phone_number))

        except KeyboardInterrupt:
            print("\nConsumer stopped manually")

        finally:
            print(f"\nTotal messages consumed: {message_count}")
            self.consumer.close()


def main():
    parser = argparse.ArgumentParser(description='Kafka Customer Data Consumer')
    parser.add_argument('--config', type=str, default='config.json', help='Path to configuration file')
    args = parser.parse_args()

    consumer = KafkaConsumer(args.config)
    consumer.consume_messages()


if __name__ == '__main__':
    main()