import uuid
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField, StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from faker import Faker
from datetime import datetime
import argparse
import json
import os

from models.customer import Customer, customer_to_dict


class KafkaProducer:
    def __init__(self, config_path):
        # Load configuration
        with open(config_path, 'r') as f:
            self.config = json.load(f)

        # Initialize Faker
        self.fake = Faker(self.config.get('faker_locale', 'en_IN'))

        # Create Kafka producer
        self.producer = Producer(self.config['kafka'])

        # Set up Schema Registry client
        schema_registry_conf = {
            'url': self.config['schema_registry']['url'],
            'basic.auth.user.info': f"{self.config['schema_registry']['api_key']}:{self.config['schema_registry']['api_secret']}"
        }
        self.schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        # Load schema
        with open(self.config['schema_registry']['schema_path']) as f:
            self.schema_str = f.read()

        # Initialize serializers
        serializer_configs = {
            'auto.register.schemas': True,
            'use.latest.version': False
        }
        self.string_serializer = StringSerializer('utf_8')
        self.avro_serializer = AvroSerializer(
            self.schema_registry_client,
            self.schema_str,
            customer_to_dict,
            serializer_configs
        )

    def delivery_report(self, err, event):
        """Callback for produced messages"""
        if err is not None:
            print(f'Delivery failed on reading for {event.key().decode("utf8")}: {err}')
        else:
            print(f'Customer reading for {event.key().decode("utf8")} produced to {event.topic()}')

    def generate_customer(self):
        """Generate a fake customer record"""
        return {
            "customer_id": str(uuid.uuid4()),
            "email": self.fake.email(),
            "first_name": self.fake.first_name(),
            "last_name": self.fake.last_name(),
            "phone_number": self.fake.numerify("+91-##########"),
            "created_at": str(datetime.now().isoformat()),
            "acquisition_channel": self.config.get('default_acquisition_channel', 'web')
        }

    def produce_messages(self, count=5):
        """Produce a specified number of messages to Kafka"""
        topic = self.config.get('topic', 'customer-data-topic')

        for _ in range(count):
            customer_json = self.generate_customer()
            customer = Customer(**customer_json)
            self.producer.produce(
                topic=topic,
                key=self.string_serializer(str(uuid.uuid4())),
                value=self.avro_serializer(customer, SerializationContext(topic, MessageField.VALUE)),
                on_delivery=self.delivery_report
            )

        print("\nFlushing records...")
        self.producer.flush()


def main():
    parser = argparse.ArgumentParser(description='Kafka Customer Data Producer')
    parser.add_argument('--config', type=str, default='config.json', help='Path to configuration file')
    parser.add_argument('--count', type=int, default=5, help='Number of messages to produce')
    args = parser.parse_args()

    producer = KafkaProducer(args.config)
    producer.produce_messages(args.count)


if __name__ == '__main__':
    main()