# WarpStream Kafka Producer and Consumer

Simple code for producing and consuming customer data messages to/from a WarpStream Kafka cluster.

## About This Code

This repository contains producer and consumer scripts to interact with a WarpStream Kafka cluster. The WarpStream cluster uses TLS encryption and SASL authentication for security.

## Project Structure

```
warpstream-kafka-client/
│
├── producer.py                # Kafka producer script
├── consumer.py                # Kafka consumer script
├── config.json                # Configuration file
├── requirements.txt           # Project dependencies
│
├── models/                    # Data models
│   └── customer.py            # Customer data model
│
└── schemas/                   # Avro schemas
    └── customer.avsc          # Customer Avro schema
```

## Prerequisites

- Python 3.8+
- Access to a WarpStream Kafka cluster with SASL authentication and TLS
- Access to a Schema Registry service
- TLS certificate file for the WarpStream cluster

## Installation

1. Clone this repository
2. Create a virtual environment and activate it
3. Install dependencies: `pip install -r requirements.txt`
4. Update the `config.json` file with your WarpStream and Schema Registry details

## Configuration

Edit `config.json` to configure:

- Topic name (at the top level)
- WarpStream Kafka broker connection details
- Schema Registry URL and credentials
- Faker locale for data generation

Example:
```json
{
  "topic": "customer-data-topic",
  "kafka": {
    "bootstrap.servers": "warpstream-host:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "your-username",
    "sasl.password": "your-password",
    "ssl.ca.location": "/path/to/your/tls.crt"
  },
  "schema_registry": {
    "url": "https://your-schema-registry-url",
    "api_key": "YOUR_SCHEMA_REGISTRY_KEY",
    "api_secret": "YOUR_SCHEMA_REGISTRY_SECRET",
    "schema_path": "schemas/customer.avsc"
  }
}
```

## Usage

### Running the Producer

Generate and send customer data to WarpStream Kafka:

```bash
python producer.py --config config.json --count 10
```

### Running the Consumer

Process customer data from WarpStream Kafka:

```bash
python consumer.py --config config.json
```

## Security Configuration

This application connects to a WarpStream Kafka cluster with:

1. **TLS Encryption**: All communication with the Kafka brokers is encrypted
2. **SASL Authentication**: The WarpStream Agents require SASL PLAIN authentication

Make sure to replace the placeholders in the config with your actual WarpStream credentials and the correct path to your TLS certificate file.

## Troubleshooting

Common issues:
- Incorrect SASL credentials
- Invalid TLS certificate path
- Schema Registry connection problems
- Topic doesn't exist in the cluster

## Security Considerations

- Never commit `config.json` with real credentials to version control
- Consider using environment variables for sensitive information