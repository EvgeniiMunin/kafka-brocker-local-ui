from confluent_kafka.avro import AvroProducer
from confluent_kafka import avro
import logging
import json

logging.basicConfig(level=logging.DEBUG)


if __name__ == "__main__":
    # Define the Avro schema
    schema_str = """
        {
            "type": "record",
            "name": "Message",
            "fields": [
                {"name": "event_id", "type": "int"},
                {"name": "website", "type": "string"},
                {"name": "name", "type": "string"},
                {"name": "action", "type": "string"}
            ]
        }
        """

    print("schema: ", json.loads(schema_str))

    # load avro schema
    schema = avro.loads(schema_str)

    producer = AvroProducer(
        {
            "bootstrap.servers": "broker:29092",
            #"schema.registry.url": "http://localhost:8081"
        },
        #default_value_schema=schema
    )

    message = {
        "event_id": 123,
        "website": "https://www.rolex.com/",
        "name": "John Doe",
        "action": "click",
    }

    producer.produce(topic="quickstart", value=message)
    print("msgs json sent")

    producer.flush()

# connection refused error