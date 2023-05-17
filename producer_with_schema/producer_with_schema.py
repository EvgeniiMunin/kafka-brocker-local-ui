from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka import KafkaClient, KafkaAdminClient
from kafka.admin import NewTopic
import logging
import json

logging.basicConfig(level=logging.DEBUG)


if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers="broker:29092",
        api_version=(0, 10, 5),
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )

    message = {
        "event_id": 123,
        "website": "https://www.rolex.com/",
        "name": "John Doe",
        "action": "click",
    }

    # convert the key to bytes
    key = str(message["event_id"]).encode("utf-8")

    # produce keyed messages to enable hashed partitioning
    producer.send("quickstart", key=key, value=message)
    print("msgs json sent")

    producer.flush()
    producer.close()

# consumer output {"event_id": 123, "website": "https://www.rolex.com/", "name": "John Doe", "action": "click"}