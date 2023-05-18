import csv
from kafka import KafkaProducer
import logging
import json
import time

logging.basicConfig(level=logging.DEBUG)


def read_csv(file_path):
    with open(file_path, "r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            yield row


def publish(producer, topic, key, message):
    # produce keyed messages to enable hashed partitioning
    producer.send("quickstart", key=key, value=message)
    producer.flush()
    print("msgs json sent")


if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers="broker:29092",
        api_version=(0, 10, 5),
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )

    data = read_csv("wines_SPA.csv")
    for i, item in enumerate(data):
        print("item: ", i, item)
        publish(
            producer=producer,
            topic="quickstart",
            key=str(i).encode("utf-8"),
            message=item
        )
        time.sleep(3)

    producer.close()

# consumer output {"event_id": 123, "website": "https://www.rolex.com/", "name": "John Doe", "action": "click"}
# json serialization, no schema enforced
