from kafka import KafkaProducer
import logging

logging.basicConfig(level=logging.DEBUG)

if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers="broker:29092", api_version=(0, 10, 5))

    # produce keyed messages to enable hashed partitioning
    producer.send("test-topic", key=b"key1", value=b"val1")
    print("msgs sent")

    producer.flush()
    producer.close()

# consumer output val1