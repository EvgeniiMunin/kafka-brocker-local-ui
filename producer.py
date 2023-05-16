from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka import KafkaClient, KafkaAdminClient
from kafka.admin import NewTopic
import logging

logging.basicConfig(level=logging.DEBUG)

if __name__ == "__main__":
    # Create 'my-topic' Kafka topic
    #admin = KafkaAdminClient(bootstrap_servers="localhost:9092", api_version=(0, 10, 5))
    #topic = NewTopic(
    #    name="quickstart",
    #    num_partitions=1,
    #    replication_factor=1
    #)
    #admin.create_topics([topic])
    #print("topic successfully created")

    #producer = KafkaProducer(bootstrap_servers=["localhost:9092"])
    producer = KafkaProducer(bootstrap_servers="broker:29092", api_version=(0, 10, 5))

    ## asynchronous by default
    #future = producer.send("quickstart", b"raw_bytes")
    ## block for synchronous send
    #try:
    #    record_metadata = future.get(timeout=10)
    #except KafkaError:
    #    # decide what to do if produce request failed...
    #    # log.exception()
    #    print("KafkaError")
    #    pass
    ## successful result returns assigned partition and offset
    #print("topic: ", record_metadata.topic)
    #print("partition: ", record_metadata.partition)
    #print("offset: ", record_metadata.offset)

    # produce keyed messages to enable hashed partitioning
    producer.send("quickstart", key=b"key1", value=b"val1")
    print("msgs sent")

    producer.flush()
    producer.close()
