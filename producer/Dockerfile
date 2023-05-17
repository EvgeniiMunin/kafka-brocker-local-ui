# Set the base image
FROM python:3.9

# Copy the Kafka producer code to the image
COPY producer.py .

# Install the Kafka Python client library
RUN pip install kafka-python

# Set the command to run the Kafka producer
CMD ["python", "producer.py"]