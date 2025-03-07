import argparse
from kafka import KafkaConsumer
import boto3
import sys

# Kafka consumer settings
kafka_bootstrap_servers = ["kafka-broker:9092"]

# MinIO settings
minio_endpoint = "minio:9000"
minio_access_key = "admin"
minio_secret_key = "password"
minio_bucket_name = "warehouse"

# Create a Kafka consumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_bootstrap_servers,
    topic_partition=kafka_topic,
    auto_offset_reset="earliest",
)


# Consume messages from Kafka and write to MinIO
for message in consumer:
    # Get the message value as a string
    message_value = message.value.decode("utf-8")

    # Write the message to MinIO
    minio_client.put_object(
        bucket_name=minio_bucket_name,
        object_name=message_value,
        data=message_value,
        length=len(message_value),
    )

    # Print a success message
    print(f"Wrote message to MinIO: {message_value}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--topic", dest="topic", required=False, help="specify kafka topic to consume"
    )
    parser.add_argument(
        "--bootstrap-servers",
        dest="bootstrap_servers",
        required=True,
        help="specify network and brokers",
    )
    parser.add_argument(
        "--file",
        dest="file",
        required=False,
        help="specify file to stream to kafka topic",
    )

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)

    main(
        topic_name=known_args.topic,
        bootstrap_servers=known_args.bootstrap_servers,
        file_path=known_args.file,
    )
