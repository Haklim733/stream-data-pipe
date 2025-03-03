import argparse
import logging
import os
import random
import sys
import time

from kafka import KafkaProducer
from kafka.admin import NewTopic, KafkaAdminClient

RUNTIME_ENV = os.getenv("RUNTIME_ENV", "local")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:19092")


def create_topic(topic_name: str, admin_client: KafkaAdminClient):

    topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
    topics = admin_client.describe_topics()
    if topic_name in [x["topic"] for x in topics]:
        admin_client.delete_topics([topic_name])
    admin_client.create_topics([topic], validate_only=False)


def check_file_size(file_path):
    try:
        file_size = os.path.getsize(file_path)
        if file_size > 10 * 1024 * 1024:  # 10 MB
            raise Exception("File size exceeds 10 MB")
    except FileNotFoundError:
        print(f"File {file_path} not found.")


def send_to_kafka(topic_name: str, file_path: str, bootstrap_servers: list[str]):

    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    with open(file_path, "r") as f:
        for line in f:
            producer.send(topic_name, value=line.strip().encode("utf-8"))
            # delay = random.uniform(0.25, 1.5)
            # time.sleep(delay)

    # Close the producer
    producer.close()


def main(topic_name: str, file_path: str = None, client_id: str = "local"):
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    bootstrap_servers = BOOTSTRAP_SERVERS.split(",")
    # Create admin client
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id=client_id,
    )
    create_topic(topic_name=topic_name, admin_client=admin_client)
    if file_path:
        check_file_size(file_path)
        send_to_kafka(
            topic_name=topic_name,
            bootstrap_servers=bootstrap_servers,
            file_path=file_path,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--topic", dest="topic", required=False, help="specify kafka topic to consume"
    )
    parser.add_argument(
        "--file",
        dest="file",
        required=False,
        help="specify file to stream to kafka topic",
    )

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)

    main(topic_name=known_args.topic, file_path=known_args.file)
