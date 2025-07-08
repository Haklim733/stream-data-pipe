import argparse
import csv
from datetime import datetime
from enum import Enum
import json
import io
import logging
import os
import re
import sys
import time


import avro.schema
from avro.io import BinaryEncoder, DatumWriter
from kafka import KafkaProducer
from kafka.admin import NewTopic, KafkaAdminClient

RUNTIME_ENV = os.getenv("RUNTIME_ENV", "local")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:19092")
logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")


class FileFormat(Enum):
    TEXT = "text"
    AVRO = "avro"
    CSV = "csv"
    JSON = "json"


def create_topic(topic_name: str, admin_client: KafkaAdminClient):

    topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
    topics = admin_client.describe_topics()
    if topic_name in [x["topic"] for x in topics]:
        admin_client.delete_topics([topic_name])
    admin_client.create_topics([topic], validate_only=False)
    admin_client.close()


def check_file_size(file_path):
    try:
        file_size = os.path.getsize(file_path)
        if file_size > 10 * 1024 * 1024:  # 10 MB
            raise Exception("File size exceeds 10 MB")
    except FileNotFoundError:
        print(f"File {file_path} not found.")


def sanitize_avro(s):
    """
    Sanitizes a string to match the pattern (?:^|\\.)[A-Za-z_][A-Za-z0-9_]*$
    """
    s = re.sub(r"[^\w.]", "_", s)
    s = s.strip("_")
    s = s.lstrip(".")
    if not s:
        return "_"
    if s[0].isdigit():
        s = "_" + s
    return s


def send_to_kafka(
    topic_name: str,
    file_path: str,
    bootstrap_servers: list[str],
    format: str,
    **kwargs,
) -> dict[str, int]:

    file_format = FileFormat(format)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks="all",
        retries=3,
        retry_backoff_ms=200,
        request_timeout_ms=30500,
    )

    fields = kwargs.get("fields") or [
        {"name": "line", "type": "int"},
        {"name": "text", "type": "string"},
    ]
    schema = {
        "type": "record",
        "name": sanitize_avro(topic_name),
        "fields": fields,
    }
    schema = avro.schema.parse(json.dumps(schema))

    logger.info(f"file format - {file_format}")
    messages = 0
    file_size = os.path.getsize(file_path)

    with open(file_path, "r") as f:
        if file_format == FileFormat.AVRO:
            writer = DatumWriter(schema)

            for line_number, line in enumerate(f, start=1):
                record = {"line": line_number, "text": line.strip()}
                bytes_writer = io.BytesIO()
                encoder = BinaryEncoder(bytes_writer)
                writer.write(record, encoder)
                bytes_data = bytes_writer.getvalue()
                producer.send(topic_name, value=bytes_data)
                messages += 1

        elif file_format == FileFormat.CSV:
            reader = csv.DictReader(f)
            for row in reader:
                producer.send(topic_name, value=json.dumps(row).encode("utf-8"))
                messages += 1

        elif file_format == FileFormat.JSON:
            if file_path.endswith(".json"):
                data = json.load(f)
                for item in data:
                    producer.send(topic_name, value=json.dumps(item).encode("utf-8"))
                    messages += 1
            else:
                for line_number, line in enumerate(f, start=1):
                    record = {"line": line_number, "text": line.strip()}
                    producer.send(
                        topic_name,
                        value=json.dumps(record).encode("utf-8"),
                    )
                    messages += 1
        else:  # text
            for line in f:
                producer.send(topic_name, value=line.strip().encode("utf-8"))

                messages += 1
    producer.close(timeout=1000)
    return {"messages": messages, "file_size": file_size}


def main():
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
    parser.add_argument(
        "--format",
        dest="format",
        required=False,
        default="text",
        help="specify file format when streaming to kafka topic",
    )
    parser.add_argument(
        "--input",
        dest="input",
        required=False,
        help="input for kafka topic",
    )

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)

    start_time = time.time()

    bootstrap_servers = BOOTSTRAP_SERVERS.split(",")
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id="local",
    )

    create_topic(topic_name=known_args.topic, admin_client=admin_client)
    if known_args.file:
        check_file_size(known_args.file)
        summary = send_to_kafka(
            topic_name=known_args.topic,
            bootstrap_servers=bootstrap_servers,
            format=known_args.format,
            file_path=known_args.file,
        )
        logger.info(
            f"Sent {summary['messages']} messages to Kafka for file in {summary['file_size']:.2f} bytes"
        )
    end_time = time.time()
    logger.info(f"execution time: {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    main()
