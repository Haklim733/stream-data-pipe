import argparse
import csv
from datetime import datetime
from enum import Enum
import json
import io
import logging
import os
import random
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


def check_file_size(file_path):
    try:
        file_size = os.path.getsize(file_path)
        if file_size > 10 * 1024 * 1024:  # 10 MB
            raise Exception("File size exceeds 10 MB")
    except FileNotFoundError:
        print(f"File {file_path} not found.")


def sanitize_avro(s):
    """
    Sanitizes a string to match the pattern (?:^|\.)[A-Za-z_][A-Za-z0-9_]*$
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
):
    file_format = FileFormat(format)
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
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

        elif file_format == FileFormat.CSV:
            reader = csv.DictReader(f)
            for row in reader:
                producer.send(topic_name, value=json.dumps(row).encode("utf-8"))

        elif file_format == FileFormat.JSON:
            if file_path.endswith(".json"):
                data = json.load(f)
                for item in data:
                    producer.send(topic_name, value=json.dumps(item).encode("utf-8"))
            else:
                for line_number, line in enumerate(f, start=1):
                    record = {"line": line_number, "text": line.strip()}
                    producer.send(
                        topic_name,
                        value=json.dumps(record).encode("utf-8"),
                    )
        else:  # text
            for line in f:
                producer.send(topic_name, value=line.strip().encode("utf-8"))

    producer.close()


def main(
    topic_name: str,
    file_path: str = None,
    format: str = None,
    client_id: str = "local",
):
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    bootstrap_servers = BOOTSTRAP_SERVERS.split(",")
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id=client_id,
    )

    if not format:
        format = "text"

    create_topic(topic_name=topic_name, admin_client=admin_client)
    if file_path:
        check_file_size(file_path)
        send_to_kafka(
            topic_name=topic_name,
            bootstrap_servers=bootstrap_servers,
            format=format,
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
    parser.add_argument(
        "--format",
        dest="format",
        required=False,
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

    main(
        topic_name=known_args.topic, file_path=known_args.file, format=known_args.format
    )
