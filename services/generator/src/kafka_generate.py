import argparse
import logging
import os
import sys

from faker import Faker
from faker.providers import lorem
from models import Publisher, KafkaClient, TextMessage

RUNTIME_ENV = os.getenv("RUNTIME_ENV", "local")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:19092")
logger = logging.getLogger(__name__)


def main(topic: str, max_time: int):
    # Debug: print the type and value received
    print(f"DEBUG: main() received max_time type: {type(max_time)}, value: {max_time}")

    if not max_time:
        max_time = 120
    client = KafkaClient(BOOTSTRAP_SERVERS)
    client.create_topics([topic], num_partitions=1, replication_factor=1)
    generator = Publisher(client=client)
    fake = Faker()
    fake.add_provider(lorem)
    logger.info('publishing messages to topic "%s"', topic)
    generator.send(
        topic=topic,
        message=TextMessage(fake.sentence),
        message_params={"nb_words": 15},
        max_time=max_time,
    )


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--topic", dest="topic", required=True, help="specify kafka topic to consume"
    )
    parser.add_argument(
        "--max-time",
        dest="max_time",
        type=int,
        required=False,
        help="max time (sec) to generate messages and publish to kafka topic",
    )

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)
    max_time = known_args.max_time or 120

    # Debug: print the type and value
    print(f"DEBUG: max_time type: {type(max_time)}, value: {max_time}")

    main(topic=known_args.topic, max_time=max_time)
