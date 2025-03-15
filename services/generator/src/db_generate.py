import argparse
import logging
import os
import sys

from faker import Faker
from faker.providers import lorem
from models import DbClient, DBWriter, TextMessage

RUNTIME_ENV = os.getenv("RUNTIME_ENV", "local")
DB_NAME = os.getenv("DB_NAME", "default")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "admin")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

logger = logging.getLogger(__name__)


def main(max_time: int):
    if not max_time:
        max_time = 120
    client = DbClient(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    generator = DBWriter(
        client=client,
        table_name="messages",
        schema={"id": "varchar", "created_at": "bigint", "message": "text"},
        primary_keys=["id", "created_at"],
    )
    fake = Faker()
    fake.add_provider(lorem)
    generator.send(
        message=TextMessage(fake.sentence),
        message_params={"nb_words": 15},
        max_time=max_time,
    )


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--max-time",
        dest="max_time",
        required=False,
        help="max time (sec) to generate messages",
    )

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)
    max_time = known_args.max_time or 120
    main(max_time=max_time)
