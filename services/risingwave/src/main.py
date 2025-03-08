import argparse
import os
import sys

from psycopg2 import sql
from src.db import DatabaseConnection
from src.sinks import IcebergSink
from src.sources import Source, SourceFactory


RUNTIME_ENV = os.getenv("RUNTIME_ENV", "local")


def initialize(source: Source):
    with source.db_connect.cursor() as cur:
        cur.execute("DROP SOURCE IF EXISTS iceberg CASCADE;")
        cur.execute("DROP SOURCE IF EXISTS kafka CASCADE;")
        cur.execute("DROP MATERIALIZED VIEW IF EXISTS kafka_view CASCADE;")


def main(
    topic: str,
    bootstrap_servers: str,
    schema: str,
    source: str,
    write_to: str = None,
):
    factory = SourceFactory()
    kwargs = {
        "bootstrap_servers": bootstrap_servers,
        "topic": topic,
        "schema": schema,
    }
    source = factory.get_source(source, **kwargs)
    source.drop()
    source.create()

    with source.db_connect.cursor() as cur:
        cur.execute(
            sql.SQL(
                """CREATE MATERIALIZED VIEW IF NOT EXISTS {}
        AS SELECT * FROM {};"""
            ).format(sql.Identifier(f"{topic}_view"), sql.Identifier(topic))
        )
    source.db_connect.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--topic", dest="topic", required=True, help="specify kafka topic to consume"
    )
    parser.add_argument(
        "--bootstrap-servers",
        dest="bootstrap_servers",
        required=True,
        help="specify kafka broker",
    )
    parser.add_argument(
        "--schema",
        dest="schema",
        required=True,
        help="schema for view. Must be risingwave types",
    )
    parser.add_argument(
        "--source",
        dest="source",
        required=True,
        help="source of data",
    )

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)

    main(
        topic=known_args.topic,
        bootstrap_servers=known_args.bootstrap_servers,
        schema=known_args.schema,
        source=known_args.source,
    )
