import argparse
import os
import sys
import psycopg2

conn = psycopg2.connect(host="localhost", port=4566, user="root", dbname="dev")
conn.autocommit = True
RUNTIME_ENV = os.getenv("RUNTIME_ENV", "local")
BOOTSTRAP_SERVERS = "kafka-broker:9092"


def initialize():
    with conn.cursor() as cur:
        cur.execute("DROP SOURCE IF EXISTS iceberg CASCADE;")
        cur.execute("DROP SOURCE IF EXISTS kafka CASCADE;")
        cur.execute("DROP MATERIALIZED VIEW IF EXISTS kafka_view CASCADE;")


def main(topic: str, source: str = None):
    initialize()
    if source == "iceberg":
        with conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE SOURCE IF NOT EXISTS iceberg
                WITH (
                    connector = 'iceberg',
                    warehouse.path = 's3://warehouse/wh',
                    s3.endpoint = 'http://minio:9000',
                    s3.region = 'us-east-1',
                    s3.access.key = 'admin',
                    s3.secret.key = 'password',
                    s3.path.style.access = 'true',
                    catalog.type = 'rest',
                    catalog.uri = 'http://iceberg-rest:8181/api/catalog',
                    database.name = 'spark',
                    table.name = 'text',
                    catalog.credential='admin:password',
                    catalog.scope='PRINCIPAL_ROLE:ALL'
                ) """
            )
    else:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE SOURCE IF NOT EXISTS kafka (
                    line INTEGER,
                    text VARCHAR 
                    )
                INCLUDE header AS kafka_header,
                INCLUDE timestamp AS kafka_timestamp
                WITH (
                    connector = 'kafka',
                    topic='{topic}',
                    properties.bootstrap.server='{BOOTSTRAP_SERVERS}',
                    scan.startup.mode='earliest'
                ) FORMAT PLAIN ENCODE JSON;
                """
            )

            cur.execute(
                """CREATE MATERIALIZED VIEW IF NOT EXISTS kafka_view
            AS SELECT * FROM kafka;"""
            )

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--topic", dest="topic", required=False, help="specify kafka topic to consume"
    )

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)

    main(
        topic=known_args.topic,
    )
