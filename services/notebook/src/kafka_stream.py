import argparse
import json
import logging
from enum import Enum
import os
import re
import random
import sys
import time

import avro
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.catalog import load_catalog
from pyspark.sql.avro.functions import from_avro
from pyspark.sql import SparkSession

RUNTIME_ENV = os.getenv("RUNTIME_ENV", "local")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:19092")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "password")
S3_BUCKET = os.getenv("S3_BUCKET", "warehouse")


class FileFormat(Enum):
    TEXT = "text"
    AVRO = "avro"
    CSV = "csv"
    JSON = "json"


# Create a SparkSession
jars = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4",
    "org.apache.spark:spark-avro_2.12:3.5.4",
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.0",
]
# Dockerfile installed following packages
# "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.4",
# "org.apache.kafka:kafka-clients:3.4.1",
# "org.apache.commons:commons-pool2-2.12.0.jar:2.11.1",

spark = (
    SparkSession.builder.appName("KafkaStreaming")
    .master("local[1]")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.streaming.schemaInference", "true")
    .config("spark.jars.packages", ",".join(jars))
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.default", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.defaultCatalog", "default")
    .config("spark.sql.catalog.default.type", "rest")
    .config("spark.sql.catalog.default.uri", "http://iceberg-rest:8181")
    .config("spark.sql.catalog.default.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.default.s3.endpoint", "http://minio:9000")
    .config("spark.sql.catalog.default.warehouse", "s3://warehouse/wh")
    .config("spark.sql.catalog.default.s3.access-key", AWS_ACCESS_KEY_ID)
    .config("spark.sql.catalog.default.s3.secret-key", AWS_SECRET_ACCESS_KEY)
    .getOrCreate()
)

kafka_params = {
    "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
    "group.id": "test-kafka-stream",
    "auto.offset.reset": "earliest",
}

spark.sparkContext.setLogLevel("INFO")


def initialize_iceberg():
    catalog = load_catalog(name="rest", uri="http://iceberg-rest:8181")

    try:
        catalog.create_namespace("spark")
    except NamespaceAlreadyExistsError:
        pass

    try:
        catalog.drop_table("spark.text")
    except NoSuchTableError:
        pass

    spark.sql("CREATE DATABASE IF NOT EXISTS spark")
    spark.sql(
        """
        CREATE TABLE default.spark.text (
            `line` INTEGER,
            `text` VARCHAR(100)
        )
        USING iceberg
        TBLPROPERTIES(
            'write.format.default'='avro',
            'write.target-file-size-bytes'=5242880
        )
        """
    )


def sanitize_avro(s):
    """
    Sanitizes a string to match the pattern (?:^|\.)[A-Za-z_][A-Za-z0-9_]*$
    """
    s = re.sub(r"[^\w\.]", "_", s)
    s = s.strip("_")
    s = s.lstrip(".")
    if not s:
        return "_"
    if s[0].isdigit():
        s = "_" + s
    return s


def main(topic: str, output: str, terminate: int, format: FileFormat = FileFormat.TEXT):
    initialize_iceberg()
    file_format = FileFormat(format)
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("failOnDataLoss", "false")
        .option("subscribe", topic)
        .load()
    )

    df.printSchema()
    df = df.selectExpr("CAST(value AS STRING) as value")

    if file_format == FileFormat.AVRO:
        sanitized_topic = sanitize_avro(topic)
        avro_schema = {
            "type": "record",
            "name": sanitized_topic,
            "fields": [
                {"name": "line", "type": "int"},
                {"name": "text", "type": "string"},
            ],
        }
        df = df.selectExpr("CAST(value AS BINARY) as value")
        df = df.select(
            from_avro(df.value, json.dumps(avro_schema)).alias("data")
        ).select("data.*")

    if output.startswith("iceberg"):
        (
            df.writeStream.format("iceberg")
            .outputMode("append")
            .option("checkpointLocation", "/tmp/checkpoint")
            .toTable("default.spark.text")
            .awaitTermination(terminate)
        )
    else:
        (
            df.writeStream.format(file_format.value)
            .option("truncate", False)
            .option("path", output)
            .option("checkpointLocation", "/tmp/checkpoint")
            .start()
            .awaitTermination(terminate)
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--topic", dest="topic", required=False, help="specify kafka topic to consume"
    )
    parser.add_argument(
        "--terminate",
        dest="terminate",
        type=int,
        required=True,
        help="specify time to terminate spark stream job",
    )
    parser.add_argument(
        "--output",
        dest="output",
        required=True,
        help="specify file to write to",
    )
    parser.add_argument(
        "--format",
        dest="format",
        required=False,
        help="specify format of file to write to",
    )

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)

    main(
        topic=known_args.topic,
        output=known_args.output,
        terminate=known_args.terminate,
        format=known_args.format,
    )
