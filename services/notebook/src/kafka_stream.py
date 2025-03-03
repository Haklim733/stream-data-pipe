import argparse
import logging
import os
import random
import sys
import time
from pyspark.sql import SparkSession

RUNTIME_ENV = os.getenv("RUNTIME_ENV", "local")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:19092")

# Create a SparkSession
jars = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4",
]
# Dockerfile installed following packages
# "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.4",
# "org.apache.kafka:kafka-clients:3.4.1",
# "org.apache.commons:commons-pool2-2.12.0.jar:2.11.1",

spark = (
    SparkSession.builder.appName("KafkaStreaming")
    .master("local[*]")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.streaming.schemaInference", "true")
    .config("spark.jars.packages", ",".join(jars))
    .getOrCreate()
)

kafka_params = {
    "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
    "group.id": "test-kafka-stream",
    "auto.offset.reset": "earliest",
}


spark.sparkContext.setLogLevel("INFO")


def main(topic: str, output: str, terminate: int):
    print(BOOTSTRAP_SERVERS)
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("failOnDataLoss", "false")
        .option("subscribe", topic)
        .load()
    )

    df.printSchema()
    df = df.selectExpr("CAST(value AS STRING) as value")

    (
        df.writeStream.format("text")
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

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)

    main(
        topic=known_args.topic,
        output=known_args.output,
        terminate=known_args.terminate,
    )
