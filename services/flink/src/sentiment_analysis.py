# see https://quix.io/blog/pyflink-deep-dive as reference
import argparse
import json
import logging
import os
import sys
import time

from nltk.tokenize import sent_tokenize, word_tokenize
from pyflink.common import WatermarkStrategy, Types, Configuration, Row

# Iceberg functionality is available through JAR files, not Python imports
# We'll use the Java classes directly
from pyflink.table import DataTypes, Schema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
    DeliveryGuarantee,
)
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer
from pyflink.datastream.functions import MapFunction
from pyflink.datastream.execution_mode import RuntimeExecutionMode

# Test file sink to MinIO first
from pyflink.datastream.connectors.file_system import (
    FileSink,
    RollingPolicy,
    OutputFileConfig,
)
from pyflink.common import Encoder

# File system imports - will be handled differently if needed
from pyflink.table import StreamTableEnvironment


logger = logging.getLogger(__name__)

RUNTIME_ENV = os.getenv("RUNTIME_ENV", "local")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:19092")

config = Configuration()

# Set S3 configuration for MinIO (simplified approach)
config.set_string("s3.access.key", "admin")
config.set_string("s3.secret.key", "password")
config.set_string("s3.endpoint", "http://minio:9000")
config.set_string("s3.path.style.access", "true")

# Iceberg configuration
config.set_string("iceberg.catalog.iceberg.type", "rest")
config.set_string("iceberg.catalog.iceberg.uri", "http://iceberg-rest:8181")
config.set_string("iceberg.catalog.iceberg.warehouse", "s3a://iceberg/wh")
config.set_string("iceberg.catalog.iceberg.s3.endpoint", "http://minio:9000")
config.set_string("iceberg.catalog.iceberg.s3.path-style-access", "true")
config.set_string("iceberg.catalog.iceberg.s3.access-key", "admin")
config.set_string("iceberg.catalog.iceberg.s3.secret-key", "password")

jars = [
    "file:///opt/flink/opt/flink-python-1.20.1.jar",
    "file:///opt/flink/lib/flink-connector-kafka-3.4.0-1.20.jar",
    "file:///opt/flink/lib/iceberg-flink-runtime-1.20-1.9.1.jar",
]
config.set_integer("parallelism.default", 1)
config.set_string("pipeline.jars", ";".join(jars))
# see: https://www.mail-archive.com/issues@flink.apache.org/msg765038.html

env = StreamExecutionEnvironment.get_execution_environment(config)
env.set_parallelism(1)
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
# env.add_python_archive("/opt/flink/punkt_tab.zip")


def sentiment_analysis_job(source_topic_name: str, group_id: str):
    """
    Sets up a PyFlink job that consumes social media posts from a Kafka   topic, performs sentiment analysis, and outputs results to another Kafka topic.
    """

    class SentimentAnalysis(MapFunction):
        def tokenize_text(self, line):
            try:
                sentences = sent_tokenize(line)
                words = [word_tokenize(sentence) for sentence in sentences]
                return words
            except Exception as e:
                logger.error(f"Error tokenizing text: {e}")
                return [line]  # Return original text as fallback

        def map(self, value):
            try:
                logger.info(f"Processing message: {value}")

                # Parse JSON message
                try:
                    message_data = json.loads(value)
                    message_text = message_data.get("message", value)
                except json.JSONDecodeError:
                    # If not JSON, use the value as is
                    message_text = value

                # Example of simple sentiment analysis logic
                positive_keywords = [
                    "happy",
                    "joyful",
                    "love",
                    "good",
                    "great",
                    "excellent",
                ]
                negative_keywords = ["sad", "angry", "hate", "bad", "terrible", "awful"]

                # Simple word-based sentiment analysis
                words = message_text.lower().split()
                sentiment = "Neutral"  # Default sentiment

                for keyword in positive_keywords:
                    if keyword in words:
                        sentiment = "Positive"
                        break

                for keyword in negative_keywords:
                    if keyword in words:
                        sentiment = "Negative"
                        break

                # Return structured data
                result = {
                    "message": message_text,
                    "sentiment": sentiment,
                    "timestamp": int(time.time() * 1000),
                    "processed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                }

                logger.info(f"Sentiment result: {result}")
                return json.dumps(result)  # <-- Fix: return as JSON string

            except Exception as e:
                logger.error(f"Error in sentiment analysis: {e}")
                # Return a default result on error
                return json.dumps(
                    {
                        "message": str(value),
                        "sentiment": "Error",
                        "timestamp": int(time.time() * 1000),
                        "processed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )

    # Define a source to read from Kafka.
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics(source_topic_name)
        .set_group_id(group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    ds = env.from_source(
        source, WatermarkStrategy.for_monotonous_timestamps(), "Kafka Source"
    )

    dest_topic = "sentiment_analysis"
    kafka_topic_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(dest_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    # Keep the stream as string output to avoid Row serialization issues
    stream = ds.map(SentimentAnalysis(), output_type=Types.STRING())

    stream.sink_to(kafka_topic_sink)
    # Comment out Iceberg for now - test file sink first
    # table_env = StreamTableEnvironment.create(env)
    # Define schema for Iceberg table
    # schema = (
    #     Schema.new_builder()
    #     .column("message", DataTypes.STRING())
    #     .column("sentiment", DataTypes.STRING())
    #     .column("timestamp", DataTypes.BIGINT())
    #     .column("processed_at", DataTypes.STRING())
    #     .build()
    # )

    # # Create Iceberg catalog first (simplified configuration)
    # table_env.execute_sql(
    #     """
    #     CREATE CATALOG iceberg WITH (
    #         'type' = 'iceberg',
    #         'catalog-type' = 'rest',
    #         'uri' = 'http://iceberg-rest:8181'
    #     )
    # """
    # )

    # # Convert stream to table
    # table = table_env.from_data_stream(stream, schema)

    # # Write to Iceberg table using SQL
    # table_env.execute_sql(
    #     """
    #     CREATE TABLE IF NOT EXISTS `iceberg`.`flink`.`sentiment` (
    #         `message` STRING,
    #         `sentiment` STRING,
    #         `timestamp` BIGINT,
    #         `processed_at` STRING
    #     )
    # """
    # )

    # # Insert into Iceberg table
    # table.execute_insert("`iceberg`.`flink`.`sentiment`")

    # Create file sink to MinIO
    # file_sink = (
    #     FileSink.for_row_format(
    #         "s3a://iceberg/raw/flink/sentiment/", Encoder.simple_string_encoder()
    #     )
    #     .with_output_file_config(
    #         OutputFileConfig.builder()
    #         .with_part_prefix("sentiment_")
    #         .with_part_suffix(".txt")
    #         .build()
    #     )
    #     .with_rolling_policy(RollingPolicy.default_rolling_policy())
    #     .build()
    # )

    # # Write to MinIO
    # stream.sink_to(file_sink)

    env.execute("sentiment analysis job")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source-topic",
        dest="source_topic",
        required=True,
        help="specify kafka topic to consume",
    )
    parser.add_argument(
        "--group-id", dest="group_id", required=True, help="specify group id"
    )

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)

    sentiment_analysis_job(
        source_topic_name=known_args.source_topic, group_id=known_args.group_id
    )
