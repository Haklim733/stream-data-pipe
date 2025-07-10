import argparse
import json
import logging
from enum import Enum
import os
import re
import sys
import typing

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    ArrayType,
    MapType,
    BooleanType,
    BinaryType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType,
    TimestampType,
)


RUNTIME_ENV = os.getenv("RUNTIME_ENV", "local")
BOOTSTRAP_SERVERS = os.getenv(
    "BOOTSTRAP_SERVERS", "kafka-broker:19092"
)  # 19092 is external
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "password")
S3_BUCKET = os.getenv("S3_BUCKET", "iceberg")

logger = logging.getLogger(__name__)


class FileFormat(Enum):
    TEXT = "text"
    AVRO = "avro"
    CSV = "csv"
    JSON = "json"


# Create a SparkSession
jars = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6",
    "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.6",
    "org.apache.spark:spark-avro_2.12:3.5.6",
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1",
    "org.apache.kafka:kafka-clients:3.4.1",
    "org.apache.commons:commons-pool2:2.11.1",
    "com.github.luben:zstd-jni:1.5.2-5",
]
# Dockerfile installed following packages
# "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.4",
# "org.apache.kafka:kafka-clients:3.4.1",
# "org.apache.commons:commons-pool2-2.12.0.jar:2.11.1",

spark = (
    SparkSession.builder.appName("KafkaStreaming")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.streaming.schemaInference", "true")
    .config("spark.jars.packages", ",".join(jars))
    .config("spark.sql.catalog.iceberg.type", "rest")
    .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181")
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://iceberg/wh")
    .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000")
    .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
    .config("spark.sql.catalog.iceberg.s3.access-key", AWS_ACCESS_KEY_ID)
    .config("spark.sql.catalog.iceberg.s3.secret-key", AWS_SECRET_ACCESS_KEY)
    .getOrCreate()
)

kafka_params = {
    "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
    "group.id": "test-kafka-stream",
    "auto.offset.reset": "earliest",
}

spark.sparkContext.setLogLevel("INFO")

map_types_avro = {
    "string": "VARCHAR(255)",
    "long": "BIGINT",
    "integer": "INT",
}


def initialize_iceberg(topic: str, fields: str, format: str):
    """
    Initialize Iceberg namespace and table using Spark SQL
    """
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.spark")
    logger.info("Created namespace 'iceberg.spark'")

    spark.sql(
        f"""CREATE OR REPLACE TABLE iceberg.spark.`{topic}` (
        {fields} 
    )
    USING iceberg
    TBLPROPERTIES(
        'write.format.default'= '{format}',
        'write.parquet.compression-codec' = 'zstd',
        'branch.enabled' = 'true',
        'write.merge.isolation-level' = 'none',
        "write.wap.enabled" = 'true',
        'write.delete.enabled' = 'false',
        'write.update.enabled' = 'false',
        'write.upsert.enabled' = 'false',
        'write.merge.enabled' = 'false',
        'comment' = 'Raw data table'
    )
    """
    )
    spark.sql(f"ALTER TABLE iceberg.spark.`{topic}` CREATE OR REPLACE BRANCH main")
    spark.sql(f"ALTER TABLE iceberg.spark.`{topic}` CREATE OR REPLACE BRANCH staging")
    logger.info(f"Successfully created table iceberg.spark.{topic}")


def sanitize_avro(s):
    """
    Sanitizes a string to match the pattern (?:^|\.)[A-Za-z_][A-Za-z0-9_]*$
    """
    s = re.sub(r"[^\w]\.", "_", s)
    if s[0].isdigit():
        return Exception("Invalid topic name. First character must be a letter.")
    return s


def json_to_spark_type(json_type):
    if json_type == "string":
        return StringType()
    elif json_type == "number":
        return LongType()
    elif json_type == "timestamp":
        return TimestampType()
    else:
        raise ValueError(f"Unsupported JSON type: {json_type}")


def json_schema_to_spark(schema_dict: typing.Dict[str, typing.Any]):
    fields = []
    for k, v in schema_dict.items():
        field_type = json_to_spark_type(v)
        fields.append(StructField(k, field_type, True))

    return StructType(fields)


def avro_schema_to_spark(schema_d: typing.List[typing.Dict[str, typing.Any]]):
    avro_to_spark_type = {
        "string": StringType(),
        "long": LongType(),
        "integer": IntegerType(),
    }

    fields = []
    for field in schema_d:
        field_name = field["name"]
        field_type = field["type"]
        spark_type = avro_to_spark_type.get(field_type)

        if spark_type is None:
            raise ValueError(f"Unsupported Avro type: {field_type}")

        fields.append(StructField(field_name, spark_type, True))

    return StructType(fields)


def generate_spark_schema(schema_d, format: FileFormat):
    if format == FileFormat.JSON:
        return json_schema_to_spark(schema_d)
    elif format == FileFormat.AVRO:
        return avro_schema_to_spark(schema_d)
    else:
        raise ValueError(f"Unsupported file format: {format}")


def map_spark_to_iceberg(spark_type):
    """
    Maps Spark SQL types to Iceberg table types.

    Args:
    spark_type (DataType): Spark SQL DataType

    Returns:
    str: Corresponding Iceberg type as a string
    """
    type_mapping = {
        StringType: "string",
        IntegerType: "int",
        LongType: "long",
        FloatType: "float",
        DoubleType: "double",
        DateType: "date",
        TimestampType: "timestamp",
        BooleanType: "boolean",
        BinaryType: "binary",
    }

    if type(spark_type) in type_mapping:
        return type_mapping[type(spark_type)]
    elif isinstance(spark_type, DecimalType):
        return f"decimal({spark_type.precision},{spark_type.scale})"
    elif isinstance(spark_type, ArrayType):
        return f"list<{map_spark_to_iceberg(spark_type.elementType)}>"
    elif isinstance(spark_type, MapType):
        return f"map<{map_spark_to_iceberg(spark_type.keyType)},{map_spark_to_iceberg(spark_type.valueType)}>"
    elif isinstance(spark_type, StructType):
        fields = ", ".join(
            [
                f"{field.name}:{map_spark_to_iceberg(field.dataType)}"
                for field in spark_type.fields
            ]
        )
        return f"struct<{fields}>"
    else:
        raise ValueError(f"Unsupported Spark type: {spark_type}")


def main(
    topic: str,
    output: str,
    terminate: int,
    format: FileFormat = FileFormat.TEXT,
    schema: str | None = None,
):
    schema_d = json.loads(schema) if schema else None
    file_format = format  # Use the enum directly
    iceberg_format = "parquet"

    spark_schema = generate_spark_schema(schema_d, file_format)
    iceberg_fields = []
    for field in spark_schema.fields:
        iceberg_fields.append(f"{field.name} {map_spark_to_iceberg(field.dataType)}")

    if file_format == FileFormat.AVRO:
        assert isinstance(schema_d, list)
        iceberg_format = "avro"  # Fixed: was "format"
    else:
        assert isinstance(schema_d, dict)

    initialize_iceberg(topic, ", ".join(iceberg_fields), iceberg_format)

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("failOnDataLoss", "false")
        .option("subscribe", topic)
        .option("mode", "PERMISSIVE")
        .load()
    )
    # if file_format == FileFormat.AVRO:
    #     sanitized_topic = sanitize_avro(topic)
    #     avro_schema = {
    #         "type": "record",
    #         "name": sanitized_topic,
    #         "fields": schema_d,
    #     }
    #     logger.info(avro_schema)
    #     df = df.selectExpr("CAST(value AS BINARY) as value")
    #     df = df.select(
    #         from_avro(df.value, json.dumps(avro_schema)).alias("data")
    #     ).select("data.*")
    # else:
    #     df = df.selectExpr("CAST(value AS STRING) as value")
    spark_schema = generate_spark_schema(schema_d, file_format)
    parsed_df = (
        df.selectExpr("CAST(value AS STRING) as json_value")
        .select(from_json(col("json_value"), spark_schema).alias("data"))
        .select("data.*")
    )

    if output.startswith("iceberg"):
        (
            parsed_df.writeStream.format("iceberg")
            .outputMode("append")
            .option("checkpointLocation", f"/tmp/checkpoint-{topic}")
            .option("mode", "PERMISSIVE")
            .trigger(processingTime="30 seconds")
            .toTable(f"iceberg.spark.{topic}.branch_staging")
            .awaitTermination(terminate)
        )
    else:
        (
            df.writeStream.format(file_format.value)
            .option("truncate", False)
            .option("path", output)
            .option("checkpointLocation", f"/tmp/checkpoint-{topic}")
            .start()
            .awaitTermination(terminate)
        )


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
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
    parser.add_argument(
        "--schema",
        dest="schema",
        required=True,
        help="specify schema file to write",
    )

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)
    if known_args.format == "AVRO" and not known_args.schema:
        raise Exception("Schema must be provided if format is AVRO")

    # Convert string format to FileFormat enum
    file_format = FileFormat.TEXT  # default
    if known_args.format:
        try:
            file_format = FileFormat(known_args.format.lower())
        except ValueError:
            raise ValueError(f"Unsupported format: {known_args.format}")

    main(
        topic=known_args.topic,
        output=known_args.output,
        terminate=known_args.terminate,
        format=file_format,
        schema=known_args.schema,
    )
