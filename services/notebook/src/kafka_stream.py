import argparse
import json
import logging
from enum import Enum
import os
import re
import sys
import typing

from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.catalog import load_catalog
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
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:19092")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "password")
S3_BUCKET = os.getenv("S3_BUCKET", "warehouse")

logger = logging.getLogger(__name__)


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

map_types_avro = {
    "string": "VARCHAR(255)",
    "long": "BIGINT",
    "integer": "INT",
}


def initialize_iceberg(topic: str, fields: str, format: str):
    catalog = load_catalog(name="rest", uri="http://iceberg-rest:8181")

    try:
        catalog.create_namespace("spark")
    except NamespaceAlreadyExistsError:
        pass

    try:
        catalog.drop_table(f"spark.{topic}")
    except NoSuchTableError:
        pass

    logger.info("Creating iceberg database")
    spark.sql("CREATE DATABASE IF NOT EXISTS spark")
    logger.info(
        f"Creating iceberg table spark.{topic} with fields {fields} in format {format}"
    )
    spark.sql(f"DROP TABLE IF EXISTS default.spark.{topic}")
    spark.sql(
        f"""
        CREATE TABLE default.spark.{topic}(
            {fields}
        )
        USING iceberg
        TBLPROPERTIES(
            'write.format.default'= '{format}'
        )
        """
    )


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


def json_schema_to_spark(schema_dict: dict[str, typing.Any]):
    fields = []
    for k, v in schema_dict.items():
        field_type = json_to_spark_type(v)
        fields.append(StructField(k, field_type, True))

    return StructType(fields)


def avro_schema_to_spark(schema_d: list[dict[str, typing.Any]]):
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
    schema_d = json.loads(schema)
    file_format = FileFormat(format)
    iceberg_format = "parquet"

    spark_schema = generate_spark_schema(schema_d, file_format)
    iceberg_fields = []
    for field in spark_schema.fields:
        iceberg_fields.append(f"{field.name} {map_spark_to_iceberg(field.dataType)}")

    if file_format == FileFormat.AVRO:
        assert isinstance(schema_d, list)
        iceberg_format = "format"
    else:
        assert isinstance(schema_d, dict)

    initialize_iceberg(topic, ", ".join(iceberg_fields), iceberg_format)

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("failOnDataLoss", "false")
        .option("subscribe", topic)
        .option("mode", "PERMISSIVE")  # does not work with pyiceberg to_pandas
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
    spark_schema = generate_spark_schema(
        schema_d, file_format
    )  # using avro field type schema structure
    parsed_df = (
        df.selectExpr("CAST(value AS STRING) as json_value")
        .select(from_json(col("json_value"), spark_schema).alias("data"))
        .select("data.*")
    )

    if output.startswith("iceberg"):
        (
            parsed_df.writeStream.format("iceberg")
            .outputMode("append")
            .option("checkpointLocation", "/tmp/checkpoint")
            .option("mode", "PERMISSIVE")
            .toTable(f"default.spark.{topic}")
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

    main(
        topic=known_args.topic,
        output=known_args.output,
        terminate=known_args.terminate,
        format=known_args.format,
        schema=known_args.schema,
    )
