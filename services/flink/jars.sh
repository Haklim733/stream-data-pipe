#!/bin/bash

export KAFKA_VERSION=3.3.0
export FLINK_VERSION=1.20
export ICEBERG_VERSION=1.8.0
export SCALA_VERSION=2.12


wget -O jars/iceberg-flink-runtime-${FLINK_VERSION}-${ICEBERG_VERSION}.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-${FLINK_VERSION}/${ICEBERG_VERSION}/iceberg-flink-runtime-${FLINK_VERSION}-${ICEBERG_VERSION}.jar


wget -O jars/flink-sql-connector-kafka-${KAFKA_VERSION}-${FLINK_VERSION}.jar https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_VERSION}-${FLINK_VERSION}/flink-sql-connector-kafka-${KAFKA_VERSION}-${FLINK_VERSION}.jar

wget -O jars/flink-connector-kafka-${KAFKA_VERSION}-${FLINK_VERSION}.jar https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/${KAFKA_VERSION}-${FLINK_VERSION}/flink-connector-kafka-${KAFKA_VERSION}-${FLINK_VERSION}.jar

wget -O jars/kafka-clients-3.9.0.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.0/kafka-clients-3.9.0.jar