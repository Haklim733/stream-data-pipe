#!/bin/bash

export KAFKA_VERSION=3.4.0
export FLINK_VERSION=1.20
export ICEBERG_VERSION=1.8.0
export SCALA_VERSION=2.12


wget -P /opt/flink/lib https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-${FLINK_VERSION}/${ICEBERG_VERSION}/iceberg-flink-runtime-${FLINK_VERSION}-${ICEBERG_VERSION}.jar -P ./jars/

wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/${KAFKA_VERSION}-${FLINK_VERSION}/flink-connector-kafka-${KAFKA_VERSION}-${FLINK_VERSION}.jar -P ./jars/
\
wget https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_VERSION}-${FLINK_VERSION}/flink-sql-connector-kafka-${KAFKA_VERSION}-${FLINK_VERSION}.jar -P ./jars