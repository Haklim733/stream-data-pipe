FROM flink:scala_2.12-java17

ENV SHELL=/bin/bash \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    LANGUAGE=C.UTF-8

ENV KAFKA_VERSION=3.4.0
ENV FLINK_VERSION=1.20
ENV ICEBERG_VERSION=1.8.0

RUN wget https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_VERSION}-${FLINK_VERSION}/flink-sql-connector-kafka-${KAFKA_VERSION}-${FLINK_VERSION}.jar -P /opt/flink/lib/


RUN wget -P /opt/flink/lib https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-${FLINK_VERSION}/${ICEBERG_VERSION}/iceberg-flink-runtime-${FLINK_VERSION}-${ICEBERG_VERSION}.jar -P /opt/flink/lib
