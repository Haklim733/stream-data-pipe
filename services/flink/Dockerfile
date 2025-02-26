FROM flink:1.20.1-scala_2.12 

ENV SHELL=/bin/bash \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    LANGUAGE=C.UTF-8

ENV KAFKA_VERSION=3.3.0
ENV FLINK_VERSION=1.20
ENV ICEBERG_VERSION=1.8.0
ENV SCALA_VERSION=2.12

RUN apt-get update -y && \
    apt-get install -y python3 \
    python3-pip \
    python3-dev \
    vim \
    ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python
COPY requirements.txt .
RUN pip3 install -r requirements.txt

RUN wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-${FLINK_VERSION}/${ICEBERG_VERSION}/iceberg-flink-runtime-${FLINK_VERSION}-${ICEBERG_VERSION}.jar -P /opt/flink/lib

RUN wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/${KAFKA_VERSION}-${FLINK_VERSION}/flink-connector-kafka-${KAFKA_VERSION}-${FLINK_VERSION}.jar -P /opt/flink/lib

RUN wget https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_VERSION}-${FLINK_VERSION}/flink-sql-connector-kafka-${KAFKA_VERSION}-${FLINK_VERSION}.jar -P /opt/flink/lib

RUN wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.0/kafka-clients-3.9.0.jar -P /opt/flink/lib

RUN chown -R flink:flink /opt/flink/lib
# RUN chown -R flink:flink /opt/flink/usrlib