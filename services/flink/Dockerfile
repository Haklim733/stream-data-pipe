FROM flink:1.20.1-scala_2.12 

ENV SHELL=/bin/bash \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    LANGUAGE=C.UTF-8

ENV KAFKA_VERSION=3.4.0
ENV FLINK_VERSION=1.20
ENV ICEBERG_VERSION=1.9.1
ENV SCALA_VERSION=2.12

RUN apt-get update -y && \
    apt-get install -y python3 \
    python3-pip \
    python3-dev \
    vim \
    unzip \
    zip \
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

# Add AWS bundle for S3A connectivity (CRITICAL for MinIO)
RUN wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar -P /opt/flink/lib

# Add hadoop-common.jar (CRITICAL dependency for hadoop-aws)
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.6/hadoop-common-3.3.6.jar -P /opt/flink/lib

RUN chown -R flink:flink /opt/flink/lib

# Download S3 filesystem plugins properly
RUN mkdir -p /opt/flink/plugins/s3-fs-presto && \
    wget -O /opt/flink/plugins/s3-fs-presto/flink-s3-fs-presto-1.20.1.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-presto/1.20.1/flink-s3-fs-presto-1.20.1.jar

RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    wget -O /opt/flink/plugins/s3-fs-hadoop/flink-s3-fs-hadoop-1.20.1.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.20.1/flink-s3-fs-hadoop-1.20.1.jar

RUN wget https://raw.githubusercontent.com/nltk/nltk_data/gh-pages/packages/tokenizers/punkt_tab.zip
RUN mkdir -p /opt/flink/nltk_data/tokenizers && \
    unzip punkt_tab.zip -d /opt/flink/nltk_data/tokenizers \
    && rm -f punkt_tab.zip
RUN zip -j punkt_tab.zip /opt/flink/nltk_data/tokenizers/punkt_tab/english/*

# Copy SQL client 
COPY ./sql-client.sh /opt/flink/
RUN chmod +x /opt/flink/sql-client.sh