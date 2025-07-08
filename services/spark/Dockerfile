FROM bitnami/spark:3.5.6

ENV SPARK_VERSION=3.5.6
ENV SPARK_MAJOR_VERSION=3.5
ENV ICEBERG_VERSION=1.9.1
ENV HADOOP_VERSION=3.3.4

# Configure environment
ENV SHELL=/bin/bash \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    LANGUAGE=C.UTF-8

ENV HOME="/home/app"
ENV SPARK_HOME="/opt/bitnami/spark"

USER root

WORKDIR ${HOME}

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    git \
    iputils-ping \
    rsync \
    sudo \
    ssh \
    vim \
    unzip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Download Iceberg JARs
RUN curl https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar \
    -Lo ${SPARK_HOME}/jars/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar

RUN curl https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/${SPARK_VERSION}/spark-avro_2.12-${SPARK_VERSION}.jar \
    -Lo ${SPARK_HOME}/jars/spark-avro_2.12-${SPARK_VERSION}.jar

# Download Kafka JARs
RUN curl -s https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar \
    -Lo ${SPARK_HOME}/jars/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar

RUN curl -s https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar \
    -Lo ${SPARK_HOME}/jars/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar

# Download Kafka clients JAR
RUN curl -s https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
    -Lo ${SPARK_HOME}/jars/kafka-clients-3.4.1.jar

# Download Commons Pool2 JAR
RUN curl -s https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    -Lo ${SPARK_HOME}/jars/commons-pool2-2.11.1.jar

RUN curl -s https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
    -Lo ${SPARK_HOME}/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

# Download Hadoop JARs for S3 support
RUN curl -s https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/${HADOOP_VERSION}/hadoop-common-${HADOOP_VERSION}.jar \
    -Lo ${SPARK_HOME}/jars/hadoop-common-${HADOOP_VERSION}.jar

RUN curl -s https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar \
    -Lo ${SPARK_HOME}/jars/hadoop-aws-${HADOOP_VERSION}.jar

# Add PostgreSQL JDBC driver for Iceberg catalog
RUN curl -s https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    -Lo ${SPARK_HOME}/jars/postgresql-42.7.3.jar

# Download Spark Connect JAR
RUN curl -s https://repo1.maven.org/maven2/org/apache/spark/spark-connect_2.12/${SPARK_VERSION}/spark-connect_2.12-${SPARK_VERSION}.jar \
    -Lo ${SPARK_HOME}/jars/spark-connect_2.12-${SPARK_VERSION}.jar

# Set up Python environment
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Make sure scripts are executable
RUN chmod u+x ${SPARK_HOME}/sbin/* && \
    chmod u+x ${SPARK_HOME}/bin/*

ENV PATH="${SPARK_HOME}/sbin:${SPARK_HOME}/bin:${PATH}"

# Create necessary directories
RUN mkdir -p ${SPARK_HOME}/logs ${SPARK_HOME}/conf

# Set proper permissions for logs directory
RUN chmod 777 ${SPARK_HOME}/logs

CMD [ "bash" ]