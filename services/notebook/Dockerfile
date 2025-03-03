# from https://github.com/databricks/docker-spark-iceberg/blob/main/docker-compose.yml
# syntax=docker/dockerfile:1
FROM spark:3.5.4-java17-python3 

# Configure environment
ENV SHELL=/bin/bash \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    LANGUAGE=C.UTF-8

ENV HOME="/home/app"
ENV SPARK_HOME="/opt/spark"

USER root

WORKDIR ${HOME}

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    python3-pip \ 
    python3-dev \
    vim \
    unzip \
    sudo \
    ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

ENV SPARK_VERSION=3.5.4
ENV SPARK_MAJOR_VERSION=3.5
ENV ICEBERG_VERSION=1.8.0

# Download iceberg spark runtime
RUN curl https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar -Lo ${SPARK_HOME}/jars/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar

# Download dependencies for kafka
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar -P ${SPARK_HOME}/jars
RUN wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar -P ${SPARK_HOME}/jars
# see https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.4/spark-sql-kafka-0-10_2.12-3.5.4.pom
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar -P ${SPARK_HOME}/jars
RUN wget https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar -P ${SPARK_HOME}/jars

# Download AWS bundle
RUN curl -s https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar -Lo ${SPARK_HOME}/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

# Install AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && sudo ./aws/install \
    && rm awscliv2.zip \
    && rm -rf aws/


# Download and install IJava jupyter kernel
# RUN curl https://github.com/SpencerPark/IJava/releases/download/v1.3.0/ijava-1.3.0.zip -Lo ijava-1.3.0.zip \
#     && unzip ijava-1.3.0.zip \
#     && python3 install.py --sys-prefix \
#     && rm ijava-1.3.0.zip
# ENV IJAVA_CLASSPATH=${SPARK_HOME}/jars/*

ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip

# Install Jupyter and other python deps
COPY requirements.txt .
RUN pip3 install -r requirements.txt
RUN jupyter server extension enable --py jupyterlab --sys-prefix
ENV JUPYTER_PORT=8888

RUN mkdir -p ${HOME}/data ${HOME}/localwarehouse ${HOME}/notebooks ${HOME}/warehouse %{HOME}/spark-events
COPY notebooks/ ${HOME}/notebooks
# Add a notebook command
RUN echo '#! /bin/sh' >> /bin/notebook \
    && echo 'export PYSPARK_DRIVER_PYTHON=jupyter-notebook' >> /bin/notebook \
    && echo "export PYSPARK_DRIVER_PYTHON_OPTS=\"--notebook-dir=${HOME}/notebooks --ip='*' --NotebookApp.token='' --NotebookApp.password='' --port=8888 --no-browser --allow-root\"" >> /bin/notebook \
    && echo "pyspark" >> /bin/notebook \
    && chmod u+x /bin/notebook

RUN mkdir -p /root/.ipython/profile_default/startup
COPY ipython/startup/00-prettytables.py /root/.ipython/profile_default/startup
COPY ipython/startup/README /root/.ipython/profile_default/startup
COPY ipython/startup/startup.py /root/.ipython/profile_default/startup

RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"

COPY .pyiceberg.yaml /root/.pyiceberg.yaml

# from https://github.com/jupyter/docker-stacks/blob/main/images/docker-stacks-foundation/Dockerfile
COPY ipython_kernel_config.py "/etc/ipython/"

RUN pwd
COPY entrypoint.sh ${HOME}/
RUN chmod +x ${HOME}/entrypoint.sh

# copy flink binaries to submit jobs only
COPY --from=flink:1.20-scala_2.12 /opt/flink/bin /opt/flink/bin

ENTRYPOINT ["./entrypoint.sh"]
CMD ["notebook"]