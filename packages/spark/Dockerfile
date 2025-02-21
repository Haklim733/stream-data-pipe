# from https://github.com/databricks/docker-spark-iceberg/blob/main/docker-compose.yml
# syntax=docker/dockerfile:1
FROM spark:3.5.4-java17-python3 

# Configure environment
ENV SHELL=/bin/bash \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    LANGUAGE=C.UTF-8

ENV HOME="/home/spark"
ENV SPARK_HOME="/opt/spark"

USER root

WORKDIR "${HOME}"

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    vim \
    unzip \
    sudo \
    ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR ${SPARK_HOME}

ENV SPARK_VERSION=3.5.4
ENV SPARK_MAJOR_VERSION=3.5
ENV ICEBERG_VERSION=1.8.0

# Download iceberg spark runtime
RUN curl https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar -Lo ${SPARK_HOME}/jars/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar

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

ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

RUN mkdir -p /home/iceberg/data /home/iceberg/localwarehouse /home/iceberg/notebooks /home/iceberg/warehouse /home/iceberg/spark-events /home/iceberg
COPY notebooks/ /home/iceberg/notebooks

USER ${NB_UID}
# Install Jupyter and other python deps
COPY requirements.txt .
RUN pip3 install -r requirements.txt
RUN jupyter server extension enable --py jupyterlab --sys-prefix
ENV JUPYTER_PORT=8888

# Add a notebook command
RUN echo '#! /bin/sh' >> /bin/notebook \
    && echo 'export PYSPARK_DRIVER_PYTHON=jupyter notebook' >> /bin/notebook \
    && echo "export PYSPARK_DRIVER_PYTHON_OPTS=\"--notebook-dir=/home/iceberg/notebooks --ip='*' --NotebookApp.token='' --NotebookApp.password='' --port=8888 --no-browser --allow-root\"" >> /bin/notebook \
    && echo "pyspark" >> /bin/notebook \
    && chmod u+x /bin/notebook

RUN mkdir -p /root/.ipython/profile_default/startup
COPY ipython/startup/00-prettytables.py /root/.ipython/profile_default/startup
COPY ipython/startup/README /root/.ipython/profile_default/startup

COPY polaris.conf /opt/spark/conf
RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"

COPY .pyiceberg.yaml /root/.pyiceberg.yaml

# from https://github.com/jupyter/docker-stacks/blob/main/images/docker-stacks-foundation/Dockerfile
COPY ipython_kernel_config.py "/etc/ipython/"

WORKDIR "${HOME}"

COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
CMD ["notebook"]