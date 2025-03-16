# Overview

Repo to learn and test real-time data ingestion and processing using kafka,flink, spark streaming, ml inference and apache iceberg.

![](./assets/stream-data-pipeline.svg)

# Data flow

1. apache kafka -> apache flink -> kafka
2. apache kafka -> apache flink -> iceberg / s3
3. apache kafka -> apache spark -> iceberg / s3
4. apache kafka -> risingwave
5. apache kafka -> emotion sentiment analysis (python) -> kafka

# Examples

The first step is to run `chmod +x setup.sh && ./setup.sh` from the project root. This will download the raw source data that will be sent to kafka line by line.

## apache kafka -> apache flink -> kafka

### Sentiment Analysis

This creates a kafka topic as a source and then runs a flink job that tokenizes and classifies each line of text. The results are piped into a separate kafka topic.

Steps:
(1) Run `export COMPOSE_BAKE=true && docker compose -f docker-compose-flink.yaml up -d`
(2) After the kafka-broker is up and running, run `./scripts/run-flink-sentiment.sh`
(3) Navigate to `localhost:8888` and opening the `kafka-flink-sentiment` notebook to look at the results.

## apache kafka -> ml inference (python) -> kafka

Messages are published to a kafka topic which is ingested by the 'ml' service using python faststream and huggingface to produce an emotion sentiment analysis that is sent back as a kafka topic. The topic names can be set in the docker-compose.yaml for notebook and ml services.

Steps:
(1) Run `export COMPOSE_BAKE=true && docker compose -f docker-compose-ml.yaml up -d`
(2) navigate to `localhost:8888` and execute the cells in the `kafka-ml-inference` notebook.


### apache kafka -> s3 & apache kafka -> apache flink -> s3

work in progress

### apache kafka -> apache spark -> iceberg / storage

This program publishes data to a kafka topic which is then ingested using spark structured streaming and saved to an iceberg table. Results can be seen by navigating to `localhost:8888` and opening the `kafka-spark-stream` notebook.

Specify --output=iceberg when running the relevant docker exec command to save to iceberg.
Specify --output=><path> when running the relevant docker exec command to save to file.

Steps:
(1) Run `docker compose -f docker-compose-spark.yaml up -d && ./scripts/run-spark-stream.sh`
(2) Results can be seen by navigating to `localhost:8888` and opening the `kafka-spark-stream` notebook.

## apache kafka -> risingwave
Messages are published a kafka topic which is then ingested as a source table and materialized view using rising wave, a real-time database.

Steps:
(1) Run `docker compose -f docker-compose-rw.yaml up -d`
(2) Navigate to `localhost:8888` and execute the cells in the `kafka-risingwave` notebook.

Results can be also seen via `psql -h localhost -p 4566 -d dev -U root` and querying the `<topic>_view` table.

## logs

see mounted volumes in the docker-compose.yaml
ex. services/flink/log/jobmanager

# Notes

Work is an extension of https://github.com/databricks/docker-spark-iceberg, https://quix.io/blog/pyflink-deep-dive, and
https://github.com/jaehyeon-kim/flink-demos
