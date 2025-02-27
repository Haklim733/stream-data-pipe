# Overview

Repo to learn and test data pipelines for kafka,flink, spark streaming and iceberg:

1. apache kafka -> apache flink -> kafka
2. apache kafka -> apache flink -> s3
3. apache kafka -> apache flink -> iceberg / s3
4. apache kafka -> apache spark -> iceberg / s3

first step is to run `chmod +x setup.sh && ./setup.sh` from the project root.

Work is an extension of https://github.com/databricks/docker-spark-iceberg, https://quix.io/blog/pyflink-deep-dive, and
https://github.com/jaehyeon-kim/flink-demos

## apache kafka -> apache flink -> kafka

### Sentiment Analysis

This creates a kafka topic as a source and then runs a flink job that tokenizes and classifies each line of text. The results are piped into a kafka topic.

`./scripts/run-sentiment.sh`

## apache kafka -> s3 & apache kafka -> apache flink -> s3

work in progress

## apache kafka -> apache flink -> iceberg

work in progress

## apache kafka -> apache spark -> iceberg

work in progress

## logs

see mounted volumes in the docker-compose.yaml
ex. services/flink/log/jobmanager
