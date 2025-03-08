# Overview

Repo to learn and test data pipelines for kafka,flink, spark streaming, ml inference and iceberg:

1. apache kafka -> apache flink -> kafka
2. apache kafka -> apache flink -> s3
3. apache kafka -> apache flink -> iceberg / s3
4. apache kafka -> apache spark -> iceberg / s3
5. apache kafka -> risingwave
6. apache kafka -> emotion sentiment analysis (python) -> kafka

first step is to run `chmod +x setup.sh && ./setup.sh` from the project root.

Work is an extension of https://github.com/databricks/docker-spark-iceberg, https://quix.io/blog/pyflink-deep-dive, and
https://github.com/jaehyeon-kim/flink-demos

## apache kafka -> apache flink -> kafka

### Sentiment Analysis

This creates a kafka topic as a source and then runs a flink job that tokenizes and classifies each line of text. The results are piped into a kafka topic.

`./scripts/run-flink-sentiment.sh`

Results can be seen by navigating to `localhost:8888` and opening the `kafka-flink-sentiment` notebook.

## apache kafka -> s3 & apache kafka -> apache flink -> s3

work in progress

## apache kafka -> apache spark -> iceberg

program saves kafka topic using spark structured streaming to an iceberg table
`./scripts/run-spark-stream.sh`
\*You need to specify --output=iceberg when running the relevant docker exec command.
Results can be seen by navigating to `localhost:8888` and opening the `kafka-spark-stream` notebook.

## apache kafka -> apache spark -> file

program saves kafka topic as a datastream to a text file
`./scripts/run-spark-stream.sh`
You need to specify --output=>path/to/folder? when running the relevant docker exec command.
Results can be seen by navigating to `localhost:8888` and opening the `kafka-spark-stream` notebook.

## apache kafka -> risingwave

Messages are published a kafka topic which is then ingested as a source table and materialized view using rising wave.
Navigate to `localhost:8888` and open the `kafka-risingwave` notebook.

Results can be also seen via `psql -h localhost -p 4566 -d dev -U root` and querying the `<topic>_view` table.

## apache kafka -> ml inference -> kafka

Messages are published to a kafka topic which is ingested by the 'ml' service using python faststream and huggingface to produce an emotion sentiment analysis that is sent back as a kafka topic. The topic names can be set in the docker-compose.yaml for notebook and ml services.

navigate to `localhost:8888` and open the `kafka-ml-inference` notebook.

## logs

see mounted volumes in the docker-compose.yaml
ex. services/flink/log/jobmanager
