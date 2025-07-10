# Stream Data Pipeline

A testing environment for real-time data ingestion and processing using modern streaming technologies. This project demonstrates various data pipeline patterns from simple message streaming to complex ML inference workflows.

![](./assets/stream-data-pipeline.svg)

## Data Flow Patterns

This project showcases several streaming data pipeline patterns:

1. **Apache Kafka → Apache Flink → Kafka** - Real-time stream processing with sentiment analysis
2. **Apache Kafka → Apache Spark → Iceberg/S3** - Structured streaming with data lake storage
3. **Apache Kafka → RisingWave** - Real-time database with materialized views
4. **Apache Kafka → ML Inference (Python) → Kafka** - AI/ML pipeline with emotion analysis
5. **PostgreSQL → Sequin → Kafka** - Change Data Capture (CDC) streaming

## Getting Started

The first step is to run the setup script to download raw source data that will be streamed to Kafka:

```bash
chmod +x setup.sh && ./setup.sh
```

## Examples

### 1. Apache Kafka → Apache Flink → Kafka

#### Sentiment Analysis Pipeline

This example creates a Kafka topic as a source and runs a Flink job that tokenizes and classifies each line of text. The results are piped into a separate Kafka topic for downstream consumption.

**Steps:**
1. Start the Flink environment: `export COMPOSE_BAKE=true && docker compose -f docker-compose-flink.yaml up -d --build`
2. After the Kafka broker is up and running, execute: `./scripts/run-flink-sentiment.sh`
3. Navigate to `localhost:8888` and open the `kafka-flink-sentiment` notebook to analyze the results

The Flink job performs simple keyword-based sentiment analysis on incoming messages and outputs structured results with sentiment classifications.

### 2. Apache Kafka → Apache Spark → Iceberg/Storage

This pipeline publishes data to a Kafka topic which is then ingested using Spark Structured Streaming and saved to an Iceberg table. The results provide a scalable, ACID-compliant data lake solution.

**Features:**
- Specify `--output=iceberg` when running the relevant docker exec command to save to Iceberg
- Specify `--output=<path>` to save to file storage
- Supports both JSON and Avro formats with schema validation

**Steps:**
1. Start the Spark environment: `docker compose -f docker-compose-spark.yaml up -d --build && ./scripts/run-spark-stream.sh`
2. View results by navigating to `localhost:8888` and opening the `kafka-spark-stream` notebook

**Note:**
- The Iceberg table is created with two branches: `main` and `staging`
- The `staging` branch is used for the streaming write to follow the Write-Audit-Publish pattern
- the writes occur in 30 second intervals
- spark structured streaming writes does not seem to respect the write.wap options.

### 3. Apache Kafka → RisingWave

This example demonstrates RisingWave, a real-time database that ingests Kafka topics as source tables and creates materialized views for real-time analytics.

**Steps:**
1. Start RisingWave: `docker compose -f docker-compose-rw.yaml up -d --build`
2. Navigate to `localhost:8888` and execute the cells in the `kafka-risingwave` notebook

**Alternative Access:**
You can also query the materialized views directly via PostgreSQL:
```bash
psql -h localhost -p 4566 -d dev -U root
```
Then query the `<topic>_view` table for real-time results.
### 4. Apache Kafka → ML Inference (Python) → Kafka

This pipeline demonstrates real-time ML inference using Python FastStream and Hugging Face transformers. Messages are published to a Kafka topic, processed by the ML service for emotion sentiment analysis, and results are sent back to a separate Kafka topic.

**Configuration:**
- Topic names can be configured in `docker-compose.yaml` for notebook and ML services
- The ML service uses a pre-trained emotion classification model (DistilRoBERTa)

**Steps:**
1. Start the ML pipeline: `export COMPOSE_BAKE=true && docker compose -f docker-compose-ml.yaml up -d --build`
2. Navigate to `localhost:8888` and execute the cells in the `kafka-ml-inference` notebook


### 5. PostgreSQL → Sequin → Kafka

This CDC (Change Data Capture) pipeline demonstrates streaming database changes to Kafka. The generator service inserts messages into PostgreSQL, and the Sequin service streams these changes to a Kafka topic named 'cdc'.

**Monitoring:**
- Verify stream source and sink throughput at http://localhost:7376/
- Check CDC messages using the 'postgres-cdc' notebook at localhost:8888

## Logs and Debugging

Service logs are available in mounted volumes as specified in the docker-compose.yaml files:
- Flink logs: `services/flink/log/jobmanager`
- Spark logs: `services/spark/logs`
- Kafka logs: `services/kafka/logs`

## Architecture Notes

This project extends and builds upon several open-source projects:
- [Databricks Docker Spark Iceberg](https://github.com/databricks/docker-spark-iceberg)
- [Quix PyFlink Deep Dive](https://quix.io/blog/pyflink-deep-dive)
- [Flink Demos by Jaehyeon Kim](https://github.com/jaehyeon-kim/flink-demos)

The project demonstrates modern streaming data architecture patterns including:
- Event-driven microservices
- Real-time ML inference
- Change Data Capture
- Stream processing with Apache Flink and Spark
- Data lake storage with Apache Iceberg
- Real-time databases with RisingWave
