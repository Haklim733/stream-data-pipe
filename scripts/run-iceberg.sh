#!/bin/bash
export KAFKA_BROKER='kafka-broker:9092' # run in docker containers
docker exec -it flink-jobmanager python /opt/flink/app/iceberg.py -n iceberg1 --bootstrap-servers=${KAFKA_BROKER}