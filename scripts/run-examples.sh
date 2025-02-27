#!/bin/bash
## testing examples provided by apache flink
export KAFKA_BROKER='kakfa-broker:9092'
export TOPIC='test'

docker exec -it flink-jobmanager python /opt/flink/examples/python/datastream/basic_operations.py
# docker exec -it flink-jobmanager python /opt/flink/app/kafka_json_example.py