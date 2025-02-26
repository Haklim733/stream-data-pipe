export KAFKA_BROKER='kakfa-broker:9092'
export TOPIC='test'

$(pwd)/services/kafka/.venv/bin/python $(pwd)/services/kafka/src/create_topic.py --topic=${TOPIC} --bootstrap-servers='localhost:19092' --file=$(pwd)/services/kafka/data/brothers-karamazov.txt
# docker exec -it flink-jobmanager python /opt/flink/examples/python/datastream/basic_operations.py
# docker exec -it flink-jobmanager python /opt/flink/app/kafka_json_example.py

# docker exec -it flink-jobmanager python /opt/flink/app/test-kafka.py