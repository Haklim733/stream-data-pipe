export KAFKA_BROKER='kafka-broker:9092' # run in docker containers
export TOPIC='brothers-karamazov'

$(pwd)/services/kafka/.venv/bin/python $(pwd)/services/kafka/src/create_topic.py \
    --topic=${TOPIC} --file=$(pwd)/services/kafka/data/brothers-karamazov.txt --format=json
$(pwd)/services/risingwave/.venv/bin/python $(pwd)/services/risingwave/src/main.py --topic=${TOPIC}