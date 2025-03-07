export KAFKA_BROKER='kafka-broker:9092' # run in docker containers
export TOPIC='book'

docker exec notebook /usr/bin/python /home/app/kafka/src/create_topic.py --topic=${TOPIC} --file=/home/app/data/brothers-karamazov.txt --format=json
