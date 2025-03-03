# assume docker compose is up and running
# call from project root dir

export KAFKA_BROKER='kafka-broker:9092' # run in docker containers
export TOPIC='sentiment'

#source
docker exec notebook /usr/bin/python /home/spark/kafka/src/create_topic.py --topic=${TOPIC} --file=/home/iceberg/data/brothers-karamazov.txt

docker exec -d flink-jobmanager flink run -py /opt/flink/app/sentiment_analysis.py --source-topic ${TOPIC} --group-id sentiment-an