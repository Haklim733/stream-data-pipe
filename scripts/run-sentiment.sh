# assume docker compose is up and running
# call from root dir

export KAFKA_BROKER='kafka-broker:9092' # run in docker containers
export TOPIC='sentiment'

$(pwd)/services/kafka/.venv/bin/python $(pwd)/services/kafka/main.py --topic=${TOPIC} --bootstrap-servers='localhost:19092' --file=$(pwd)/services/kafka/data/brothers-karamazov.txt

# docker exec -it flink-jobmanager flink run -py /opt/flink/app/sentiment_analysis.py -n sentiment_analysis --topic ${TOPIC} --bootstrap-servers='kafka-broker:9092' --group-id sentiment-an # does not work

docker exec -it flink-jobmanager python /opt/flink/app/sentiment_analysis.py -n sentiment_analysis --topic sentiment --bootstrap-servers='kafka-broker:9092' --group-id sentiment-an #this works