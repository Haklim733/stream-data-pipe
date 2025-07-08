# assume docker compose is up and running
# call from project root dir

export KAFKA_BROKER='kafka-broker:9092' # run in docker containers
export TOPIC='message'

# #source
docker compose -f docker-compose-spark.yaml exec -itd generator python src/kafka_generate.py --topic message --max-time 300

# flink will output to topic sentiment_analysis
docker compose -f docker-compose-flink.yaml exec -itd flink-jobmanager flink run -py /opt/flink/app/sentiment_analysis.py --source-topic ${TOPIC} --group-id sentiment-an