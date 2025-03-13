export KAFKA_BROKER='kafka-broker:9092' # run in docker containers
export TOPIC='message'

# run spark stream output to file
# already running via generator on docker compose up

#spark stream output to iceberg
json_schema='{"message":"string","created_at":"number","id":"string"}'
docker exec -itd notebook spark-submit --conf spark.cores.max=1 /home/app/src/kafka_stream.py \
    --topic=${TOPIC} --terminate=45 --output=iceberg --format=json --schema=$json_schema

# docker exec -itd notebook spark-submit --conf spark.cores.max=1 /home/app/src/kafka_stream.py \
#     --topic=${TOPIC} --terminate=45 --output=iceberg --format=avro --schema='[{"name": "message", "type": "string"}, {"name": "created_at", "type": "long"}, {"name": "id", "type": "string"}]'