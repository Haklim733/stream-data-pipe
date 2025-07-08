export KAFKA_BROKER='kafka-broker:9092' # run in docker containers
export TOPIC='message'

#spark stream output to iceberg
json_schema='{"message":"string","created_at":"number","event_id":"string", "origin_id": "string"}'
docker compose -f docker-compose-spark.yaml exec -itd spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /home/app/src/kafka_stream.py \
  --topic=${TOPIC} \
  --terminate=300 \
  --output=iceberg \
  --format=json \
  --schema="$json_schema"

# send messages
 docker compose -f docker-compose-spark.yaml exec -itd generator python src/kafka_generate.py --topic message --max-time 300



# docker exec -itd notebook spark-submit --conf spark.cores.max=1 /home/app/src/kafka_stream.py \
#     --topic=${TOPIC} --terminate=45 --output=iceberg --format=avro --schema='[{"name": "message", "type": "string"}, {"name": "created_at", "type": "long"}, {"name": "id", "type": "string"}]'