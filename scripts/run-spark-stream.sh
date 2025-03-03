export KAFKA_BROKER='kafka-broker:9092' # run in docker containers
export TOPIC='brothers-karamazov'

#create topic first
docker exec notebook /usr/bin/python /home/app/kafka/src/create_topic.py --topic=${TOPIC}

# run spark stream job
## terminate time can be adjusted
docker exec -itd notebook spark-submit --conf spark.cores.max=1 /home/app/src/kafka_stream.py \
    --topic=${TOPIC} --terminate=15 --output=/home/app/spark-stream
sleep 5

#stream to topic
docker exec notebook /usr/bin/python /home/app/kafka/src/create_topic.py --topic=${TOPIC} \
    --file=/home/app/data/brothers-karamazov.txt