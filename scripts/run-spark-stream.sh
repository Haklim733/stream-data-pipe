export KAFKA_BROKER='kafka-broker:9092' # run in docker containers
export TOPIC='brothers-karamazov'


## run script since starting stream via jupyter notebook will close before messages are populated
#create topic first - required for streaming
docker exec notebook /usr/bin/python /home/app/kafka/src/create_topic.py --topic=${TOPIC}

# run spark stream output to file
## terminate time can be adjusted 
# docker exec -itd notebook spark-submit --conf spark.cores.max=1 /home/app/src/kafka_stream.py \
#     --topic=${TOPIC} --terminate=20 --output=/home/app/output --format=avro

#spark stream output to iceberg
docker exec -itd notebook spark-submit --conf spark.cores.max=1 /home/app/src/kafka_stream.py \
    --topic=${TOPIC} --terminate=45 --output=iceberg --format=avro


#stream to topic after waiting for stream job to initialize
sleep 10
docker exec notebook /usr/bin/python /home/app/kafka/src/create_topic.py --topic=${TOPIC} \
    --file=/home/app/data/brothers-karamazov.txt --format=avro