# step1
FROM apache/kafka-native:latest AS base
USER root
ARG KAFKA_VERSION
ARG CLI_VERSION
ENV FILENAME=kafka_${CLI_VERSION}-${KAFKA_VERSION}
RUN wget https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/${FILENAME}.tgz
RUN tar -xzf ${FILENAME}.tgz
RUN rm kafka_2.13-3.8.0.tgz
RUN mv kafka_2.13-3.8.0 kafka
RUN cd kafka
# cd doesn't seem to work
RUN bin/kafka-topics.sh --create --topic mqtt-in --bootstrap-server localhost:9092
RUN bin/kafka-topics.sh --create --topic mqtt-out --bootstrap-server localhost:9092
RUN bin/kafka-topics.sh --create --topic test-in --bootstrap-server localhost:9092
RUN bin/kafka-topics.sh --create --topic test-out --bootstrap-server localhost:9092
CMD ["/etc/kafka/docker/run"]