import argparse
import logging
import sys
import logging
import os

from nltk.tokenize import sent_tokenize, word_tokenize
from pyflink.common import WatermarkStrategy, Types, Configuration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee 
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer
from pyflink.datastream.functions import MapFunction
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy, BucketAssigner
from pyflink.common import Encoder
from pyflink.common.restart_strategy import RestartStrategies


logger = logging.getLogger(__name__)

RUNTIME_ENV = os.getenv("RUNTIME_ENV", "local")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:19092")

config = Configuration()
config.set_integer("taskmanager.numberOfTaskSlots", 1)
config.set_string('state.backend', 'filesystem')
config.set_string('state.backend.type', 'hashmap')
# config.set_string("state.backend.fs.checkpointdir", "s3p://warehouse/checkpoints")
# config.set_string("state.backend.fs.checkpointstorage", "filesystem")

jars = ["file:///opt/flink/opt/flink-python-1.20.1.jar", "file:///opt/flink/lib/flink-connector-kafka-3.3.0-1.20.jar", "file:///opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.20.jar"]
config.set_integer("parallelism.default", 1)
config.set_string("pipeline.jars", ";".join(jars))
# see: https://www.mail-archive.com/issues@flink.apache.org/msg765038.html

env = StreamExecutionEnvironment.get_execution_environment(config)
env.set_parallelism(1)
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
env.set_restart_strategy(RestartStrategies.no_restart())
# env.add_python_archive("/opt/flink/punkt_tab.zip")

def sentiment_analysis_job(source_topic_name: str, group_id: str ):
    """
    Sets up a PyFlink job that consumes social media posts from a Kafka   topic, performs sentiment analysis, and outputs results to another Kafka topic.
    """

    class SentimentAnalysis(MapFunction):
        def tokenize_text(self, line):
            sentences = sent_tokenize(line)
            words = [word_tokenize(sentence) for sentence in sentences]
            return words
        
        def map(self, value):
            # Example of simple sentiment analysis logic
            positive_keywords = ['happy', 'joyful', 'love']
            negative_keywords = ['sad', 'angry', 'hate']
            # Assuming 'value' is a text of the social media post
            post_text = self.tokenize_text(value)
            sentiment = "Neutral"  # Default sentiment
            for keyword in positive_keywords:
                if keyword in post_text:
                    sentiment = "Positive"
                    break  # Stop searching if any positive keyword is found
            for keyword in negative_keywords:
                if keyword in post_text:
                    sentiment = "Negative"
                    break  # Stop searching if any negative keyword is found
            return f"Post: {value} | Sentiment: {sentiment}"


    # Define a source to read from Kafka.
    source = KafkaSource.builder() \
        .set_bootstrap_servers(BOOTSTRAP_SERVERS) \
        .set_topics(source_topic_name) \
        .set_group_id(group_id) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
     
    ds = env.from_source(source, WatermarkStrategy.for_monotonous_timestamps(), "Kafka Source")
    stream = ds.map(SentimentAnalysis(), output_type=Types.STRING())

    dest_topic = 'sentiment_analysis'
    kafka_topic_sink = KafkaSink.builder() \
            .set_bootstrap_servers(BOOTSTRAP_SERVERS) \
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                    .set_topic(dest_topic)
                    .set_value_serialization_schema(SimpleStringSchema())
                    .build()
            ) \
            .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
            .build()
    
    stream.sink_to(kafka_topic_sink)

    # Create a BucketAssigner (e.g., based on date)
    bucket_assigner = BucketAssigner.date_time_bucket_assigner()

    
    output_path = "s3a://warehouse/output/"
    # https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/filesystems/s3/

    # env.enable_checkpointing(30*1000)
    # file_sink = FileSink \
    #     .for_row_format(output_path, Encoder.simple_string_encoder()) \
    #     .with_bucket_assigner(bucket_assigner) \
    #     .with_output_file_config(OutputFileConfig.builder()
    #                             .with_part_prefix('sentiment_')
    #                             .with_part_suffix('.txt')
    #                             .build()) \
    #     .with_rolling_policy(RollingPolicy.default_rolling_policy(
    #         inactivity_interval=30 * 1000)) \
    #     .build()
    
    # stream.sink_to(file_sink)
    env.execute("sentiment analysis job")
 
if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--source-topic',
        dest='source_topic',
        required=True,
        help='specify kafka topic to consume') 
    parser.add_argument(
        '--group-id',
        dest='group_id',
        required=True,
        help='specify group id')
    
    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)

    sentiment_analysis_job(source_topic_name=known_args.source_topic, group_id=known_args.group_id)