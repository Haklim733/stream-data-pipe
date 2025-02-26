import argparse
import logging
import sys
import logging
import os

import nltk
nltk.download('punkt_tab')
from nltk.tokenize import sent_tokenize, word_tokenize

from pyflink.common import WatermarkStrategy, Types, Configuration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee 
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer
from pyflink.datastream.functions import MapFunction
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy, BucketAssigner
from pyflink.common import Duration, Encoder
from pyflink.datastream import TimeCharacteristic
from pyflink.datastream.formats.json import JsonRowDeserializationSchema

logger = logging.getLogger(__name__)

config = Configuration()
config.set_string("s3.endpoint", "http://minio:9000")
config.set_string("s3.path.style.access", "true")
config.set_string("s3.access.key", "admin")
config.set_string("s3.secret.key", "password")
# config.set_string("state.backend", "filesystem")
# Configure TaskManager connection
# config.set_string("jobmanager.address", "localhost")
# config.set_integer("jobmanager.port", 8081)
# config.set_integer("taskmanager.numberOfTaskSlots", 1)
# config.set_integer("parallelism.default", 1)

env = StreamExecutionEnvironment.get_execution_environment(config)
env.set_parallelism(1)
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

env.add_jars(
    "file:////opt/flink/opt/flink-python-1.20.1.jar",
    "file:///opt/flink/lib/flink-connector-kafka-3.3.0-1.20.jar",
    "file:///opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.20.jar",
    )
# env.add_classpaths(
#     "file:///opt/flink/opt/flink-python-1.20.1.jar",
#     "file:///opt/flink/usrlib/flink-connector-kafka-3.3.0-1.20.jar",
#     "file:///opt/flink/usrlib/flink-sql-connector-kafka-3.3.0-1.20.jar",
#     )

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

class SimpleBucketAssigner(BucketAssigner):
    def get_bucket_id(self, element, context):
        return "flink-output"

def sentiment_analysis_job(topic_name: str, bootstrap_servers: str, group_id: str ):
    """
    Sets up a PyFlink job that consumes social media posts from a Kafka   topic, performs sentiment analysis, and outputs results to another Kafka topic.
    """
    # Declare the execution environment.

    # Define a source to read from Kafka.
    source = KafkaSource.builder() \
        .set_bootstrap_servers(bootstrap_servers) \
        .set_topics(topic_name) \
        .set_group_id(group_id) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
     
    ds = env.from_source(source, WatermarkStrategy.for_monotonous_timestamps(), "Kafka Source")
    stream = ds.map(SentimentAnalysis(), output_type=Types.STRING())

    sink = KafkaSink.builder() \
            .set_bootstrap_servers(bootstrap_servers) \
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                    .set_topic("sentiment_analysis_results")
                    .set_value_serialization_schema(SimpleStringSchema())
                    .build()
            ) \
            .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
            .build()
    
    env.enable_checkpointing(30*1000)

    output_path = "s3a://warehouse/output/"

    file_sink = FileSink \
        .for_row_format(output_path, Encoder.simple_string_encoder()) \
        .with_bucket_assigner(BucketAssigner.date_time_bucket_assigner()) \
        .with_output_file_config(OutputFileConfig.builder()
                                .with_part_prefix('data')
                                .with_part_suffix('.txt')
                                .build()) \
        .with_rolling_policy(RollingPolicy.default_rolling_policy(
            inactivity_interval=30 * 1000)) \
        .build()
    
    stream.sink_to(file_sink)
    # # Direct the processed data to the sink.
    stream.sink_to(sink)
    # Execute the job.
    env.execute()
 
if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--topic',
        dest='topic',
        required=True,
        help='specify kafka topic to consume') 
    parser.add_argument(
        '--bootstrap-servers',
        dest='bootstrap_servers',
        required=True,
        help='specify network and brokers')
    parser.add_argument(
        '--group-id',
        dest='group_id',
        required=True,
        help='specify group id')
    
    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)

    sentiment_analysis_job(topic_name=known_args.topic, bootstrap_servers=known_args.bootstrap_servers, group_id=known_args.group_id)