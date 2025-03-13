from abc import ABC, abstractmethod
import json
import logging
import random
import time
import typing
import uuid


from faker import Faker
from faker.providers import lorem
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import (
    KafkaError,
    UnknownTopicOrPartitionError,
    TopicAlreadyExistsError,
)

logger = logging.getLogger(__name__)


class KafkaClient:
    def __init__(self, bootstrap_servers: str) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.admin = self.create_admin()
        self.producer = self.create_producer()

    def create_admin(self):
        return KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)

    def create_producer(self):
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            key_serializer=lambda v: json.dumps(v).encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
            retry_backoff_ms=100,
            request_timeout_ms=3000,  # 30 sec
        )

    def delete_topics(self, topic_names: typing.List[str]):
        existing_topics = self.admin.describe_topics()
        for name in topic_names:
            if name in existing_topics:
                try:
                    self.admin_client.delete_topics([name], timeout_ms=1000)
                except UnknownTopicOrPartitionError:
                    pass
                except Exception as err:
                    raise RuntimeError(f"fails to delete topic - {name}") from err

    def create_topics(
        self, topics: typing.List[str], to_recreate: bool = True, **kwargs
    ):
        if to_recreate:
            self.delete_topics([t for t in topics])
        for topic in topics:
            try:
                resp = self.admin.create_topics([NewTopic(name=topic, **kwargs)])
                name, error_code, error_message = resp.topic_errors[0]
                logger.info(
                    f"topic created, name - {name}, error code - {error_code}, error message - {error_message}"
                )
            except TopicAlreadyExistsError:
                pass
            except KafkaError as err:
                raise RuntimeError(
                    f"fails to create topics - {', '.join([t for t in topics])}"
                ) from err
        logging.info(f"topics created successfully - {', '.join([t for t in topics])}")

    def send_items(self, topic_name: str, item: dict[str, typing.Any]):
        try:
            self.producer.send(
                topic=topic_name,
                value=item,
            )
            logging.info(f"record sent, topic - {topic_name}")
        except Exception as err:
            raise RuntimeError("fails to send a message") from err


class Message(ABC):
    def __init__(self, generate_method: typing.Callable) -> dict[str, typing.Any]:
        self.generate_method = generate_method

    @abstractmethod
    def generate(self):
        raise NotImplementedError


class TextMessage(Message):

    def generate(self, **kwargs):
        created_at = int(time.time() * 1000)
        message = self.generate_method(**kwargs)
        return {"message": message, "created_at": created_at}


class Generator(ABC):
    def __init__(self, client: KafkaClient) -> None:
        self.id = str(uuid.uuid4())
        self.client = client

    @abstractmethod
    def publish(self, message: Message):
        raise NotImplementedError


class Publisher(Generator):

    def publish(
        self,
        *,
        topic: str,
        max_time: int,
        message: Message,
        message_params: dict[str, typing.Any] = {},
    ):
        start_time = time.time()
        while time.time() - start_time < max_time:
            data = message.generate(**message_params)
            data.update({"id": self.id})
            logger.info(f"publishing message - {data}")
            self.client.send_items(topic, data)
            sleep_time = random.uniform(0.1, 1.0)  # Sleep for 100-1000 milliseconds
            time.sleep(sleep_time)


if __name__ == "__main__":
    from unittest.mock import MagicMock

    client = MagicMock()
    generator = Publisher(client=client)
    fake = Faker()
    fake.add_provider(lorem)
    generator.publish(
        topic="test",
        message=TextMessage(fake.sentence),
        message_params={"nb_words": 15},
        max_time=2,
    )
