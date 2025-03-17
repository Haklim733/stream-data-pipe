from abc import ABC, abstractmethod
from contextlib import contextmanager
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
import psycopg
from psycopg import sql

logger = logging.getLogger(__name__)


class DbClient:
    def __init__(self, dbname, user, password, host="localhost", port=5432):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None

    def connect(self):
        if self.conn is None or self.conn.closed:
            self.conn = psycopg.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
            )

    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()
            self.conn = None

    def cursor(self):
        return self.conn.cursor()

    @contextmanager
    def cursor(self):
        self.connect()
        cursor = self.conn.cursor()
        try:
            yield cursor
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise e
        finally:
            cursor.close()

    def execute(self, stmt: str):
        with self.cursor() as cur:
            cur.execute(stmt)

    def upsert(self, table_name: str, data: tuple[str | int], primary_key: list[str]):
        """
        Upsert data into the specified table.

        Args:
        - table_name (str): Name of the table to upsert data into.
        - data (dict): Dictionary containing the data to upsert.
        - primary_key (str): Name of the primary key column.
        Returns:
        - None
        """
        columns = sql.SQL(", ").join([sql.Identifier(key) for key in data.keys()])
        placeholders = sql.SQL(", ").join([sql.Placeholder() for _ in data.keys()])

        upsert_query = sql.SQL(
            """
            INSERT INTO {table} ({columns})
            VALUES ({values})
            ON CONFLICT ({primary_key}) DO UPDATE
            SET {updates}
        """
        ).format(
            table=sql.Identifier(table_name),
            columns=columns,
            values=placeholders,
            primary_key=sql.SQL(", ").join(
                [sql.Identifier(key) for key in primary_key]
            ),
            updates=sql.SQL(", ").join(
                sql.SQL("{0} = EXCLUDED.{0}").format(sql.Identifier(key))
                for key in data.keys()
                if key not in primary_key
            ),
        )
        with self.cursor() as cur:
            cur.execute(upsert_query, list(data.values()))


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
    def __init__(self, client: KafkaClient | DbClient) -> None:
        self.id = str(uuid.uuid4())
        self.client = client

    @abstractmethod
    def send(self, message: Message):
        raise NotImplementedError


class Publisher(Generator):

    def send(
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
            data.update({"generator_id": self.id})
            logger.info(f"publishing message - {data}")
            self.client.send_items(topic, data)
            sleep_time = random.uniform(0.1, 1.0)  # Sleep for 100-1000 milliseconds
            time.sleep(sleep_time)


class DBWriter(Generator):
    def __init__(
        self,
        client,
        table_name: str,
        schema: dict[str, str],
        primary_keys: list[str] = [],
        create_table: bool = False,
    ):
        self.id = str(uuid.uuid4())
        self.client = client
        self.table_name: str = table_name
        self.schema: dict[str, str] = schema
        self.primary_keys: str = primary_keys

        if create_table:
            self._create_table()

    def _drop_table(self):
        query = sql.SQL(
            """
            DROP TABLE IF EXISTS {table};
        """
        ).format(
            table=sql.Identifier(self.table_name),
        )
        self.client.execute(query)

    def _create_table(self):
        self._drop_table()

        columns = [
            sql.SQL("{} {}").format(sql.Identifier(key), sql.SQL(value))
            for key, value in self.schema.items()
            if key != "primary_key"
        ]

        primary_key_constraint = sql.SQL(
            "PRIMARY KEY {}".format(self.schema["primary_key"])
        )
        query = sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {table} (
                {columns},
                {primary_key_constraint},
                inserted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
        """
        ).format(
            table=sql.Identifier(self.table_name),
            columns=sql.SQL(", ").join(columns),
            primary_key_constraint=primary_key_constraint,
        )
        self.client.execute(query)

    def send(
        self,
        *,
        max_time: int,
        message: Message,
        message_params: dict[str, typing.Any] = {},
    ):
        start_time = time.time()
        while time.time() - start_time < max_time:
            data = message.generate(**message_params)
            data.update({"id": self.id})
            logger.info(f"writing record- {data}")
            self.client.upsert(self.table_name, data, primary_key=self.primary_keys)
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
