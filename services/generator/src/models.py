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
            self.conn.autocommit = False

    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()
            self.conn = None

    def cursor(self):
        return self.conn.cursor()

    @contextmanager
    def cursor(self, force_rollback=False):
        self.connect()
        cursor = self.conn.cursor()
        try:
            yield cursor
            if force_rollback:
                logger.info("TRANSACTION - ROLLBACK (forced)")
                self.conn.rollback()
            else:
                logger.info("TRANSACTION - COMMIT")
                self.conn.commit()
        except Exception as e:
            logger.info("TRANSACTION - ROLLBACK (exception)")
            self.conn.rollback()
            raise e
        finally:
            cursor.close()

    def execute(self, stmt: str, values: list[str] = None, rollback: bool = False):
        with self.cursor(force_rollback=rollback) as cur:
            if values:
                cur.execute(stmt, values)
            else:
                cur.execute(stmt)

    def upsert(
        self,
        table_name: str,
        data: tuple[str | int],
        primary_key: list[str],
        random_rollback: float = 0,
    ):
        """
        Upsert data into the specified table.

        Args:
        - table_name (str): Name of the table to upsert data into.
        - data (dict): Dictionary containing the data to upsert.
        - primary_key (str): Name of the primary key column.
        - random_rollback (float): Probability of rolling back the transaction. Between 0 and 1 (inclusive)
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
            values=sql.SQL(", ").join(
                (
                    sql.SQL("to_timestamp({} / 1000)").format(sql.Placeholder())
                    if key == "created_at"
                    else sql.Placeholder()
                )
                for key in data.keys()
            ),
            primary_key=sql.SQL(", ").join(
                [sql.Identifier(key) for key in primary_key]
            ),
            updates=sql.SQL(", ").join(
                sql.SQL("{0} = EXCLUDED.{0}").format(sql.Identifier(key))
                for key in data.keys()
                if key not in primary_key
            ),
        )

        if random.random() < random_rollback:
            self.execute(upsert_query, list(data.values()), rollback=True)
        else:
            self.execute(upsert_query, list(data.values()), rollback=False)


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
        event_id: str = uuid.uuid4().hex
        return {"message": message, "created_at": created_at, "event_id": event_id}


class Generator(ABC):
    def __init__(self, client: KafkaClient | DbClient) -> None:
        self.origin_id = uuid.uuid4().hex
        self.client = client

    @abstractmethod
    def send(self, message: Message):
        raise NotImplementedError


class Publisher(Generator):
    def __init__(self, client: KafkaClient):
        super().__init__(client)
        self.client = client

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
            data.update({"origin_id": self.origin_id})
            logger.info(f"publishing message - {data}")
            self.client.send_items(topic, data)
            sleep_time = random.uniform(0.1, 1.0)  # Sleep for 100-1000 milliseconds
            time.sleep(sleep_time)


class DBWriter(Generator):
    def __init__(
        self,
        client: DbClient,
        table_name: str,
        primary_keys: list[str],
        schema: dict[str, str] = {},
        create_table: bool = False,
    ):
        super().__init__(client)
        self.client = client
        self.table_name: str = table_name
        self.schema: dict[str, str] = schema
        self.primary_keys: list[str] = primary_keys

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
        if not self.schema:
            raise Exception("Schema is empty")

        columns = [
            sql.SQL("{} {}").format(sql.Identifier(key), sql.SQL(value))
            for key, value in self.schema.items()
            if key != "primary_key"
        ]
        j
        primary_key_constraint = sql.SQL("PRIMARY KEY ({})").format(
            sql.SQL(", ").join([sql.Identifier(key) for key in self.primary_keys])
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
        random_rollback: float = 0,
    ):
        start_time = time.time()
        while time.time() - start_time < max_time:
            data = message.generate(**message_params)
            data.update({"origin_id": self.origin_id})
            logger.info(f"writing record- {data}")
            self.client.upsert(
                self.table_name,
                data,
                primary_key=["event_id"],
                random_rollback=random_rollback,
            )
            # sleep_time = random.uniform(0.1, 1.0)  # Sleep for 100-1000 milliseconds
            sleep_time = 1
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
