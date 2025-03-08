from abc import ABC, abstractmethod
from dataclasses import dataclass
import json
from psycopg2 import sql
from .db import DatabaseConnection
from .utils import dict_to_table_schema, validate_params


@dataclass
class SourceConfig:
    pass


@dataclass
class KafkaSourceConfig(SourceConfig):
    def __init__(self, bootstrap_servers: str, topic: str, schema: str, group_id=None):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

        self.schema = dict_to_table_schema(json.loads(schema))
        self.group_id = group_id


class Source(ABC):
    def __init__(
        self, database_connection: DatabaseConnection, source_config: KafkaSourceConfig
    ):
        self.database_connection = database_connection
        self.config = source_config

    @abstractmethod
    def create(self):
        raise NotImplementedError

    @abstractmethod
    def drop(self):
        raise NotImplementedError


class KafkaSource(Source):
    def __init__(
        self,
        database_connection: DatabaseConnection,
        config: KafkaSourceConfig,
    ):
        self.db_connect = database_connection
        self.config = config
        self.table_name = validate_params(config.topic)

    def drop(self):
        with self.db_connect.cursor() as cur:
            sql.SQL("""DROP SOURCE IF EXISTS {} CASCADE;""").format(
                sql.Identifier(self.config.topic)
            )

    def create(self):
        with self.db_connect.cursor() as cursor:
            cursor.execute(
                sql.SQL(
                    """
                    CREATE SOURCE IF NOT EXISTS {} (
                        {}
                        )
                    INCLUDE header AS kafka_header,
                    INCLUDE timestamp AS kafka_timestamp
                    WITH (
                        connector = 'kafka',
                        topic='{}',
                        properties.bootstrap.server='{}',
                        scan.startup.mode='earliest'
                    ) FORMAT PLAIN ENCODE JSON;
                    """
                ).format(
                    sql.Identifier(self.table_name),
                    sql.Identifier(self.config.schema),
                    sql.Identifier(self.config.topic),
                    sql.Identifier(self.config.bootstrap_servers),
                )
            )


class SourceFactory:
    def __init__(self):
        self.sources = {"kafka": KafkaSource}
        self.configs = {
            "kafka": KafkaSourceConfig,
        }

    def get_source(self, source_type, **kwargs):
        if source_type in self.sources:
            source_class = self.sources[source_type]
            config_class = self.configs[source_type]
            config_instance = config_class(**kwargs)
            return source_class(DatabaseConnection(), config_instance)
        else:
            raise ValueError(f"Invalid source type: {source_type}")
