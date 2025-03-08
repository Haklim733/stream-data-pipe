from abc import ABC, abstractmethod
from .db import DatabaseConnection
from .utils import dict_to_table_schema


class Sink(ABC):
    def __init__(self, database_connection):
        self.database_connection = database_connection

    @abstractmethod
    def create(self, table_name: str):
        raise NotImplementedError


class IcebergSink(Sink):
    def create(self, table_name: str, database: str):
        f"""
            CREATE SINK IF NOT EXISTS iceberg
            WITH (
                connector = 'iceberg',
                type = 'append-only',
                warehouse.path = 's3://warehouse/wh',
                s3.endpoint = 'http://minio:9000',
                s3.region = 'us-east-1',
                s3.access.key = 'admin',
                s3.secret.key = 'password',
                s3.path.style.access = 'true',
                create_table_if_not_exists = 'true',
                catalog.type = 'rest',
                catalog.uri = 'http://iceberg-rest:8181/api/catalog',
                database.name = '{database}',
                table.name = '{table_name}',
                catalog.credential='admin:password',
                catalog.scope='PRINCIPAL_ROLE:ALL'
            ) """
