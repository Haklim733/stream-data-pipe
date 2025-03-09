from abc import ABC, abstractmethod

from psycopg2 import sql
from src.db import DatabaseConnection
from src.utils import dict_to_table_schema, validate_params


class SinkSettings:

    _aliases = {}

    def __init__(self, **kwargs):
        # Set default values
        for key, value in self.__class__.__dict__.items():
            if not key.startswith("__") and not callable(value):
                python_key = key.replace(".", "_")
                setattr(self, python_key, value)
        # Override with provided keyword arguments
        for key, value in kwargs.items():
            python_key = key.replace(".", "_")
            if hasattr(self, python_key):
                setattr(self, python_key, value)
        print(f"Initialized {self.__class__.__name__} with attributes: {self.__dict__}")

    def sql(self) -> list:
        params = []
        for key, value in self.__dict__.items():
            if not key.startswith("__") and "_aliases" not in key:
                sql_key = key.replace("_", ".")
                if key in self._aliases:
                    sql_key = self._aliases[key]
                params.append(
                    sql.SQL("{} = {}").format(
                        sql.Identifier(sql_key), sql.Literal(value)
                    )
                )
        return params


class IcebergSinkConfig(SinkSettings):
    _aliases = {"create_table_if_not_exists": "create_table_if_not_exists"}

    def __init__(self, **kwargs):
        self.connector: str = "iceberg"
        self.type: str = "append-only"
        self.warehouse_path: str = "s3://warehouse/wh"
        self.s3_endpoint: str = "http://minio:9000"
        self.s3_region: str = "us-east-1"
        self.s3_access_key: str = "admin"
        self.s3_secret_key: str = "password"
        self.s3_path_style_access: bool = True
        self.create_table_if_not_exists: bool = True
        super().__init__(**kwargs)


class IcebergCatalogConfig(SinkSettings):
    def __init__(self, **kwargs):
        self.catalog_name: str = "default"
        self.catalog_type: str = "rest"
        self.catalog_uri: str = "http://iceberg-rest:8181"
        self.catalog_credential: str = "admin:password"
        super().__init__(**kwargs)


class IcebergTableConfig(SinkSettings):
    def __init__(self, **kwargs):
        self.database_name: str = "default"
        self.table_name: str = "default"
        super().__init__(**kwargs)


class IcebergSinkSettings(SinkSettings):
    def __init__(self, **kwargs):
        self.config = IcebergSinkConfig(**kwargs)
        self.catalog = IcebergCatalogConfig(**kwargs)
        self.table = IcebergTableConfig(**kwargs)

    def sql(self) -> list:
        params = []
        params.extend(self.config.sql())
        params.extend(self.catalog.sql())
        params.extend(self.table.sql())
        return params


class S3SinkSettings(SinkSettings):
    _aliases = {
        "s3_region_name": "s3.region_name",
        "s3_bucket_name": "s3.bucket_name",
        "s3_credentials_access": "s3.credentials.access",
        "s3_credentials_secret": "s3.credentials.secret",
        "s3_endpoint_url": "s3.endpoint_url",
    }

    def __init__(self, **kwargs):
        self.s3_path: str = "rw/sink"
        self.s3_region_name: str = "us-east-1"
        self.s3_bucket_name: str = "warehouse"
        self.s3_credentials_access: str = "admin"
        self.s3_credentials_secret: str = "password"
        self.s3_endpoint_url: str = "http://minio:9000"
        self.type = "append-only"
        self.connector = "s3"
        super().__init__(**kwargs)


class EncodeSettings(SinkSettings):

    def __init__(self, **kwargs):
        self.format: str = "PLAIN"
        self.encode: str = "JSON"
        super().__init__(**kwargs)


class Sink(ABC):
    def __init__(self, database_connection):
        self.database_connection = database_connection

    @abstractmethod
    def create(self, table_name: str):
        raise NotImplementedError


class S3Sink(Sink):

    def __init__(self, database_connection: DatabaseConnection, **kwargs):
        self.db_connect = database_connection
        self.s3_settings = S3SinkSettings(**kwargs)
        self.encode_settings = EncodeSettings(**kwargs)
        self.source_name: str = validate_params(kwargs["source_name"])
        self.sink_name: str = validate_params(kwargs["sink_name"])
        self.replace: bool = False

    def drop(self):
        self.db_connect.execute(
            sql.SQL("""DROP SINK IF EXISTS {} CASCADE;""").format(
                sql.Identifier(self.sink_name)
            )
        )

    def construct_with_clause(self, options):
        """
        Constructs the WITH clause with unquoted keys and properly quoted values.
        """
        clauses = []
        for item in options.split(","):
            key, value = item.split("=")
            escaped_value = key.strip().replace("'", "")
            clauses.append(f"{escaped_value} = {value.strip()}")
        return ", ".join(clauses)

    def create(self):
        stmt = sql.SQL(
            """
            CREATE SINK IF NOT EXISTS {}
            FROM {}
            WITH (
                {}
            ) FORMAT {} ENCODE {} 
            """
        ).format(
            sql.Identifier(self.sink_name),
            sql.Identifier(self.source_name),
            sql.SQL(", ").join(self.s3_settings.sql()),
            sql.Identifier(self.encode_settings.format),
            sql.Identifier(self.encode_settings.encode),
        )
        self.db_connect.execute(stmt)


class IcebergSink(Sink):

    def __init__(self, database_connection: DatabaseConnection, **kwargs):
        self.db_connect = database_connection
        self.settings = IcebergSinkSettings(**kwargs)
        self.source_name: str = validate_params(kwargs["source_name"])

    def drop(self):
        self.db_connect.execute(
            sql.SQL("""DROP SINK IF EXISTS {} CASCADE;""").format(
                sql.Identifier(self.config.topic)
            )
        )

    def create(self):
        with self.db_connect.cursor() as cur:
            # stmt = sql.SQL(
            #     """
            #     CREATE SINK IF NOT EXISTS iceberg
            #     FROM {}
            #     WITH (
            #         {}
            #     )
            #     """
            # ).format(
            #     sql.Identifier(self.source_name),
            #     # params
            #     sql.SQL(", ").join(self.settings.sql()),
            # )
            # print(stmt.as_string(cur))
            stmt = """
                CREATE SINK IF NOT EXISTS iceberg
                FROM book_emotions_view
                WITH (
                    connector = 'iceberg', 
                    type = 'append-only', 
                    warehouse.path = 's3://warehouse', 
                    s3.endpoint = 'http://minio:9000', 
                    s3.region = 'us-east-1', 
                    s3.access.key = 'user', 
                    s3.secret.key = 'User@12345', 
                    s3.path.style.access = true, 
                    create_table_if_not_exists = true, 
                    catalog.name = 'default', 
                    catalog.type = 'rest', 
                    catalog.uri = 'http://iceberg-rest:8181', 
                    database.name = 'default', 
                    table.name = 'book_emotions',
                ); 
            """
            stmt = """
                CREATE SOURCE IF NOT EXISTS iceberg
                WITH (
                    connector = 'iceberg', 
                    warehouse.path = 's3://warehouse/wh', 
                    s3.endpoint = 'http://minio:9000', 
                    s3.region = 'us-east-1', 
                    s3.access.key = 'user', 
                    s3.secret.key = 'User@12345', 
                    s3.path.style.access = true, 
                    catalog.type = 'rest', 
                    catalog.uri = 'http://iceberg-rest:8181', 
                    catalog.name = 'default',
                    database.name = 'spark', 
                    table.name = 'text'
                ); 

            """

            cur.execute(stmt)


class SinkFactory:
    def __init__(self):
        self.sinks = {"iceberg": IcebergSink}
        self.configs = {
            "iceberg": IcebergSinkConfig,
        }

    def get_sink(self, sink_type: str, **kwargs):
        if sink_type in self.sinks:
            sink_class = self.sinks[sink_type]
            config_class = self.configs[sink_type]
            config_instance = config_class(**kwargs)
            return sink_class(DatabaseConnection(), config_instance)
        else:
            raise ValueError(f"Invalid sink type: {sink_type}")


if __name__ == "__main__":
    # s3_sink = S3Sink(
    #     DatabaseConnection(),
    #     source_name="book_emotions_view",
    #     sink_name="rw_s3_sink",
    #     s3_credentials_access="user",
    #     s3_credentials_secret="User@12345",
    # )
    # s3_sink.drop()
    # s3_sink.create()
    print(
        IcebergSink(
            DatabaseConnection(),
            source_name="book_emotions_view",
            table_name="book_emotions",
            database_name="default",
            catalog_name="default",
            catalog_type="rest",
        ).create()
    )
