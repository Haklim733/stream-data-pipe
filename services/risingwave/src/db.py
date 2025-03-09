import psycopg2
import os

HOST = os.getenv("DB_HOST", "localhost")


class DatabaseConnection:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseConnection, cls).__new__(cls)
            cls._instance.conn = psycopg2.connect(
                host=HOST, port=4566, user="root", dbname="dev"
            )
            cls._instance.conn.autocommit = True
        return cls._instance

    def cursor(self):
        return self.conn.cursor()

    def execute(self, stmt: psycopg2.sql.SQL):
        with self.conn.cursor() as cur:
            print(stmt.as_string(cur))
            cur.execute(stmt)

    def close(self):
        return self.conn.close()
