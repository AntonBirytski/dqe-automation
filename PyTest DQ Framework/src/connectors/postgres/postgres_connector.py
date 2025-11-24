import psycopg2
import pandas as pd


class PostgresConnectorContextManager:
    """
    PostgreSQL Database Context Manager.
    """
    def __init__(self, db_host: str, db_name: str, db_port: int, db_user: str, db_password: str):
        self.db_host = db_host
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_port = db_port
        self.conn = None
        self.cursor = None

    def __enter__(self):
        self.conn = psycopg2.connect(
            host=self.db_host,
            dbname=self.db_name,
            user=self.db_user,
            password=self.db_password,
            port=self.db_port
        )
        self.cursor = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def get_data_sql(self, sql):
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()
        columns = [desc[0] for desc in self.cursor.description]
        return pd.DataFrame(rows, columns=columns)
