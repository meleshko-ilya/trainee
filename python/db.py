import psycopg2
from psycopg2 import extensions
import os
from dotenv import load_dotenv

load_dotenv()

class Database:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
        )

    def cursor(self) -> extensions.cursor:
        return self.conn.cursor()

    def commit(self) -> None:
        self.conn.commit()
