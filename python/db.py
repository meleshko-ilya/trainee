import psycopg2
from psycopg2 import extensions
import os
from dotenv import load_dotenv

load_dotenv()


class Database:
    """
    Manages the PostgreSQL database connection.
    """

    def __init__(self):
        """
        Initialize a database connection using environment variables
        from the .env file.
        """
        self.conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
        )

    def cursor(self) -> extensions.cursor:
        """
        Create and return a database cursor.

        :return: psycopg2 cursor object
        """
        return self.conn.cursor()

    def commit(self) -> None:
        """
        Commit the current transaction to the database.
        """
        self.conn.commit()
