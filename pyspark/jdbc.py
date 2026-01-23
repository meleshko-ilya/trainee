import os
from dotenv import load_dotenv

load_dotenv()

def jdbc_options():
    return {
        "url": f"jdbc:postgresql://{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB')}",
        "driver": "org.postgresql.Driver",
        "user": os.getenv("PG_USER"),
        "password": os.getenv("PG_PASSWORD"),
        "fetchsize": "10000"
    }

def load_tables(spark):
    jdbc = jdbc_options()

    tables = [
        "film", "category", "film_category", "actor", "film_actor",
        "inventory", "rental", "payment", "customer", "address", "city"
    ]

    return {
        t: (
            spark.read
            .format("jdbc")
            .options(**jdbc)
            .option("dbtable", t)
            .load()
        )
        for t in tables
    }
