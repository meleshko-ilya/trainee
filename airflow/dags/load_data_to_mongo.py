from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.datasets import Dataset
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime
import pandas as pd
import os

DATA_FILE = "/opt/airflow/data/tiktok_google_play_reviews.csv"
processed_dataset = Dataset(DATA_FILE)


def create_collection():
    hook = MongoHook(mongo_conn_id="mongo_local")
    client = hook.get_conn()
    db = client["airflow"]

    if "reviews" not in db.list_collection_names():
        db.create_collection("reviews")
        print("Collection 'reviews' created")
    else:
        print("Collection already exists")


def load_to_mongo():
    if not os.path.exists(DATA_FILE):
        print(f"File {DATA_FILE} not found, skipping load.")
        return

    hook = MongoHook(mongo_conn_id="mongo_local")
    client = hook.get_conn()
    db = client["airflow"]
    collection = db["reviews"]

    df = pd.read_csv(DATA_FILE)

    df["created_at"] = pd.to_datetime(df["created_at"])
    df["content"] = df["content"].astype(str)
    records = df.to_dict("records")

    collection.delete_many({})
    if records:
        collection.insert_many(records)
        print(f"Inserted {len(records)} records into MongoDB.")
    else:
        print("No records to insert.")


with DAG(
        dag_id="load_data_to_mongo",
        start_date=datetime(2026, 1, 1),
        schedule=[processed_dataset],
        catchup=False,
        tags={"mongo", "dataset"},
) as dag:
    t1 = PythonOperator(
        task_id="create_collection",
        python_callable=create_collection,
    )

    t2 = PythonOperator(
        task_id="load_data_to_mongo",
        python_callable=load_to_mongo,
    )

    t1 >> t2
