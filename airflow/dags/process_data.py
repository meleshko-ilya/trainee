# dag1_process_csv.py
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import TaskGroup
from airflow.datasets import Dataset
from datetime import datetime
import pandas as pd
import re
import os

DATA_FILE = "/opt/airflow/data/tiktok_google_play_reviews.csv"
DATASET_FILE = Dataset(DATA_FILE)

def check_file_empty(**kwargs):
    df = pd.read_csv(DATA_FILE)
    if df.empty:
        return "file_empty"
    return "process_data.replace_nulls"

def replace_nulls():
    df = pd.read_csv(DATA_FILE)
    df.fillna("-", inplace=True)
    df.to_csv(DATA_FILE, index=False)

def sort_data():
    df = pd.read_csv(DATA_FILE)
    df['created_at'] = pd.to_datetime(df['at'])
    df.sort_values("created_at", inplace=True)
    df.to_csv(DATA_FILE, index=False)

def clean_content():
    df = pd.read_csv(DATA_FILE)
    df['content'] = df['content'].apply(lambda x: re.sub(r"[^a-zA-Z0-9\s\.,!?]", "", str(x)))
    df.to_csv(DATA_FILE, index=False)

with DAG(
    dag_id="process_data",
    start_date=datetime(2026, 2, 16),
    schedule=None,
    catchup=False,
    tags={"csv"},
) as dag:

    wait_file = FileSensor(
        task_id="wait_file",
        filepath=DATA_FILE,
        poke_interval=10,
        timeout=600,
    )

    check_empty = BranchPythonOperator(
        task_id="check_empty",
        python_callable=check_file_empty,
    )

    file_empty = BashOperator(
        task_id="file_empty",
        bash_command='echo "File is empty!"'
    )

    with TaskGroup("process_data") as process_group:
        t1 = PythonOperator(task_id="replace_nulls", python_callable=replace_nulls)
        t2 = PythonOperator(task_id="sort_data", python_callable=sort_data)
        t3 = PythonOperator(
            task_id="clean_content",
            python_callable=clean_content,
            outlets=[DATASET_FILE]
        )
        t1 >> t2 >> t3

    wait_file >> check_empty
    check_empty >> file_empty
    check_empty >> process_group
