from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
import os
from datetime import datetime
import pandas as pd
from sqlalchemy import Table, MetaData, Column, Integer, String, inspect, DateTime, func
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datahelper.postgres import (
    send_files_to_postgres,
    check_if_table_exists,
    check_last_uploaded_file,
)

POSTGRES_CONN_ID = "postgres-airflow"
HOOK = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
ENGINE = HOOK.get_sqlalchemy_engine()

TARGET_TABLE = "tracking"
UNIQUE_KEY = "oid__id"
DATETIME_COLUMNS = ["createdAt", "updatedAt", "lastSyncTracker", "createdAt"]

CSV_PATH = "/sources/data/extracted"


def list_files(path):
    """Lista os arquivos em um diretório"""
    files = os.listdir(path)
    for file in files:
        print(file)
    return files


# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["your-email@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Instantiate the DAG
dag = DAG(
    "postgres_etl",
    default_args=default_args,
    description="A simple DAG to list files in the data directory",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
)


@dag.task
def get_files_list():
    """Lista os arquivos no diretório CSV_PATH"""
    files = list_files(CSV_PATH)
    files.sort()
    return files


@dag.task
def extract_treat_and_load(files):
    """Extrai os dados dos arquivos, trata e carrega no banco de dados"""
    if check_if_table_exists(TARGET_TABLE, ENGINE):
        print("Table tracking exists. Getting last uploaded file...")
        last_uploaded_file = check_last_uploaded_file(TARGET_TABLE, ENGINE)
        files = [file for file in files if file > last_uploaded_file]
    send_files_to_postgres(
        files, CSV_PATH, TARGET_TABLE, ENGINE, DATETIME_COLUMNS, [], UNIQUE_KEY
    )


files_list = get_files_list()
_extract_treat_and_load = extract_treat_and_load(files_list)

files_list >> _extract_treat_and_load
