"""DAG para fazer ETL para a tabela de eventos"""
# pylint: disable=R0801
import os
from datetime import timedelta

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from datahelper.postgres import (
    check_if_table_exists,
    check_last_uploaded_file,
    send_files_to_postgres,
)

POSTGRES_CONN_ID = "postgres-airflow"
HOOK = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
ENGINE = HOOK.get_sqlalchemy_engine()

UNIQUE_KEY = "oid__id"
EVENTS_TABLE = "events"
DATETIME_COLUMNS = [
    "createdAt",
    "updatedAt",
    "lastSyncTracker",
    "createdAt",
    "createdAt.$date",
    "eventCreatedAt",
    "uploadDate",
]
EVENTS_COLUMNS = [
    "oid__id",
    "trackingCode",
    "status",
    "description",
    "trackerType",
    "from",
    "to",
    "eventCreatedAt",
]
COLUMN_TO_EXPLODE = "array_trackingEvents"

CSV_PATH = "/sources/data/extracted"


def list_files(path):
    """Lista os arquivos em um diretório"""
    files = os.listdir(path)
    for file in files:
        print(file)
    return files


default_args = {
    "owner": "Gustavo",
    "depends_on_past": False,
}

dag = DAG(
    "postgres_etl.events_table",
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
def get_last_uploaded_file():
    """Obtém o último arquivo carregado na tabela, se existir"""
    if check_if_table_exists(EVENTS_TABLE, ENGINE):
        print("Table exists. Getting last uploaded file...")
        last_uploaded_file = check_last_uploaded_file(EVENTS_TABLE, ENGINE)
    else:
        print("Table does not exist. No last uploaded file.")
        last_uploaded_file = None
    return last_uploaded_file


@dag.task
def extract_treat_and_load(files, last_uploaded_file):
    """Extrai os dados dos arquivos, trata e carrega no banco de dados"""
    if last_uploaded_file:
        files = [file for file in files if file > last_uploaded_file]
        print("Starting ETL process...")
    send_files_to_postgres(
        files=files,
        csv_path=CSV_PATH,
        target_table=EVENTS_TABLE,
        engine=ENGINE,
        datetime_columns=DATETIME_COLUMNS,
        int_columns=[],
        unique_key=UNIQUE_KEY,
        normalize_column=COLUMN_TO_EXPLODE,
        filter_columns=EVENTS_COLUMNS,
        n_batch=3,
    )


_files = get_files_list()
_last_uploaded_file = get_last_uploaded_file()
_EXTRACT_TREAT_AND_LOAD = extract_treat_and_load(_files, _last_uploaded_file)

# pylint: disable=pointless-statement
(_files >> _last_uploaded_file >> _EXTRACT_TREAT_AND_LOAD)
