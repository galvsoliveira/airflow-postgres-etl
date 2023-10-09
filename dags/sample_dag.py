from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
import os
import zipfile
import tempfile
from datetime import datetime
import pandas as pd
from sqlalchemy import Table, MetaData, Column, Integer, String, inspect, DateTime, func
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "postgres-airflow"
HOOK = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
ENGINE = HOOK.get_sqlalchemy_engine()

TARGET_TABLE = "tracking"
DATETIME_COLUMNS = ["createdAt", "updatedAt", "lastSyncTracker", "createdAt"]

CSV_PATH = "/sources/data/extracted"


def list_files(path):
    files = os.listdir(path)
    for file in files:
        print(file)
    return files


def delete_row(table_name, unique_key, engine):
    # Reflita a tabela do banco de dados
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)

    # Inicie uma nova sessão
    with engine.begin() as connection:
        # Execute a operação de delete
        delete = table.delete().where(table.c.id == unique_key)
        connection.execute(delete)


def insert_rows_bulk(table_name, data_list, engine):
    # Reflita a tabela do banco de dados
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)

    # Inicie uma nova sessão
    with engine.begin() as connection:
        # Execute a operação de insert para todas as linhas de uma vez
        connection.execute(table.insert(), data_list)


def delete_and_insert(table_name, data_list, unique_key, engine):
    # Reflita a tabela do banco de dados
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)

    # Inicie uma nova sessão
    with engine.begin() as connection:
        # Obtenha as chaves únicas de data_list
        unique_keys = [data[unique_key] for data in data_list]

        # Execute a operação de delete
        delete = table.delete().where(table.c[unique_key].in_(unique_keys))
        connection.execute(delete)

        # Execute a operação de insert para todas as linhas de uma vez
        connection.execute(table.insert(), data_list)


def create_columns_dict(df):
    columns_dict = {}
    for column in df.columns:
        if column in DATETIME_COLUMNS:
            columns_dict[column] = "DateTime"
        else:
            columns_dict[column] = "String"
    return columns_dict


def check_if_table_exists(table_name, engine):
    # Use o inspetor para verificar se a tabela existe
    inspector = inspect(engine)
    return inspector.has_table(table_name)


def create_table_if_not_exists(table_name, columns_dict, engine):
    if not check_if_table_exists(table_name, engine):
        print(f"Creating table {table_name}...")
        # Se a tabela não existir, crie-a
        metadata = MetaData()
        columns = [Column(name, eval(type)) for name, type in columns_dict.items()]
        table = Table(table_name, metadata, *columns)
        table.create(engine)


def check_last_uploaded_file(table_name, column="file_name", engine=ENGINE):
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)
    # Inicie uma nova sessão
    with engine.begin() as connection:
        # encontre max (column) from table_name usando a função func.max
        result = connection.execute(func.max(table.columns[column]))
        # Recupere o resultado da operação
        last_uploaded_file = result.fetchone()
        return last_uploaded_file[0]


def process_data(df):
    for col in DATETIME_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], unit="s")
    df["upload_date"] = datetime.now()
    return df


def send_files_to_postgres(files):
    file_counter = 0
    df = pd.DataFrame()
    for file in files:
        file_counter += 1
        print(f"Reading file {file_counter} of {len(files)}")
        temp_df = pd.read_csv(f"{CSV_PATH}/{file}")
        temp_df["file_name"] = file
        df = pd.concat([df, temp_df])
        if file_counter % 5 == 0 or file_counter == len(files):
            df = process_data(df)
            create_table_if_not_exists(TARGET_TABLE, create_columns_dict(df), ENGINE)
            print("Sending data to postgres...")
            delete_and_insert(TARGET_TABLE, df.to_dict("records"), "oid__id", ENGINE)
            df = pd.DataFrame()
            print("Data sent successfully.")
    print("All files sent successfully.")


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
    files = list_files(CSV_PATH)
    files.sort()
    return files


@dag.task
def extract_treat_and_load(files):
    if check_if_table_exists(TARGET_TABLE, ENGINE):
        print("Table tracking exists. Getting last uploaded file...")
        last_uploaded_file = check_last_uploaded_file(TARGET_TABLE)
        files = [file for file in files if file > last_uploaded_file]
    send_files_to_postgres(files)


files_list = get_files_list()
_extract_treat_and_load = extract_treat_and_load(files_list)

files_list >> _extract_treat_and_load
