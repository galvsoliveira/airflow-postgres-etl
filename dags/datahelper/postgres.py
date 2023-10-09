""" Funções para projetos postgres """

from datetime import datetime
import pandas as pd
from sqlalchemy import Table, MetaData, Column, Integer, String, inspect, DateTime, func


def delete_and_insert(table_name, data_list, unique_key, engine):
    """Deleta e insere linhas em uma tabela

    Args:
        table_name: Nome da tabela
        data_list: Lista de dicionários com os dados a serem inseridos
        unique_key: Nome da chave única
        engine: Conexão com o banco de dados, obtida com PostgresHook
    """
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)

    with engine.begin() as connection:
        unique_keys = [data[unique_key] for data in data_list]

        # Deleta as linhas na tabela que estão nos dados a serem inseridos
        delete = table.delete().where(table.c[unique_key].in_(unique_keys))
        connection.execute(delete)

        # Executa a operação de insert para todas as linhas de uma vez
        connection.execute(table.insert(), data_list)


def create_columns_dict(df, datetime_columns, int_columns):
    """Cria um dict com os nomes das colunas e os tipos de dados

    Args:
        df: DataFrame com os dados
        datetime_columns: Colunas que devem ser do tipo DateTime
        int_columns: Colunas que devem ser do tipo Integer

    Returns:
        columns_dict: Dicionário com os nomes das colunas e os tipos de dados
    """
    columns_dict = {}
    for column in df.columns:
        if column in datetime_columns:
            columns_dict[column] = "DateTime"
        elif column in int_columns:
            columns_dict[column] = "Integer"
        else:
            columns_dict[column] = "String"
    return columns_dict


def check_if_table_exists(table_name, engine):
    """Verifica se uma tabela existe no banco de dados

    Args:
        table_name: Nome da tabela
        engine: Conexão com o banco de dados, obtida com PostgresHook

    Returns:
        True se a tabela existir, False caso contrário
    """
    inspector = inspect(engine)
    return inspector.has_table(table_name)


def create_table_if_not_exists(table_name, columns_dict, engine):
    """Cria uma tabela no banco de dados se ela não existir

    Args:
        table_name: Nome da tabela
        columns_dict: Dicionário com os nomes das colunas e os tipos de dados
        engine: Conexão com o banco de dados, obtida com PostgresHook
    """
    if not check_if_table_exists(table_name, engine):
        print(f"Creating table {table_name}...")
        metadata = MetaData()
        columns = [Column(name, eval(type)) for name, type in columns_dict.items()]
        table = Table(table_name, metadata, *columns)
        table.create(engine)


def check_last_uploaded_file(table_name, engine, column="file_name"):
    """Verifica o último arquivo enviado para o banco de dados

    Args:
        table_name: Nome da tabela
        engine: Conexão com o banco de dados, obtida com PostgresHook
        column (str, optional): Nome da coluna que contém o nome do arquivo.

    Returns:
        Nome do último arquivo enviado para o banco de dados.
    """
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)
    with engine.begin() as connection:
        result = connection.execute(func.max(table.columns[column]))
        last_uploaded_file = result.fetchone()
        return last_uploaded_file[0]


def process_data(df, datetime_columns):
    """Processa os dados antes de enviá-los para o banco de dados

    Args:
        df: dataframe com os dados
        datetime_columns: colunas que devem ser do tipo DateTime

    Returns:
        df: dataframe com os dados processados
    """
    for col in datetime_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], unit="s")
    df["upload_date"] = datetime.now()
    return df


def send_files_to_postgres(
    files, csv_path, target_table, engine, datetime_columns, int_columns, unique_key
):
    """Envia os arquivos para uma tabela no banco de dados, criando-a se necessário
    e atualizando os dados se a tabela já existir. O tratamento dos dados é feito
    com as funções process_data.

    Args:
        files: Lista com os nomes dos arquivos
        csv_path: Caminho para os arquivos
        target_table: Nome da tabela alvo
        engine: Conexão com o banco de dados, obtida com PostgresHook
        datetime_columns: Colunas que devem ser do tipo DateTime
        int_columns: Colunas que devem ser do tipo Integer
        unique_key: Nome da chave única
    """
    file_counter = 0
    df = pd.DataFrame()
    for file in files:
        file_counter += 1
        print(f"Reading file {file_counter} of {len(files)}")
        temp_df = pd.read_csv(f"{csv_path}/{file}")
        temp_df["file_name"] = file
        df = pd.concat([df, temp_df])
        if file_counter % 5 == 0 or file_counter == len(files):
            df = process_data(df, datetime_columns)
            columns_dict = create_columns_dict(df, datetime_columns, int_columns)
            create_table_if_not_exists(
                target_table,
                columns_dict,
                engine,
            )
            print("Sending data to postgres...")
            delete_and_insert(target_table, df.to_dict("records"), unique_key, engine)
            df = pd.DataFrame()
            print("Data sent successfully.")
    print("All files sent successfully.")