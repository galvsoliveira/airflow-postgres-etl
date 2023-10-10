# Airflow-Postgres ETL (Ubuntu/WSL2) <!-- omit in toc -->

Neste projeto, efetuo um ETL de arquivos csv para um banco de dados Postgres utilizando o Airflow. O projeto foi desenvolvido em Ubuntu/WSL2.

---

## Table of Contents <!-- omit in toc -->

- [Estrutura do Repositório](#estrutura-do-repositório)
- [Configurando a Máquina](#configurando-a-máquina)
- [Motivação](#motivação)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Estrutura das DAGs](#estrutura-das-dags)
- [Rodando o Projeto](#rodando-o-projeto)
- [Potenciais Problemas a Serem Investigados em Próximas Versões](#potenciais-problemas-a-serem-investigados-em-próximas-versões)
- [Melhorias](#melhorias)

---

## Estrutura do Repositório

O projeto está estruturado da seguinte forma:

```txt
airflow-postgres-etl/
├── config-scripts/
│ ├── 01_config_ubuntu_env.sh
│ ├── 02_install_asdf_plugins.sh
│ ├── 03_install_proj_dep.sh
│ ├── 04_requirements_txt_to_poetry(optional).sh
│ ├── stuck_resolving_dependencies_fix.sh
│ └── unistall_env.sh
├── dags/
│ ├── __init__.py
│ ├── .airflowignore
│ ├── datahelper/
│ │ ├── __init__.py
│ │ └── postgres.py
│ └── postgres_etl/
│   ├── events_table.py
│   └── tracking_table.py
├── data/
│ ├── extracted/
│ └── data.zip
├── initial-script/
│ └── extract-zip.py
├── local-run/
├── .gitignore
├── .pre-commit-config.yaml
├── poetry.lock
├── pyproject.toml
├── credentials.json
└── .tool-versions
```

## Configurando a Máquina

- Configure o WSL2 com o seguinte tutorial: <https://github.com/galvsoliveira/airflow-postgres-etl#configuring-wsl2-for-windows-users>
- Clone o repositório e entre na pasta do projeto usando o terminal do Ubuntu.
- Rode os seguintes comandos:
  - `bash config-scripts/01_config_ubuntu_env.sh`
    - Reinicie o terminal e rode: `source ~/.bashrc`.
  - `bash config-scripts/02_install_asdf_plugins.sh`
  - `bash config-scripts/03_install_proj_dep.sh`
- Abra o VSCode usando o comando `code .`.
- Renomeie o arquivo `credentials.json.example` para `credentials.json`. As credenciais do exemplo são as mesmas do arquivo `docker-compose.yml`, então não é necessário alterar nada.

## Motivação

O projeto inicialmente tinha o intuito de criar um ETL para o postgres usando containers, mas devido à flexibilidade do Airflow, e a possibilidade de utilização em diferentes projetos, decidi utilizá-lo para o ETL. O Airflow permite que o ETL seja executado em um ambiente de desenvolvimento, e depois seja facilmente migrado para um ambiente de produção, sem a necessidade de alterar o código.

Estou ciente da limitação do Airflow quanto à memória, mas decidi utilizá-lo mesmo assim devido ao fato de também ser uma ferramenta muito utilizada no mercado.

Sabendo dessas limitações, também me certifiquei de aliviar o Airflow o máximo possível, utilizando o Docker para executar o Postgres e o Airflow, e utilizando o PostgresHook para executar as queries no banco de dados e limpando os batches de dados após cada envio para o banco de dados.

## Estrutura do Projeto

Escolhi utilizar a última versão do Airflow disponível no momento, a 2.7.1, e a última versão do Python, a 3.11.5, garanti que as versões do Python no projeto e no Airflow fossem as mesmas, e utilizei o Docker para executar o Airflow e o Postgres. O banco do Postgres utilizado é o mesmo banco do Airflow e para poder acessá-lo, tive que abrir a porta 5432 no arquivo `docker-compose.yml`.

O projeto é composto por duas DAGs, `postgres_etl.events_table` e `postgres_etl.tracking_table`. Cada DAG é responsável por enviar os dados para uma tabela do banco, criando-as se necessário. A DAG `postgres_etl.events_table` é responsável por enviar os dados da tabela `events`, e a DAG `postgres_etl.tracking_table` os dados da tabela `tracking`.

## Estrutura das DAGs

Temos duas funções principais que nos ajudam a entender o funcionamento do ETL:

```python
def send_files_to_postgres(
    files,
    csv_path,
    target_table,
    engine,
    datetime_columns,
    int_columns,
    unique_key,
    filter_columns,
    normalize_column=None,
    n_batch=5,
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
        normalize_column: Lista de colunas que devem ser "explodidas" em linhas
            e normalizadas
        filter_columns: Lista de colunas que devem ser mantidas
    """
    file_counter = 0
    df = pd.DataFrame()
    for file in files:
        file_counter += 1
        print(f"Reading file {file_counter} of {len(files)}")
        temp_df = read_file(file, csv_path)
        df = pd.concat([df, temp_df])
        if file_counter % n_batch == 0 or file_counter == len(files):
            df = process_data(df, datetime_columns, normalize_column, filter_columns)
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
```

```python
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
        n_batch=1,
    )
```
Basicamente, checamos todos os arquivos na pasta `data/extracted` e checamos o último arquivo enviado para o banco de dados. Se o último arquivo enviado for o mesmo que o último arquivo na pasta, não fazemos nada. Caso contrário, enviamos os arquivos para o banco de dados a partir do último arquivo enviado. O tratamento dos dados é feito com a função `process_data`, que faz o seguinte:

```python
def process_data(df, datetime_columns, normalize_column=None, filter_columns=None):
    """Processa os dados antes de enviá-los para o banco de dados

    Args:
        df: dataframe com os dados
        datetime_columns: colunas que devem ser do tipo DateTime
        normalize_column (str, optional): coluna que deve ser "explodida" em linhas
        filter_columns (list, optional): colunas que devem ser mantidas

    Returns:
        df: dataframe com os dados processados
    """
    if normalize_column:
        df = explode_and_normalize(df, normalize_column)
    cols_to_fix = set(datetime_columns).intersection(df.columns)
    for col in cols_to_fix:
        if col == "createdAt.$date":
            df[col] = pd.to_datetime(df[col], unit="ms")
            df = df.rename(columns={col: "orderCreatedAt"})
        else:
            df[col] = pd.to_datetime(df[col], unit="s")
    df["uploadDate"] = datetime.now()
    if filter_columns:
        df = df[filter_columns + ["uploadDate", "fileName"]]
    df = df.replace({"NaN": None, "NaT": None, "None": None, "": None, pd.NaT: None})
    return df
```

A função `process_data` é flexível o bastante para tratar os dados de acordo com as necessidades de cada tabela, permitindo mudar o tipo de colunas, normalizar colunas e filtrar colunas. O replace no final é necessário porque o pandas não consegue converter alguns valores para None, e o Postgres não aceita valores como "NaN" ou "NaT".

## Rodando o Projeto

- No terminal, rode:
  - `cd local-run`, para entrar na pasta local-run.
  - `chmod +x ./local-run`, para dar permissão de execução ao script.
  - `./local-run build-and-start`, pára construir o container do airflow e do postgres e iniciar o projeto.

Para parar o projeto, rode `./local-run stop`.
Agora, para acessar o Airflow, abra o navegador e digite `localhost:8080`. O usuário e senha são `airflow`. Rode as DAGs `postgres_etl.events_table` e `postgres_etl.tracking_table` para executar o ETL.

![Alt text](data/image-1.png)

O lineage das DAGs é o seguinte:

![Alt text](data/image-2.png)

## Potenciais Problemas a Serem Investigados em Próximas Versões

Como fazemos um explode seguido de uma normalização, o número de linhas por arquivo escala bastante, o que faz o envio de dados para o banco de dados demorar muito no caso da tabela de eventos, mesmo fazendo um batch de um arquivo só. Isso pode ser potencialmente resolvido escolhendo uma abordagem ELT ao invés de ETL, deixando o processamento para o banco de dados, não necessitando mais de diversas conexões para enviar os dados. Vale a pena investigar também a performance do SQLAchemy, que é utilizado pelo Airflow para se conectar com o Postgres, e ver se é possível melhorar a performance do envio utilizando outra ferramenta.

## Melhorias

Uma melhoria legal que pode ser feita nesse projeto é a utilização do MongoDB para armazenar os dados brutos, e o Postgres para armazenar os dados tratados. O MongoDB é um banco de dados não relacional, e é muito utilizado para armazenar dados provenientes de Tech, enquanto o Postgres acaba sendo mais utilizado para armazenar dados tratados. O Airflow permite fazer a conexão com o MongoDB e o Postgres, então seria possível fazer essa migração facilmente.

Outra melhoria interessante seria o dbt, que é uma ferramenta de transformação de dados. O dbt permite que as transformações sejam feitas no próprio banco de dados, e também permite a criação de testes automatizados para garantir a qualidade dos dados. Dessa forma, teríamos um ELT, em que os dados brutos são contidos no MongoDB, replicados para o Postgres, e transformados no próprio Postgres com o dbt. Acredito que essa seria uma solução mais escalável e possivelmente mais rápida, já que os dados já estariam dentro do banco e o envio e tratamento não ficariam abrindo e fechando conexões com o banco de dados, além de podermos implementar diversos testes de forma bastante simples com o dbt, garantindo a qualidade dos dados.
