# Airflow-Postgres ETL (Ubuntu/WSL2)

In this project, I perform an ETL (Extract, Transform, Load) process on CSV files to a Postgres database using Airflow. The project was developed on Ubuntu/WSL2.

---

## Table of Contents

- [Repository Structure](#repository-structure)
- [Machine Configuration](#machine-configuration)
- [Motivation](#motivation)
- [Project Structure](#project-structure)
- [DAGs Structure](#dags-structure)
- [Running the Project](#running-the-project)
- [Test Queries](#test-queries)
- [Potential Issues for Future Versions](#potential-issues-for-future-versions)
- [Improvements](#improvements)
- [References](#references)

---

## Repository Structure

The project is structured as follows:

```txt
airflow-postgres-etl/
├── config-scripts/
│   ├── 01_config_ubuntu_env.sh
│   ├── 02_install_asdf_plugins.sh
│   ├── 03_install_proj_dep.sh
│   ├── 04_requirements_txt_to_poetry(optional).sh
│   ├── stuck_resolving_dependencies_fix.sh
│   └── uninstall_env.sh
├── dags/
│   ├── __init__.py
│   ├── .airflowignore
│   ├── datahelper/
│   │   ├── __init__.py
│   │   └── postgres.py
│   └── postgres_etl/
│       ├── events_table.py
│       └── tracking_table.py
├── data/
│   ├── extracted/
│   └── data.zip
├── initial-script/
│   └── extract-zip.py
├── local-run/
├── .gitignore
├── .pre-commit-config.yaml
├── poetry.lock
├── pyproject.toml
├── credentials.json
└── .tool-versions
```

## Machine Configuration

- Configure WSL2 using the following tutorial: <https://github.com/galvsoliveira/airflow-postgres-etl#configuring-wsl2-for-windows-users>
- Clone the repository and navigate to the project folder using the Ubuntu terminal.
- Run the following commands:
  - `bash config-scripts/01_config_ubuntu_env.sh`
    - Restart the terminal and run: `source ~/.bashrc`.
  - `bash config-scripts/02_install_asdf_plugins.sh`
  - `bash config-scripts/03_install_proj_dep.sh`
- Open VSCode using the command `code .`.
- Install Docker and Docker Compose.
  - For WSL2, follow the tutorial: <https://docs.docker.com/desktop/windows/wsl/>.
  - For Ubuntu, follow the tutorial: <https://docs.docker.com/engine/install/ubuntu/>.
- Rename the file `credentials.json.example` to `credentials.json`. The example credentials are the same as in the `docker-compose.yml` file.

## Motivation

The initial aim of the project was to create an ETL for Postgres using containers. However, due to Airflow's flexibility and its applicability to various projects, I decided to use it for the ETL. Airflow allows the ETL to be run in a development environment and then easily migrated to a production environment without changing the code.

## Project Structure

Considering Airflow's memory limitations, I ensured minimal resource usage by using Docker to run Postgres and Airflow, and utilizing PostgresHook to execute database queries and clear data batches after each database insertion.

I chose to use the latest version of Airflow available at the time, 2.7.1, and the latest Python version, 3.11.5. I ensured that the Python versions in the project and Airflow were the same and used Docker to run both Airflow and Postgres. The Postgres database used is the same as the Airflow database, and to access it, I had to open port 5432 in the `docker-compose.yml` file.

The project consists of two DAGs: `postgres_etl.events_table` and `postgres_etl.tracking_table`. Each DAG is responsible for sending data to a table in the database, creating them if necessary. The `postgres_etl.events_table` DAG handles the `events` table data, and the `postgres_etl.tracking_table` DAG handles the `tracking` table data.

I divided the columns between the tables as follows:

Tracking:

- oid__id: Unique key
- Op: Transaction operation type
- createdAt: Tracking creation date
- updatedAt: Tracking update date
- lastSyncTracker: Last tracking sync date
- fileName: File name
- uploadDate: File upload date

Events:

- oid__id: Foreign key
- trackingCode: Tracking code
- description: Event description
- status: Event status
- trackerType: Tracking type
- from: Origin location
- to: Destination location
- eventCreatedAt: Order creation date
- uploadDate: File upload date
- fileName: File name

## DAGs Structure

We have two main functions that help us understand how the ETL works:

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
    """Sends the files to a table in the database, creating it if necessary
    and updating the data if the table already exists. Data processing is done
    with the process_data functions.

    Args:
        files: List of file names
        csv_path: Path to the files
        target_table: Name of the target table
        engine: Database connection, obtained with PostgresHook
        datetime_columns: Columns that should be of type DateTime
        int_columns: Columns that should be of type Integer
        unique_key: Name of the unique key
        normalize_column: List of columns to be "exploded" into rows and normalized
        filter_columns: List of columns to be kept
    """
    file_counter = 0
    df = pd.DataFrame()
    for file in files:
        file_counter += 1
        print(f"Reading file {file_counter} of {len(files)}")
        temp_df = read_file(file, csv_path)
        df = pd.concat([df, temp_df])
        if file_counter % n_batch == 0 or file_counter == len(files):
            df = process_data(
                df, datetime_columns, unique_key, normalize_column, filter_columns
            )
            columns_dict = create_columns_dict(df, datetime_columns, int_columns)
            create_table_if_not_exists(
                target_table,
                columns_dict,
                engine,
            )
            print("Sending data to postgres...")
            delete_and_insert(target_table, df.to_dict("records"), unique_key, engine)
            rows_inserted = len(df)
            print(f"Data sent successfully. {rows_inserted} rows inserted.")
            df = pd.DataFrame()
    print("All files sent successfully.")
```

```python
@dag.task
def extract_treat_and_load(files, last_uploaded_file):
    """Extracts data from files, processes, and loads it into the database"""
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
```

Basically, we check all the files in the `data/extracted` folder and check the last file sent to the database. If the last file sent is the same as the last file in the folder, we do nothing. Otherwise, we send the files to the database starting from the last file sent. Data processing is done with the `process_data` function, which does the following:

```python
def process_data(
    df, datetime_columns, unique_key, normalize_column=None, filter_columns=None
):
    """Processes data before sending it to the database

    Args:
        df: Dataframe with the data
        datetime_columns: Columns that should be of type DateTime
        unique_key: Name of the unique key
        normalize_column (str, optional): Column to be "exploded" into rows
        filter_columns (list, optional): Columns to be kept

    Returns:
        df: Dataframe with processed data
    """
    df.drop_duplicates(subset=[unique_key], keep="last")
    if normalize_column:
        df = explode_and_normalize(df, normalize_column)

    # Convert timestamp columns to datetime
    cols_to_fix = set(datetime_columns).intersection(df.columns)
    for col in cols_to_fix:
        if (col == "createdAt.$date"):
            df.loc[:, col] = pd.to_datetime(df.loc[:, col], unit="ms")
            df = df.rename(columns={col: "orderCreatedAt"})
        else:
            df.loc[:, col] = pd.to_datetime(df.loc[:, col], unit="s")

    # Add column "uploadDate"
    df.loc[:, "uploadDate"] = datetime.now()

    if filter_columns:
        df = df[filter_columns + ["uploadDate", "fileName"]]

    # Remove values that will cause errors when sending to Postgres
    df = df.replace(
        {"NaN":

 None, "NaT": None, "None": None, "": None, pd.NaT: None}
    ).drop_duplicates()
    return df
```

The `process_data` function is flexible enough to handle data according to each table's needs, allowing column type changes, column normalization, and column filtering. The replace operation at the end is necessary because pandas cannot convert some values to None, and Postgres does not accept values like "NaN" or "NaT". Note: the `unique_key` parameter can be ambiguous, as it is the unique key of the main table before the explode operation, not after.

## Running the Project

I decided to upload the CSVs to the repository to facilitate project execution, but if you want to extract the data from the zip file, run the script `initial-script/extract-zip.py`. To run the project, follow these steps:

- In the terminal, run:
  - `cd local-run` to navigate to the local-run folder.
  - `chmod +x ./local-run` to give execute permission to the script.
  - `./local-run build-and-start` to build the Airflow and Postgres containers and start the project. To stop the project, run `./local-run stop`.

Now, to access Airflow, open your browser and go to `localhost:8080`. The username and password are `airflow`. Run the DAGs `postgres_etl.events_table` and `postgres_etl.tracking_table` to execute the ETL.

![Alt text](data/image-1.png)

The DAGs lineage is as follows:

![Alt text](data/image-2.png)

## Test Queries

To test the project, use the following Postgres credentials to connect:

```txt
host: localhost
port: 5432
username: airflow
password: airflow
database: postgres
```

You can use a VSCode extension with id `cweijan.vscode-postgresql-client2` (search for it in the VSCode extension marketplace) to run the queries directly in VSCode. Here are some test queries:

Total trackings created per minute:

```sql
SELECT
    DATE_TRUNC('minute', "createdAt") AS minute,
    COUNT(*) AS total_trackings
FROM
    public.tracking
GROUP BY
    minute
ORDER BY
    minute
LIMIT 1000;
```

Total events per tracking code:

```sql
SELECT
    "trackingCode",
    COUNT(*) AS total_events
FROM
    public.events
GROUP BY
    "trackingCode"
ORDER BY
    total_events DESC
LIMIT 1000;
```

Top 10 most common descriptions:

```sql
WITH ranked_events AS (
    SELECT
        "description",
        COUNT(*) AS total_events,
        RANK() OVER (ORDER BY COUNT(*) DESC) AS event_rank
    FROM
        public.events
    GROUP BY
        "description"
)
SELECT
    "description",
    total_events,
    event_rank
FROM
    ranked_events
WHERE
    event_rank <= 10
ORDER BY
    event_rank;
```

## Potential Issues for Future Versions

Since we perform an explode followed by normalization, the number of rows per file scales significantly, causing data upload to the database to take a long time for the events table (about 2 hours). I also noticed that this performance issue is due to the DELETE being used before the INSERT with various parameters. I chose this method because it works with any database, although I later discovered that SQLAlchemy does not work with Redshift.

We have two options to solve the performance issue: the first would be not to do the DELETE+INSERT but rather an UPSERT, which I couldn't get to work, and the second would be to change the approach from an ETL to an ELT, transferring raw data directly to the database, performing a QUALIFY in a temporary table, and inserting it into the final table with an insert.

## Improvements

One improvement for this project could be using MongoDB to store raw data and Postgres to store processed data. MongoDB is a non-relational database commonly used for storing data from tech sources, while Postgres is more commonly used for storing processed data. Airflow allows connections with both MongoDB and Postgres, so this migration would be feasible.

Another interesting improvement would be using dbt, a data transformation tool. dbt allows transformations to be performed in the database itself and enables the creation of automated tests to ensure data quality. Thus, we would have an ELT process where raw data is stored in MongoDB, replicated to Postgres, and transformed in Postgres using dbt. I believe this would be a more scalable and possibly faster solution, as the data would already be within the database and the transfer and processing would not involve opening and closing database connections, plus implementing various tests with dbt would be quite simple, ensuring data quality.

## References

The project was based on a previous project of mine (<https://github.com/galvsoliveira/python-poetry-production-level-repository-template>) and the following tutorials:
- <https://www.youtube.com/watch?v=mMqaiNbeeUU&t=943s&ab_channel=Codifike>. For setting up Airflow and Postgres.
- <https://essentl.io/running-astronomer-cosmos-in-mwaa/> For modifying the docker-compose.yml.

I also had prior knowledge of Airflow from professional experiences.