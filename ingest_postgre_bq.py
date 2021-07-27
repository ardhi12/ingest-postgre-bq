import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from google.cloud import bigquery
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

# set default arguments to schedule DAG
default_args = {
    'owner': 'ardhi',
    'start_date': datetime(2021, 7, 20),
    # the number of repetitions of task instances in case of failure
    'retries': 1,
    # delay task instance to retry when failure occurs
    'retry_delay': timedelta(minutes=10),
}

# configuration BigQuery
project_id = "<your_project_id>"
dataset_id = "<your_dataset_id>"
table_id = "<your_table_id>"
table_location = f'{project_id}.{dataset_id}.{table_id}'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="<PATH_TO_YOUR_CREDENTIAL>"

# schema table
col_names = ["customer_id", "store_id", "first_name", "last_name", "email", "address_id", "activebool", "create_date", "last_update", "active"]
col_types = ["INTEGER", "INTEGER", "STRING", "STRING", "STRING", "INTEGER", "BOOLEAN", "DATE", "DATETIME", "INTEGER"]
col_modes = ["REQUIRED", "REQUIRED", "REQUIRED", "REQUIRED", "NULLABLE", "REQUIRED", "REQUIRED", "REQUIRED", "REQUIRED", "NULLABLE"]

def create_bq_table(client):
    """
    This function is used to create BQ table with defined schema
    """

    # check if table doesn't exists
    tables = [tables.table_id for tables in client.list_tables(dataset_id)]
    if table_id not in tables:
        # define schema
        schema = []
        for col_idx in range(len(col_names)):
            field = bigquery.SchemaField(col_names[col_idx], col_types[col_idx], col_modes[col_idx])
            schema.append(field)
        # create table
        table = bigquery.Table(table_location, schema=schema)
        client.create_table(table)
        print(f"Table {table_id} successfully created!")
    else:
        print(f"Table {table_id} already exists!")

def db_connection():
    """
    This function is used to create connection to Postgre database
    """
    try:
        engine = create_engine('postgresql://<username>:<password>@<host>:<port>/dvdrental')
    except Exception as e:
        print(e)
    finally:
        return engine

def main():
    # create database connection
    db = db_connection()
    # create bigquery connection and table
    client = bigquery.Client()
    create_bq_table(client)
    # retrieve all datas from Postgre and convert into pandas dataframe
    df = pd.read_sql_query("SELECT * FROM customer;", db)
    df = df.sort_values(by=["customer_id"])
    # change data type datetime to string
    df['create_date'] = df['create_date'].astype('str')
    df['last_update'] = df['last_update'].astype('str')
    # calculate split
    pagination = 5
    last_row = 0
    number_of_rows = df.shape[0]
    rows_per_part = float(number_of_rows/pagination)
    before_decimal = int(str(rows_per_part).split('.')[0])
    after_decimal = int(str(rows_per_part).split('.')[1][:1])
    # split data into 5 parts
    for x in range(pagination):
        print(f"INSERT PART-{x+1}")
        split = df.iloc[last_row:(before_decimal*(x+1)+after_decimal),:]
        if x == 0:
            split = df.iloc[before_decimal*x:(before_decimal*(x+1))+after_decimal,:]
        last_row = last_row + split.shape[0]

        # insert data
        rows_to_insert = {}
        for x, row in split.iterrows():
            for n in range(len(col_names)):
                rows_to_insert[col_names[n]] = row[col_names[n]]
            errors = client.insert_rows_json(table_location, [rows_to_insert])
            if errors:
                print("[ERROR INSERT]", rows_to_insert)

# create DAG object
# DAG akan berjalan efektif ketika start_date + schedule_interval
with DAG("ingest_postgre_bq", default_args=default_args,
    schedule_interval="@daily", catchup=False) as dag:
        ingest = PythonOperator(
            task_id = "ingest",
            python_callable=main
        )

        # start task
        ingest