import datetime
import psycopg2
import psycopg2.extras as extras
import numpy as np
import pandas as pd
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


with DAG (
    "transfer_step_up_data",
    default_args={
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0
    },
    description= "tranfer datafrom posgres to gp",
    start_date = datetime.datetime(2022, 8, 10),
    catchup=False,
    max_active_runs=1,
    tags=['Cherniaev'],
    as dag:

        task_0_dummy = DummyOperator (task_id="task_0")

        def connect(param_dict):
    conn = None
    try:
        conn = psycopg2.connect(**param_dict)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        exit(1)

    return conn

    def etl_transactions_changes(**kwargs):
        load_hs_transactions = kwargs.get("func")
        #conn_pg_params = {
        #    "host": "10.4.49.3",
        #    "database": "test",
        #    "user": "posgres",
        #    "password": "t73XTdEwwcZwsS9oa"

        conn_gp_params = {
            "host": "10.4.49.6",
            "database": "test",
            "user": "airflow",
            "password": "airflow"
        }
        conn_pg = connect(conn_gp_params)
        pd.io.sql.read_sql_querry(f"SELECT rdv_2.{load_hs_transactions};", conn_pg)

    load_hs_transactions = PythonOperator(
        task_id = "running_gp_func_load_hs_transactions",
        python_callable = etl_transactions_changes,
        func = "load_hs_transactions")
    load_l_transactions = PythonOperator(
        task_id = "running_gp_func_load_l_transactions",
        python_callable = etl_transactions_changes,
        func = "load_l_transactions")
    load_sat_transactions = PythonOperator(
        task_id = "running_gp_func_load_sat_transactions",
        python_callable = etl_transactions_changes,
        func = "load_sat_transactions"
    )
    task_0 >> [load_hs_transactions, load_l_transactions, load_sat_transactions]
