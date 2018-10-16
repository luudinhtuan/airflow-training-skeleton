import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from godatadriven.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator

dag = DAG(
    dag_id="ukland",
    schedule_interval="30 7 * * *",
    default_args={
        "owner": "airflow",
        "start_date": dt.datetime(2018, 10, 10),
        "depends_on_past": True,
        "email_on_failure": True,
        "email": "airflow_errors@myorganisation.com",
    },
)

pg_2_gcs = PostgresToGoogleCloudStorageOperator(
    task_id='pg_2_gcs',
    postgres_conn_id='pgairflow',
    sql='SELECT COUNT(*) FROM land_registry_price_paid_uk',
    bucket='gs://tuanairflow/',
    filename='landuk',
    dag=dag
)