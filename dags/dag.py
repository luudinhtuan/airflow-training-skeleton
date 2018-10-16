import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataProcPySparkOperator,
    DataprocClusterDeleteOperator
)
from airflow.utils.trigger_rule import TriggerRule

dag = DAG(
    dag_id="mydag",
    schedule_interval="30 7 * * *",
    default_args={
        "owner": "airflow",
        "start_date": dt.datetime(2018, 10, 1),
        "depends_on_past": True,
        "email_on_failure": True,
        "email": "airflow_errors@myorganisation.com",
    },
)
def print_exec_date(**context):
    print(context["execution_date"])


my_task = PythonOperator(
    task_id="task_mydag", python_callable=print_exec_date, provide_context=True, dag=dag
)

dataproc_create_cluster = DataprocClusterCreateOperator(
    task_id="create_dataproc",
    cluster_name="analyse-pricing-{{ ds }}",
    project_id='airflowbolcom-d0bcb1627a89fedd',
    num_workers=2,
    zone="europe-west4-a",
    dag=dag,
)

compute_aggregates = DataProcPySparkOperator(
    task_id='compute_aggregates',
    main='gs://gdd-training-bucket/build_statistics.py',
    cluster_name='analyse-pricing-{{ ds }}',
    arguments=["{{ ds }}"],
    dag=dag,
)

dataproc_delete_cluster = DataprocClusterDeleteOperator(
    task_id="delete_dataproc",
    cluster_name="analyse-pricing-{{ ds }}",
    project_id='airflowbolcom-d0bcb1627a89fedd',
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

dataproc_create_cluster >> compute_aggregates >> dataproc_delete_cluster