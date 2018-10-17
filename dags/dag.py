import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataProcPySparkOperator,
    DataprocClusterDeleteOperator
)

from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from operators import user_operator

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

default_args = {
            "owner": "airflow",
            "start_date": dt.datetime(2018, 10, 1),
            "depends_on_past": True,
            "email_on_failure": True,
            "email": "airflow_errors@myorganisation.com",
        }

dag.add_task(user_operator.HttpToGcsOperator(default_args))



# dataproc_create_cluster = DataprocClusterCreateOperator(
#     task_id="create_dataproc",
#     cluster_name="analyse-pricing-{{ ds }}",
#     project_id='airflowbolcom-d0bcb1627a89fedd',
#     num_workers=2,
#     zone="europe-west4-a",
#     dag=dag,
# )
#
# compute_aggregates = DataProcPySparkOperator(
#     task_id='compute_aggregates',
#     main='gs://europe-west1-training-airfl-4c5b98dd-bucket/dags/other/build_statistics_simple.py',
#     cluster_name='analyse-pricing-{{ ds }}',
#     arguments=["{{ ds }}"],
#     dag=dag,
# )
#
# compute_aggregates_next = DataProcPySparkOperator(
#     task_id='compute_aggregates_next',
#     main='gs://europe-west1-training-airfl-4c5b98dd-bucket/dags/other/build_statistics_simple_next.py',
#     cluster_name='analyse-pricing-{{ ds }}',
#     arguments=["{{ ds }}"],
#     dag=dag,
# )
#
# compute_aggregates_con = DataProcPySparkOperator(
#     task_id='compute_aggregates_con',
#     main='gs://europe-west1-training-airfl-4c5b98dd-bucket/dags/other/build_statistics_simple_con.py',
#     cluster_name='analyse-pricing-{{ ds }}',
#     arguments=["{{ ds }}"],
#     dag=dag,
# )
#
# dataproc_delete_cluster = DataprocClusterDeleteOperator(
#     task_id="delete_dataproc",
#     cluster_name="analyse-pricing-{{ ds }}",
#     project_id='airflowbolcom-d0bcb1627a89fedd',
#     trigger_rule=TriggerRule.ALL_DONE,
#     dag=dag,
# )
#
# dataproc_create_cluster >> compute_aggregates >> compute_aggregates_next >>dataproc_delete_cluster
#
# dataproc_create_cluster >> compute_aggregates_con >> dataproc_delete_cluster