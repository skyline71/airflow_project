# Lesson 37: Airflow DAGs, Custom operators

from datetime import datetime
from airflow.models import DAG
from airflow.hooks.base_hook import BaseHook
from dag_lesson_37_plugin.dag_lesson_37_operator import ExampleOperator


connection = BaseHook.get_connection("main_postgresql_connection")

default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 28),
}

dag = DAG('dag_lesson_37', default_args=default_args, schedule_interval='0 1 * * *', catchup=True,
          max_active_tasks=3, max_active_runs=1, tags=["api-dataset", "currency", "weather"])

task1 = ExampleOperator(
    task_id='task1',
    postgre_conn=connection,
    dag=dag)

task2 = ExampleOperator(
    task_id='task2',
    postgre_conn=connection,
    dag=dag)

task1 >> task2
