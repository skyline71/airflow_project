# Lesson 36: Airflow DAGs, Connections

from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base_hook import BaseHook


connection = BaseHook.get_connection("main_postgresql_connection")

default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 28),
}

dag = DAG('dag_lesson_36', default_args=default_args, schedule_interval='0 7 * * *', catchup=True,
          max_active_tasks=3, max_active_runs=1, tags=["api-dataset", "currency", "weather"])

task1 = BashOperator(
    task_id='task1',
    bash_command='python3 /airflow/scripts/dag_lesson_36/task1.py --date {{ ds }} ' + f'--host {connection.host} --dbname {connection.schema} --user {connection.login} --jdbc_password {connection.password} --port 5432',
    dag=dag)

task2 = BashOperator(
    task_id='task2',
    bash_command='python3 /airflow/scripts/dag_lesson_36/task2.py',
    dag=dag)

task1 >> task2
