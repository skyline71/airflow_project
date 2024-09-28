# Lesson 35: Airflow DAGs

from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 28),
    #"retry_delay": timedelta(minutes=0.1)
}

dag = DAG('dag_lesson_35', default_args=default_args, schedule_interval='0 * * * *', catchup=True,
          max_active_tasks=3, max_active_runs=1, tags=["api-dataset", "currency", "weather"])

task1 = BashOperator(
    task_id='task1',
    bash_command='python3 /airflow/scripts/dag_lesson_35/task1.py',
    dag=dag)

task2 = BashOperator(
    task_id='task2',
    bash_command='python3 /airflow/scripts/dag_lesson_35/task2.py',
    dag=dag)

task1 >> task2
