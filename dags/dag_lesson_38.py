from datetime import datetime
from airflow.models import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.bash import BashOperator
from utils.currency_sensor import CurrencySensor


connection = BaseHook.get_connection("main_postgresql_connection")

default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 28),
}

dag = DAG('dag_lesson_38', default_args=default_args, schedule_interval='0 * * * *', catchup=True,
          max_active_tasks=3, max_active_runs=1, tags=["api-dataset", "currency", "sensor"])

task1 = BashOperator(
    task_id='task1',
    bash_command='python3 /airflow/scripts/dag_lesson_38/task1.py',
    dag=dag)

currency_sensor = CurrencySensor(
    task_id=f'currency_sensor',
    timeout=1000,
    mode='reschedule',
    poke_interval=10,
    conn=connection,
    table_name='currency_data',
    dag=dag
)

task3 = BashOperator(
    task_id='task3',
    bash_command='python3 /airflow/scripts/dag_lesson_38/task3.py',
    dag=dag)

for i in [1, 2, 3, 4, 5]:
    some_task = BashOperator(
    task_id=f'task4_{str(i)}',
    bash_command='python3 /airflow/scripts/dag_lesson_38/task1.py',
    dag=dag)
    task3 >> some_task

task1 >> currency_sensor >> task3
