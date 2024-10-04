from datetime import datetime
from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2024, 10, 2),
}

dag = DAG('dag_lesson_39', default_args=default_args, schedule_interval='0 5 * * *', catchup=True,
          max_active_tasks=3, max_active_runs=1, tags=["data-mart", "orders"])

clear_day = PostgresOperator(
    task_id='clear_day',
    postgres_conn_id='main_postgresql_connection',
    sql="""DELETE FROM public.dm_orders WHERE "buy_date" = '{{ ds }}'::date""",
    dag=dag)

calc_day = PostgresOperator(
    task_id='calc_day',
    postgres_conn_id='main_postgresql_connection',
    sql="""INSERT INTO dm_orders (buy_date, order_city_name, product_name, customer_name, customer_city_name, bank_account, total_amount, total_costs)
            SELECT O.buy_time::date AS buy_date,
                   C.city_name AS order_city_name, 
                   P.product_name AS product_name, 
                   CU.customer_name AS customer_name,
                   C1.city_name AS customer_city_name,
                   CU.bank_account AS bank_account,  
                   SUM(amount) AS total_amount,
                   SUM(costs) AS total_costs
            FROM f_orders O
            LEFT JOIN d_cities C ON
                O.city_id = C.id
            LEFT JOIN d_products P ON
                O.product_id = P.id
            LEFT JOIN d_customers CU ON
                O.customer_id = CU.id
            LEFT JOIN d_cities C1 ON
                CU.city_id = C1.id
            WHERE O.buy_time::date = '{{ ds }}'::date
            GROUP BY buy_date, order_city_name, product_name, customer_name, customer_city_name, bank_account;""",
    dag=dag)

clear_day >> calc_day
