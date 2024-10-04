from airflow.models.connection import Connection
from airflow.sensors.base import BaseSensorOperator
import psycopg2 as pg
import pandas.io.sql as psql


class CurrencySensor(BaseSensorOperator):
    poke_context_fields = ['conn', 'table_name']

    def __init__(self, conn: Connection, table_name: str, *args, **kwargs):
        self.conn = conn
        self.table_name = table_name
        super(CurrencySensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        connection = pg.connect(f"host={self.conn.host} dbname={self.conn.schema} user={self.conn.login} password={self.conn.password}")
        query = f'''WITH last_values AS (
        	SELECT DISTINCT currency_date, currency, "rate_USD"
        	FROM {self.table_name}
        	ORDER BY currency_date DESC
        	LIMIT 2
        ),
        last_rub AS (
        	SELECT "rate_USD" AS last_rub
        	FROM last_values
        	WHERE currency = 'RUB'
        ),
        last_eur AS (
        	SELECT "rate_USD" AS last_eur
        	FROM last_values
        	WHERE currency = 'EUR'
        ),
        avg_rub AS (
        	SELECT AVG("rate_USD") AS avg_rate
        	FROM {self.table_name}
        	WHERE currency = 'RUB'
        	GROUP BY currency
        ),
        avg_eur AS (
        	SELECT AVG("rate_USD") AS avg_rate
        	FROM {self.table_name}
        	WHERE currency = 'EUR'
        	GROUP BY currency
        )
        SELECT (
        	CASE 
        		WHEN (SELECT * FROM last_rub) > (SELECT * FROM avg_rub)
        			AND (SELECT * FROM last_eur) > (SELECT * FROM avg_eur)
        		THEN 1
        		ELSE 0
        	END
        	) AS flag'''
        df_currency = psql.read_sql(f'{query}', connection)
        if df_currency.iloc[0]['flag'] == 1:
            return True
        else:
            return False
