import psycopg2 as pg
import pandas.io.sql as psql


connection = pg.connect(f"host=194.87.80.157 dbname=de_lessons_db user=test_user password=155423")

query = f'''WITH last_values AS (
	SELECT DISTINCT currency_date, currency, "rate_USD"
	FROM currency_data
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
	FROM currency_data
	WHERE currency = 'RUB'
	GROUP BY currency
),
avg_eur AS (
	SELECT AVG("rate_USD") AS avg_rate
	FROM currency_data
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
print(df_currency)

if df_currency.iloc[0]['flag'] == 1:
    print('123')