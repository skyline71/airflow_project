from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy import Column, Integer, VARCHAR, Date, Float, TIMESTAMP
from sqlalchemy.orm import declarative_base
from config.global_config import CURRENCY_API_KEY, CURRENCY_API_URL, WEATHER_API_KEY, WEATHER_API_URL
import requests
import pandas as pd
import yaml
import json
import argparse
import sys
sys.path.append('..')


parser = argparse.ArgumentParser()
parser.add_argument("--date", dest="date")
parser.add_argument("--host", dest="host")
parser.add_argument("--dbname", dest="dbname")
parser.add_argument("--user", dest="user")
parser.add_argument("--jdbc_password", dest="jdbc_password")
parser.add_argument("--port", dest="port")
args = parser.parse_args()

print('date = ' + str(args.date))
print('host = ' + str(args.host))
print('dbname = ' + str(args.dbname))
print('user = ' + str(args.user))
print('jdbc_password = ' + str(args.jdbc_password))
print('port = ' + str(args.port))

v_host = str(args.host)
v_dbname = str(args.dbname)
v_user = str(args.user)
v_password = str(args.jdbc_password)
v_port = str(args.port)

SQLALCHEMY_DATABASE_URI = f"postgresql://{str(v_user)}:{str(v_password)}@{str(v_host)}:{str(v_port)}/{str(v_dbname)}"


class Weather:
    __cities: list

    def __init__(self):
        self.__cities = []

    def add_city(self, city_name: str):
        try:
            if type(city_name) is str:
                self.__cities.append(city_name)
            else:
                raise TypeError
        except TypeError:
            print('City name must be string')
            return None

    def remove_city(self, city_name: str):
        try:
            if type(city_name) is str:
                self.__cities.remove(city_name)
            else:
                raise TypeError
        except TypeError:
            print('City name must be string')
            return None

    def get_cities_list(self):
        return self.__cities

    def __str__(self):
        return str(self.__cities)


class FileWriter:
    __cities: list
    __df: pd.DataFrame

    def __init__(self, cities: list):
        self.__cities = cities
        self.__df = pd.DataFrame(self.__cities)

    def add_cities(self, cities: list):
        self.__cities.extend(cities)

    def write_yaml(self, file: str):
        try:
            with open(file, 'w') as f:
                yaml.dump(self.__cities, f)
        except Exception as e:
            print(f'File name should be string ({e})')
            return None

    def write_json(self, file: str):
        try:
            with open(file, 'w') as f:
                json_object = json.dumps(self.__cities)
                f.write(json_object)
        except Exception as e:
            print(f'File name should be string ({e})')
            return None

    def write_csv(self, file: str):
        try:
            self.__df.to_csv(file, index=False)
        except Exception as e:
            print(f'File name should be string ({e})')
            return None

    def sort_values(self, column: str):
        try:
            if type(column) is str:
                self.__df = self.__df.sort_values(column)
            else:
                raise ValueError
        except ValueError:
            print(f'Column name should be string')
            return None
        except Exception as e:
            print(f'Column {str(column)} does not exist ({e})')
            return None


class FileReader:
    __yaml_output: list
    __json_output: list
    __csv_output: pd.DataFrame

    def __init__(self):
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        pd.set_option('display.width', 2000)

    def read_yaml(self, file: str):
        try:
            with open(file, 'r') as f:
                self.__yaml_output = yaml.load(f, Loader=yaml.FullLoader)
            return self.__yaml_output
        except Exception as e:
            print(f'File {file} not found ({e})')
            return None

    def read_json(self, file: str):
        try:
            with open(file, 'r') as f:
                self.__json_output = json.load(f)
            return self.__json_output
        except Exception as e:
            print(f'File {file} not found ({e})')
            return None

    def read_csv(self, file: str):
        try:
            self.__csv_output = pd.read_csv(file)
            return self.__csv_output
        except Exception as e:
            print(f'File {file} not found ({e})')
            return None


class RequestHandler:
    __url: str
    __api_key: str
    __request: str
    __results: list

    def __init__(self):
        self.__url = WEATHER_API_URL
        self.__api_key = WEATHER_API_KEY
        self.__results = []

    def send_get_request(self, cities: list, days: int):
        try:
            if type(days) is int and days > 0:
                for day in range(0, days):
                    for city in cities:
                        self.__request = f'{str(self.__url)}?key={str(self.__api_key)}&q={str(city)}&days={str(days)}'
                        r = requests.get(url=self.__request)
                        result = r.json()
                        self.__results.append(
                            {
                                'city': result.get('location').get('name'),
                                'country': result.get('location').get('country'),
                                'localtime': pd.to_datetime(result.get('location').get('localtime')).strftime(
                                    format='%Y-%m-%d %H:%M'),
                                'current_temp_c': result.get('current').get('temp_c'),
                                'forecast_date': pd.to_datetime(
                                    result.get('forecast').get('forecastday')[day].get('date')).date(),
                                'forecast_temp_c': result.get('forecast').get('forecastday')[day].get('day').get(
                                    'maxtemp_c'),
                                'wind_speed_kph': result.get('forecast').get('forecastday')[day].get('day').get(
                                    'maxwind_kph'),
                                'condition': result.get('forecast').get('forecastday')[day].get('day').get(
                                    'condition').get('text')
                            }
                        )
                return 1
            else:
                raise ValueError
        except ValueError:
            print(f'Days of forecasting must be integer and positive')
            return None
        except Exception as e:
            print(e)
            return None

    def get_results_list(self):
        return self.__results


class DatabaseSender:
    __session_local: Session()
    __currency_data: pd.DataFrame()
    __weather_data: pd.DataFrame()

    def __init__(self):
        engine = create_engine(SQLALCHEMY_DATABASE_URI)
        Base.metadata.create_all(bind=engine)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        self.__session_local = SessionLocal()

    def set_currency_data(self, data):
        self.__currency_data = data

    def set_weather_data(self, data):
        self.__weather_data = data

    def commit_currency_data(self):
        try:
            for index, record in self.__currency_data.iterrows():
                new_record = CurrencyTable(
                    currency_date=record['date'],
                    currency=record['currency'],
                    rate_USD=record['rate_to_USD']
                )
                self.__session_local.add(new_record)
            self.__session_local.commit()
        except Exception as e:
            print(e)
            return None

    def commit_weather_data(self):
        try:
            for index, record in self.__weather_data.iterrows():
                new_record = WeatherTable(
                    city=record['city'],
                    country=record['country'],
                    local_time=record['localtime'],
                    current_temp_c=record['current_temp_c'],
                    forecast_date=record['forecast_date'],
                    forecast_temp_c=record['forecast_temp_c'],
                    wind_speed_kph=record['wind_speed_kph'],
                    condition=record['condition']
                )
                self.__session_local.add(new_record)
            self.__session_local.commit()
        except Exception as e:
            print(e)
            return None


Base = declarative_base()


class CurrencyTable(Base):
    __tablename__ = 'currency_data'
    id = Column(Integer, nullable=False, unique=True, primary_key=True, autoincrement=True)
    currency_date = Column(TIMESTAMP, nullable=False, index=True)
    currency = Column(VARCHAR(50), nullable=False)
    rate_USD = Column(Float, nullable=False)


class WeatherTable(Base):
    __tablename__ = 'weather_data'
    id = Column(Integer, nullable=False, unique=True, primary_key=True, autoincrement=True)
    city = Column(VARCHAR(50), nullable=False)
    country = Column(VARCHAR(50), nullable=False)
    local_time = Column(TIMESTAMP, nullable=False, index=True)
    current_temp_c = Column(Float, nullable=False)
    forecast_date = Column(Date, nullable=False)
    forecast_temp_c = Column(Float, nullable=False)
    wind_speed_kph = Column(Float, nullable=False)
    condition = Column(VARCHAR(50), nullable=False)


class CurrencyParser:
    __url = CURRENCY_API_URL
    __key = CURRENCY_API_KEY
    __currencies = 'USD,RUB,EUR'
    __rates = list()

    def __init__(self):
        pass

    def parse_csv(self):
        try:
            for i in range(31):
                currency_date = str(datetime.now().date() - timedelta(days=i))
                api_request = (f'{self.__url}?access_key={self.__key}&currencies={self.__currencies}&'
                               f'start_date={currency_date}&end_date={currency_date}')
                r = requests.get(url=api_request)
                result = r.json()
                if result.get('quotes') is not None:
                    self.__rates.append({
                        'date': currency_date,
                        'currency': 'RUB',
                        'rate_to_USD': result.get('quotes').get('USDRUB').get('start_rate')
                    })
                    self.__rates.append({
                        'date': currency_date,
                        'currency': 'EUR',
                        'rate_to_USD': result.get('quotes').get('USDEUR').get('start_rate')
                    })

            df = pd.DataFrame(self.__rates)
            df = df.sort_values('date')
            return df
        except Exception as e:
            print(f'Application interrupted ({e})')
            return None


class WeatherParser:
    __weather: Weather

    def __init__(self):
        pass

    def parse_csv(self):
        try:
            self.__weather = Weather()
            self.__weather.add_city('Lisbon')
            self.__weather.add_city('Chicago')
            self.__weather.add_city('Budapest')
            self.__weather.add_city('Stockholm')
            self.__weather.add_city('Tokyo')
            self.__weather.add_city('Moscow')
            self.__weather.add_city('Kazan')
            self.__weather.add_city('Omsk')
            cities = self.__weather.get_cities_list()

            request_handler = RequestHandler()
            if request_handler.send_get_request(cities, 2) is not None:
                request_results = request_handler.get_results_list()
                df = pd.DataFrame(request_results)
                df = df.sort_values('city')
                return df
            else:
                raise ValueError
        except ValueError:
            print('Application interrupted (Incorrect input)')
            return None
        except Exception as e:
            print(f'Application interrupted ({e})')
            return None


if __name__ == '__main__':
    print('Loading...')

    app1 = CurrencyParser()
    app2 = WeatherParser()

    data1 = app1.parse_csv()
    data2 = app2.parse_csv()

    db_sender = DatabaseSender()

    db_sender.set_currency_data(data1)
    db_sender.set_weather_data(data2)
    db_sender.commit_currency_data()
    db_sender.commit_weather_data()
