import datetime
import csv
import requests
import os
import psycopg2
import schedule
import time

from dotenv import load_dotenv

from script_bd import create_connection

load_dotenv()


DATAFILE = os.getenv('DATAFILE')
APPID = os.getenv('APPID')


def get_city_to_bd(filepath):
    cities = []
    with open(filepath, encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            name_city = row['name']
            latitude_city = float(row['latitude'])
            longitude_city = float(row['longitude'])
            population_city = int(row['population'])
            cities.append((name_city, latitude_city, longitude_city, population_city))

    cities_records = ', '.join(["%s"] * len(cities))

    insert_query = (
        f'INSERT INTO city (name, latitude, longitude, population) '
        f'VALUES {cities_records}'
    )

    connection.autocommit = True
    cursor = connection.cursor()
    cursor.execute(insert_query, cities)


def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except psycopg2.OperationalError as e:
        print(f"The error '{e}' occurred")


def get_weather_to_bd():
    select_cities = "SELECT * FROM city"
    cities = execute_read_query(connection, select_cities)
    weath = []

    for row in cities:
        latitude = row[2]
        longitude = row[3]
        city_id = row[0]
        info_result = requests.get(
            f'https://api.openweathermap.org/data/2.5/weather?lat={latitude}'
            f'&lon={longitude}&appid={APPID}&units=metric'
        )
        weather_info = info_result.json()
        temp = weather_info['main']['temp']
        feels = weather_info['main']['feels_like']
        humidity = weather_info['main']['humidity']
        time = datetime.datetime.utcnow()
        weath.append((temp, feels, humidity, time, city_id))

    weath_records = ', '.join(["%s"] * len(weath))

    insert_query = (
        f'INSERT INTO weather (temp, feels, humidity, time, city_id) '
        f'VALUES {weath_records}'
    )

    connection.autocommit = True
    cursor = connection.cursor()
    cursor.execute(insert_query, weath)


def scheduler():
    schedule.every().hour.do(get_weather_to_bd)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    connection = create_connection(
        'weather_fortdata',
        os.getenv('POSTGRES_USER'),
        os.getenv('POSTGRES_PASSWORD'),
        os.getenv('DB_HOST'),
        os.getenv('DB_PORT')
    )
    get_city_to_bd(DATAFILE)
    get_weather_to_bd()
