import os
import psycopg2

from dotenv import load_dotenv

load_dotenv()


def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except psycopg2.OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection


def create_database(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
    except psycopg2.OperationalError as e:
        print(f"The error '{e}' occurred")


def execute_query(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
    except psycopg2.OperationalError as e:
        print(f"The error '{e}' occurred")


create_cities_table = """
CREATE TABLE IF NOT EXISTS city (
id SERIAL PRIMARY KEY,
name TEXT NOT NULL,
latitude REAL,
longitude REAL,
population INTEGER
);
"""


create_weather_table = """
CREATE TABLE IF NOT EXISTS weather (
id SERIAL PRIMARY KEY,
temp REAL,
feels REAL,
humidity INTEGER,
time TIMESTAMP,
city_id INTEGER REFERENCES city(id)
);
"""


if __name__ == '__main__':
    connection = create_connection(
        os.getenv('DB_ENGINE'),
        os.getenv('POSTGRES_USER'),
        os.getenv('POSTGRES_PASSWORD'),
        os.getenv('DB_HOST'),
        os.getenv('DB_PORT')
    )
    create_database_query = 'CREATE DATABASE weather_fortdata'
    create_database(connection, create_database_query)
    connection = create_connection(
        'weather_fortdata',
        os.getenv('POSTGRES_USER'),
        os.getenv('POSTGRES_PASSWORD'),
        os.getenv('DB_HOST'),
        os.getenv('DB_PORT')
    )
    execute_query(connection, create_cities_table)
    execute_query(connection, create_weather_table)
