from typing import Any
import requests
import psycopg2


def get_employers(companies: list) -> list[dict[str, Any]]:
    """Функция для получения данных о компаниях и их вакансиях."""
    employers = []
    for company in companies:
        url = f'https://api.hh.ru/employers/{company}'
        response = requests.get(url).json()
        company_response = response['name']
        company_description = filter_strings(response['description'])
        vacancy_response = requests.get(response['vacancies_url']).json()
        employers.append({
            'company': {'name': company_response, 'description': company_description, 'employer_id': company},
            'vacancies': vacancy_response
        })
    return employers


def filter_strings(string: str) -> str:
    """
    Принимает в качестве аргумента строку,
    Возвращает измененную строку без символов, прописанных в списке symbols.
    """

    symbols = ['\n', '<strong>', '\r', '</strong>', '</p>', '<p>', '</li>', '<li>',
               '<b>', '</b>', '<ul>', '<li>', '</li>', '<br />', '</ul>', '<em>']

    for symbol in symbols:
        string = string.replace(symbol, '')

    return string


def filter_salary(salary):
    if salary is not None:
        if salary['from'] is not None and salary['to'] is not None:
            return round((salary['from'] + salary['to']) / 2)
        elif salary['from'] is not None:
            return salary['from']
        elif salary['to'] is not None:
            return salary['to']
    return None


def create_database(database_name: str, params: dict) -> None:
    """Создание базы данных для сохранения данных о работодателях и вакансиях."""

    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f'DROP DATABASE IF EXISTS {database_name}')
    cur.execute(f'CREATE DATABASE {database_name}')

    cur.close()
    conn.close()


def create_tables(database_name, params):
    """Создание таблиц базы данных."""

    conn = psycopg2.connect(dbname=database_name, **params)
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE companies (
                company_id serial PRIMARY KEY,
                company_name varchar(100) NOT NULL,
                description text,
                company_url text,
                vacancies_url text
            )
        """)

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE vacancies (
                vacancy_id serial PRIMARY KEY,
                vacancy_title varchar(150) NOT NULL,
                company_id int REFERENCES companies (company_id) NOT NULL,
                salary int,
                vacancy_url text,
                description text,
                experience text
            )
        """)
    conn.commit()
    conn.close()


def fill_database(employers, database_name, params):
    """Заполнение таблиц базы данных."""

    conn = psycopg2.connect(dbname=database_name, **params)

    with conn.cursor() as cur:
        for employer in employers:
            cur.execute(
                """
                INSERT INTO companies (company_name, description, company_url, vacancies_url)
                VALUES (%s, %s, %s, %s)
                RETURNING company_id
                """,
                (employer['company']['name'], employer['company']['description'],
                 f"https://hh.ru/employer/{employer['company']['employer_id']}", employer['vacancies'].get('alternate_url'))
            )
            company_id = cur.fetchone()[0]
            vacancy_data = employer['vacancies']['items']
            for vacancy in vacancy_data:
                cur.execute(
                    """
                    INSERT INTO vacancies (
                    vacancy_title, company_id, salary, vacancy_url, description, experience
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (vacancy['name'], company_id, filter_salary(vacancy['salary']), vacancy['alternate_url'],
                     vacancy['snippet'].get('responsibility'), vacancy['experience'].get('name'))
                )

    conn.commit()
    conn.close()