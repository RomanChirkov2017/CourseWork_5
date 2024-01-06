import psycopg2
from config import config


class DBManager:
    def __init__(self, database_name, params=config()):
        self.database_name = database_name
        self.params = params

    def get_companies_and_vacancies_count(self):
        """Получает список всех компаний и количество вакансий у каждой компании."""
        conn = psycopg2.connect(dbname=self.database_name, **self.params)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT company_name, COUNT(vacancy_title) AS number_of_vacancies
                FROM companies
                JOIN vacancies USING (company_id)
                GROUP BY company_name;
                """
            )
            data = cur.fetchall()
        conn.close()
        return data


    def get_all_vacancies(self):
        """
        Получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию.
        """
        conn = psycopg2.connect(dbname=self.database_name, **self.params)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT vacancy_title, company_name, salary, vacancy_url
                FROM vacancies
                JOIN companies USING (company_id);
                """
            )
            data = cur.fetchall()
        conn.close()
        return data


    def get_avg_salary(self):
        """Получает среднюю зарплату по вакансиям."""
        conn = psycopg2.connect(dbname=self.database_name, **self.params)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT company_name, round(AVG(salary)) AS average_salary
                FROM companies
                JOIN vacancies USING (company_id)
                GROUP BY company_name;
                """
            )
            data = cur.fetchall()
        conn.close()
        return data

    def get_vacancies_with_higher_salary(self):
        """
        Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.
        """
        conn = psycopg2.connect(dbname=self.database_name, **self.params)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM vacancies
                WHERE salary > (SELECT AVG(salary) FROM vacancies);
                """
            )
            data = cur.fetchall()
        conn.close()
        return data

    def get_vacancies_with_keyword(self, keyword):
        """
        Получает список всех вакансий, в названии которых содержатся переданные в метод слова.
        """
        conn = psycopg2.connect(dbname=self.database_name, **self.params)
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT * FROM vacancies
                WHERE lower(vacancy_title) LIKE '%{keyword}%'
                OR lower(vacancy_title) LIKE '%{keyword}'
                OR lower(vacancy_title) LIKE '{keyword}%';
                """
            )
            data = cur.fetchall()
        conn.close()
        return data
