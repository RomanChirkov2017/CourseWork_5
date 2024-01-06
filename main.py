from src.DBManager import DBManager
from src.utils import get_employers, create_database, create_tables, fill_database
from config import config



def main():
    companies = [
        78638,  # Тинькофф
        3529,  # Сбер IT
        5390761,  # Совкомбанк
        2324020,  # Точка
        2136954,  # Домклик
        1102601,  # Самолет
        4649269,  # Иннотех
        1740,  # Яндекс
        26624,  # Positive Technologies
        1057  # Лаборатория Касперского
    ]

    database_name = 'course_work_5'
    params = config()

    create_database(database_name, params)
    create_tables(database_name, params)
    fill_database(get_employers(companies), database_name, params)

    db_manager = DBManager(database_name, params)

if __name__ == '__main__':
    main()
