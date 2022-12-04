import psycopg2 as pgsql
from psycopg2 import OperationalError
import pandas as pd


file_path = '67\AS_ADDR_OBJ_DIVISION_20221024_74408f9d-c5e3-4416-ab15-4cf7e4d45ff7.XML'
f = pd.read_xml(file_path, encoding='utf-8').to_dict(orient='records')

print('Подключение к БД PostgreSQL')
try:
    connection = pgsql.connect(database='gar', user='gar', password='gar', host='10.0.72.12', port='5432')
    print('Подключение к базе данных выполнено успешно')
except OperationalError as error:
    print(f'Ошибка подключения к БД: {error}')

cursor = connection.cursor()

for i in f:
    cursor.execute(f'''INSERT INTO as_addr_obj_division VALUES({', '.join(map(str, i.values()))})''')
    connection.commit()

cursor.close()
connection.close()