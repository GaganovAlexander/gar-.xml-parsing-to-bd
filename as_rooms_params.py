import psycopg2 as pgsql
from psycopg2 import OperationalError
import pandas as pd




file_path = '67\AS_ROOMS_PARAMS_20221024_20649bca-dd58-4841-bc1a-7c8160ed5d79.XML'
f = pd.read_xml(file_path, encoding='utf-8').to_dict(orient='records')

print('Подключение к БД PostgreSQL')
try:
    connection = pgsql.connect(database='gar', user='gar', password='gar', host='10.0.72.12', port='5432')
    print('Подключение к базе данных выполнено успешно')
except OperationalError as error:
    print(f'Ошибка подключения к БД: {error}')

cursor = connection.cursor()


ittaration = 0
for i in f:
    if ittaration % 20 == 0:
        print(1)
    if 'nan' in map(str, i.values()):
        with open('fails.txt', 'a', encoding='utf-8') as file:
            file.write(str(ittaration) + '   ' + ', '.join(map(str, i.values())) + '\n')
        ittaration += 1
        continue
    i['VALUE'] = "'" + i['VALUE'] + "'"
    i['UPDATEDATE'] = "'" + i['UPDATEDATE'] + "'"
    i['STARTDATE'] = "'" + i['STARTDATE'] + "'"
    i['ENDDATE'] = "'" + i['ENDDATE'] + "'"

    cursor.execute(f'''INSERT INTO as_rooms_params VALUES({', '.join(map(str, i.values()))})''')
    connection.commit()
    ittaration += 1

cursor.close()
connection.close()