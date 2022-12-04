import psycopg2 as pgsql
from psycopg2 import OperationalError
import pandas as pd


file_path = '67\AS_ADDR_OBJ_PARAMS_20221024_961a4e9e-9a3b-47db-acb1-59aecc9c5bf9.XML'
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
    if ittaration % 200 == 0:
        print(ittaration)
    
    if str(i['VALUE']) == 'nan':
        i['VALUE'] = 'NULL'
    else:   
        i['VALUE'] = "'" + i['VALUE'] + "'"
    i['UPDATEDATE'] = "'" + i['UPDATEDATE'] + "'"
    i['STARTDATE'] = "'" + i['STARTDATE'] + "'"
    i['ENDDATE'] = "'" + i['ENDDATE'] + "'"
    cursor.execute(f'''INSERT INTO as_addr_obj_params
                    VALUES({', '.join(map(str, i.values()))});''')
    connection.commit() 
        
    ittaration += 1
cursor.close()
connection.close()