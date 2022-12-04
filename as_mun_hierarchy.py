import psycopg2 as pgsql
from psycopg2 import OperationalError
import pandas as pd


file_path = '67\AS_MUN_HIERARCHY_20221024_097e726e-68ef-4d3d-a25e-728c6c250219.XML'
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

    for j in i.keys():
        if str(i[j]) == 'None' or str(i[j]) == 'nan':
            i[j] = 'NULL' 
        elif j in ('UPDATEDATE', 'STARTDATE', 'ENDDATE', 'PATH', 'OKTMO'):
            i[j] = "'" + str(i[j]) + "'"

    if i['ISACTIVE'] == 1:
        i['ISACTIVE'] = 'TRUE'
    elif i['ISACTIVE'] == 0:
        i['ISACTIVE'] = 'FALSE'
    cursor.execute(f'''INSERT INTO as_mun_hierarchy
                    VALUES({', '.join(map(str, i.values()))});''')
    connection.commit() 
        
    ittaration += 1
    
cursor.close()
connection.close()
