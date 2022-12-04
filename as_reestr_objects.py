import psycopg2 as pgsql
from psycopg2 import OperationalError
import pandas as pd


file_path = '67\AS_REESTR_OBJECTS_20221024_3b6223f5-806e-42f2-b314-698d5b208f6d.XML'
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
    
    if 'nan' in map(str, i.values()):
        with open('fails.txt', 'a', encoding='utf-8') as file:
            file.write(str(ittaration) + '   ' + ', '.join(map(str, i.values())) + '\n')
        ittaration += 1
        continue


    i['OBJECTGUID'] = "'" + i['OBJECTGUID'] + " '"
    i['UPDATEDATE'] = "'" + i['UPDATEDATE'] + "'"
    i['CREATEDATE'] = "'" + i['CREATEDATE'] + "'"
    if i['ISACTIVE'] == 1:
        i['ISACTIVE'] = 'TRUE'
    else:
        i['ISACTIVE'] = 'FALSE'

    cursor.execute(f'''INSERT INTO as_reestr_objects
                    VALUES({', '.join(map(str, i.values()))});''')
    connection.commit() 
        
    ittaration += 1
cursor.close()
connection.close()