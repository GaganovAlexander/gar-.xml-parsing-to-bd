import psycopg2 as pgsql
from psycopg2 import OperationalError
import pandas as pd


file_path = '67\AS_ADDR_OBJ_20221024_119b207a-545f-447f-bd19-7c282d262513.XML'
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

    if str(i['PREVID']) == 'nan':
        i['PREVID'] = 'NULL'
    if str(i['NEXTID']) == 'nan':
        i['NEXTID'] = 'NULL'  
    i['OBJECTGUID'] = "'" + i['OBJECTGUID'] + " '"
    i['NAME'] = "'" + i['NAME'] + "'"
    i['TYPENAME'] = "'" + i['TYPENAME'] + "'"
    i['UPDATEDATE'] = "'" + i['UPDATEDATE'] + "'"
    i['STARTDATE'] = "'" + i['STARTDATE'] + "'"
    i['ENDDATE'] = "'" + i['ENDDATE'] + "'"
    if i['ISACTUAL'] == 1:
        i['ISACTUAL'] = 'TRUE'
    else:
        i['ISACTUAL'] = 'FALSE'
    if i['ISACTIVE'] == 1:
        i['ISACTIVE'] = 'TRUE'
    else:
        i['ISACTIVE'] = 'FALSE'
    cursor.execute(f'''INSERT INTO as_addr_obj
                    VALUES({', '.join(map(str, i.values()))});''')
    connection.commit() 
        
    ittaration += 1
cursor.close()
connection.close()