import psycopg2 as pgsql
from psycopg2 import OperationalError
import pandas as pd


file_path = '67\AS_HOUSES_20221024_7e01289c-bb84-40af-b4e4-baac64ec9927.XML'
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
        elif j in ('HOUSENUM', 'ADDNUM1', 'ADDNUM2'):
            i[j] = "'" + i[j] + "'"    
    i['OBJECTGUID'] = "'" + i['OBJECTGUID'] + " '"
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
    cursor.execute(f'''INSERT INTO as_houses (id, objectid, objectguid, changeid, housenum, housetype,
    opertypeid, previd, nextid, updatedate, startdate, enddate, isactual, isactive, addnum1, addtype1, addnum2, addtype2)
                    VALUES({', '.join(map(str, i.values()))});''')
    connection.commit() 
        
    ittaration += 1
cursor.close()
connection.close()