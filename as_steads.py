import psycopg2 as pgsql
from psycopg2 import OperationalError
import pandas as pd


file_path = '67\AS_STEADS_20221024_6596f7f5-4d73-449d-b349-6500fd5b056c.XML'
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
        elif j == 'NUMBER':
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
    cursor.execute(f'''INSERT INTO as_steads
                    VALUES({', '.join(map(str, i.values()))});''')
    connection.commit() 
        
    ittaration += 1
    
cursor.close()
connection.close()