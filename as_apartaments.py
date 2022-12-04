import psycopg2 as pgsql
from psycopg2 import OperationalError
import pandas as pd


file_path = '67\AS_APARTMENTS_20221024_f9648f79-fb64-41aa-9bdb-c88cd31285d3.XML'
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
    
    if str(i['NUMBER']) == 'nan':
        i['NUMBER'] = 'NULL'
    else:    
        i['NUMBER'] = "'" + i['NUMBER'] + "'" 
    if str(i['NEXTID']) == 'nan':
        i['NEXTID'] = 'NULL'
    else:    
        i['NEXTID'] = "'" + i['NEXTID'] + "'" 
    if str(i['PREVID']) == 'nan':
        i['PREVID'] = 'NULL'
    else:    
        i['PREVID'] = "'" + i['PREVID'] + "'" 
    


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
    cursor.execute(f'''INSERT INTO as_apartments
                    VALUES({', '.join(map(str, i.values()))});''')
    connection.commit() 
        
    ittaration += 1
    
cursor.close()
connection.close()