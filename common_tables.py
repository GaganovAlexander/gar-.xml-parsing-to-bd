import psycopg2 as pgsql
from psycopg2 import OperationalError
from pandas import read_xml

addhouse_types = read_xml('common\AS_ADDHOUSE_TYPES_20221023_6edb00cd-90b9-41e2-8951-58693f7e49ba.XML', encoding='utf-8').to_dict(orient='records')
addr_obj_types = read_xml('common\AS_ADDR_OBJ_TYPES_20221023_719f6639-9577-40cf-9f2d-66e8947239bf.XML', encoding='utf-8').to_dict(orient='records')
apartaments_types = read_xml('common\AS_APARTMENT_TYPES_20221023_40fe74a3-ae5f-4216-a5fd-846ecc363226.XML', encoding='utf-8').to_dict(orient='records')
house_types = read_xml('common\AS_HOUSE_TYPES_20221023_a7647b2d-eafd-4a73-a9f6-fb75e4995c8d.XML', encoding='utf-8').to_dict(orient='records')

obj_levels = read_xml('common\AS_OBJECT_LEVELS_20221023_384a37a5-482f-4f15-9ca1-8817a2fad6d3.XML', encoding='utf-8').to_dict(orient='records')
operation_types = read_xml('common\AS_OPERATION_TYPES_20221023_43bb6aee-a571-49f9-80d4-1628b2425b51.XML', encoding='utf-8').to_dict(orient='records')
params_types = read_xml('common\AS_PARAM_TYPES_20221023_784f9c15-a6a9-4f73-9ea3-2857c62a8ca4.XML', encoding='utf-8').to_dict(orient='records')
room_types = read_xml('common\AS_ROOM_TYPES_20221023_8dd78183-10b3-4236-a1b9-bd978057af41.XML', encoding='utf-8').to_dict(orient='records')

print('Подключение к БД PostgreSQL')
try:
    connection = pgsql.connect(database='gar', user='gar', password='gar', host='10.0.72.12', port='5432')
    print('Подключение к базе данных выполнено успешно')
except OperationalError as error:
    print(f'Ошибка подключения к БД: {error}')
    exit()
cursor = connection.cursor()

for i in (addhouse_types, addr_obj_types, apartaments_types, house_types, obj_levels, operation_types, params_types, room_types):
    for j in i:
        j['ISACTIVE'] = str(j['ISACTIVE']).upper()
        for k in ('NAME', 'SHORTNAME', 'DESC', 'CODE', 'STARTDATE', 'UPDATEDATE', 'ENDDATE'):
            try:
                j[k] = "'" + j[k] + "'"
            except:
                pass
        try:
            if not j['SHORTNAME']:
                j['SHORTNAME'] = 'NULL'
        except:
            pass
    

            
for i in addhouse_types:
    cursor.execute(f'''INSERT INTO as_addhouses_types VALUES({', '.join(map(str, i.values()))})''')
    connection.commit()
for i in addr_obj_types:
    cursor.execute(f'''INSERT INTO as_addr_obj_type VALUES({', '.join(map(str, i.values()))})''')
    connection.commit()
for i in apartaments_types:
    cursor.execute(f'''INSERT INTO as_apartment_types VALUES({', '.join(map(str, i.values()))})''')
    connection.commit()
for i in house_types:
    cursor.execute(f'''INSERT INTO as_house_types VALUES({', '.join(map(str, i.values()))})''')
    connection.commit()

for i in obj_levels:
    cursor.execute(f'''INSERT INTO as_object_levels VALUES({', '.join(map(str, i.values()))})''')
    connection.commit()
for i in operation_types:
    cursor.execute(f'''INSERT INTO as_operation_types VALUES({', '.join(map(str, i.values()))})''')
    connection.commit()
for i in params_types:
    cursor.execute(f'''INSERT INTO as_param_types VALUES({', '.join(map(str, i.values()))})''')
    connection.commit()
for i in room_types:
    cursor.execute(f'''INSERT INTO as_room_types VALUES({', '.join(map(str, i.values()))})''')
    connection.commit()
        
cursor.close()
connection.close()