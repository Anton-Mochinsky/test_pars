import psycopg2
import requests
import re
# import schedule
# import time

# def job():
# Подключение к json файлу
response = requests.get('https://raw.githubusercontent.com/Hipo/university-domains-list/master/world_universities_and_domains.json')
data = response.json()


# Подключение к базе данных
conn = psycopg2.connect(
    database="postgres", user='postgres', password='postgres', host='localhost', port='5432'
)
cursor = conn.cursor()

# Функция определения типа заведения


def search_type(name):
    if re.search(r'Col\w*lege\b', name, flags=re.IGNORECASE):
        return 'College'
    elif re.search(r'Uni\w*vers\w*|Ünivers', name, flags=re.IGNORECASE):  # Добавляем Ünivers в качестве ключевого слова
        return 'University'
    elif re.search(r'In\w*stitute\b|\w*(\'|\/*)\w*', name, flags=re.IGNORECASE):
        return 'Institute'
    else:
        return None




# Обработка объектов


new_institutions = []
for item in data:
    name = item.get('name')
    cursor.execute("SELECT * FROM institutions WHERE name = %s", (name,))
    existing_university = cursor.fetchone()
    if existing_university:
        continue  # пропускаем запись, если университет уже существует
    country = item.get('country')
    alpha_two_code = item.get('alpha_two_code')
    state_province = item.get('state-province')
    type_institution = search_type(name)
    cursor.execute("INSERT INTO institutions (name, country, alpha_two_code, state_province, type_institution) VALUES (%s, %s, %s, %s, %s)", (name, country, alpha_two_code, state_province, type_institution))
    conn.commit()
    new_institutions.append((name, country, alpha_two_code, state_province, type_institution))

cursor.execute("SELECT * FROM institutions ORDER BY type_institution")
result = cursor.fetchall()

print("Список всех записей с сортировкой по типу заведения:")
for row in result:
    print(row)

cursor.execute("SELECT * FROM institutions WHERE type_institution = 'College' ORDER BY name")
result = cursor.fetchall()

print("Список всех колледжей:")
for row in result:
    print(row)

cursor.execute("SELECT * FROM institutions WHERE type_institution = 'Institute' ORDER BY name")
result = cursor.fetchall()

print("Список всех институтов:")
for row in result:
    print(row)

cursor.execute("SELECT * FROM institutions WHERE type_institution = 'University' ORDER BY name")
result = cursor.fetchall()

print("Список всех университетов:")
for row in result:
    print(row)

cursor.execute("SELECT * FROM institutions WHERE type_institution IS NULL ORDER BY name")
result = cursor.fetchall()

print("Список учреждений, у которых тип не был определен:")
for row in result:
    print(row)

cursor.close()
conn.close()
# cursor.execute("DELETE FROM institutions")
# conn.commit()
#
# print("Все записи были успешно удалены из таблицы institutions")

# schedule.every().day.at("03:00").do(job)
#
# while True:
#     schedule.run_pending()
#     time.sleep(1)