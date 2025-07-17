##import_from_excel.py
# Загрузка в базу данных
import sqlite3
import pandas as pd

# Подключение к базе данных
conn = sqlite3.connect(r"C:\projects\lunch_bot\lunch_bot.db")

# Загрузка users
users_df = pd.read_excel("db_export.xlsx", sheet_name="users", dtype={
    'telegram_id': 'Int64',
    'phone': 'string'
})

# Очистка и загрузка таблицы users
conn.execute("DELETE FROM users;")
users_df.to_sql("users", conn, if_exists="append", index=False)

# Загрузка orders с указанием типа для bitrix_order_id
orders_df = pd.read_excel("db_export.xlsx", sheet_name="orders", dtype={
    'bitrix_order_id': 'Int64'  # Используем Int64 для поддержки целых чисел
})

# Конвертируем float в int (на случай, если в данных уже есть значения с .0)
orders_df['bitrix_order_id'] = orders_df['bitrix_order_id'].astype('Int64')

# Очистка и загрузка таблицы orders
conn.execute("DELETE FROM orders;")
orders_df.to_sql("orders", conn, if_exists="append", index=False)

conn.close()
print("Данные успешно импортированы из db_export.xlsx в базу данных")