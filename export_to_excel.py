##export_to_excel.py
# Выгрузить содержимое бахы данных
import sqlite3
import pandas as pd

# Подключение к базе данных
conn = sqlite3.connect(r"D:\lunch_bot1\lunch_bot.db")

# Выгрузка таблицы users
users_df = pd.read_sql_query("SELECT * FROM users", conn)
users_df.to_excel("db_export.xlsx", sheet_name='users', index=False)

# Выгрузка таблицы orders
orders_df = pd.read_sql_query("SELECT * FROM orders", conn)
with pd.ExcelWriter("db_export.xlsx", mode='a', engine='openpyxl') as writer: 
    orders_df.to_excel(writer, sheet_name='orders', index=False)

conn.close()
print("Данные успешно экспортированы в db_export.xlsx")