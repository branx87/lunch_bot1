##import_from_excel.py
# загрузка заказов
import sqlite3
import pandas as pd

conn = sqlite3.connect(r"C:\projects\lunch_bot\lunch_bot.db")
df = pd.read_excel("orders.xlsx", sheet_name="orders")
df.to_sql("orders", conn, if_exists="replace", index=False)  # "replace" удалит старые данные
conn.close()

# загрузка сотрудников
import sqlite3
import pandas as pd

conn = sqlite3.connect(r"C:\projects\lunch_bot\lunch_bot.db")

# Чтение Excel с явным указанием типов
df = pd.read_excel("users.xlsx", dtype={
    'telegram_id': 'Int64',
    'phone': 'string'
})

# Очистка таблицы перед импортом
conn.execute("DELETE FROM users;")

# Импорт с правильными типами
df.to_sql("users", conn, if_exists="append", index=False)

conn.close()