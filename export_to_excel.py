# ##export_to_excel.py
import sqlite3
import pandas as pd

conn = sqlite3.connect(r"C:\projects\lunch_bot\lunch_bot.db")
df = pd.read_sql_query("SELECT * FROM users", conn)
df.to_excel("users.xlsx", index=False, sheet_name="users")
conn.close()