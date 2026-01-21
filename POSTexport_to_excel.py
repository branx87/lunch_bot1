# Выгрузить содержимое базы данных PostgreSQL
import pandas as pd
from sqlalchemy import create_engine, text
import os

def export_to_excel():
    try:
        # Используем DATABASE_URL из переменных окружения контейнера
        DATABASE_URL = os.getenv('DATABASE_URL')
        if not DATABASE_URL:
            print("Ошибка: DATABASE_URL не установлен")
            return
            
        print(f"Подключаемся к: {DATABASE_URL}")
        
        engine = create_engine(DATABASE_URL)

        # Выгрузка таблиц
        with engine.connect() as conn:
            users_df = pd.read_sql_query(text("SELECT * FROM users"), conn)
            orders_df = pd.read_sql_query(text("SELECT * FROM orders"), conn)
            menu_df = pd.read_sql_query(text("SELECT * FROM menu"), conn)

        # Конвертируем boolean колонки в int для Excel
        boolean_columns = ['is_active', 'is_cancelled', 'is_from_bitrix', 
                          'is_sent_to_bitrix', 'is_preliminary']
        
        for col in boolean_columns:
            if col in orders_df.columns:
                orders_df[col] = orders_df[col].astype(int)

        # Создаем новый Excel файл
        with pd.ExcelWriter("/app/db_export.xlsx", engine='openpyxl') as writer:
            users_df.to_excel(writer, sheet_name='users', index=False)
            orders_df.to_excel(writer, sheet_name='orders', index=False)
            menu_df.to_excel(writer, sheet_name='menu', index=False)

        print("Данные успешно экспортированы в db_export.xlsx")

    except Exception as e:
        print(f"Ошибка при экспорте: {e}")

if __name__ == "__main__":
    export_to_excel()