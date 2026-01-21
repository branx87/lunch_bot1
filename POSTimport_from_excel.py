# Загрузка в базу данных PostgreSQL
import pandas as pd
from sqlalchemy import create_engine, text
import os
from datetime import datetime

def import_from_excel():
    try:
        # Используем DATABASE_URL из переменных окружения контейнера
        DATABASE_URL = os.getenv('DATABASE_URL')
        if not DATABASE_URL:
            print("Ошибка: DATABASE_URL не установлен")
            return
            
        print(f"Подключаемся к: {DATABASE_URL}")
        
        engine = create_engine(DATABASE_URL)

        # Загрузка данных из Excel
        users_df = pd.read_excel("/app/db_export.xlsx", sheet_name="users", dtype={
            'telegram_id': 'Int64',
            'phone': 'string'
        })

        orders_df = pd.read_excel("/app/db_export.xlsx", sheet_name="orders", dtype={
            'bitrix_order_id': 'Int64'
        })
        
        menu_df = pd.read_excel("/app/db_export.xlsx", sheet_name="menu", dtype={
            'id': 'Int64'
        })

        # Конвертируем числовые boolean колонки обратно в boolean
        boolean_columns = ['is_active', 'is_cancelled', 'is_from_bitrix', 
                          'is_sent_to_bitrix', 'is_preliminary']
        
        for col in boolean_columns:
            if col in orders_df.columns:
                # Конвертируем 1/0 в True/False
                orders_df[col] = orders_df[col].astype(bool)

        # Конвертируем bitrix_order_id
        orders_df['bitrix_order_id'] = orders_df['bitrix_order_id'].astype('Int64')

        # КОНВЕРТАЦИЯ ДАТ для users_df
        date_columns = ['employment_date', 'created_at', 'updated_at']  # добавьте другие колонки с датами если нужно
        
        for col in date_columns:
            if col in users_df.columns:
                print(f"Конвертируем даты в колонке {col}...")
                # Конвертируем из DD.MM.YYYY в YYYY-MM-DD
                users_df[col] = pd.to_datetime(users_df[col], format='%d.%m.%Y', errors='coerce')
                # Для смешанных форматов (если есть и даты и datetime)
                # users_df[col] = pd.to_datetime(users_df[col], dayfirst=True, errors='coerce')

        # Очистка и загрузка данных
        with engine.begin() as conn:
            # Очищаем таблицы в правильном порядке
            conn.execute(text("DELETE FROM orders;"))
            conn.execute(text("DELETE FROM menu;"))
            conn.execute(text("DELETE FROM users;"))
            
            # Загружаем данные
            users_df.to_sql("users", conn, if_exists="append", index=False, method='multi')
            menu_df.to_sql("menu", conn, if_exists="append", index=False, method='multi')
            orders_df.to_sql("orders", conn, if_exists="append", index=False, method='multi')

        print("Данные успешно импортированы из db_export.xlsx в PostgreSQL")

    except Exception as e:
        print(f"Ошибка при импорте: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    import_from_excel()