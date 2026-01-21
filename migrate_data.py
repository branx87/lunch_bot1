import sqlite3
import psycopg2
from datetime import datetime
import os
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv('data/configs/.env')

def migrate_all_data():
    """Мигрирует все данные из SQLite в PostgreSQL"""
    
    # Подключаемся к SQLite
    sqlite_conn = sqlite3.connect('data/db/lunch_bot.db')
    sqlite_cursor = sqlite_conn.cursor()
    
    # Подключаемся к PostgreSQL
    pg_conn = psycopg2.connect(os.getenv('DATABASE_URL'))
    pg_cursor = pg_conn.cursor()
    
    try:
        print("🔄 Начинаем миграцию данных...")
        
        # 1. Мигрируем пользователей
        print("📊 Мигрируем пользователей...")
        sqlite_cursor.execute("SELECT * FROM users")
        users = sqlite_cursor.fetchall()
        
        for user in users:
            # Добавляем недостающие значения для новых полей
            user_data = list(user)
            # Если в SQLite меньше полей чем в PostgreSQL, дополняем None
            while len(user_data) < 19:  # 19 полей в таблице users
                user_data.append(None)
            
            pg_cursor.execute("""
                INSERT INTO users (
                    id, bitrix_id, crm_employee_id, telegram_id, full_name, 
                    position, department, phone, location, city, is_verified, 
                    is_employee, username, is_deleted, notifications_enabled, 
                    bitrix_entity_type, employment_date, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, user_data)
        
        # 2. Мигрируем заказы
        print("📦 Мигрируем заказы...")
        sqlite_cursor.execute("SELECT * FROM orders")
        orders = sqlite_cursor.fetchall()
        
        for order in orders:
            order_data = list(order)
            while len(order_data) < 15:  # 15 полей в таблице orders
                order_data.append(None)
            
            pg_cursor.execute("""
                INSERT INTO orders (
                    id, bitrix_order_id, is_active, user_id, target_date, 
                    order_time, quantity, bitrix_quantity_id, is_cancelled, 
                    is_from_bitrix, is_sent_to_bitrix, is_preliminary, 
                    created_at, updated_at, last_synced_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, order_data)
        
        # 3. Мигрируем праздники
        print("🎉 Мигрируем праздники...")
        sqlite_cursor.execute("SELECT * FROM holidays")
        holidays = sqlite_cursor.fetchall()
        
        for holiday in holidays:
            holiday_data = list(holiday)
            while len(holiday_data) < 5:  # 5 полей в таблице holidays
                holiday_data.append(None)
            
            pg_cursor.execute("""
                INSERT INTO holidays (id, date, name, is_recurring, created_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, holiday_data)
        
        # 4. Мигрируем меню
        print("🍽️ Мигрируем меню...")
        sqlite_cursor.execute("SELECT * FROM menu")
        menu_items = sqlite_cursor.fetchall()
        
        for item in menu_items:
            item_data = list(item)
            while len(item_data) < 7:  # 7 полей в таблице menu
                item_data.append(None)
            
            pg_cursor.execute("""
                INSERT INTO menu (id, day, first_course, main_course, salad, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, item_data)
        
        # 5. Мигрируем настройки бота
        print("⚙️ Мигрируем настройки...")
        sqlite_cursor.execute("SELECT * FROM bot_settings")
        settings = sqlite_cursor.fetchall()
        
        for setting in settings:
            setting_data = list(setting)
            while len(setting_data) < 4:  # 4 поля в таблице bot_settings
                setting_data.append(None)
            
            pg_cursor.execute("""
                INSERT INTO bot_settings (id, setting_name, setting_value, updated_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, setting_data)
        
        pg_conn.commit()
        print("✅ Миграция данных завершена успешно!")
        
    except Exception as e:
        pg_conn.rollback()
        print(f"❌ Ошибка миграции: {e}")
        raise
    finally:
        sqlite_conn.close()
        pg_conn.close()

if __name__ == "__main__":
    migrate_all_data()