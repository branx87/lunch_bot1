# ##migration_script.py
import sqlite3
from database import db
from models import *
from sqlalchemy.orm import Session

def migrate_from_sqlite(sqlite_path, postgres_session):
    """Миграция данных из SQLite в PostgreSQL"""
    
    # Подключение к SQLite
    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_conn.row_factory = sqlite3.Row
    
    try:
        # Миграция пользователей
        users = sqlite_conn.execute('SELECT * FROM users').fetchall()
        for user in users:
            new_user = User(
                id=user['id'],
                bitrix_id=user['bitrix_id'],
                crm_employee_id=user['crm_employee_id'],
                telegram_id=user['telegram_id'],
                full_name=user['full_name'],
                position=user['position'],
                department=user['department'],
                phone=user['phone'],
                location=user['location'],
                city=user.get('city'),
                is_verified=bool(user['is_verified']),
                is_employee=bool(user['is_employee']),
                username=user['username'],
                is_deleted=bool(user['is_deleted']),
                notifications_enabled=bool(user['notifications_enabled']),
                bitrix_entity_type=user['bitrix_entity_type'],
                created_at=user['created_at'],
                updated_at=user['updated_at']
            )
            postgres_session.merge(new_user)
        
        # Аналогично для других таблиц...
        postgres_session.commit()
        print("✅ Миграция завершена успешно")
        
    except Exception as e:
        postgres_session.rollback()
        print(f"❌ Ошибка миграции: {e}")
        raise
    finally:
        sqlite_conn.close()