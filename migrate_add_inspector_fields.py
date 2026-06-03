"""
Миграция: добавляет поле is_for_inspector в таблицу orders.

Использование:
    python migrate_add_inspector_fields.py
"""
import os
import sys
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Добавляем корень проекта в путь
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import db
from models import Order
from sqlalchemy import text


def run_migration():
    """Добавляет колонку is_for_inspector в таблицу orders."""
    try:
        with db.get_session() as session:
            # Проверяем, существует ли уже колонка is_for_inspector
            result = session.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'orders' AND column_name = 'is_for_inspector'
            """))
            if result.fetchone():
                logger.info("✅ Колонка is_for_inspector уже существует")
            else:
                logger.info("➕ Добавляем колонку is_for_inspector...")
                session.execute(text("""
                    ALTER TABLE orders 
                    ADD COLUMN is_for_inspector BOOLEAN DEFAULT FALSE
                """))
                logger.info("✅ Колонка is_for_inspector добавлена")
            
            # На случай если была добавлена inspector_name (из предыдущей версии) — удаляем
            result = session.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'orders' AND column_name = 'inspector_name'
            """))
            if result.fetchone():
                logger.info("🗑️ Удаляем более не нужную колонку inspector_name...")
                session.execute(text("""
                    ALTER TABLE orders DROP COLUMN inspector_name
                """))
                logger.info("✅ Колонка inspector_name удалена")
            
            session.commit()
            logger.info("✅ Миграция успешно завершена")
                
    except Exception as e:
        logger.error(f"❌ Ошибка миграции: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    run_migration()
