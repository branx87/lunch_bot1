#!/usr/bin/env python3
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_database():
    try:
        from database import db
        logger.info("🔄 Инициализация таблиц в PostgreSQL...")
        db.init_db()
        logger.info("✅ Таблицы созданы в PostgreSQL!")
        
        # Проверим что таблицы есть
        with db.get_session() as session:
            from models import User
            count = session.query(User).count()
            logger.info(f"✅ Проверка: в таблице users {count} записей")
            
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации: {e}")
        raise

if __name__ == "__main__":
    init_database()