import os
from sqlalchemy import create_engine, text  # ← ДОБАВИТЬ text
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import SQLAlchemyError
from contextlib import contextmanager
import logging
from models import Base, User, Order, Menu, Holiday, AdminMessage, BitrixMapping, FeedbackMessage, BotSetting

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        self.database_url = os.getenv('DATABASE_URL', 'postgresql://bot_user:password@localhost:5432/lunch_bot')
        self.engine = create_engine(self.database_url, pool_pre_ping=True, pool_recycle=300)
        self.SessionLocal = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=self.engine))
        self.session = self.SessionLocal()
        
    def init_db(self):
        """Инициализация таблиц"""
        try:
            Base.metadata.create_all(bind=self.engine)
            logger.info("✅ Таблицы базы данных созданы/проверены")
            
            # Исправляем последовательности для всех таблиц
            self.fix_sequences()
            
        except SQLAlchemyError as e:
            logger.error(f"❌ Ошибка инициализации БД: {e}")
            raise

    def fix_sequences(self):
        """Исправляет последовательности ID для всех таблиц"""
        try:
            with self.get_session() as session:
                # Для всех таблиц с автоинкрементом
                tables = ['orders', 'users', 'holidays', 'menu', 'admin_messages', 'feedback_messages', 'bot_settings']
                for table in tables:
                    try:
                        session.execute(text(f"SELECT setval('{table}_id_seq', COALESCE((SELECT MAX(id) FROM {table}), 1))"))
                    except Exception as e:
                        logger.warning(f"Не удалось исправить последовательность для {table}: {e}")
                session.commit()
            logger.info("✅ Последовательности ID исправлены")
        except Exception as e:
            logger.error(f"❌ Ошибка исправления последовательностей: {e}")
    
    @contextmanager
    def get_session(self):
        """Контекстный менеджер для сессий"""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def get_db(self):
        """Генератор сессий для зависимостей"""
        with self.get_session() as session:
            yield session

    def reset_failed_transaction(self):
        """Сбрасывает состояние транзакции при ошибках"""
        try:
            self.session.rollback()
            logger.info("✅ Сброшена неудачная транзакция")
        except Exception as e:
            logger.error(f"❌ Ошибка сброса транзакции: {e}")
            self.session = self.SessionLocal()

    def reconnect(self):
        """Переподключение к базе данных после восстановления из бекапа"""
        try:
            # Закрываем текущую сессию
            self.session.close()
            # Удаляем все сессии из пула
            self.SessionLocal.remove()
            # Пересоздаем engine и сессии
            self.engine.dispose()
            self.engine = create_engine(self.database_url, pool_pre_ping=True, pool_recycle=300)
            self.SessionLocal = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=self.engine))
            self.session = self.SessionLocal()
            logger.info("✅ Переподключение к БД выполнено")
        except Exception as e:
            logger.error(f"❌ Ошибка переподключения к БД: {e}")
            raise

# Глобальный экземпляр БД
db = Database()