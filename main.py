import asyncio
import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler
from datetime import datetime, time
import signal
import atexit
import sys

# 1. СНАЧАЛА настраиваем логирование
def setup_logging():
    # Создаем папку для логов, если ее нет
    logs_dir = Path('data/logs')
    logs_dir.mkdir(parents=True, exist_ok=True)
    
    handler = RotatingFileHandler(
        logs_dir / 'bot.log',
        maxBytes=5*1024*1024,
        backupCount=3,
        encoding='utf-8'
    )
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[handler]
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("telegram").setLevel(logging.WARNING)

# Настраиваем логирование СРАЗУ
setup_logging()
logger = logging.getLogger(__name__)

# 2. ПОТОМ импортируем остальные модули
try:
    from database import db
    from models import Base
    from config import CONFIG
    
    if db is None:
        logger.error("❌ База данных не инициализирована")
        sys.exit(1)
        
    logger.info("✅ Базовые модули успешно импортированы")
    
except ImportError as e:
    logger.error(f"❌ Ошибка импорта модулей: {e}")
    sys.exit(1)
except Exception as e:
    logger.error(f"❌ Ошибка инициализации: {e}")
    sys.exit(1)

# 3. ИНИЦИАЛИЗАЦИЯ БАЗЫ ДАННЫХ (после настройки логирования)
try:
    Base.metadata.create_all(bind=db.engine)
    logger.info("✅ Таблицы базы данных инициализированы")
except Exception as e:
    logger.error(f"❌ Ошибка инициализации БД: {e}")
    sys.exit(1)

def handle_shutdown(signum, frame):
    """Обработчик graceful shutdown"""
    logger.info("Получен сигнал завершения работы...")
    # Вызываем очистку базы
    try:
        if hasattr(db, 'cleanup'):
            db.cleanup()
    except Exception as e:
        logger.error(f"Ошибка при очистке базы: {e}")
    exit(0)

# Регистрируем обработчики сигналов
signal.signal(signal.SIGTERM, handle_shutdown)  # для docker stop
signal.signal(signal.SIGINT, handle_shutdown)   # для Ctrl+C

async def main():
    try:
        # 🔥 ОТЛОЖЕННЫЙ ИМПОРТ bot_core - после настройки логирования
        from bot_core import LunchBot
        
        logger.info("🚀 Запуск бота...")
        logger.info("Проверка логирования перед созданием бота...")
        
        # 🔥 ШАГ 1: СНАЧАЛА создаем бота
        logger.info("Создание экземпляра LunchBot...")
        bot = LunchBot()
        
        # 🔥 ШАГ 2: ЗАТЕМ создаем BitrixSync с application из бота
        bitrix_sync = None
        try:
            from bitrix.sync import BitrixSync
            # ВАЖНО: bot.application будет создан в bot.run(), поэтому
            # мы создаем BitrixSync без application, а обновим его после
            bitrix_sync = BitrixSync()
            logger.info("BitrixSync инициализирован")
        except ImportError as e:
            logger.error(f"Ошибка импорта BitrixSync: {e}")
        except Exception as e:
            logger.error(f"Ошибка инициализации BitrixSync: {e}")
        
        # 🔥 ШАГ 3: Передаем bitrix_sync в бота
        bot.bitrix_sync = bitrix_sync
        
        logger.info("Запуск бота...")
        await bot.run()
        
    except Exception as e:
        logger.critical(f"⛔ Фатальная ошибка: {e}", exc_info=True)
        raise
    finally:
        logger.info("✅ Работа бота полностью завершена")
        try:
            if hasattr(db, 'cleanup'):
                db.cleanup()
        except Exception as e:
            logger.error(f"Ошибка при очистке базы: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Бот остановлен")
    except Exception as e:
        # Используем базовый logging, так как наш логгер может быть не настроен
        print(f"КРИТИЧЕСКАЯ ОШИБКА: {e}")
        raise