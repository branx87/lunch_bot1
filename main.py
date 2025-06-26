# ##main.py - Главный файл для запуска бота
# Версия: 26.06.2025

import asyncio
import logging
import logging.handlers
from pathlib import Path
from bot_core import LunchBot
from config import CONFIG
from db import Database
from report_generators import ReportGenerator

def setup_logging():
    """Настраивает систему логирования"""
    log_file = CONFIG.logs_dir / "bot.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    
    file_handler = logging.handlers.RotatingFileHandler(
        filename=log_file,
        maxBytes=5*1024*1024,
        backupCount=3,
        encoding='utf-8'
    )
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            file_handler,
            logging.StreamHandler()
        ]
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)

async def main():
    """Основная функция запуска бота"""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("=== ЗАПУСК ПРОГРАММЫ ===")
        
        # 1. Инициализация базы данных
        logger.info("Инициализация базы данных...")
        db = Database(
            db_path=str(CONFIG.db_path),
            configs_dir=str(CONFIG.configs_dir)  # Теперь этот атрибут существует
        )
        
        # 2. Загрузка данных в конфиг из БД
        logger.info("Загрузка конфигурации...")
        CONFIG.init_db_data(db)
        
        # 3. Создаем генератор отчетов
        logger.info("Инициализация генератора отчетов...")
        report_generator = ReportGenerator(db)
        
        # 4. Создаем и запускаем бота
        logger.info("Создание экземпляра бота...")
        bot = LunchBot(db, report_generator)
        
        logger.info("Запуск основного цикла бота...")
        await bot.run()
        
    except KeyboardInterrupt:
        logger.info("🛑 Корректная остановка по Ctrl+C")
    except Exception as e:
        logger.critical(f"⛔ Критическая ошибка: {e}", exc_info=True)
    finally:
        logger.info("✅ Работа программы завершена")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Бот остановлен")
    except Exception as e:
        logging.getLogger(__name__).critical(f"КРИТИЧЕСКАЯ ОШИБКА: {e}")
        raise