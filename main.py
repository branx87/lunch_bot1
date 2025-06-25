# ##main.py 
# Last update: 25.06.25 - Переход на DI архитектуру

import asyncio
import logging
from pathlib import Path
from bot_core import LunchBot
from db import Database
from config import CONFIG
from report_generators import ReportGenerator

def setup_logging():
    """Настройка системы логирования с ротацией и ограничением размера"""
    LOG_FILE = CONFIG.logs_dir / "bot.log"
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    # Создаем rotating file handler
    file_handler = logging.handlers.RotatingFileHandler(
        filename=LOG_FILE,
        maxBytes=5*1024*1024,  # 5 MB
        backupCount=3,
        encoding='utf-8'
    )
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            file_handler,
            logging.StreamHandler()  # Вывод в консоль
        ]
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)

async def main():
    """Основная функция запуска бота с новой DI архитектурой"""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("=== ЗАПУСК ПРОГРАММЫ ===")
        
        # Инициализация компонентов
        db = Database(str(CONFIG.db_path))
        CONFIG.init_db_data(db)
        report_generator = ReportGenerator(db)
        bot = LunchBot(db, report_generator)
        
        logger.info("Запуск основного цикла бота...")
        await bot.run()
        
    except KeyboardInterrupt:
        logger.info("🛑 Корректная остановка по Ctrl+C")
    except Exception as e:
        logger.critical(f"⛔ Ошибка: {e}", exc_info=True)
    finally:
        logger.info("✅ Работа завершена")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Бот остановлен")
    except Exception as e:
        logging.getLogger(__name__).critical(f"КРИТИЧЕСКАЯ ОШИБКА: {e}")
        raise