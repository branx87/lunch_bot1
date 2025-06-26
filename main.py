# main.py - Главный файл для запуска бота
# Версия: 25.06.2024

import asyncio
import logging
import logging.handlers  # Для ротации логов
from pathlib import Path
from bot_core import LunchBot  # Основной класс бота
from db import Database  # Работа с базой данных
from config import CONFIG  # Настройки приложения
from report_generators import ReportGenerator  # Генератор отчетов

def setup_logging():
    """Настраивает систему логирования"""
    # Путь к файлу логов (в папке data/logs)
    log_file = CONFIG.logs_dir / "bot.log"
    
    # Создаем папку для логов, если ее нет
    log_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Настройка обработчика логов с ротацией:
    # - Максимальный размер файла: 5 МБ
    # - Сохранять 3 предыдущих файла логов
    file_handler = logging.handlers.RotatingFileHandler(
        filename=log_file,
        maxBytes=5*1024*1024,
        backupCount=3,
        encoding='utf-8'
    )
    
    # Основные настройки логирования:
    # - Уровень: INFO и выше
    # - Формат вывода сообщений
    # - Обработчики: файл и консоль
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            file_handler,  # Запись в файл
            logging.StreamHandler()  # Вывод в консоль
        ]
    )
    
    # Уменьшаем уровень логов для библиотеки httpx
    logging.getLogger("httpx").setLevel(logging.WARNING)

async def main():
    """Основная функция запуска бота"""
    # Настраиваем логирование
    setup_logging()
    
    # Получаем логгер для этого модуля
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("=== ЗАПУСК ПРОГРАММЫ ===")
        
        # 1. Инициализация базы данных
        logger.info("Инициализация базы данных...")
        db = Database(
            db_path=str(CONFIG.db_path),
            configs_dir=str(CONFIG.configs_dir)
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
        # Обработка остановки по Ctrl+C
        logger.info("🛑 Корректная остановка по Ctrl+C")
    except Exception as e:
        # Логирование критических ошибок
        logger.critical(f"⛔ Критическая ошибка: {e}", exc_info=True)
    finally:
        # Завершение работы
        logger.info("✅ Работа программы завершена")

if __name__ == "__main__":
    try:
        # Запуск асинхронной функции main()
        asyncio.run(main())
    except KeyboardInterrupt:
        # Обработка Ctrl+C в консоли
        print("\n🛑 Бот остановлен")
    except Exception as e:
        # Обработка непредвиденных ошибок
        logging.getLogger(__name__).critical(f"КРИТИЧЕСКАЯ ОШИБКА: {e}")
        raise