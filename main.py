# ##main.py
import asyncio
import logging
from pathlib import Path
from bot_core import LunchBot
from logging.handlers import RotatingFileHandler
import matplotlib
from datetime import datetime, time

from db import CONFIG
matplotlib.use('Agg')

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

async def main():
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("🚀 Запуск бота...")
        
        # Инициализация BitrixSync
        bitrix_sync = None
        try:
            from bitrix import BitrixSync
            bitrix_sync = BitrixSync()
            logger.info("BitrixSync инициализирован")
            asyncio.create_task(bitrix_sync.run_sync_tasks())
        except ImportError as e:
            logger.error(f"Ошибка импорта BitrixSync: {e}")
        except Exception as e:
            logger.error(f"Ошибка инициализации BitrixSync: {e}")

        # Инициализация бота
        bot = LunchBot(bitrix_sync=bitrix_sync) if bitrix_sync else LunchBot()
        
        await bot.run()
    except Exception as e:
        logger.critical(f"⛔ Фатальная ошибка: {e}", exc_info=True)
    finally:
        logger.info("✅ Работа бота полностью завершена")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Бот остановлен")
    except Exception as e:
        logging.getLogger(__name__).critical(f"КРИТИЧЕСКАЯ ОШИБКА: {e}")
        raise