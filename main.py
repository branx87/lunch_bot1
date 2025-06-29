# ##main.py
import asyncio
import logging
from pathlib import Path
from bot_core import LunchBot
from logging.handlers import RotatingFileHandler
import matplotlib

from config import CONFIG
matplotlib.use('Agg')

def setup_logging():
    # Создаем папку для логов, если ее нет
    logs_dir = Path('data/logs')
    logs_dir.mkdir(parents=True, exist_ok=True)
    
    handler = RotatingFileHandler(
        logs_dir / 'bot.log',  # Теперь логи тоже в папке data
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
    """Финальная версия основной функции"""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("🚀 Запуск бота...")
        
        # Инициализируем бота БЕЗ передачи db_path (оставляем как было)
        print("Токен бота:", CONFIG.token)
        print("Проверка конфига:")
        print("Токен существует:", hasattr(CONFIG, '_token'))
        print("Путь к .env:", (Path(__file__).parent / 'data' / 'configs' / '.env').exists())
        bot = LunchBot()
        
        await bot.run()
    except KeyboardInterrupt:
        logger.info("🛑 Получен сигнал KeyboardInterrupt")
    except asyncio.CancelledError:
        logger.info("🛑 Асинхронные задачи были отменены")
    except Exception as e:
        logger.critical(f"⛔ Фатальная ошибка: {str(e)}", exc_info=True)
    finally:
        logger.info("Начало завершения работы...")
        try:
            if 'bot' in locals():
                await asyncio.wait_for(bot.stop(), timeout=5)
        except asyncio.TimeoutError:
            logger.error("⚠️ Превышено время ожидания остановки!")
        except Exception as e:
            logger.error(f"⚠️ Ошибка при остановке: {str(e)}")
        finally:
            await asyncio.sleep(0.1)
            logger.info("✅ Работа бота полностью завершена")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Бот остановлен")
    except Exception as e:
        logging.getLogger(__name__).critical(f"КРИТИЧЕСКАЯ ОШИБКА: {e}")
        raise