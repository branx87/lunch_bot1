# ##main.py
import asyncio
import logging
from bot_core import LunchBot
from logging.handlers import RotatingFileHandler
import matplotlib
matplotlib.use('Agg')

def setup_logging():
    handler = RotatingFileHandler(
        'bot.log',
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
    
    bot = LunchBot()
    try:
        logger.info("🚀 Запуск бота...")
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
            await asyncio.wait_for(bot.stop(), timeout=5)
        except asyncio.TimeoutError:
            logger.error("⚠️ Превышено время ожидания остановки!")
        except Exception as e:
            logger.error(f"⚠️ Ошибка при остановке: {str(e)}")
        finally:
            # Финализируем ресурсы
            await asyncio.sleep(0.1)  # Даем время на финализацию
            logger.info("✅ Работа бота полностью завершена")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Бот остановлен")
    except Exception as e:
        logging.getLogger(__name__).critical(f"КРИТИЧЕСКАЯ ОШИБКА: {e}")
        raise