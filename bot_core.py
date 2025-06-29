# ##bot_core.py
import sqlite3
from telegram.ext import ApplicationBuilder
from middleware import AccessControlHandler
import logging
import asyncio
from config import CONFIG
from cron_jobs import CronManager

logger = logging.getLogger(__name__)

class LunchBot:
    def __init__(self):
        # Исправляем подключение к базе данных
        self.conn = sqlite3.connect('data/lunch_bot.db', 
                                   check_same_thread=False,
                                   timeout=10,
                                   isolation_level=None)
        self.application = None
        self._running = False
        self.cron_manager = None

    async def run(self):
        try:
            self.application = ApplicationBuilder().token(CONFIG.token).build()
            self.application.bot_data['admin_ids'] = CONFIG.admin_ids
            
            # Инициализируем CronManager
            self.cron_manager = CronManager(self.application)
            await self.cron_manager.setup()
            
            # Добавляем обработчик контроля доступа первым
            self.application.add_handler(AccessControlHandler(), group=-1)
            
            # Настройка обработчиков
            from handlers import setup_handlers
            setup_handlers(self.application)
            
            from handlers.commands import setup as setup_commands
            setup_commands(self.application)

            await self.application.initialize()
            await self.application.start()

            bot_info = await self.application.bot.get_me()
            logger.info(f"Бот @{bot_info.username} запущен")

            await self.application.updater.start_polling()
            self._running = True
            
            while self._running:
                try:
                    await asyncio.sleep(1)
                except asyncio.CancelledError:
                    logger.info("Получен запрос на остановку")
                    break
                    
        except asyncio.CancelledError:
            logger.info("Бот остановлен по запросу")
        except Exception as e:
            logger.error(f"Ошибка: {e}")
        finally:
            await self.stop()

    async def stop(self):
        """Финальная версия метода остановки"""
        stop_timeout = 3
        try:
            self._running = False
            if not self.application:
                logger.info("Бот не был инициализирован")
                return

            logger.info("Начинаем процесс остановки...")
            
            if hasattr(self.application, 'updater') and self.application.updater:
                try:
                    if self.application.updater.running:
                        logger.debug("Останавливаем updater...")
                        await asyncio.wait_for(self.application.updater.stop(), timeout=stop_timeout)
                except asyncio.TimeoutError:
                    logger.warning("Таймаут при остановке updater")
                except Exception as e:
                    logger.warning(f"Ошибка остановки updater: {str(e)}")

            if hasattr(self.application, 'running') and self.application.running:
                try:
                    logger.debug("Останавливаем application...")
                    await asyncio.wait_for(self.application.stop(), timeout=stop_timeout)
                    await asyncio.wait_for(self.application.shutdown(), timeout=stop_timeout)
                except asyncio.TimeoutError:
                    logger.warning("Таймаут при остановке application")
                except Exception as e:
                    logger.warning(f"Ошибка остановки application: {str(e)}")

            if hasattr(self, 'cron_manager') and self.cron_manager:
                try:
                    logger.debug("Останавливаем cron задачи...")
                    if hasattr(self.cron_manager, 'shutdown'):
                        await asyncio.wait_for(self.cron_manager.shutdown(), timeout=stop_timeout)
                    elif hasattr(self.cron_manager, 'stop'):
                        await asyncio.wait_for(self.cron_manager.stop(), timeout=stop_timeout)
                except Exception as e:
                    logger.warning(f"Ошибка остановки cron: {str(e)}")

        except Exception as e:
            logger.error(f"Критическая ошибка при остановке: {str(e)}", exc_info=True)
        finally:
            logger.info("Все компоненты успешно остановлены")
            self._running = False