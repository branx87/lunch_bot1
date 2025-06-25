# ##bot_core.py 25.06.25 9.39
from telegram.ext import ApplicationBuilder
from middleware import AccessControlHandler
import logging
import asyncio
from config import CONFIG
from cron_jobs import CronManager

logger = logging.getLogger(__name__)

class LunchBot:
    def __init__(self, db_connection, report_generator):
        self.db = db_connection
        self.report_generator = report_generator
        self.application = None
        self._running = False
        self.cron_manager = None

    async def run(self):
        try:
            self.application = ApplicationBuilder().token(CONFIG.token).build()
            self.application.bot_data.update({
                'db': self.db,
                'admin_ids': CONFIG.admin_ids,
                'report_generator': self.report_generator
            })
            
            # Инициализация CronManager
            self.cron_manager = CronManager(self.application)
            self.cron_manager.set_db(self.db)
            await self.cron_manager.setup()
            
            self.application.add_handler(AccessControlHandler(self.db), group=-1)
            
            from handlers import setup_handlers
            setup_handlers(self.application, self.db)

            await self.application.initialize()
            await self.application.start()

            bot_info = await self.application.bot.get_me()
            logger.info(f"Бот @{bot_info.username} запущен")
            await self.application.updater.start_polling()
            self._running = True
            
            while self._running:
                await asyncio.sleep(1)
                
        except asyncio.CancelledError:
            logger.info("Бот остановлен по запросу")
        except Exception as e:
            logger.error(f"Ошибка запуска бота: {e}", exc_info=True)
        finally:
            await self.stop()

    async def stop(self):
        """Улучшенная версия с остановкой cron задач"""
        try:
            self._running = False
            if not self.application:
                return

            logger.info("Начинаем процесс остановки...")
            
            # 1. Остановка cron задач
            if self.cron_manager:
                try:
                    if hasattr(self.cron_manager, 'shutdown'):
                        await self.cron_manager.shutdown()
                    elif hasattr(self.cron_manager, 'stop'):
                        await self.cron_manager.stop()
                except Exception as e:
                    logger.error(f"Ошибка остановки cron: {e}")

            # 2. Остановка updater и application
            if hasattr(self.application, 'updater') and self.application.updater:
                await self.application.updater.stop()
            
            if hasattr(self.application, 'running') and self.application.running:
                await self.application.stop()
                await self.application.shutdown()

        except Exception as e:
            logger.error(f"Критическая ошибка при остановке: {e}")
        finally:
            logger.info("Все компоненты остановлены")