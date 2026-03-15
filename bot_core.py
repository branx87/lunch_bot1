import logging
import asyncio
from telegram.ext import ApplicationBuilder
from telegram.request import HTTPXRequest
from telegram.error import NetworkError, TimedOut


class LunchBot:
    def __init__(self, bitrix_sync=None):
        self.bitrix_sync = bitrix_sync
        self.application = None
        self._running = False
        self.cron_manager = None

        # Простой логгер без сложной логики
        self.logger = logging.getLogger(__name__)

    async def error_handler(self, update, context):
        """Обработчик ошибок для игнорирования временных сетевых проблем"""
        error = context.error

        # Игнорируем временные сетевые ошибки (они автоматически повторяются)
        if isinstance(error, (NetworkError, TimedOut)):
            self.logger.warning(f"Временная сетевая ошибка (автоповтор): {error.__class__.__name__}")
            return

        # Все остальные ошибки логируем полностью
        self.logger.error(f"Необработанная ошибка: {error}", exc_info=context.error)


    async def run(self):
        try:
            self.logger.info("=== НАЧАЛО РАБОТЫ BOT_CORE ===")
            
            # Отложенный импорт ВСЕХ модулей
            from config import CONFIG
            from database import db
            
            self.logger.info("1. Конфиг и БД импортированы")
            
            if CONFIG is None:
                self.logger.error("CONFIG не загружен")
                return
                
            self.logger.info("2. Создаем application с устойчивостью к сетевым ошибкам")
            
            # 🔥 КАСТОМНЫЙ REQUEST С УВЕЛИЧЕННЫМИ ТАЙМАУТАМИ
            # read_timeout должен быть больше, чем polling_timeout Telegram (обычно 30-60 сек)
            request_kwargs = dict(
                connection_pool_size=8,
                connect_timeout=10.0,
                read_timeout=90.0,  # Увеличено для long polling
                write_timeout=30.0,  # Увеличено для отправки файлов
                pool_timeout=10.0,
                httpx_kwargs={"http1": True, "http2": False},  # HTTP/2 конфликтует с Telegram при отправке файлов
            )
            if CONFIG.proxy_url:
                request_kwargs['proxy'] = CONFIG.proxy_url
                self.logger.info(f"Используем прокси: {CONFIG.proxy_url}")
            request = HTTPXRequest(**request_kwargs)
            
            # ✅ УБРАЛИ connect_timeout, read_timeout и т.д. из ApplicationBuilder
            self.application = (
                ApplicationBuilder()
                .token(CONFIG.token)
                .request(request)  # ← ВСЕ ТАЙМАУТЫ УЖЕ В request
                .build()
            )
            
            # Передаем application в BitrixSync если он был создан
            if self.bitrix_sync:
                self.bitrix_sync.bot_application = self.application
                # Запускаем sync задачи
                asyncio.create_task(self.bitrix_sync.run_sync_tasks())
                self.logger.info("2a. BitrixSync подключен к application")
            
            self.logger.info("3. Настраиваем admin_ids")
            admin_ids = getattr(CONFIG, 'admin_ids', [])
            self.application.bot_data['admin_ids'] = admin_ids
            
            self.logger.info("4. Импортируем CronManager")
            from cron_jobs import CronManager
            self.cron_manager = CronManager(self.application)
            await self.cron_manager.setup()
            
            self.logger.info("5. Импортируем middleware")
            from middleware import AccessControlHandler
            self.application.add_handler(AccessControlHandler(), group=-1)
            
            self.logger.info("6. Настраиваем обработчики")
            from handlers import setup_handlers
            setup_handlers(self.application)
            
            from handlers.commands import setup as setup_commands
            setup_commands(self.application)

            self.logger.info("7. Добавляем обработчик ошибок")
            self.application.add_error_handler(self.error_handler)

            self.logger.info("8. Инициализируем application")
            await self.application.initialize()
            await self.application.start()

            bot_info = await self.application.bot.get_me()
            self.logger.info(f"9. Бот @{bot_info.username} запущен")

            self.logger.info("10. Запускаем polling с увеличенными таймаутами")
            # 🔥 ТОЛЬКО ПАРАМЕТРЫ POLLING, БЕЗ ТАЙМАУТОВ (они уже в request)
            await self.application.updater.start_polling(
                allowed_updates=None,
                drop_pending_updates=False,
                bootstrap_retries=5  # 5 попыток при старте
            )
            self._running = True

            self.logger.info("11. Бот успешно запущен, переходим в основной цикл")
            while self._running:
                await asyncio.sleep(1)
                
        except Exception as e:
            self.logger.error(f"ОШИБКА В RUN: {e}", exc_info=True)
            await self.stop()
            
    async def stop(self):
        self.logger.info("=== НАЧАЛО ОСТАНОВКИ ===")
        try:
            self._running = False
            
            # Остановка BitrixSync
            if self.bitrix_sync:
                await self.bitrix_sync.close()
                self.logger.info("BitrixSync остановлен")
            
            if self.application:
                if hasattr(self.application, 'updater') and self.application.updater:
                    await self.application.updater.stop()
                await self.application.stop()
                await self.application.shutdown()
            self.logger.info("Бот успешно остановлен")
        except Exception as e:
            self.logger.error(f"Ошибка при остановке: {e}")