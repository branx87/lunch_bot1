# ##handlers/cron_jobs.py
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from database import db
from config import CONFIG
from datetime import datetime, timedelta
import logging
from telegram.ext import Application
from models import User, Order
from sqlalchemy import text
from bitrix.sync import BitrixSync
from time_config import TIME_CONFIG
from backup_manager import backup_manager

logger = logging.getLogger(__name__)

class CronManager:
    async def close(self):
        """Корректное закрытие CronManager"""
        try:
            # Закрываем BitrixSync если он есть
            if hasattr(self, 'bitrix_sync') and self.bitrix_sync:
                await self.bitrix_sync.close()
                logger.info("✅ BitrixSync в CronManager закрыт")

            # Останавливаем планировщик
            if hasattr(self, 'scheduler') and self.scheduler and self.scheduler.running:
                self.scheduler.shutdown()
                logger.info("✅ Планировщик CronManager остановлен")
        except Exception as e:
            logger.error(f"❌ Ошибка закрытия CronManager: {e}")

    def __init__(self, application: Application):
        self.application = application
        self.scheduler = AsyncIOScheduler(timezone=TIME_CONFIG.TIMEZONE)
        self._calendar_cache: dict = {}  # Кэш производственного календаря

    async def is_workday(self, date: datetime) -> bool:
        """Проверяет, является ли день рабочим (включая производственный календарь РФ)"""
        # Стандартные выходные (сб, вс)
        if date.weekday() in TIME_CONFIG.WEEKEND_DAYS:
            return False
        # Ручной список праздников из БД
        if date.strftime("%Y-%m-%d") in CONFIG.holidays:
            return False
        # Производственный календарь РФ (переносы, официальные выходные)
        return await self._check_production_calendar(date)

    async def _check_production_calendar(self, date: datetime) -> bool:
        """Проверяет производственный календарь через isdayoff.ru.
        Возвращает True если день рабочий, False если выходной."""
        date_key = date.strftime("%Y%m%d")

        if date_key in self._calendar_cache:
            return self._calendar_cache[date_key]

        try:
            import httpx
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(f"https://isdayoff.ru/{date_key}")
                if response.status_code == 200:
                    result = response.text.strip()
                    if result == '1':
                        is_work = False  # Выходной/праздник по производственному календарю
                        logger.info(f"📅 Производственный календарь: {date_key} — выходной день")
                    else:
                        is_work = True  # '0' — рабочий, '100'/'101' — неизвестно, считаем рабочим
                    self._calendar_cache[date_key] = is_work
                    return is_work
        except Exception as e:
            logger.warning(f"⚠️ Не удалось проверить производственный календарь для {date_key}: {e}")

        # Если API недоступен — считаем рабочим днём
        return True

    async def setup(self):
        """Инициализация cron-задач в боевом режиме"""
        logger.info(f"Начало настройки cron задач в {datetime.now(TIME_CONFIG.TIMEZONE)}")
        self._add_production_jobs()
        self.scheduler.start()
        logger.info(f"Cron задачи настроены в боевом режиме в {datetime.now(TIME_CONFIG.TIMEZONE)}")

        # Логируем информацию о настроенных задачах
        logger.info("Созданные расписания:")
        logger.info(f"  Напоминания: {TIME_CONFIG.MORNING_REMINDER_TIME.strftime('%H:%M')} (Пн-Пт)")
        logger.info(f"  Отчеты: {TIME_CONFIG.MORNING_REPORTS_TIME.strftime('%H:%M')} (Пн-Пт)")
        logger.info(f"  Бух.отчет: {TIME_CONFIG.ACCOUNTING_REPORT_TIME.strftime('%H:%M')} (28-31 числа)")
        logger.info(f"  Синхронизация: {TIME_CONFIG.SYNC_EMPLOYEES_TIME.strftime('%H:%M')} (Пн-Пт)")
        logger.info(f"  Бекап: 03:00 (каждый день)")

        # 🔥 ТЕСТ: запуск через 3 минуты для проверки работы планировщика
        test_time = datetime.now(TIME_CONFIG.TIMEZONE) + timedelta(minutes=3)
        self.scheduler.add_job(
            self._test_cron_working,
            'cron',
            minute=test_time.minute,
            hour=test_time.hour,
            second=0
        )
        logger.info(f"🧪 ТЕСТ: задача настроена на {test_time.strftime('%H:%M')}")

    async def _test_cron_working(self):
        """Тестовая функция для проверки работы планировщика"""
        logger.info(f"✅ ТЕСТ УСПЕШЕН: Планировщик работает! Время: {datetime.now(TIME_CONFIG.TIMEZONE)}")

    def _add_production_jobs(self):
        """Боевые задачи по расписанию"""
        logger.info(f"🕒 Настройка задач планировщика в {datetime.now(TIME_CONFIG.TIMEZONE)}")

        # Получаем рабочие дни в формате APScheduler
        work_days_cron = self._get_cron_days(TIME_CONFIG.WORK_DAYS)

        # Утренние напоминания
        self.scheduler.add_job(
            self._morning_reminder,
            'cron',
            minute=TIME_CONFIG.MORNING_REMINDER_TIME.minute,
            hour=TIME_CONFIG.MORNING_REMINDER_TIME.hour,
            day_of_week=work_days_cron,
            second=0
        )
        logger.info(f"📅 Напоминания: {TIME_CONFIG.MORNING_REMINDER_TIME.strftime('%H:%M')} (Пн-Пт)")

        # Утренние отчеты
        self.scheduler.add_job(
            self._morning_reports,
            'cron',
            minute=TIME_CONFIG.MORNING_REPORTS_TIME.minute,
            hour=TIME_CONFIG.MORNING_REPORTS_TIME.hour,
            day_of_week=work_days_cron,
            second=0
        )
        logger.info(f"📊 Отчеты: {TIME_CONFIG.MORNING_REPORTS_TIME.strftime('%H:%M')} (Пн-Пт)")

        # Бухгалтерский отчет (в последние дни месяца)
        self.scheduler.add_job(
            self._accounting_report,
            'cron',
            minute=TIME_CONFIG.ACCOUNTING_REPORT_TIME.minute,
            hour=TIME_CONFIG.ACCOUNTING_REPORT_TIME.hour,
            day='28-31',
            second=0
        )
        logger.info(f"💰 Бух.отчет: {TIME_CONFIG.ACCOUNTING_REPORT_TIME.strftime('%H:%M')} (28-31 числа)")

        # Синхронизация сотрудников
        self.scheduler.add_job(
            self._sync_employees,
            'cron',
            minute=TIME_CONFIG.SYNC_EMPLOYEES_TIME.minute,
            hour=TIME_CONFIG.SYNC_EMPLOYEES_TIME.hour,
            day_of_week=work_days_cron,
            second=0
        )
        logger.info(f"🔄 Синхронизация: {TIME_CONFIG.SYNC_EMPLOYEES_TIME.strftime('%H:%M')} (Пн-Пт)")

        # Автоматическое резервное копирование (каждую ночь в 03:00)
        self.scheduler.add_job(
            self._create_backup,
            'cron',
            hour=3,
            minute=0,
            second=0
        )
        logger.info("📦 Бекап БД: 03:00 (каждый день)")

    def _get_cron_days(self, days_list):
        """Конвертирует список дней в формат APScheduler"""
        # days_list: [0,1,2,3,4] -> 'mon,tue,wed,thu,fri'
        day_names = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']
        return ','.join([day_names[day] for day in days_list])

    async def _morning_reminder(self):
        """Утренние напоминания пользователям без заказов на сегодня"""
        if await self.is_workday(datetime.now(CONFIG.timezone)):
            now = datetime.now(CONFIG.timezone)
            today = now.date().isoformat()
            logger.info(f"Запуск напоминаний в {now} для даты {today}")

            with db.get_session() as session:
                # Сначала проверим общее количество подходящих пользователей
                total_users = session.query(User).filter(
                    User.is_verified == True,
                    User.is_deleted == False,
                    User.notifications_enabled == True,
                    User.is_employee == True
                ).count()
                logger.info(f"Всего подходящих пользователей: {total_users}")

                # Проверим количество пользователей с заказами на сегодня
                users_with_orders = session.query(User).join(
                    Order, User.id == Order.user_id
                ).filter(
                    Order.target_date == today,
                    Order.is_cancelled == False,
                    Order.is_active == True,
                    Order.quantity > 0
                ).distinct().count()
                logger.info(f"Пользователей с заказами на сегодня: {users_with_orders}")

                # Основной запрос для получения telegram_id
                users_without_orders = session.execute(text("""
                    SELECT u.telegram_id 
                    FROM users u
                    WHERE u.is_verified = TRUE 
                    AND u.is_deleted = FALSE
                    AND u.notifications_enabled = TRUE
                    AND u.is_employee = TRUE
                    AND NOT EXISTS (
                        SELECT 1 
                        FROM orders o 
                        WHERE o.user_id = u.id
                        AND o.target_date = :today
                        AND o.is_cancelled = FALSE
                        AND o.is_active = TRUE
                        AND o.quantity > 0
                    )
                """), {'today': today}).fetchall()
                
                logger.info(f"Найдено {len(users_without_orders)} пользователей без заказов")
                
                # Дополнительная отладочная информация
                if len(users_without_orders) == 0 and total_users > 0 and users_with_orders < total_users:
                    logger.warning("""
                        ВНИМАНИЕ: Найдено 0 пользователей без заказов, но:
                        - Всего подходящих пользователей: %d
                        - С заказами на сегодня: %d
                        Проверьте данные в БД!
                    """, total_users, users_with_orders)
                    
                    # Выведем примеры пользователей для проверки
                    sample_users = session.query(User.telegram_id, User.full_name).limit(5).all()
                    logger.info(f"Пример пользователей: {sample_users}")
                    
                    sample_orders = session.query(Order.user_id, Order.target_date).filter(
                        Order.target_date == today
                    ).limit(5).all()
                    logger.info(f"Пример заказов на сегодня: {sample_orders}")

                for user in users_without_orders:
                    user_id = user[0]
                    try:
                        await self.application.bot.send_message(
                            chat_id=user_id,
                            text=(
                                "⏰ <b>Не забудьте заказать обед!</b> 🍽\n\n"
                                "Прием заказов открыт до 9:30.\n\n"
                                "Чтобы отключить напоминания отправьте: /notifications_off"
                            ),
                            parse_mode="HTML"
                        )
                        logger.debug(f"Напоминание отправлено пользователю {user_id}")
                    except Exception as e:
                        logger.error(f"Ошибка при отправке напоминания пользователю {user_id}: {e}")

    async def _morning_reports(self):
        """Утренние отчеты"""
        from scheduled_reports import send_scheduled_reports
        if await self.is_workday(datetime.now(CONFIG.timezone)):
            await send_scheduled_reports(self.application, ['admins', 'providers'])

    async def _accounting_report(self):
        """Бухгалтерский отчет"""
        from scheduled_reports import send_scheduled_reports
        now = datetime.now(CONFIG.timezone)
        if (now.month != (now + timedelta(days=1)).month and 
           await self.is_workday(now)):
            await send_scheduled_reports(self.application, ['accounting'])

    async def _sync_employees(self):
        """Автоматическая синхронизация сотрудников Bitrix"""
        # Инициализация синхронизатора
        bitrix_sync = BitrixSync()
        await bitrix_sync.sync_employees()
        logger.info("Ежедневная синхронизация сотрудников с Bitrix выполнена")

    async def _create_backup(self):
        """Автоматическое создание резервной копии базы данных"""
        try:
            logger.info("🔄 Запуск автоматического бекапа базы данных...")
            backup_path = await backup_manager.create_backup(upload_to_cloud=True)

            if backup_path:
                logger.info(f"✅ Автоматический бекап успешно создан: {backup_path}")
            else:
                logger.error("❌ Не удалось создать автоматический бекап")

        except Exception as e:
            logger.error(f"❌ Ошибка при создании автоматического бекапа: {e}", exc_info=True)
