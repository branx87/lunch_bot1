# ##handlers/cron_jobs.py
import aiocron
from db import CONFIG
from datetime import datetime, timedelta
import logging
from db import db
from telegram.ext import Application

logger = logging.getLogger(__name__)

class CronManager:
    def __init__(self, application: Application):
        self.application = application
        self.jobs = []

    async def is_workday(self, date: datetime) -> bool:
        """Проверяет, является ли день рабочим"""
        if date.weekday() >= 5:  # Суббота или воскресенье
            return False
        return date.strftime("%Y-%m-%d") not in CONFIG.holidays

    async def setup(self):
        """Инициализация cron-задач в боевом режиме"""
        self._add_production_jobs()
        logger.info("Cron задачи настроены в боевом режиме")

    def _add_production_jobs(self):
        """Боевые задачи по расписанию"""
        # Утренние напоминания в 9:00
        self.jobs.append(aiocron.crontab(
            '0 9 * * 1-5',
            # '* * * * * ',
            func=self._morning_reminder,
            tz=CONFIG.timezone
        ))
        
        # Утренние отчеты в 9:31
        self.jobs.append(aiocron.crontab(
            '31 9 * * 1-5',
            func=self._morning_reports,
            tz=CONFIG.timezone
        ))
        
        # Бухгалтерский отчет в 11:00 последнего дня месяца
        self.jobs.append(aiocron.crontab(
            '0 11 28-31 * *',
            func=self._accounting_report,
            tz=CONFIG.timezone
        ))

    async def _morning_reminder(self):
        """Утренние напоминания пользователям без заказов на сегодня"""
        if await self.is_workday(datetime.now(CONFIG.timezone)):
            now = datetime.now(CONFIG.timezone)
            today = now.date().isoformat()
            logger.info(f"Запуск напоминаний в {now} для даты {today}")

            # Сначала проверим общее количество подходящих пользователей
            db.cursor.execute("""
                SELECT COUNT(*) 
                FROM users 
                WHERE is_verified = TRUE 
                AND is_deleted = FALSE
                AND notifications_enabled = TRUE
                AND is_employee = TRUE
            """)
            total_users = db.cursor.fetchone()[0]
            logger.info(f"Всего подходящих пользователей: {total_users}")

            # Проверим количество пользователей с заказами на сегодня
            db.cursor.execute("""
                SELECT COUNT(DISTINCT u.id)
                FROM users u
                JOIN orders o ON u.id = o.user_id
                WHERE o.target_date = ?
                AND o.is_cancelled = FALSE
                AND o.is_active = TRUE
                AND o.quantity > 0
            """, (today,))
            users_with_orders = db.cursor.fetchone()[0]
            logger.info(f"Пользователей с заказами на сегодня: {users_with_orders}")

            # Основной запрос для получения telegram_id
            db.cursor.execute("""
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
                    AND o.target_date = ?
                    AND o.is_cancelled = FALSE
                    AND o.is_active = TRUE
                    AND o.quantity > 0
                )
            """, (today,))
            
            users = db.cursor.fetchall()
            logger.info(f"Найдено {len(users)} пользователей без заказов")
            
            # Дополнительная отладочная информация
            if len(users) == 0 and total_users > 0 and users_with_orders < total_users:
                logger.warning("""
                    ВНИМАНИЕ: Найдено 0 пользователей без заказов, но:
                    - Всего подходящих пользователей: %d
                    - С заказами на сегодня: %d
                    Проверьте данные в БД!
                """, total_users, users_with_orders)
                
                # Выведем примеры пользователей для проверки
                db.cursor.execute("SELECT telegram_id, full_name FROM users LIMIT 5")
                sample_users = db.cursor.fetchall()
                logger.info(f"Пример пользователей: {sample_users}")
                
                db.cursor.execute("SELECT user_id, target_date FROM orders WHERE target_date = ? LIMIT 5", (today,))
                sample_orders = db.cursor.fetchall()
                logger.info(f"Пример заказов на сегодня: {sample_orders}")

            for user in users:
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