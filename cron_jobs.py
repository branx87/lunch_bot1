# ##handlers/cron_jobs.py
import aiocron
from datetime import datetime, timedelta
import logging
from telegram.ext import Application
from config import CONFIG

logger = logging.getLogger(__name__)

class CronManager:
    def __init__(self, application: 'Application'):
        self.application = application
        self.jobs = []
        self.db = None  # Будет установлено позже

    def set_db(self, db):
        """Устанавливает соединение с БД"""
        self.db = db

    async def is_workday(self, date: datetime) -> bool:
        """Проверяет, является ли день рабочим"""
        if date.weekday() >= 5:  # Суббота или воскресенье
            return False
        return date.strftime("%Y-%m-%d") not in CONFIG.holidays

    async def setup(self):
        """Инициализация cron-задач"""
        self._add_production_jobs()
        logger.info("Cron задачи настроены в боевом режиме")

    def _add_production_jobs(self):
        """Боевые задачи по расписанию"""
        # Утренние напоминания в 9:00
        self.jobs.append(aiocron.crontab(
            '0 9 * * 1-5',
            func=self._morning_reminder,
            tz=CONFIG.timezone
        ))
        
        # Утренние отчеты в 9:30
        self.jobs.append(aiocron.crontab(
            '30 9 * * 1-5',
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
        """Ваш код утренних напоминаний"""
        if await self.is_workday(datetime.now(CONFIG.timezone)):
            now = datetime.now(CONFIG.timezone)
            logger.info(f"Запуск напоминаний в {now}")
            
            self.db.cursor.execute("""
                SELECT telegram_id 
                FROM users 
                WHERE is_verified = TRUE 
                AND telegram_id NOT IN (
                    SELECT u.telegram_id 
                    FROM users u 
                    JOIN orders o ON u.id = o.user_id 
                    WHERE o.target_date = ? AND o.is_preliminary = FALSE AND o.is_cancelled = FALSE
                )
            """, (now.date().isoformat(),))
            
            users = self.db.cursor.fetchall()
            logger.info(f"Найдено {len(users)} пользователей без заказов")
            
            for user in users:
                user_id = user[0]
                try:
                    await self.application.bot.send_message(
                        chat_id=user_id,
                        text="⏰ *Не забудьте заказать обед!* 🍽\n\n"
                             "Прием заказов открыт до 9:30.\n\n"
                             "Заказы принимаются через *БИТРИКС*, бот в тестовом режиме!",
                        parse_mode="Markdown"
                    )
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