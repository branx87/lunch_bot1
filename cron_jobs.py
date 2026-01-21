# ##handlers/cron_jobs.py
import aiocron
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
            if hasattr(self, 'scheduler') and self.scheduler:
                self.scheduler.shutdown()
                logger.info("✅ Планировщик CronManager остановлен")
        except Exception as e:
            logger.error(f"❌ Ошибка закрытия CronManager: {e}")
            
    def __init__(self, application: Application):
        self.application = application
        self.jobs = []

    async def is_workday(self, date: datetime) -> bool:
        """Проверяет, является ли день рабочим"""
        # Используем настройки из TIME_CONFIG вместо жестких значений
        if date.weekday() in TIME_CONFIG.WEEKEND_DAYS:
            return False
        return date.strftime("%Y-%m-%d") not in CONFIG.holidays

    async def setup(self):
        """Инициализация cron-задач в боевом режиме"""
        logger.info(f"Начало настройки cron задач в {datetime.now(TIME_CONFIG.TIMEZONE)}")
        self._add_production_jobs()
        logger.info(f"Cron задачи настроены в боевом режиме в {datetime.now(TIME_CONFIG.TIMEZONE)}")
        
        # Убираем проблемное логирование объектов cron
        logger.info(f"Всего настроено {len(self.jobs)} cron задач")
        
        # Вместо этого логируем сами cron выражения которые мы создали
        logger.info("Созданные cron выражения:")
        logger.info(f"  Напоминания: 0 9 * * 1,2,3,4,5")
        logger.info(f"  Отчеты: 31 9 * * 1,2,3,4,5") 
        logger.info(f"  Бух.отчет: 0 11 28-31 * *")
        logger.info(f"  Синхронизация: 0 18 * * 1,2,3,4,5")

        # 🔥 ТЕСТ: запуск через 3 минуты для проверки работы cron
        test_time = datetime.now(TIME_CONFIG.TIMEZONE) + timedelta(minutes=3)
        test_cron = aiocron.crontab(
            f'{test_time.minute} {test_time.hour} * * *',  # каждый день в это время
            func=self._test_cron_working,
            tz=TIME_CONFIG.TIMEZONE
        )
        self.jobs.append(test_cron)
        logger.info(f"🧪 ТЕСТ: задача настроена на {test_time.strftime('%H:%M')}")

    async def _test_cron_working(self):
        """Тестовая функция для проверки работы cron"""
        logger.info(f"✅ ТЕСТ УСПЕШЕН: Cron работает! Время: {datetime.now(TIME_CONFIG.TIMEZONE)}")

    def _add_production_jobs(self):
        """Боевые задачи по расписанию"""
        logger.info(f"🕒 Настройка cron задач в {datetime.now(TIME_CONFIG.TIMEZONE)}")
        
        # Утренние напоминания
        cron_expression = f'{TIME_CONFIG.MORNING_REMINDER_TIME.minute} {TIME_CONFIG.MORNING_REMINDER_TIME.hour} * * {self._get_cron_days(TIME_CONFIG.WORK_DAYS)}'
        logger.info(f"📅 Напоминания: {cron_expression}")
        
        self.jobs.append(aiocron.crontab(
            cron_expression,
            func=self._morning_reminder,
            tz=TIME_CONFIG.TIMEZONE
        ))
        
        # Утренние отчеты
        self.jobs.append(aiocron.crontab(
            f'{TIME_CONFIG.MORNING_REPORTS_TIME.minute} {TIME_CONFIG.MORNING_REPORTS_TIME.hour} * * {self._get_cron_days(TIME_CONFIG.WORK_DAYS)}',
            func=self._morning_reports,
            tz=TIME_CONFIG.TIMEZONE
        ))
        
        # Бухгалтерский отчет
        self.jobs.append(aiocron.crontab(
            f'{TIME_CONFIG.ACCOUNTING_REPORT_TIME.minute} {TIME_CONFIG.ACCOUNTING_REPORT_TIME.hour} 28-31 * *',
            func=self._accounting_report,
            tz=TIME_CONFIG.TIMEZONE
        ))

        # Синхронизация сотрудников
        self.jobs.append(aiocron.crontab(
            f'{TIME_CONFIG.SYNC_EMPLOYEES_TIME.minute} {TIME_CONFIG.SYNC_EMPLOYEES_TIME.hour} * * {self._get_cron_days(TIME_CONFIG.WORK_DAYS)}',
            func=self._sync_employees,
            tz=TIME_CONFIG.TIMEZONE
        ))

        # Автоматическое резервное копирование (каждую ночь в 03:00)
        self.jobs.append(aiocron.crontab(
            '0 3 * * *',  # Каждый день в 03:00
            func=self._create_backup,
            tz=TIME_CONFIG.TIMEZONE
        ))
        logger.info("📦 Настроен автоматический бекап БД: каждый день в 03:00")

    def _get_cron_days(self, days_list):
        """Конвертирует список дней в формат cron (0=воскресенье, 1=понедельник)"""
        # Конвертируем: 0(пн)->1, 1(вт)->2, ..., 4(пт)->5
        cron_days = [str(day + 1) for day in days_list]
        return ','.join(cron_days)

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
