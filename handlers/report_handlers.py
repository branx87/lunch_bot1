# ##handlers/report_handlers.py
import logging
from datetime import date, datetime, timedelta
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import ContextTypes
import matplotlib

matplotlib.use('Agg')

from db import CONFIG
from constants import SELECT_MONTH_RANGE
from handlers.common import show_main_menu
from report_generators import export_accounting_report, export_daily_admin_report, export_monthly_report, export_orders_for_provider

logger = logging.getLogger(__name__)

# async def handle_report_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     """Обработчик запросов отчетов"""
#     from admin import export_orders_for_provider, export_accounting_report, export_monthly_report
#     user_id = update.effective_user.id
#     text = update.message.text
    
#     if text == "📊 Отчет за день":
#         today = datetime.now(TIMEZONE).date()
#         await generate_report(update, context, user_id, today, today)
#     elif text == "📅 Отчет за месяц":
#         await update.message.reply_text(
#             "Выберите период:",
#             reply_markup=ReplyKeyboardMarkup([
#                 ["Текущий месяц", "Прошлый месяц"],
#                 ["Вернуться в главное меню"]
#             ], resize_keyboard=True)
#         )
#         return SELECT_MONTH_RANGE

async def generate_report(update: Update, context: ContextTypes.DEFAULT_TYPE, 
                         user_id: int, start_date: date, end_date: date):
    """
    Генерирует отчеты в зависимости от роли пользователя:
    - Администраторы: полный месячный отчет (export_monthly_report)
    - Бухгалтеры: бухгалтерский отчет (export_accounting_report)
    - Поставщики: отчет по заказам (export_orders_for_provider)
    Обрабатывает ошибки генерации и проверяет права доступа.
    """
    from admin import export_orders_for_provider, export_accounting_report, export_monthly_report
    try:
        if user_id in CONFIG.admin_ids:
            await export_monthly_report(update, context, start_date, end_date)
        elif user_id in CONFIG.accounting_ids:
            await export_accounting_report(update, context, start_date, end_date)
        elif user_id in CONFIG.provider_ids:
            await export_orders_for_provider(update, context, start_date, end_date)
        else:
            await update.message.reply_text("❌ Нет прав")
    except Exception as e:
        logger.error(f"Ошибка: {e}", exc_info=True)
        await update.message.reply_text("❌ Ошибка отчета")

async def send_admin_daily_report(application):
    """
    Автоматически отправляет ежедневный отчет администраторам.
    Использует временные объекты Update и Context для интеграции с существующей системой отчетов.
    Логирует результаты отправки каждому администратору.
    Вызывается по расписанию из внешнего планировщика задач.
    """
    try:
        logger.info("Запуск отправки дневного админ отчета")
        
        now = datetime.now(CONFIG.timezone)
        today = now.date()
        
        # Создаем временный объект Update для передачи в функцию отчета
        class FakeUpdate:
            def __init__(self, bot, chat_id):
                self.effective_user = type('', (), {'id': 0})()  # Заглушка для user.id
                self.effective_chat = type('', (), {'id': chat_id})()
                self.message = type('', (), {'text': ''})()  # Заглушка
        
        # Создаем временный контекст
        class FakeContext:
            def __init__(self, bot):
                self.bot = bot
        
        fake_context = FakeContext(application.bot)
        
        # Отправляем каждому админу
        success = 0
        for admin_id in CONFIG.admin_ids:
            try:
                fake_update = FakeUpdate(application.bot, admin_id)
                await export_daily_admin_report(fake_update, fake_context, today)
                success += 1
            except Exception as e:
                logger.error(f"Ошибка отправки админского отчета админу {admin_id}: {e}")
        
        logger.info(f"Отправлено {success}/{len(CONFIG.get('admin_ids', []))} админам")
        
    except Exception as e:
        logger.error(f"Ошибка в send_admin_daily_report: {e}")
