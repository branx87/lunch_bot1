# ##handlers/report_callbacks.py
from telegram import Update
from telegram.ext import CallbackQueryHandler
from telegram.ext import ContextTypes
from datetime import date, datetime, timedelta
import logging
import matplotlib

matplotlib.use('Agg')

from config import CONFIG, TIMEZONE
from constants import SELECT_MONTH_RANGE
from handlers.common import show_main_menu
from report_generators import export_accounting_report, export_monthly_report, export_orders_for_provider

logger = logging.getLogger(__name__)

async def generate_report(update: Update, context: ContextTypes.DEFAULT_TYPE, 
                         user_id: int, start_date: date, end_date: date):
    """
    Генерирует отчеты в зависимости от роли пользователя:
    - Администраторы получают полный отчет (export_monthly_report)
    - Бухгалтеры - бухгалтерский отчет (export_accounting_report)
    - Поставщики - отчет по заказам (export_orders_for_provider)
    Обрабатывает ошибки генерации отчетов.
    """
    from admin import export_orders_for_provider, export_accounting_report, export_monthly_report
    try:
        if user_id in CONFIG.get('admin_ids', []):
            await export_monthly_report(update, context, start_date, end_date)
        elif user_id in CONFIG.get('accounting_ids', []):
            await export_accounting_report(update, context, start_date, end_date)
        elif user_id in CONFIG.get('provider_ids', []):
            await export_orders_for_provider(update, context, start_date, end_date)
        else:
            await update.message.reply_text("❌ Нет прав")
    except Exception as e:
        logger.error(f"Ошибка: {e}", exc_info=True)
        await update.message.reply_text("❌ Ошибка отчета")