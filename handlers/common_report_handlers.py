# ##handlers/common_report_handlers.py

from telegram import Update
from telegram.ext import ContextTypes, ConversationHandler
from datetime import datetime, date, timedelta
from config import CONFIG, TIMEZONE
from constants import SELECT_MONTH_RANGE
from handlers.common import show_main_menu
from utils import check_registration, logger
from report_generators import ReportGenerator


async def select_month_range(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обрабатывает выбор периода для формирования отчета
    """
    try:
        text = update.message.text.strip()
        user_id = update.effective_user.id
        report_type = context.user_data.get('report_type')

        # Если нет типа отчета — выходим
        if not report_type:
            await update.message.reply_text("❌ Не выбран тип отчета")
            return await show_main_menu(update, user_id)

        now = datetime.now(TIMEZONE)
        start_date = end_date = None

        if text == "Текущий месяц":
            start_date = now.replace(day=1).date()
            end_date = now.date()
        elif text == "Прошлый месяц":
            first_day_current_month = now.replace(day=1)
            last_day_prev_month = first_day_current_month - timedelta(days=1)
            start_date = last_day_prev_month.replace(day=1).date()
            end_date = last_day_prev_month.date()
        elif text == "Вернуться в главное меню":
            return await show_main_menu(update, user_id)
        else:
            await update.message.reply_text("❌ Пожалуйста, выберите период из предложенных вариантов")
            return SELECT_MONTH_RANGE

        # Определяем базовый тип отчета (без суффикса _daily/_monthly)
        base_report_type = report_type.split('_')[0]
        
        # Вызываем нужный отчет
        if base_report_type == 'admin':
            if report_type.endswith('_daily'):
                await export_daily_admin_report(update, context, start_date)
            else:
                await export_monthly_report(update, context, start_date, end_date)
        elif base_report_type == 'accounting':
            await export_accounting_report(update, context, start_date, end_date)
        elif base_report_type == 'provider':
            if report_type.endswith('_daily'):
                await export_daily_orders_for_provider(update, context, start_date)
            else:
                await export_orders_for_provider(update, context, start_date, end_date)
        else:
            await update.message.reply_text("❌ Неизвестный тип отчета")

        return await show_main_menu(update, user_id)

    except Exception as e:
        logger.error(f"Ошибка в select_month_range: {e}", exc_info=True)
        await update.message.reply_text("❌ Ошибка при формировании отчета")
        return await show_main_menu(update, user_id)