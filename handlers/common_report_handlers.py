# ##handlers/common_report_handlers.py

from telegram import Update
from telegram.ext import ContextTypes, ConversationHandler
from datetime import datetime, date, timedelta
from database import db
from config import CONFIG
from constants import SELECT_MONTH_RANGE, SELECT_REPORT_DATE, SELECT_REPORT_DATE_STAFF
from handlers.common import show_main_menu
from utils import check_registration, logger
from report_generators import export_daily_admin_report, export_daily_orders_for_provider, export_monthly_report, export_accounting_report, export_orders_for_provider


async def select_report_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выбор дня для дневного отчёта.

    Под новым поставщиком заказ на сегодня не принимается, поэтому
    дневной отчёт всегда строится за конкретный выбранный день.
    """
    try:
        text = update.message.text.strip()
        user_id = update.effective_user.id
        report_type = context.user_data.get('report_type')

        if not report_type:
            await update.message.reply_text("❌ Не выбран тип отчёта")
            return await show_main_menu(update, user_id)

        if text in ("🏠 Главное меню", "Вернуться в главное меню"):
            return await show_main_menu(update, user_id)

        from services.time_service import report_date_options
        from services.report_service import date_caption

        target = None
        for d, label in report_date_options():
            if label == text:
                target = d
                break

        if target is None:
            await update.message.reply_text(
                "❌ Пожалуйста, выберите день из предложенных вариантов"
            )
            return context.user_data.get('report_date_state', SELECT_REPORT_DATE)

        base_type = report_type.split('_')[0]
        caption = date_caption(target)

        if base_type == 'admin':
            await export_daily_admin_report(update, context, target, caption)
        elif base_type == 'provider':
            await export_daily_orders_for_provider(update, context, target, caption)
        elif base_type == 'accounting':
            await export_accounting_report(update, context, target, target)
        else:
            await update.message.reply_text("❌ Неизвестный тип отчёта")

        return await show_main_menu(update, user_id)

    except Exception as e:
        logger.error(f"Ошибка в select_report_date: {e}", exc_info=True)
        await update.message.reply_text("❌ Ошибка при формировании отчёта")
        return await show_main_menu(update, update.effective_user.id)


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

        from services.time_service import get_month_date_range
        
        start_date = end_date = None

        if text == "Текущий месяц":
            start_date, end_date = get_month_date_range("current")
        elif text == "Прошлый месяц":
            start_date, end_date = get_month_date_range("previous")
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