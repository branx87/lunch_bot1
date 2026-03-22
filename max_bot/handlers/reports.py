"""Report handlers for Max bot."""
import logging
from datetime import datetime

from maxapi import Router, F
from maxapi.types import MessageCreated, MessageCallback, InputMedia

from database import db
from config import CONFIG
from services.user_service import get_user_role, MESSENGER_MAX
from services.report_service import (
    generate_provider_report_text,
    generate_accounting_report_file,
    generate_admin_report_file,
)
from max_bot.keyboards import month_selection_keyboard, report_type_keyboard, main_menu_keyboard

logger = logging.getLogger(__name__)
router = Router()


@router.message_created(F.message.body.text == "📊 Отчет за день")
async def daily_report(event: MessageCreated):
    """Generate and send daily report."""
    user_id = event.user.user_id
    role = get_user_role(user_id, MESSENGER_MAX, CONFIG)

    if role not in ('admin', 'provider', 'accountant'):
        await event.message.answer("❌ У вас нет прав для просмотра отчетов.")
        return

    today = datetime.now(CONFIG.timezone).date()

    with db.get_session() as session:
        if role in ('provider', 'admin'):
            text, total = generate_provider_report_text(today, today, session)
            await event.message.answer(text)

        if role in ('admin',):
            file_path, file_name, caption = generate_admin_report_file(today, today, session, is_daily=True)
            if file_path:
                await event.message.answer(caption, attachments=[InputMedia(path=file_path)])
            else:
                await event.message.answer(caption)  # "no data" message


@router.message_created(F.message.body.text == "📅 Отчет за месяц")
async def monthly_report_menu(event: MessageCreated):
    """Show month selection for monthly report."""
    user_id = event.user.user_id
    role = get_user_role(user_id, MESSENGER_MAX, CONFIG)

    if role not in ('admin', 'provider', 'accountant'):
        await event.message.answer("❌ У вас нет прав для просмотра отчетов.")
        return

    await event.message.answer("Выберите период:", attachments=[month_selection_keyboard()])


@router.message_callback(F.callback.payload.startswith("month_"))
async def on_month_selected(event: MessageCallback):
    """Handle month selection, show report type selection."""
    user_id = event.user.user_id
    role = get_user_role(user_id, MESSENGER_MAX, CONFIG)

    if role not in ('admin', 'provider', 'accountant'):
        await event.answer(notification="❌ Нет прав")
        return

    # Store selection
    month = event.callback.payload  # "month_current" or "month_previous"

    now = datetime.now(CONFIG.timezone)
    if month == "month_current":
        start_date = now.replace(day=1).date()
        end_date = now.date()
    else:
        first_of_month = now.replace(day=1)
        last_month_end = first_of_month - __import__('datetime').timedelta(days=1)
        start_date = last_month_end.replace(day=1).date()
        end_date = last_month_end.date()

    # For providers — send directly
    if role == 'provider':
        with db.get_session() as session:
            text, total = generate_provider_report_text(start_date, end_date, session)
            await event.message.answer(text)
        await event.answer()
        return

    # For admin/accountant — show report type selector
    # Store dates in callback for next step (encoded in simple format)
    # For simplicity, we'll use the month_type and recalculate
    if role == 'accountant':
        # Accountant only gets accounting report
        with db.get_session() as session:
            file_path, file_name, caption = generate_accounting_report_file(start_date, end_date, session)
        await event.message.answer(caption, attachments=[InputMedia(path=file_path)])
        await event.answer()
        return

    # Admin gets to choose
    await event.message.answer(
        f"Период: {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}\nВыберите тип отчета:",
        attachments=[report_type_keyboard()]
    )
    await event.answer()


@router.message_callback(F.callback.payload.startswith("report_"))
async def on_report_type(event: MessageCallback):
    """Generate specific report type."""
    user_id = event.user.user_id
    role = get_user_role(user_id, MESSENGER_MAX, CONFIG)
    report_type = event.callback.payload.replace("report_", "")

    if role != 'admin':
        await event.answer(notification="❌ Нет прав")
        return

    now = datetime.now(CONFIG.timezone)
    start_date = now.replace(day=1).date()
    end_date = now.date()

    with db.get_session() as session:
        if report_type == "provider":
            text, total = generate_provider_report_text(start_date, end_date, session)
            await event.message.answer(text)

        elif report_type == "accounting":
            file_path, file_name, caption = generate_accounting_report_file(start_date, end_date, session)
            await event.message.answer(caption, attachments=[InputMedia(path=file_path)])

        elif report_type == "admin":
            file_path, file_name, caption = generate_admin_report_file(start_date, end_date, session)
            if file_path:
                await event.message.answer(caption, attachments=[InputMedia(path=file_path)])
            else:
                await event.message.answer(caption)

    await event.answer()
