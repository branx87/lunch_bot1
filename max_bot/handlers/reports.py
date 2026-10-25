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
    date_caption,
)
from max_bot.keyboards import (month_selection_keyboard, report_type_keyboard,
                                main_menu_keyboard, report_date_keyboard)

logger = logging.getLogger(__name__)
router = Router()


@router.message_created(F.message.body.text == "📊 Отчет за день")
async def daily_report(event: MessageCreated):
    """Выбор дня для дневного отчёта.

    Под новым поставщиком заказ на сегодня не принимается, поэтому
    день отчёта выбирается явно.
    """
    user_id = event.user.user_id
    role = get_user_role(user_id, MESSENGER_MAX, CONFIG)

    if role not in ('admin', 'provider', 'accountant'):
        await event.message.answer("❌ У вас нет прав для просмотра отчетов.")
        return

    await event.message.answer("📅 Выберите день для отчёта:", attachments=[report_date_keyboard()])


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

    from services.time_service import get_month_date_range
    period = "current" if month == "month_current" else "previous"
    start_date, end_date = get_month_date_range(period)

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


@router.message_callback(F.callback.payload.startswith("rday_"))
async def on_report_date(event: MessageCallback):
    """Формирует дневной отчёт за выбранный день."""
    user_id = event.user.user_id
    role = get_user_role(user_id, MESSENGER_MAX, CONFIG)

    if role not in ('admin', 'provider', 'accountant'):
        await event.answer(notification="❌ Нет прав")
        return

    raw_date = event.callback.payload[len("rday_"):]
    try:
        target = datetime.strptime(raw_date, "%Y-%m-%d").date()
    except ValueError:
        await event.answer(notification="❌ Некорректная дата")
        return

    caption_text = date_caption(target)

    with db.get_session() as session:
        if role in ('provider', 'admin'):
            text, total = generate_provider_report_text(
                target, target, session, caption_text)
            await event.message.answer(text)

        if role == 'admin':
            file_path, file_name, caption = generate_admin_report_file(
                target, target, session, is_daily=True, caption=caption_text)
            if file_path:
                await event.message.answer(caption, attachments=[InputMedia(path=file_path)])
            else:
                await event.message.answer(caption)
        elif role == 'accountant':
            file_path, file_name, caption = generate_accounting_report_file(
                target, target, session)
            await event.message.answer(caption, attachments=[InputMedia(path=file_path)])

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
