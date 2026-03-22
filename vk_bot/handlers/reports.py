"""Report handlers for VK bot."""
import logging
from datetime import datetime, timedelta

from vkbottle.bot import BotLabeler, Message, MessageEvent, rules
from vkbottle import GroupEventType
from vkbottle.tools import DocMessagesUploader

from database import db
from config import CONFIG
from services.user_service import get_user_role, MESSENGER_VK
from services.report_service import (
    generate_provider_report_text,
    generate_accounting_report_file,
    generate_admin_report_file,
)
from vk_bot.keyboards import month_selection_keyboard, report_type_keyboard

logger = logging.getLogger(__name__)
reports_labeler = BotLabeler()


@reports_labeler.private_message(text="📊 Отчет за день")
async def daily_report(message: Message):
    """Generate and send daily report."""
    user_id = message.from_id
    role = get_user_role(user_id, MESSENGER_VK, CONFIG)

    if role not in ('admin', 'provider', 'accountant'):
        await message.answer("❌ У вас нет прав для просмотра отчетов.")
        return

    today = datetime.now(CONFIG.timezone).date()

    with db.get_session() as session:
        if role in ('provider', 'admin'):
            text, total = generate_provider_report_text(today, today, session)
            await message.answer(text)

        if role == 'admin':
            file_path, file_name, caption = generate_admin_report_file(today, today, session, is_daily=True)
            if file_path:
                doc_uploader = DocMessagesUploader(message.ctx_api)
                doc = await doc_uploader.upload(
                    file_source=file_path,
                    peer_id=message.peer_id,
                )
                await message.answer(caption, attachment=doc)
            else:
                await message.answer(caption)


@reports_labeler.private_message(text="📅 Отчет за месяц")
async def monthly_report_menu(message: Message):
    """Show month selection."""
    user_id = message.from_id
    role = get_user_role(user_id, MESSENGER_VK, CONFIG)

    if role not in ('admin', 'provider', 'accountant'):
        await message.answer("❌ У вас нет прав для просмотра отчетов.")
        return

    await message.answer("Выберите период:", keyboard=month_selection_keyboard())


REPORT_CMDS = {"month", "report"}


@reports_labeler.raw_event(
    GroupEventType.MESSAGE_EVENT,
    MessageEvent,
    rules.FuncRule(lambda e: e.object.payload.get("cmd") in REPORT_CMDS),
)
async def on_report_callback(event: MessageEvent):
    """Handle month selection and report type callbacks."""
    payload = event.object.payload
    cmd = payload.get("cmd")
    user_id = event.object.peer_id

    if cmd == "month":
        role = get_user_role(user_id, MESSENGER_VK, CONFIG)
        if role not in ('admin', 'provider', 'accountant'):
            await event.show_snackbar("Нет прав")
            return

        period = payload.get("p", "current")
        now = datetime.now(CONFIG.timezone)

        if period == "current":
            start_date = now.replace(day=1).date()
            end_date = now.date()
        else:
            first = now.replace(day=1)
            last_month_end = first - timedelta(days=1)
            start_date = last_month_end.replace(day=1).date()
            end_date = last_month_end.date()

        if role == 'provider':
            with db.get_session() as session:
                text, total = generate_provider_report_text(start_date, end_date, session)
            await event.edit_message(text)
            return

        if role == 'accountant':
            with db.get_session() as session:
                file_path, file_name, caption = generate_accounting_report_file(start_date, end_date, session)
            doc_uploader = DocMessagesUploader(event.ctx_api)
            doc = await doc_uploader.upload(file_source=file_path, peer_id=user_id)
            await event.ctx_api.messages.send(
                peer_id=user_id, message=caption, attachment=doc, random_id=0
            )
            await event.send_empty_answer()
            return

        # Admin — show report type selector
        await event.edit_message(
            f"Период: {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}\n"
            "Выберите тип отчета:",
            keyboard=report_type_keyboard()
        )
        return

    if cmd == "report":
        role = get_user_role(user_id, MESSENGER_VK, CONFIG)
        if role != 'admin':
            await event.show_snackbar("Нет прав")
            return

        report_type = payload.get("t", "")
        now = datetime.now(CONFIG.timezone)
        start_date = now.replace(day=1).date()
        end_date = now.date()

        with db.get_session() as session:
            if report_type == "provider":
                text, total = generate_provider_report_text(start_date, end_date, session)
                await event.edit_message(text)

            elif report_type == "accounting":
                file_path, file_name, caption = generate_accounting_report_file(start_date, end_date, session)
                doc_uploader = DocMessagesUploader(event.ctx_api)
                doc = await doc_uploader.upload(file_source=file_path, peer_id=user_id)
                await event.ctx_api.messages.send(
                    peer_id=user_id, message=caption, attachment=doc, random_id=0
                )
                await event.send_empty_answer()

            elif report_type == "admin":
                file_path, file_name, caption = generate_admin_report_file(start_date, end_date, session)
                if file_path:
                    doc_uploader = DocMessagesUploader(event.ctx_api)
                    doc = await doc_uploader.upload(file_source=file_path, peer_id=user_id)
                    await event.ctx_api.messages.send(
                        peer_id=user_id, message=caption, attachment=doc, random_id=0
                    )
                    await event.send_empty_answer()
                else:
                    await event.edit_message(caption)
