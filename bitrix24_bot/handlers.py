"""
Bitrix24 bot command handlers.
Uses the same services/report_service as Telegram and VK bots.
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional

from config import CONFIG
from database import db
from services.report_service import (
    generate_provider_report_text,
    generate_accounting_report_file,
    generate_admin_report_file,
)
from services.user_service import get_user_role, MESSENGER_BITRIX24
from bitrix24_bot.api import BitrixBotAPI

logger = logging.getLogger(__name__)

# Keyboard shown after every response
MAIN_KEYBOARD = [
    [
        {"TEXT": "📋 Заказы сегодня", "COMMAND": "orders_today", "COMMAND_PARAMS": "", "BG_COLOR": "#29619b", "TEXT_COLOR": "#fff"},
        {"TEXT": "📊 Отчёт за день",  "COMMAND": "report_day",   "COMMAND_PARAMS": "", "BG_COLOR": "#29619b", "TEXT_COLOR": "#fff"},
    ],
    [
        {"TEXT": "📊 За неделю", "COMMAND": "report_week",  "COMMAND_PARAMS": "", "BG_COLOR": "#3b7abf", "TEXT_COLOR": "#fff"},
        {"TEXT": "📊 За месяц",  "COMMAND": "report_month", "COMMAND_PARAMS": "", "BG_COLOR": "#3b7abf", "TEXT_COLOR": "#fff"},
    ],
]

# Text aliases → command name (lowercase)
_TEXT_ALIASES = {
    "помощь": "help", "help": "help", "/help": "help",
    "заказы": "orders_today", "заказы сегодня": "orders_today", "сегодня": "orders_today",
    "за день": "report_day", "день": "report_day",
    "за неделю": "report_week", "неделя": "report_week",
    "за месяц": "report_month", "месяц": "report_month",
}


async def handle_message(
    api: BitrixBotAPI,
    dialog_id: str,
    from_user_id: int,
    text: str,
    command: str = "",
    command_params: str = "",
) -> None:
    """
    Entry point: process one incoming bot message and send response.
    Runs in a background task so FastAPI responds to Bitrix immediately.
    """
    role = get_user_role(from_user_id, MESSENGER_BITRIX24, CONFIG)
    if not role or role == "employee":
        await api.send_message(dialog_id, "⛔ Доступ только для администраторов, поставщиков и бухгалтеров.")
        return

    await api.send_typing(dialog_id)

    # Keyboard button command takes priority over typed text
    cmd = command or _TEXT_ALIASES.get(text.strip().lower(), "")

    try:
        if not cmd or cmd == "help":
            await api.send_message(dialog_id, _help_text(role), keyboard=MAIN_KEYBOARD)
        elif cmd == "orders_today":
            await _orders_today(api, dialog_id, role)
        elif cmd == "report_day":
            await _report_day(api, dialog_id, role)
        elif cmd == "report_week":
            await _report_week(api, dialog_id, role)
        elif cmd == "report_month":
            await _report_month(api, dialog_id, role)
        else:
            await api.send_message(
                dialog_id,
                "Не понял команду. Используйте кнопки меню ниже.",
                keyboard=MAIN_KEYBOARD,
            )
    except Exception as e:
        logger.error(f"[B24Bot] Ошибка обработки команды '{cmd}': {e}", exc_info=True)
        await api.send_message(dialog_id, "❌ Ошибка при формировании отчёта.", keyboard=MAIN_KEYBOARD)


# ------------------------------------------------------------------
# Command implementations
# ------------------------------------------------------------------

async def _orders_today(api: BitrixBotAPI, dialog_id: str, role: str) -> None:
    today = datetime.now(CONFIG.timezone).date()
    text, total = await _run_sync(generate_provider_report_text, today, today)
    await api.send_message(dialog_id, text, keyboard=MAIN_KEYBOARD)


async def _report_day(api: BitrixBotAPI, dialog_id: str, role: str) -> None:
    today = datetime.now(CONFIG.timezone).date()

    if role in ("admin", "provider"):
        text, total = await _run_sync(generate_provider_report_text, today, today)
        await api.send_message(dialog_id, text, keyboard=MAIN_KEYBOARD)

    if role == "admin":
        file_path, file_name, caption = await _run_sync(
            generate_admin_report_file, today, today, is_daily=True
        )
        if file_path:
            await api.send_file(dialog_id, file_path, file_name, caption)
        else:
            await api.send_message(dialog_id, caption, keyboard=MAIN_KEYBOARD)

    elif role == "accountant":
        file_path, file_name, caption = await _run_sync(
            generate_accounting_report_file, today, today
        )
        await api.send_file(dialog_id, file_path, file_name, caption)


async def _report_week(api: BitrixBotAPI, dialog_id: str, role: str) -> None:
    today = datetime.now(CONFIG.timezone).date()
    monday = today - timedelta(days=today.weekday())
    await _send_period_report(api, dialog_id, role, monday, today)


async def _report_month(api: BitrixBotAPI, dialog_id: str, role: str) -> None:
    today = datetime.now(CONFIG.timezone).date()
    first_day = today.replace(day=1)
    await _send_period_report(api, dialog_id, role, first_day, today)


async def _send_period_report(
    api: BitrixBotAPI,
    dialog_id: str,
    role: str,
    start_date,
    end_date,
) -> None:
    if role in ("admin", "provider"):
        text, total = await _run_sync(generate_provider_report_text, start_date, end_date)
        await api.send_message(dialog_id, text, keyboard=MAIN_KEYBOARD)

    if role == "admin":
        file_path, file_name, caption = await _run_sync(
            generate_admin_report_file, start_date, end_date
        )
        if file_path:
            await api.send_file(dialog_id, file_path, file_name, caption)
        else:
            await api.send_message(dialog_id, caption, keyboard=MAIN_KEYBOARD)

    elif role == "accountant":
        file_path, file_name, caption = await _run_sync(
            generate_accounting_report_file, start_date, end_date
        )
        await api.send_file(dialog_id, file_path, file_name, caption)


def _help_text(role: str) -> str:
    lines = ["[B]Бот отчётов ЕРС Обеды[/B]\n"]
    if role in ("admin", "provider"):
        lines.append("📋 [B]заказы сегодня[/B] — список заказов на сегодня по локациям")
        lines.append("📊 [B]за день[/B] — сводка за сегодня")
        lines.append("📊 [B]за неделю[/B] — сводка за текущую неделю")
        lines.append("📊 [B]за месяц[/B] — сводка за текущий месяц")
    if role == "accountant":
        lines.append("📊 [B]за неделю[/B] — ведомость удержаний за неделю (Excel)")
        lines.append("📊 [B]за месяц[/B] — ведомость удержаний за месяц (Excel)")
    return "\n".join(lines)


# ------------------------------------------------------------------
# Helper: run synchronous DB call in thread pool
# ------------------------------------------------------------------

def _run_in_session(fn, *args, **kwargs):
    """Execute a report_service function that takes (start, end, session) or (start, end, session, **kw)."""
    with db.get_session() as session:
        return fn(*args, session, **kwargs)


async def _run_sync(fn, *args, **kwargs):
    """Run synchronous report_service function in a thread pool executor."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: _run_in_session(fn, *args, **kwargs))
