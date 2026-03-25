"""
Bitrix24 bot command handlers.
Uses the same services/report_service as Telegram and VK bots.

Returns list of message dicts for PHP to send via \Bitrix\Im\Bot::addMessage().
Each message: {"text": "...", "keyboard": [...], "file_path": "...", "file_name": "..."}
"""
import asyncio
import base64
import logging
from datetime import datetime, timedelta

from config import CONFIG
from database import db
from services.report_service import (
    generate_provider_report_text,
    generate_accounting_report_file,
    generate_admin_report_file,
)
from services.user_service import get_user_role, MESSENGER_BITRIX24

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

# Keyboard for admins — includes accounting report buttons
ADMIN_KEYBOARD = MAIN_KEYBOARD + [
    [
        {"TEXT": "🧾 Ведомость за неделю", "COMMAND": "accounting_week",  "COMMAND_PARAMS": "", "BG_COLOR": "#5c7a3e", "TEXT_COLOR": "#fff"},
        {"TEXT": "🧾 Ведомость за месяц",  "COMMAND": "accounting_month", "COMMAND_PARAMS": "", "BG_COLOR": "#5c7a3e", "TEXT_COLOR": "#fff"},
    ],
]

# Text aliases → command name (lowercase)
_TEXT_ALIASES = {
    "помощь": "help", "help": "help", "/help": "help",
    "заказы": "orders_today", "заказы сегодня": "orders_today", "сегодня": "orders_today",
    "за день": "report_day", "день": "report_day",
    "за неделю": "report_week", "неделя": "report_week",
    "за месяц": "report_month", "месяц": "report_month",
    "ведомость за неделю": "accounting_week",
    "ведомость за месяц": "accounting_month",
}


def _msg(text: str, keyboard=None, file_path: str = None, file_name: str = None) -> dict:
    """Build a message dict for the response."""
    m = {"text": text}
    if keyboard is not None:
        m["keyboard"] = keyboard
    if file_path and file_name:
        try:
            with open(file_path, "rb") as f:
                m["file_base64"] = base64.b64encode(f.read()).decode("ascii")
            m["file_name"] = file_name
        except Exception as e:
            logger.error(f"[B24Bot] Ошибка чтения файла {file_path}: {e}")
    return m


async def handle_message(
    dialog_id: str,
    from_user_id: int,
    text: str,
    command: str = "",
    command_params: str = "",
) -> list[dict]:
    """
    Process one incoming bot message.
    Returns list of message dicts for PHP to send.
    """
    role = get_user_role(from_user_id, MESSENGER_BITRIX24, CONFIG)
    if not role or role == "employee":
        return [_msg("⛔ Доступ только для администраторов, поставщиков и бухгалтеров.")]

    keyboard = ADMIN_KEYBOARD if role == "admin" else MAIN_KEYBOARD

    # Keyboard button command takes priority over typed text
    cmd = command or _TEXT_ALIASES.get(text.strip().lower(), "")

    try:
        if not cmd or cmd == "help":
            return [_msg(_help_text(role), keyboard=keyboard)]
        elif cmd == "orders_today":
            return await _orders_today(role)
        elif cmd == "report_day":
            return await _report_day(role)
        elif cmd == "report_week":
            return await _report_week(role)
        elif cmd == "report_month":
            return await _report_month(role)
        elif cmd == "accounting_week":
            return await _accounting_week()
        elif cmd == "accounting_month":
            return await _accounting_month()
        else:
            return [_msg("Не понял команду. Используйте кнопки меню ниже.", keyboard=keyboard)]
    except Exception as e:
        logger.error(f"[B24Bot] Ошибка обработки команды '{cmd}': {e}", exc_info=True)
        return [_msg("❌ Ошибка при формировании отчёта.", keyboard=keyboard)]


# ------------------------------------------------------------------
# Command implementations
# ------------------------------------------------------------------

async def _orders_today(role: str) -> list[dict]:
    today = datetime.now(CONFIG.timezone).date()
    text, total = await _run_sync(generate_provider_report_text, today, today)
    return [_msg(text, keyboard=MAIN_KEYBOARD)]


async def _report_day(role: str) -> list[dict]:
    today = datetime.now(CONFIG.timezone).date()
    messages = []

    if role in ("admin", "provider"):
        text, total = await _run_sync(generate_provider_report_text, today, today)
        messages.append(_msg(text, keyboard=MAIN_KEYBOARD))

    if role == "admin":
        file_path, file_name, caption = await _run_sync(
            generate_admin_report_file, today, today, is_daily=True
        )
        if file_path:
            messages.append(_msg(caption, file_path=file_path, file_name=file_name))
        else:
            messages.append(_msg(caption, keyboard=MAIN_KEYBOARD))

    elif role == "accountant":
        file_path, file_name, caption = await _run_sync(
            generate_accounting_report_file, today, today
        )
        if file_path:
            messages.append(_msg(caption, keyboard=MAIN_KEYBOARD, file_path=file_path, file_name=file_name))
        else:
            messages.append(_msg(caption, keyboard=MAIN_KEYBOARD))

    return messages


async def _report_week(role: str) -> list[dict]:
    today = datetime.now(CONFIG.timezone).date()
    monday = today - timedelta(days=today.weekday())
    return await _send_period_report(role, monday, today)


async def _report_month(role: str) -> list[dict]:
    today = datetime.now(CONFIG.timezone).date()
    first_day = today.replace(day=1)
    return await _send_period_report(role, first_day, today)


async def _accounting_week() -> list[dict]:
    today = datetime.now(CONFIG.timezone).date()
    monday = today - timedelta(days=today.weekday())
    return await _send_accounting_report(monday, today)


async def _accounting_month() -> list[dict]:
    today = datetime.now(CONFIG.timezone).date()
    first_day = today.replace(day=1)
    return await _send_accounting_report(first_day, today)


async def _send_accounting_report(start_date, end_date) -> list[dict]:
    file_path, file_name, caption = await _run_sync(
        generate_accounting_report_file, start_date, end_date
    )
    if file_path:
        return [_msg(caption, keyboard=ADMIN_KEYBOARD, file_path=file_path, file_name=file_name)]
    return [_msg(caption, keyboard=ADMIN_KEYBOARD)]


async def _send_period_report(role: str, start_date, end_date) -> list[dict]:
    keyboard = ADMIN_KEYBOARD if role == "admin" else MAIN_KEYBOARD
    messages = []

    if role in ("admin", "provider"):
        text, total = await _run_sync(generate_provider_report_text, start_date, end_date)
        messages.append(_msg(text, keyboard=keyboard))

    if role == "admin":
        file_path, file_name, caption = await _run_sync(
            generate_admin_report_file, start_date, end_date
        )
        if file_path:
            messages.append(_msg(caption, file_path=file_path, file_name=file_name))
        else:
            messages.append(_msg(caption, keyboard=keyboard))

    elif role == "accountant":
        file_path, file_name, caption = await _run_sync(
            generate_accounting_report_file, start_date, end_date
        )
        if file_path:
            messages.append(_msg(caption, keyboard=keyboard, file_path=file_path, file_name=file_name))
        else:
            messages.append(_msg(caption, keyboard=keyboard))

    return messages


def _help_text(role: str) -> str:
    lines = ["[B]Бот отчётов ЕРС Обеды[/B]\n"]
    if role in ("admin", "provider"):
        lines.append("📋 [B]заказы сегодня[/B] — список заказов на сегодня по локациям")
        lines.append("📊 [B]за день[/B] — сводка за сегодня")
        lines.append("📊 [B]за неделю[/B] — сводка за текущую неделю")
        lines.append("📊 [B]за месяц[/B] — сводка за текущий месяц")
    if role == "admin":
        lines.append("🧾 [B]ведомость за неделю[/B] — ведомость удержаний за неделю (Excel)")
        lines.append("🧾 [B]ведомость за месяц[/B] — ведомость удержаний за месяц (Excel)")
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
