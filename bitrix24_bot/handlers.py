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

# ------------------------------------------------------------------
# Dialog state storage: dialog_id → state dict
# ------------------------------------------------------------------

_state: dict[str, dict] = {}

# State keys
S_STEP   = "step"       # current step name
S_PERIOD = "period"     # "day" | "month_current" | "month_prev"
S_RTYPE  = "rtype"      # "admin" | "accounting" | "provider"

STEP_IDLE        = "idle"
STEP_PERIOD      = "select_period"
STEP_RTYPE       = "select_rtype"
STEP_MONTH_RANGE = "select_month_range"

# ------------------------------------------------------------------
# Keyboards
# ------------------------------------------------------------------

def _kb(*rows):
    """Build keyboard: list of button rows, each button is a dict."""
    return list(rows)


def _btn(text: str, value: str, bg: str = "#29619b") -> dict:
    return {"TEXT": text, "ACTION": "SEND", "ACTION_VALUE": value,
            "BG_COLOR": bg, "TEXT_COLOR": "#fff"}


KB_MAIN_ADMIN = _kb(
    [_btn("📊 Отчёты", "отчёты"), _btn("📋 Заказы сегодня", "заказы сегодня")],
)

KB_MAIN_PROVIDER = _kb(
    [_btn("📋 Заказы сегодня", "заказы сегодня")],
)

KB_MAIN_ACCOUNTANT = _kb(
    [_btn("📊 Отчёты", "отчёты")],
)

KB_PERIOD = _kb(
    [_btn("📅 За сегодня", "за сегодня", "#3b7abf"),
     _btn("📆 За месяц",   "за месяц",   "#3b7abf")],
    [_btn("🏠 Главное меню", "главное меню", "#555")],
)

KB_RTYPE_ADMIN = _kb(
    [_btn("👨‍💼 Админский",    "тип админский",    "#29619b"),
     _btn("💰 Бухгалтерский", "тип бухгалтерский", "#5c7a3e"),
     _btn("📦 Поставщика",    "тип поставщика",    "#7a5c3e")],
    [_btn("🏠 Главное меню", "главное меню", "#555")],
)

KB_RTYPE_LIMITED = _kb(
    [_btn("📊 Мой отчёт", "тип авто", "#29619b")],
    [_btn("🏠 Главное меню", "главное меню", "#555")],
)

KB_MONTH_RANGE = _kb(
    [_btn("📅 Текущий месяц", "текущий месяц", "#3b7abf"),
     _btn("📆 Прошлый месяц", "прошлый месяц", "#3b7abf")],
    [_btn("🏠 Главное меню", "главное меню", "#555")],
)


def _main_kb(role: str) -> list:
    if role == "admin":
        return KB_MAIN_ADMIN
    elif role == "accountant":
        return KB_MAIN_ACCOUNTANT
    else:
        return KB_MAIN_PROVIDER


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------

async def handle_message(
    dialog_id: str,
    from_user_id: int,
    text: str,
    command: str = "",
    command_params: str = "",
) -> list[dict]:
    role = get_user_role(from_user_id, MESSENGER_BITRIX24, CONFIG)
    if not role or role == "employee":
        return [_msg("⛔ Доступ только для администраторов, поставщиков и бухгалтеров.")]

    # Use ACTION_VALUE (text) since buttons send text via ACTION=SEND
    raw = text.strip().lower()

    # Global resets
    if raw in ("главное меню", "/start", "start", "помощь", "help", "/help"):
        _state.pop(dialog_id, None)
        return [_msg(_help_text(role), keyboard=_main_kb(role))]

    state = _state.get(dialog_id, {S_STEP: STEP_IDLE})
    step  = state.get(S_STEP, STEP_IDLE)

    # Step routing
    if raw in ("отчёты", "отчеты", "/reports"):
        _state[dialog_id] = {S_STEP: STEP_PERIOD}
        return [_msg("Выберите период:", keyboard=KB_PERIOD)]

    if raw == "заказы сегодня":
        _state.pop(dialog_id, None)
        return await _do_orders_today()

    # ---- SELECT PERIOD ----
    if step == STEP_PERIOD or raw in ("за сегодня", "за месяц"):
        if raw == "за сегодня":
            _state[dialog_id] = {S_STEP: STEP_RTYPE, S_PERIOD: "day"}
            kb = KB_RTYPE_ADMIN if role == "admin" else KB_RTYPE_LIMITED
            return [_msg("Выберите тип отчёта:", keyboard=kb)]

        if raw == "за месяц":
            _state[dialog_id] = {S_STEP: STEP_MONTH_RANGE, S_PERIOD: "month"}
            kb = KB_RTYPE_ADMIN if role == "admin" else KB_RTYPE_LIMITED
            return [_msg("Выберите тип отчёта:", keyboard=kb)]

        return [_msg("Используйте кнопки ниже:", keyboard=KB_PERIOD)]

    # ---- SELECT REPORT TYPE ----
    if step == STEP_RTYPE:
        period = state.get(S_PERIOD, "day")
        rtype  = _parse_rtype(raw, role)
        if rtype is None:
            kb = KB_RTYPE_ADMIN if role == "admin" else KB_RTYPE_LIMITED
            return [_msg("Используйте кнопки ниже:", keyboard=kb)]

        if period == "day":
            _state.pop(dialog_id, None)
            return await _do_report(rtype, "day", None, role)

        # month — ask range
        _state[dialog_id] = {S_STEP: STEP_MONTH_RANGE, S_PERIOD: "month", S_RTYPE: rtype}
        return [_msg("Выберите период:", keyboard=KB_MONTH_RANGE)]

    # ---- SELECT MONTH RANGE ----
    if step == STEP_MONTH_RANGE:
        rtype = state.get(S_RTYPE)
        if rtype is None:
            # rtype not chosen yet — maybe user came here via direct "за месяц"
            rtype = _parse_rtype(raw, role)
            if rtype is None:
                # they're choosing range but rtype still unknown — ask type first
                kb = KB_RTYPE_ADMIN if role == "admin" else KB_RTYPE_LIMITED
                _state[dialog_id] = {S_STEP: STEP_RTYPE, S_PERIOD: "month"}
                return [_msg("Выберите тип отчёта:", keyboard=kb)]
            _state[dialog_id] = {S_STEP: STEP_MONTH_RANGE, S_PERIOD: "month", S_RTYPE: rtype}
            return [_msg("Выберите период:", keyboard=KB_MONTH_RANGE)]

        if raw in ("текущий месяц", "текущий"):
            _state.pop(dialog_id, None)
            return await _do_report(rtype, "month_current", None, role)

        if raw in ("прошлый месяц", "прошлый"):
            _state.pop(dialog_id, None)
            return await _do_report(rtype, "month_prev", None, role)

        return [_msg("Используйте кнопки ниже:", keyboard=KB_MONTH_RANGE)]

    # ---- IDLE: unknown text ----
    return [_msg(_help_text(role), keyboard=_main_kb(role))]


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _parse_rtype(raw: str, role: str) -> str | None:
    """Map button text → internal report type. Auto-detect for non-admins."""
    if raw in ("тип авто",):
        return role  # accountant→"accounting" handled below
    if raw == "тип админский":
        return "admin"
    if raw == "тип бухгалтерский":
        return "accounting"
    if raw == "тип поставщика":
        return "provider"
    return None


async def _do_orders_today() -> list[dict]:
    today = datetime.now(CONFIG.timezone).date()
    text, _ = await _run_sync(generate_provider_report_text, today, today)
    return [_msg(text)]


async def _do_report(rtype: str, period: str, _unused, role: str) -> list[dict]:
    """Generate report for given type and period."""
    now   = datetime.now(CONFIG.timezone)
    today = now.date()

    if period == "day":
        start_date = end_date = today
    elif period == "month_current":
        start_date = today.replace(day=1)
        end_date   = today
    else:  # month_prev
        first_this = today.replace(day=1)
        last_prev  = first_this - timedelta(days=1)
        start_date = last_prev.replace(day=1)
        end_date   = last_prev

    # For non-admin roles, rtype is the role name — map to report type
    if rtype == "accountant":
        rtype = "accounting"
    elif rtype == "provider":
        rtype = "provider"

    messages = []

    if rtype in ("admin", "provider"):
        text, _ = await _run_sync(generate_provider_report_text, start_date, end_date)
        messages.append(_msg(text))

    if rtype == "admin":
        file_path, file_name, caption = await _run_sync(
            generate_admin_report_file, start_date, end_date,
            is_daily=(period == "day")
        )
        if file_path:
            messages.append(_msg(caption, file_path=file_path, file_name=file_name))
        else:
            messages.append(_msg(caption))

    elif rtype == "accounting":
        file_path, file_name, caption = await _run_sync(
            generate_accounting_report_file, start_date, end_date
        )
        if file_path:
            messages.append(_msg(caption, file_path=file_path, file_name=file_name))
        else:
            messages.append(_msg(caption))

    return messages or [_msg("Нет данных за выбранный период.")]


def _help_text(role: str) -> str:
    lines = ["[B]Бот отчётов ЕРС Обеды[/B]\n"]
    if role in ("admin", "provider"):
        lines.append("📋 [B]заказы сегодня[/B] — список заказов на сегодня по локациям")
    if role in ("admin", "provider", "accountant"):
        lines.append("📊 [B]отчёты[/B] — формирование отчётов")
    return "\n".join(lines)


def _msg(text: str, keyboard=None, file_path: str = None, file_name: str = None) -> dict:
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


# ------------------------------------------------------------------
# Helper: run synchronous DB call in thread pool
# ------------------------------------------------------------------

def _run_in_session(fn, *args, **kwargs):
    with db.get_session() as session:
        return fn(*args, session, **kwargs)


async def _run_sync(fn, *args, **kwargs):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: _run_in_session(fn, *args, **kwargs))
