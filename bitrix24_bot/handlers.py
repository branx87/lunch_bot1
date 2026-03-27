"""
Bitrix24 bot command handlers.

Button format: {"TEXT": "...", "COMMAND": "cmd_name", ...}
Using COMMAND field (not ACTION=SEND) triggers OnImbotCommandAdd
without posting visible text to chat.
"""
import asyncio
import base64
import logging
import re
from datetime import datetime, timedelta

from config import CONFIG
from database import db
from models import User
from services.order_service import (
    get_order_for_date, get_active_orders,
    create_order, cancel_order, modify_quantity,
)
from services.menu_service import get_menu_for_day, format_menu_text
from services.report_service import (
    generate_provider_report_text,
    generate_accounting_report_file,
    generate_admin_report_file,
)
from services.user_service import get_user_role, MESSENGER_BITRIX24
from time_config import TIME_CONFIG

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Dialog state storage
# ------------------------------------------------------------------

_state: dict[str, dict] = {}

S_STEP        = "step"
S_PERIOD      = "period"
S_RTYPE       = "rtype"
S_DAY         = "day"
S_ORDER_DATES = "order_dates"

STEP_IDLE        = "idle"
STEP_PERIOD      = "select_period"
STEP_RTYPE       = "select_rtype"
STEP_MONTH_RANGE = "select_month_range"
STEP_SELECT_DAY  = "select_day"
STEP_ORDER_VIEW  = "order_view"
STEP_MY_ORDERS   = "my_orders"

# ------------------------------------------------------------------
# Keyboards
# Buttons use COMMAND field — triggers OnImbotCommandAdd silently.
# English command names (no spaces, already registered in Bitrix24).
# Russian text on button face — displayed to user.
# Typed Russian text also accepted as fallback.
# ------------------------------------------------------------------

def _kb(*rows):
    return list(rows)


def _btn(text: str, cmd: str, bg: str = "#29619b") -> dict:
    """Button using COMMAND field — no visible chat text when clicked."""
    return {"TEXT": text, "COMMAND": cmd, "BG_COLOR": bg, "TEXT_COLOR": "#fff"}


KB_MAIN_ADMIN = _kb(
    [_btn("📊 Отчёты", "reports"), _btn("📋 Заказы сегодня", "orders_today")],
    [_btn("🛒 Заказать", "order", "#2a7a2a"), _btn("📋 Мои заказы", "my_orders")],
    [_btn("🍽 Меню", "menu")],
)

KB_MAIN_PROVIDER = _kb(
    [_btn("📋 Заказы сегодня", "orders_today")],
)

KB_MAIN_ACCOUNTANT = _kb(
    [_btn("📊 Отчёты", "reports")],
)

KB_PERIOD = _kb(
    [_btn("📅 За сегодня", "period_day",   "#3b7abf"),
     _btn("📆 За месяц",   "period_month", "#3b7abf")],
    [_btn("🏠 Главное меню", "main_menu", "#555")],
)

KB_RTYPE_ADMIN = _kb(
    [_btn("👨‍💼 Админский",    "rtype_admin",      "#29619b"),
     _btn("💰 Бухгалтерский", "rtype_accounting", "#5c7a3e"),
     _btn("📦 Поставщика",    "rtype_provider",   "#7a5c3e")],
    [_btn("🏠 Главное меню", "main_menu", "#555")],
)

KB_RTYPE_LIMITED = _kb(
    [_btn("📊 Мой отчёт", "rtype_auto", "#29619b")],
    [_btn("🏠 Главное меню", "main_menu", "#555")],
)

KB_MONTH_RANGE = _kb(
    [_btn("📅 Текущий месяц", "month_current", "#3b7abf"),
     _btn("📆 Прошлый месяц", "month_prev",    "#3b7abf")],
    [_btn("🏠 Главное меню", "main_menu", "#555")],
)

KB_MAIN_EMPLOYEE = _kb(
    [_btn("🛒 Заказать", "order", "#2a7a2a"),
     _btn("📋 Мои заказы", "my_orders")],
    [_btn("🍽 Меню", "menu")],
)


def _build_select_day_kb() -> list:
    from services.menu_service import get_week_menus
    DAYS_SHORT = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    week = get_week_menus(CONFIG)
    buttons = []
    for day in week:
        if day["is_weekend"] or day["is_holiday"] or not day["menu"]:
            continue
        offset = day["day_offset"]
        date_str = day["target_date"].strftime("%d.%m")
        day_short = DAYS_SHORT[day["target_date"].weekday()]
        label = f"{'Сегодня' if offset == 0 else 'Завтра' if offset == 1 else day_short} {date_str}"
        buttons.append(_btn(f"📅 {label}", f"day_{offset}", "#3b7abf"))
    rows = [buttons[i:i+2] for i in range(0, len(buttons), 2)]
    rows.append([_btn("🏠 Главное меню", "main_menu", "#555")])
    return _kb(*rows)


def _main_kb(role: str) -> list:
    if role == "admin":
        return KB_MAIN_ADMIN
    elif role == "accountant":
        return KB_MAIN_ACCOUNTANT
    elif role == "employee":
        return KB_MAIN_EMPLOYEE
    else:
        return KB_MAIN_PROVIDER


def _order_view_kb(has_order: bool, can_modify: bool) -> list:
    rows = []
    if not has_order:
        if can_modify:
            rows.append([_btn("✅ Заказать 1 порцию", "order_portion", "#2a7a2a")])
    else:
        if can_modify:
            rows.append([
                _btn("➕ Добавить порцию", "add_portion",    "#2a7a2a"),
                _btn("➖ Убрать порцию",   "remove_portion", "#7a5c3e"),
            ])
            rows.append([_btn("❌ Отменить заказ", "cancel_order", "#7a2a2a")])
    rows.append([_btn("🏠 Главное меню", "main_menu", "#555")])
    return _kb(*rows)


def _my_orders_kb(orders: list) -> list:
    rows = []
    for i, order in enumerate(orders):
        date_str = order.target_date.strftime("%d.%m")
        rows.append([_btn(
            f"❌ Отменить {date_str} ({order.quantity} порц.)",
            f"cancel_{i}",
            "#7a2a2a",
        )])
    rows.append([_btn("🏠 Главное меню", "main_menu", "#555")])
    return _kb(*rows)


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
    if not role:
        return [_msg("⛔ Вы не зарегистрированы в системе.")]

    # COMMAND field buttons send English cmd name via 'command' param.
    # Typed text or ACTION=SEND fallback comes via 'text' param.
    raw = (command or text).strip().lower()

    # Global resets — English commands + Russian typed fallback
    if raw in ("main_menu", "главное меню", "/start", "start", "помощь", "help", "/help"):
        _state.pop(dialog_id, None)
        return [_msg(_help_text(role), keyboard=_main_kb(role))]

    state = _state.get(dialog_id, {S_STEP: STEP_IDLE})
    step  = state.get(S_STEP, STEP_IDLE)

    # Employee routing
    _employee_root_cmds = {"order", "my_orders", "menu",
                           "заказать", "мои заказы", "меню"}
    _employee_ctx_cmds  = {"order_portion", "add_portion", "remove_portion", "cancel_order",
                           "заказать порцию", "добавить порцию", "убрать порцию", "отменить заказ"}
    _employee_steps     = {STEP_SELECT_DAY, STEP_ORDER_VIEW, STEP_MY_ORDERS}
    _is_employee_action = (
        raw in _employee_root_cmds or
        (step in _employee_steps and (
            raw in _employee_ctx_cmds or
            bool(re.match(r'^day_\d+$', raw)) or
            bool(re.match(r'^cancel_\d+$', raw))
        ))
    )
    if role == "employee" or (role == "admin" and _is_employee_action):
        return await _handle_employee(dialog_id, from_user_id, raw, state, step, role)

    # Admin / provider / accountant routing
    if raw in ("reports", "отчёты", "отчеты", "/reports"):
        _state[dialog_id] = {S_STEP: STEP_PERIOD}
        return [_msg("Выберите период:", keyboard=KB_PERIOD, replace=True)]

    if raw in ("orders_today", "заказы сегодня"):
        _state.pop(dialog_id, None)
        return await _do_orders_today(role)

    # ---- SELECT PERIOD ----
    if step == STEP_PERIOD or raw in ("period_day", "period_month", "за сегодня", "за месяц"):
        if raw in ("period_day", "за сегодня"):
            _state[dialog_id] = {S_STEP: STEP_RTYPE, S_PERIOD: "day"}
            kb = KB_RTYPE_ADMIN if role == "admin" else KB_RTYPE_LIMITED
            return [_msg("Выберите тип отчёта:", keyboard=kb, replace=True)]

        if raw in ("period_month", "за месяц"):
            _state[dialog_id] = {S_STEP: STEP_MONTH_RANGE, S_PERIOD: "month"}
            kb = KB_RTYPE_ADMIN if role == "admin" else KB_RTYPE_LIMITED
            return [_msg("Выберите тип отчёта:", keyboard=kb, replace=True)]

        return [_msg("Используйте кнопки ниже:", keyboard=KB_PERIOD, replace=True)]

    # ---- SELECT REPORT TYPE ----
    if step == STEP_RTYPE:
        period = state.get(S_PERIOD, "day")
        rtype  = _parse_rtype(raw, role)
        if rtype is None:
            kb = KB_RTYPE_ADMIN if role == "admin" else KB_RTYPE_LIMITED
            return [_msg("Используйте кнопки ниже:", keyboard=kb, replace=True)]

        if period == "day":
            _state.pop(dialog_id, None)
            return await _do_report(rtype, "day", role)

        _state[dialog_id] = {S_STEP: STEP_MONTH_RANGE, S_PERIOD: "month", S_RTYPE: rtype}
        return [_msg("Выберите период:", keyboard=KB_MONTH_RANGE, replace=True)]

    # ---- SELECT MONTH RANGE ----
    if step == STEP_MONTH_RANGE:
        rtype = state.get(S_RTYPE)
        if rtype is None:
            rtype = _parse_rtype(raw, role)
            if rtype is None:
                kb = KB_RTYPE_ADMIN if role == "admin" else KB_RTYPE_LIMITED
                _state[dialog_id] = {S_STEP: STEP_RTYPE, S_PERIOD: "month"}
                return [_msg("Выберите тип отчёта:", keyboard=kb)]
            _state[dialog_id] = {S_STEP: STEP_MONTH_RANGE, S_PERIOD: "month", S_RTYPE: rtype}
            return [_msg("Выберите период:", keyboard=KB_MONTH_RANGE, replace=True)]

        if raw in ("month_current", "текущий месяц", "текущий"):
            _state.pop(dialog_id, None)
            return await _do_report(rtype, "month_current", role)

        if raw in ("month_prev", "прошлый месяц", "прошлый"):
            _state.pop(dialog_id, None)
            return await _do_report(rtype, "month_prev", role)

        return [_msg("Используйте кнопки ниже:", keyboard=KB_MONTH_RANGE, replace=True)]

    return [_msg(_help_text(role), keyboard=_main_kb(role))]


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _parse_rtype(raw: str, role: str) -> str | None:
    if raw in ("rtype_auto", "тип авто"):
        return role
    if raw in ("rtype_admin", "тип админский"):
        return "admin"
    if raw in ("rtype_accounting", "тип бухгалтерский"):
        return "accounting"
    if raw in ("rtype_provider", "тип поставщика"):
        return "provider"
    return None


async def _do_orders_today(role: str) -> list[dict]:
    today = datetime.now(CONFIG.timezone).date()
    text, _ = await _run_sync(generate_provider_report_text, today, today)
    messages = [_msg(text)]

    if role == "admin":
        file_path, file_name, caption = await _run_sync(
            generate_admin_report_file, today, today, is_daily=True
        )
        if file_path:
            messages.append(_msg(caption, file_path=file_path, file_name=file_name))
        else:
            messages.append(_msg(caption))

    messages[-1]["keyboard"] = _main_kb(role)
    return messages


async def _do_report(rtype: str, period: str, role: str) -> list[dict]:
    now   = datetime.now(CONFIG.timezone)
    today = now.date()

    if period == "day":
        start_date = end_date = today
    elif period == "month_current":
        start_date = today.replace(day=1)
        end_date   = today
    else:
        first_this = today.replace(day=1)
        last_prev  = first_this - timedelta(days=1)
        start_date = last_prev.replace(day=1)
        end_date   = last_prev

    if rtype == "accountant":
        rtype = "accounting"

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

    if not messages:
        return [_msg("Нет данных за выбранный период.", keyboard=_main_kb(role))]

    last = messages[-1]
    if "keyboard" not in last:
        last["keyboard"] = _main_kb(role)
    return messages


def _help_text(role: str) -> str:
    lines = ["[B]Бот ЕРС Обеды[/B]\n"]
    if role in ("admin", "provider"):
        lines.append("📋 [B]заказы сегодня[/B] — список заказов на сегодня по локациям")
    if role in ("admin", "provider", "accountant"):
        lines.append("📊 [B]отчёты[/B] — формирование отчётов")
    if role in ("admin", "employee"):
        lines.append("🛒 [B]заказать[/B] — оформить заказ на обед")
        lines.append("📋 [B]мои заказы[/B] — посмотреть и отменить заказы")
        lines.append("🍽 [B]меню[/B] — меню на ближайшие дни")
    return "\n".join(lines)


def _md_to_b24(text: str) -> str:
    return re.sub(r'\*([^*]+)\*', r'[B]\1[/B]', text)


def _msg(text: str, keyboard=None, file_path: str = None, file_name: str = None,
         replace: bool = False) -> dict:
    m = {"text": _md_to_b24(text)}
    if keyboard is not None:
        m["keyboard"] = keyboard
    if replace:
        m["replace"] = True
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


# ------------------------------------------------------------------
# Employee: DB helpers
# ------------------------------------------------------------------

def _fetch_user_db_id(bitrix_user_id: int, session) -> int | None:
    user = session.query(User).filter(
        User.bitrix_id == bitrix_user_id,
        User.is_deleted == False,
    ).first()
    return user.id if user else None


def _fetch_order_view(user_db_id: int, day_offset: int, session) -> tuple:
    menu, day_name, target_date = get_menu_for_day(day_offset, CONFIG)
    menu_text = format_menu_text(menu, day_name, target_date)

    order = get_order_for_date(user_db_id, target_date, session)
    if order:
        session.expunge(order)

    now = datetime.now(TIME_CONFIG.TIMEZONE)
    can_modify = (
        target_date > now.date() or
        (target_date == now.date() and now.time() < TIME_CONFIG.MODIFICATION_DEADLINE)
    )
    return menu_text, order, target_date, can_modify


def _fetch_active_orders(user_db_id: int, session) -> list:
    now = datetime.now(TIME_CONFIG.TIMEZONE).date()
    orders = get_active_orders(user_db_id, now, session)
    for o in orders:
        session.expunge(o)
    return orders


def _do_create_order_db(user_db_id: int, day_offset: int, session) -> str:
    menu, day_name, target_date = get_menu_for_day(day_offset, CONFIG)
    now = datetime.now(TIME_CONFIG.TIMEZONE)

    if target_date.weekday() in TIME_CONFIG.WEEKEND_DAYS:
        return "ℹ️ Заказы на выходные не принимаются."

    if target_date == now.date() and now.time() >= TIME_CONFIG.ORDER_DEADLINE:
        return f"ℹ️ Приём заказов на сегодня завершён в {TIME_CONFIG.ORDER_DEADLINE.strftime('%H:%M')}."

    order, err = create_order(user_db_id, target_date, session, is_preliminary=(day_offset > 0))
    if err:
        return f"ℹ️ {err}"
    return f"✅ Заказ на {target_date.strftime('%d.%m')} оформлен — 1 порция."


def _do_cancel_order_db(user_db_id: int, target_date, session) -> str:
    now = datetime.now(TIME_CONFIG.TIMEZONE)
    if (target_date == now.date() and now.time() >= TIME_CONFIG.MODIFICATION_DEADLINE
            and target_date <= now.date()):
        return f"ℹ️ Отмена невозможна после {TIME_CONFIG.MODIFICATION_DEADLINE.strftime('%H:%M')}."

    order, err = cancel_order(user_db_id, target_date, session)
    if err:
        return f"ℹ️ {err}"
    return f"✅ Заказ на {target_date.strftime('%d.%m')} отменён."


def _do_modify_qty_db(user_db_id: int, day_offset: int, delta: int, session) -> str:
    menu, day_name, target_date = get_menu_for_day(day_offset, CONFIG)
    now = datetime.now(TIME_CONFIG.TIMEZONE)

    if target_date == now.date() and now.time() >= TIME_CONFIG.MODIFICATION_DEADLINE:
        return f"ℹ️ Изменение невозможно после {TIME_CONFIG.MODIFICATION_DEADLINE.strftime('%H:%M')}."

    new_qty, order, err = modify_quantity(user_db_id, target_date, delta, session)
    if err:
        return f"ℹ️ {err}"
    if new_qty == 0:
        order.is_cancelled = True
        return f"✅ Заказ на {target_date.strftime('%d.%m')} отменён."
    return f"✅ Количество порций: {new_qty}."


# ------------------------------------------------------------------
# Employee: message handler
# ------------------------------------------------------------------

async def _handle_employee(
    dialog_id: str,
    from_user_id: int,
    raw: str,
    state: dict,
    step: str,
    role: str = "employee",
) -> list[dict]:
    try:
        return await _handle_employee_inner(dialog_id, from_user_id, raw, state, step, role)
    except Exception as e:
        logger.error(f"[B24Bot] _handle_employee error raw='{raw}' step='{step}': {e}", exc_info=True)
        return [_msg(f"❌ Ошибка: {e}", keyboard=_main_kb(role))]


async def _handle_employee_inner(
    dialog_id: str,
    from_user_id: int,
    raw: str,
    state: dict,
    step: str,
    role: str = "employee",
) -> list[dict]:
    main_kb = _main_kb(role)

    user_db_id = await _run_sync(_fetch_user_db_id, from_user_id)
    if not user_db_id:
        return [_msg("❌ Вы не найдены в базе данных. Обратитесь к администратору.",
                     keyboard=main_kb)]

    # ---- Заказать ----
    if raw in ("order", "заказать"):
        _state[dialog_id] = {S_STEP: STEP_SELECT_DAY}
        return [_msg("Выберите день:", keyboard=_build_select_day_kb(), replace=True)]

    # Parse day selection: "day_N" (button) or "день N" (typed)
    _day_match = re.match(r'^day_(\d+)$', raw) or re.match(r'^день (\d+)$', raw)
    if _day_match or step in (STEP_SELECT_DAY, STEP_ORDER_VIEW):
        if _day_match:
            day_offset = int(_day_match.group(1))
            _state[dialog_id] = {S_STEP: STEP_ORDER_VIEW, S_DAY: day_offset}
        else:
            day_offset = state.get(S_DAY, 0)

        if step == STEP_ORDER_VIEW:
            if raw in ("order_portion", "заказать порцию"):
                result = await _run_sync(_do_create_order_db, user_db_id, day_offset)
                menu_text, order, target_date, can_modify = await _run_sync(
                    _fetch_order_view, user_db_id, day_offset)
                text = f"{menu_text}\n\n{result}"
                if order:
                    text += f"\n[B]Заказано: {order.quantity} порц.[/B]"
                return [_msg(text, keyboard=_order_view_kb(order is not None, can_modify), replace=True)]

            if raw in ("add_portion", "добавить порцию"):
                result = await _run_sync(_do_modify_qty_db, user_db_id, day_offset, +1)
                menu_text, order, target_date, can_modify = await _run_sync(
                    _fetch_order_view, user_db_id, day_offset)
                text = f"{menu_text}\n\n{result}"
                if order and not order.is_cancelled:
                    text += f"\n[B]Заказано: {order.quantity} порц.[/B]"
                return [_msg(text, keyboard=_order_view_kb(order is not None and not order.is_cancelled, can_modify), replace=True)]

            if raw in ("remove_portion", "убрать порцию"):
                result = await _run_sync(_do_modify_qty_db, user_db_id, day_offset, -1)
                menu_text, order, target_date, can_modify = await _run_sync(
                    _fetch_order_view, user_db_id, day_offset)
                has_order = order is not None and not order.is_cancelled
                text = f"{menu_text}\n\n{result}"
                if has_order:
                    text += f"\n[B]Заказано: {order.quantity} порц.[/B]"
                return [_msg(text, keyboard=_order_view_kb(has_order, can_modify), replace=True)]

            if raw in ("cancel_order", "отменить заказ"):
                menu, day_name, target_date = get_menu_for_day(day_offset, CONFIG)
                result = await _run_sync(_do_cancel_order_db, user_db_id, target_date)
                menu_text, order, target_date, can_modify = await _run_sync(
                    _fetch_order_view, user_db_id, day_offset)
                text = f"{menu_text}\n\n{result}"
                return [_msg(text, keyboard=_order_view_kb(False, can_modify), replace=True)]

        menu_text, order, target_date, can_modify = await _run_sync(
            _fetch_order_view, user_db_id, day_offset)
        status = f"\n[B]Заказано: {order.quantity} порц.[/B]" if order else "\n[B]Заказ не оформлен[/B]"
        if not can_modify:
            status += "\n⏰ Приём/изменение заказов закрыт"
        return [_msg(f"{menu_text}{status}", keyboard=_order_view_kb(order is not None, can_modify), replace=True)]

    # ---- Мои заказы ----
    if raw in ("my_orders", "мои заказы"):
        orders = await _run_sync(_fetch_active_orders, user_db_id)
        if not orders:
            _state.pop(dialog_id, None)
            return [_msg("У вас нет активных заказов.", keyboard=main_kb)]
        order_dates = [o.target_date.isoformat() for o in orders]
        _state[dialog_id] = {S_STEP: STEP_MY_ORDERS, S_ORDER_DATES: order_dates}
        lines = ["[B]Ваши активные заказы:[/B]"]
        for o in orders:
            lines.append(f"• {o.target_date.strftime('%d.%m')} — {o.quantity} порц.")
        return [_msg("\n".join(lines), keyboard=_my_orders_kb(orders))]

    # Cancel by index: "cancel_N" (button)
    _cancel_match = re.match(r'^cancel_(\d+)$', raw)
    if step == STEP_MY_ORDERS and _cancel_match:
        idx = int(_cancel_match.group(1))
        order_dates = state.get(S_ORDER_DATES, [])
        if idx >= len(order_dates):
            return [_msg("❌ Заказ не найден.", keyboard=main_kb)]
        target_date = datetime.strptime(order_dates[idx], "%Y-%m-%d").date()

        result = await _run_sync(_do_cancel_order_db, user_db_id, target_date)
        orders = await _run_sync(_fetch_active_orders, user_db_id)
        if not orders:
            _state.pop(dialog_id, None)
            return [_msg(f"{result}\n\nАктивных заказов больше нет.", keyboard=main_kb)]
        order_dates = [o.target_date.isoformat() for o in orders]
        _state[dialog_id] = {S_STEP: STEP_MY_ORDERS, S_ORDER_DATES: order_dates}
        lines = [result, "", "[B]Ваши активные заказы:[/B]"]
        for o in orders:
            lines.append(f"• {o.target_date.strftime('%d.%m')} — {o.quantity} порц.")
        return [_msg("\n".join(lines), keyboard=_my_orders_kb(orders), replace=True)]

    # ---- Меню ----
    if raw in ("menu", "меню"):
        from services.menu_service import get_week_menus
        week = get_week_menus(CONFIG)
        lines = ["[B]Меню на ближайшие дни:[/B]"]
        for day in week:
            if day["is_weekend"] or day["is_holiday"]:
                continue
            date_str = day["target_date"].strftime("%d.%m")
            m = day["menu"]
            if m:
                lines.append(
                    f"\n[B]{day['day_name']} {date_str}[/B]\n"
                    f"🍲 {m['first']}\n🍛 {m['main']}\n🥗 {m['salad']}"
                )
        return [_msg("\n".join(lines), keyboard=main_kb)]

    return [_msg(_help_text(role), keyboard=main_kb)]
