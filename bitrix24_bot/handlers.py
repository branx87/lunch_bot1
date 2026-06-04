"""
Bitrix24 bot command handlers.
Uses the same services as Telegram and VK bots.

Buttons: ACTION=SEND with ACTION_VALUE — the value is sent as a chat message
when clicked, TEXT is only the visual label.
"""
import asyncio
import base64
import logging
import re
from datetime import datetime, timedelta

from config import CONFIG
from database import db
from models import User, Order
from services.order_service import (
    get_order_for_date, get_active_orders,
    create_order, cancel_order, modify_quantity,
    get_user_monthly_stats, QUANTITY_MAP,
)
from services.menu_service import get_menu_for_day, format_menu_text, get_week_menus
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

S_STEP   = "step"
S_PERIOD = "period"
S_RTYPE  = "rtype"
S_DAY    = "day"

STEP_IDLE        = "idle"
STEP_PERIOD      = "select_period"
STEP_RTYPE       = "select_rtype"
STEP_MONTH_RANGE = "select_month_range"
STEP_SELECT_DAY  = "select_day"
STEP_ORDER_VIEW  = "order_view"
STEP_MY_ORDERS   = "my_orders"
STEP_STATS       = "stats_period"

# ------------------------------------------------------------------
# Keyboards
# ACTION_VALUE is what gets sent to the bot when button is clicked.
# TEXT is the visual label only.
# ------------------------------------------------------------------

def _kb(*rows):
    return list(rows)


def _btn(text: str, value: str, bg: str = "#29619b") -> dict:
    return {"TEXT": text, "ACTION": "SEND", "ACTION_VALUE": value,
            "BG_COLOR": bg, "TEXT_COLOR": "#fff"}


KB_MAIN_ADMIN = _kb(
    [_btn("📊 Отчёты", "отчёты"), _btn("📋 Заказы сегодня", "заказы сегодня")],
    [_btn("✅ Быстрый заказ", "быстрый заказ", "#2a7a2a")],
    [_btn("📋 Мои заказы", "мои заказы"), _btn("📊 Статистика за месяц", "статистика за месяц", "#555")],
    [_btn("🍽 Меню на сегодня", "меню на сегодня", "#3b7abf"), _btn("📅 Меню на неделю", "меню на неделю", "#3b7abf")],
    [_btn("🔔 Уведомления", "уведомления", "#555")],
)

KB_MAIN_PROVIDER = _kb(
    [_btn("📋 Заказы сегодня", "заказы сегодня")],
)

KB_MAIN_ACCOUNTANT = _kb(
    [_btn("📊 Отчёты", "отчёты")],
)

KB_MAIN_EMPLOYEE = _kb(
    [_btn("✅ Быстрый заказ", "быстрый заказ", "#2a7a2a")],
    [_btn("📋 Мои заказы", "мои заказы"), _btn("📊 Статистика за месяц", "статистика за месяц", "#555")],
    [_btn("🍽 Меню на сегодня", "меню на сегодня", "#3b7abf"), _btn("📅 Меню на неделю", "меню на неделю", "#3b7abf")],
    [_btn("🔔 Уведомления", "уведомления", "#555")],
)

KB_PERIOD = _kb(
    [_btn("📅 За сегодня", "за сегодня", "#3b7abf"),
     _btn("📆 За месяц",   "за месяц",   "#3b7abf")],
    [_btn("🏠 Главное меню", "главное меню", "#555")],
)

KB_STATS_PERIOD = _kb(
    [_btn("📅 Текущий месяц", "статистика текущий месяц", "#3b7abf"),
     _btn("📆 Прошлый месяц", "статистика прошлый месяц", "#555")],
    [_btn("🏠 Главное меню", "главное меню", "#555")],
)

KB_RTYPE_ADMIN = _kb(
    [_btn("👨‍💼 Админский",    "тип админский",     "#29619b"),
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
        buttons.append(_btn(f"📅 {label}", f"день {offset}", "#3b7abf"))
    rows = [buttons[i:i+2] for i in range(0, len(buttons), 2)]
    rows.append([_btn("🏠 Главное меню", "главное меню", "#555")])
    return _kb(*rows)


def _order_view_kb(has_order: bool, can_modify: bool, user_db_id: int = None) -> list:
    rows = []
    if not has_order:
        if can_modify:
            rows.append([_btn("✅ Заказать 1 порцию", "заказать порцию", "#2a7a2a")])
            # Кнопка заказа для инспектора (проверка по bitrix_id)
            if user_db_id and _can_order_for_inspector(user_db_id):
                rows.append([_btn("🕵️ Заказать инспектору", "заказать инспектору", "#7a5c3e")])
    else:
        if can_modify:
            rows.append([
                _btn("➕ Добавить порцию", "добавить порцию", "#2a7a2a"),
                _btn("➖ Убрать порцию",   "убрать порцию",   "#7a5c3e"),
            ])
            rows.append([_btn("❌ Отменить заказ", "отменить заказ", "#7a2a2a")])
    rows.append([_btn("🏠 Главное меню", "главное меню", "#555")])
    return _kb(*rows)


def _week_day_order_kb(day_offset: int, has_order: bool, can_modify: bool, qty: int = 0) -> list:
    rows = []
    if not has_order:
        if can_modify:
            rows.append([_btn("✅ Заказать", f"заказать день {day_offset}", "#2a7a2a")])
    else:
        if can_modify:
            rows.append([
                _btn("➕ Добавить", f"добавить день {day_offset}", "#2a7a2a"),
                _btn("➖ Убрать",   f"убрать день {day_offset}",   "#7a5c3e"),
            ])
            rows.append([_btn(f"❌ Отменить ({qty} порц.)", f"отменить день {day_offset}", "#7a2a2a")])
    return rows


def _my_orders_kb(orders: list) -> list:
    rows = []
    for order in orders:
        date_str = order.target_date.strftime("%d.%m")
        rows.append([_btn(
            f"❌ Отменить {date_str} ({order.quantity} порц.)",
            f"отменить {date_str}",
            "#7a2a2a",
        )])
    rows.append([_btn("🏠 Главное меню", "главное меню", "#555")])
    return _kb(*rows)


def _notifications_kb(enabled: bool) -> list:
    if enabled:
        return _kb(
            [_btn("🔕 Отключить напоминания", "уведомления отключить", "#7a2a2a")],
            [_btn("🏠 Главное меню", "главное меню", "#555")],
        )
    return _kb(
        [_btn("🔔 Включить напоминания", "уведомления включить", "#2a7a2a")],
        [_btn("🏠 Главное меню", "главное меню", "#555")],
    )


def _main_kb(role: str) -> list:
    if role == "admin":
        return KB_MAIN_ADMIN
    elif role == "accountant":
        return KB_MAIN_ACCOUNTANT
    elif role == "employee":
        return KB_MAIN_EMPLOYEE
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
    if not role:
        return [_msg("⛔ Вы не зарегистрированы в системе.")]

    raw = text.strip().lower()

    # Global resets
    if raw in ("главное меню", "/start", "start", "помощь", "help", "/help"):
        _state.pop(dialog_id, None)
        return [_msg(_help_text(role), keyboard=_main_kb(role), replace=True)]

    state = _state.get(dialog_id, {S_STEP: STEP_IDLE})
    step  = state.get(S_STEP, STEP_IDLE)

    # Employee ordering commands
    _employee_root = {"заказать", "мои заказы", "меню", "быстрый заказ", "меню на сегодня", "меню на неделю",
                       "статистика за месяц", "статистика текущий месяц", "статистика прошлый месяц",
                       "уведомления", "уведомления отключить", "уведомления включить"}
    _employee_ctx  = {"заказать порцию", "добавить порцию", "убрать порцию", "отменить заказ", "заказать инспектору"}
    _employee_steps = {STEP_SELECT_DAY, STEP_ORDER_VIEW, STEP_MY_ORDERS, STEP_STATS}

    _is_employee_action = (
        raw in _employee_root or
        bool(re.match(r'^(заказать|добавить|убрать|отменить) день \d+$', raw)) or
        (step in _employee_steps and (
            raw in _employee_ctx or
            bool(re.match(r'^день \d+$', raw)) or
            bool(re.match(r'^отменить \d{2}\.\d{2}$', raw))
        ))
    )
    if role == "employee" or (role == "admin" and _is_employee_action):
        return await _handle_employee(dialog_id, from_user_id, raw, state, step, role)

    # Admin / provider / accountant — reports routing
    if raw in ("отчёты", "отчеты", "/reports"):
        _state[dialog_id] = {S_STEP: STEP_PERIOD}
        return [_msg("Выберите период:", keyboard=KB_PERIOD, replace=True)]

    if raw == "заказы сегодня":
        _state.pop(dialog_id, None)
        return await _do_orders_today(role)

    # ---- SELECT PERIOD ----
    if step == STEP_PERIOD or raw in ("за сегодня", "за месяц"):
        if raw == "за сегодня":
            _state[dialog_id] = {S_STEP: STEP_RTYPE, S_PERIOD: "day"}
            kb = KB_RTYPE_ADMIN if role == "admin" else KB_RTYPE_LIMITED
            return [_msg("Выберите тип отчёта:", keyboard=kb, replace=True)]

        if raw == "за месяц":
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

        if raw in ("текущий месяц", "текущий"):
            _state.pop(dialog_id, None)
            return await _do_report(rtype, "month_current", role)

        if raw in ("прошлый месяц", "прошлый"):
            _state.pop(dialog_id, None)
            return await _do_report(rtype, "month_prev", role)

        return [_msg("Используйте кнопки ниже:", keyboard=KB_MONTH_RANGE, replace=True)]

    return [_msg(_help_text(role), keyboard=_main_kb(role))]


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _parse_rtype(raw: str, role: str) -> str | None:
    if raw == "тип авто":
        return role
    if raw == "тип админский":
        return "admin"
    if raw == "тип бухгалтерский":
        return "accounting"
    if raw == "тип поставщика":
        return "provider"
    return None


async def _sync_orders_before_report():
    """Фоновая синхронизация заказов с Bitrix перед формированием отчёта.
    Аналогично Telegram bot в handlers/base_handlers.py:admin_reports_menu."""
    try:
        from bitrix.sync import BitrixSync
        sync = BitrixSync()
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
        await sync.sync_orders(start_date, end_date, incremental=True)
        logger.info("✅ Фоновая синхронизация перед отчётом B24 выполнена")
    except Exception as e:
        logger.error(f"Ошибка фоновой синхронизации перед отчётом B24: {e}")


async def _do_orders_today(role: str) -> list[dict]:
    today = datetime.now(CONFIG.timezone).date()

    # Синхронизация статусов с Bitrix перед формированием отчёта
    await _sync_orders_before_report()

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

    # Синхронизация статусов с Bitrix перед формированием отчёта
    await _sync_orders_before_report()

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
        lines.append("✅ [B]быстрый заказ[/B] — заказать 1 порцию на сегодня")
        lines.append("📋 [B]мои заказы[/B] — посмотреть и отменить заказы")
        lines.append("📊 [B]статистика за месяц[/B] — сводка заказов за месяц")
        lines.append("🍽 [B]меню на сегодня[/B] — меню и управление заказом на сегодня")
        lines.append("📅 [B]меню на неделю[/B] — меню и заказы на ближайшие дни")
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
# Employee DB helpers
# ------------------------------------------------------------------

def _get_notifications_state(user_db_id: int, session) -> bool:
    user = session.query(User).filter(User.id == user_db_id).first()
    return user.notifications_enabled if user else True


def _set_notifications(user_db_id: int, enabled: bool, session) -> bool:
    user = session.query(User).filter(User.id == user_db_id).first()
    if user:
        user.notifications_enabled = enabled
        session.commit()
    return enabled


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


def _order_status_text(order) -> str:
    """Формирует текст статуса заказа"""
    if not order:
        return "\n[B]Заказ не оформлен[/B]"
    if order.is_for_inspector:
        return f"\n[B]🕵️ Заказ для инспектора: {order.quantity} порц.[/B]"
    return f"\n[B]Заказано: {order.quantity} порц.[/B]"


def _fetch_active_orders(user_db_id: int, session) -> list:
    now = datetime.now(TIME_CONFIG.TIMEZONE).date()
    orders = get_active_orders(user_db_id, now, session)
    for o in orders:
        session.expunge(o)
    return orders


def _can_order_for_inspector(user_db_id: int) -> bool:
    """Проверяет по bitrix_id пользователя, может ли он заказывать для инспектора"""
    try:
        with db.get_session() as session:
            user = session.query(User.bitrix_id).filter(User.id == user_db_id).first()
            if user and user.bitrix_id:
                return user.bitrix_id in CONFIG.inspector_allowed_bitrix_ids
    except Exception as e:
        logger.error(f"Ошибка проверки прав инспектора: {e}")
    return False


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


def _do_create_inspector_order_db(user_db_id: int, day_offset: int, session) -> str:
    """Создаёт заказ для инспектора (расходы компании)"""
    from datetime import datetime
    menu, day_name, target_date = get_menu_for_day(day_offset, CONFIG)
    now = datetime.now(TIME_CONFIG.TIMEZONE)

    if target_date.weekday() in TIME_CONFIG.WEEKEND_DAYS:
        return "ℹ️ Заказы на выходные не принимаются."

    if target_date == now.date() and now.time() >= TIME_CONFIG.ORDER_DEADLINE:
        return f"ℹ️ Приём заказов на сегодня завершён в {TIME_CONFIG.ORDER_DEADLINE.strftime('%H:%M')}."

    # Проверяем существующий заказ
    existing = get_order_for_date(user_db_id, target_date, session)
    if existing:
        return f"ℹ️ У вас уже заказано {existing.quantity} порций. Нельзя заказать и для себя, и для инспектора на один день."

    quantity = 1
    order = Order(
        user_id=user_db_id,
        target_date=target_date,
        order_time=now.strftime("%H:%M:%S"),
        quantity=quantity,
        bitrix_quantity_id=QUANTITY_MAP[1],
        is_active=True,
        is_preliminary=(day_offset > 0),
        is_for_inspector=True,
        created_at=now.replace(tzinfo=None),
    )
    session.add(order)
    return f"✅ 🕵️ Заказ для инспектора на {target_date.strftime('%d.%m')} оформлен — 1 порция."


def _do_cancel_order_db(user_db_id: int, target_date, session) -> str:
    now = datetime.now(TIME_CONFIG.TIMEZONE)
    if (target_date <= now.date() and now.time() >= TIME_CONFIG.MODIFICATION_DEADLINE):
        return f"ℹ️ Отмена невозможна после {TIME_CONFIG.MODIFICATION_DEADLINE.strftime('%H:%M')}."

    order, err = cancel_order(user_db_id, target_date, session)
    if err:
        return f"ℹ️ {err}"
    return f"✅ Заказ на {target_date.strftime('%d.%m')} отменён."


def _fetch_week_order_status(user_db_id: int, work_days: list, session) -> list:
    now_local = datetime.now(TIME_CONFIG.TIMEZONE)
    results = []
    for day in work_days:
        offset      = day["day_offset"]
        target_date = day["target_date"]
        menu_text   = format_menu_text(day["menu"], day["day_name"], target_date)
        order       = get_order_for_date(user_db_id, target_date, session)
        has_order   = order is not None
        can_modify  = (
            target_date > now_local.date() or
            (target_date == now_local.date() and now_local.time() < TIME_CONFIG.MODIFICATION_DEADLINE)
        )
        qty = order.quantity if has_order else 0
        if order:
            session.expunge(order)
        results.append({
            "offset":      offset,
            "target_date": target_date,
            "menu_text":   menu_text,
            "has_order":   has_order,
            "can_modify":  can_modify,
            "qty":         qty,
        })
    return results


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
# Employee message handler
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

    # ---- Уведомления ----
    if raw == "уведомления":
        enabled = await _run_sync(_get_notifications_state, user_db_id)
        state_text = "[B]включены[/B] 🔔" if enabled else "[B]отключены[/B] 🔕"
        return [_msg(f"Напоминания о заказе {state_text}",
                     keyboard=_notifications_kb(enabled), replace=True)]

    if raw == "уведомления отключить":
        await _run_sync(_set_notifications, user_db_id, False)
        return [_msg("🔕 Напоминания отключены.", keyboard=_notifications_kb(False), replace=True)]

    if raw == "уведомления включить":
        await _run_sync(_set_notifications, user_db_id, True)
        return [_msg("🔔 Напоминания включены.", keyboard=_notifications_kb(True), replace=True)]

    # ---- Быстрый заказ ----
    if raw == "быстрый заказ":
        result = await _run_sync(_do_create_order_db, user_db_id, 0)
        _state.pop(dialog_id, None)
        return [_msg(result, keyboard=main_kb, replace=True)]

    # ---- Меню на сегодня ----
    if raw == "меню на сегодня":
        day_offset = 0
        _state[dialog_id] = {S_STEP: STEP_ORDER_VIEW, S_DAY: day_offset}
        menu_text, order, target_date, can_modify = await _run_sync(
            _fetch_order_view, user_db_id, day_offset)
        status = _order_status_text(order)
        if not can_modify:
            status += "\n⏰ Приём/изменение заказов закрыт"
        return [_msg(f"{menu_text}{status}",
                     keyboard=_order_view_kb(order is not None, can_modify, user_db_id), replace=True)]

    # ---- Меню на неделю / Меню (обратная совместимость) ----
    if raw in ("меню на неделю", "меню"):
        week = get_week_menus(CONFIG)
        work_days = [d for d in week if not d["is_weekend"] and not d["is_holiday"] and d["menu"]]
        if not work_days:
            return [_msg("Меню на текущую неделю отсутствует.", keyboard=main_kb, replace=True)]
        day_statuses = await _run_sync(_fetch_week_order_status, user_db_id, work_days)
        _state.pop(dialog_id, None)
        messages = []
        for i, st in enumerate(day_statuses):
            text = st["menu_text"]
            if st["has_order"]:
                text += f"\n\n[B]🛒 Заказано: {st['qty']} порц.[/B]"
            kb_rows = _week_day_order_kb(st["offset"], st["has_order"], st["can_modify"], st["qty"])
            if i == len(day_statuses) - 1:
                kb_rows = kb_rows + [[_btn("🏠 Главное меню", "главное меню", "#555")]]
            messages.append(_msg(text, keyboard=kb_rows if kb_rows else None))
        return messages

    # ---- Прямые действия из недельного меню: "заказать/добавить/убрать/отменить день N" ----
    _direct_match = re.match(r'^(заказать|добавить|убрать|отменить) день (\d+)$', raw)
    if _direct_match:
        action     = _direct_match.group(1)
        day_offset = int(_direct_match.group(2))
        return await _handle_direct_day_action(dialog_id, user_db_id, action, day_offset, role)

    # ---- Заказать ----
    if raw == "заказать":
        _state[dialog_id] = {S_STEP: STEP_SELECT_DAY}
        return [_msg("Выберите день:", keyboard=_build_select_day_kb(), replace=True)]

    # Day selection: "день N"
    _day_match = re.match(r'^день (\d+)$', raw)
    if _day_match or step in (STEP_SELECT_DAY, STEP_ORDER_VIEW):
        if _day_match:
            day_offset = int(_day_match.group(1))
            _state[dialog_id] = {S_STEP: STEP_ORDER_VIEW, S_DAY: day_offset}
        else:
            day_offset = state.get(S_DAY, 0)

        if step == STEP_ORDER_VIEW:
            if raw == "заказать порцию":
                result = await _run_sync(_do_create_order_db, user_db_id, day_offset)
                menu_text, order, target_date, can_modify = await _run_sync(
                    _fetch_order_view, user_db_id, day_offset)
                text = f"{menu_text}\n\n{result}"
                text += _order_status_text(order)
                return [_msg(text, keyboard=_order_view_kb(order is not None, can_modify, user_db_id), replace=True)]

            if raw == "заказать инспектору":
                result = await _run_sync(_do_create_inspector_order_db, user_db_id, day_offset)
                menu_text, order, target_date, can_modify = await _run_sync(
                    _fetch_order_view, user_db_id, day_offset)
                text = f"{menu_text}\n\n{result}"
                text += _order_status_text(order)
                return [_msg(text, keyboard=_order_view_kb(order is not None, can_modify, user_db_id), replace=True)]

            if raw == "добавить порцию":
                result = await _run_sync(_do_modify_qty_db, user_db_id, day_offset, +1)
                menu_text, order, target_date, can_modify = await _run_sync(
                    _fetch_order_view, user_db_id, day_offset)
                has_order = order is not None and not order.is_cancelled
                text = f"{menu_text}\n\n{result}"
                text += _order_status_text(order if has_order else None)
                return [_msg(text, keyboard=_order_view_kb(has_order, can_modify, user_db_id), replace=True)]

            if raw == "убрать порцию":
                result = await _run_sync(_do_modify_qty_db, user_db_id, day_offset, -1)
                menu_text, order, target_date, can_modify = await _run_sync(
                    _fetch_order_view, user_db_id, day_offset)
                has_order = order is not None and not order.is_cancelled
                text = f"{menu_text}\n\n{result}"
                text += _order_status_text(order if has_order else None)
                return [_msg(text, keyboard=_order_view_kb(has_order, can_modify, user_db_id), replace=True)]

            if raw == "отменить заказ":
                menu, day_name, target_date = get_menu_for_day(day_offset, CONFIG)
                result = await _run_sync(_do_cancel_order_db, user_db_id, target_date)
                menu_text, order, target_date, can_modify = await _run_sync(
                    _fetch_order_view, user_db_id, day_offset)
                text = f"{menu_text}\n\n{result}"
                return [_msg(text, keyboard=_order_view_kb(False, can_modify, user_db_id), replace=True)]

        menu_text, order, target_date, can_modify = await _run_sync(
            _fetch_order_view, user_db_id, day_offset)
        status = _order_status_text(order)
        if not can_modify:
            status += "\n⏰ Приём/изменение заказов закрыт"
        return [_msg(f"{menu_text}{status}",
                     keyboard=_order_view_kb(order is not None, can_modify, user_db_id), replace=True)]

    # ---- Мои заказы ----
    if raw == "мои заказы":
        orders = await _run_sync(_fetch_active_orders, user_db_id)
        if not orders:
            _state.pop(dialog_id, None)
            return [_msg("У вас нет активных заказов.", keyboard=main_kb, replace=True)]
        _state[dialog_id] = {S_STEP: STEP_MY_ORDERS}
        lines = ["[B]Ваши активные заказы:[/B]"]
        for o in orders:
            lines.append(f"• {o.target_date.strftime('%d.%m')} — {o.quantity} порц.")
        return [_msg("\n".join(lines), keyboard=_my_orders_kb(orders), replace=True)]

    # ---- Статистика за месяц: выбор периода ----
    if raw == "статистика за месяц":
        _state[dialog_id] = {S_STEP: STEP_STATS}
        return [_msg("Выберите период:", keyboard=KB_STATS_PERIOD, replace=True)]

    # ---- Статистика: показать результат ----
    if raw in ("статистика текущий месяц", "статистика прошлый месяц"):
        now_local = datetime.now(TIME_CONFIG.TIMEZONE)
        month_names = {
            1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
            5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
            9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
        }
        if raw == "статистика текущий месяц":
            start_date = now_local.replace(day=1).date()
            end_date   = now_local.date()
            label = f"{month_names[now_local.month]} {now_local.year}"
        else:
            first_this = now_local.replace(day=1)
            last_prev  = (first_this - timedelta(days=1))
            start_date = last_prev.replace(day=1).date()
            end_date   = last_prev.date()
            label = f"{month_names[last_prev.month]} {last_prev.year}"
        stats = await _run_sync(
            lambda session: get_user_monthly_stats(user_db_id, start_date, end_date, session))
        text = (
            f"[B]📊 Статистика за {label}:[/B]\n\n"
            f"🍽 Всего обедов: {stats['total']}\n"
            f"✅ Выполненные: {stats['completed']}\n"
            f"⏳ Предстоящие: {stats['upcoming']}"
        )
        _state.pop(dialog_id, None)
        return [_msg(text, keyboard=main_kb, replace=True)]

    # Cancel by date: "отменить DD.MM"
    _cancel_match = re.match(r'^отменить (\d{2}\.\d{2})$', raw)
    if step == STEP_MY_ORDERS and _cancel_match:
        date_part = _cancel_match.group(1)
        year = datetime.now(TIME_CONFIG.TIMEZONE).year
        try:
            target_date = datetime.strptime(f"{date_part}.{year}", "%d.%m.%Y").date()
        except ValueError:
            return [_msg("❌ Не удалось распознать дату.", keyboard=main_kb)]

        result = await _run_sync(_do_cancel_order_db, user_db_id, target_date)
        orders = await _run_sync(_fetch_active_orders, user_db_id)
        if not orders:
            _state.pop(dialog_id, None)
            return [_msg(f"{result}\n\nАктивных заказов больше нет.", keyboard=main_kb)]
        _state[dialog_id] = {S_STEP: STEP_MY_ORDERS}
        lines = [result, "", "[B]Ваши активные заказы:[/B]"]
        for o in orders:
            lines.append(f"• {o.target_date.strftime('%d.%m')} — {o.quantity} порц.")
        return [_msg("\n".join(lines), keyboard=_my_orders_kb(orders), replace=True)]

    return [_msg(_help_text(role), keyboard=main_kb)]


async def _handle_direct_day_action(
    dialog_id: str,
    user_db_id: int,
    action: str,
    day_offset: int,
    role: str,
) -> list[dict]:
    if action == "заказать":
        result = await _run_sync(_do_create_order_db, user_db_id, day_offset)
    elif action == "добавить":
        result = await _run_sync(_do_modify_qty_db, user_db_id, day_offset, +1)
    elif action == "убрать":
        result = await _run_sync(_do_modify_qty_db, user_db_id, day_offset, -1)
    else:  # отменить
        _, _, target_date = get_menu_for_day(day_offset, CONFIG)
        result = await _run_sync(_do_cancel_order_db, user_db_id, target_date)

    _state[dialog_id] = {S_STEP: STEP_ORDER_VIEW, S_DAY: day_offset}
    menu_text, order, target_date, can_modify = await _run_sync(
        _fetch_order_view, user_db_id, day_offset)
    has_order = order is not None
    text = f"{menu_text}\n\n{result}"
    text += _order_status_text(order if has_order else None)
    return [_msg(text, keyboard=_order_view_kb(has_order, can_modify, user_db_id), replace=True)]
