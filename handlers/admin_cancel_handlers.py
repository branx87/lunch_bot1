# ##handlers/admin_cancel_handlers.py
"""Отмена заказа сотрудника — только для главного админа (MASTER_ADMIN_ID).

Зачем: админ оформил заказ за сотрудника («👨‍💼 Заказать за сотрудника»),
и если что-то пошло не так — ошиблись сотрудником, датой или количеством —
отменить этот заказ было нечем. Сотрудник отменяет заказ сам, обычному
админу такая возможность не нужна, поэтому кнопка есть только у главного.
"""
import logging
import re
from datetime import datetime, timedelta

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.ext import (
    CallbackQueryHandler,
    CommandHandler,
    ConversationHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

from bot_keyboards import create_admin_keyboard, create_admin_keyboard_for
from config import CONFIG
from constants import (
    ADMIN_CANCEL_CONFIRM,
    ADMIN_CANCEL_DATE,
    ADMIN_CANCEL_EMPLOYEE,
    ADMIN_CANCEL_SEARCH,
    ADMIN_ORDERS_DATE,
    ADMIN_ORDERS_LIST,
)
from database import db
from handlers.admin_order_handlers import _employee_by_id
from models import Order, User
from services.menu_service import DAYS_RU
from services.order_service import (
    cancel_order,
    find_employees,
    get_active_orders,
    get_order_for_date,
)
from time_config import TIME_CONFIG

logger = logging.getLogger(__name__)

ADMIN_CANCEL_BUTTON = "🗑 Отменить заказ сотрудника"
ADMIN_ORDERS_BUTTON = "📋 Все заказы на дату"

_FLOW_KEY = "admin_cancel"


# ------------------------------------------------------------------
# Данные диалога
# ------------------------------------------------------------------

def _flow(context) -> dict:
    return context.user_data.get(_FLOW_KEY, {})


def _set(context, **kwargs) -> dict:
    data = context.user_data.setdefault(_FLOW_KEY, {})
    data.update(kwargs)
    return data


def _reset(context):
    context.user_data.pop(_FLOW_KEY, None)


def _is_master(update) -> bool:
    master_id = CONFIG.master_admin_id
    return bool(master_id) and update.effective_user.id == master_id


def _date_label(target_date) -> str:
    return f"{DAYS_RU[target_date.weekday()]} {target_date.strftime('%d.%m')}"


def _employee_label(emp: dict) -> str:
    label = emp.get("full_name") or "Без имени"
    detail = emp.get("position") or emp.get("location")
    if detail:
        label += f" — {detail}"
    return label


def _admin_kb():
    return create_admin_keyboard(is_master=True)


def _date_step(context, emp: dict):
    """Текст и клавиатура выбора заказа (по датам, где заказ есть)."""
    today = datetime.now(TIME_CONFIG.TIMEZONE).date()
    orders = get_active_orders(emp["id"], today, db.session)

    if not orders:
        return (
            f"У {emp['full_name']} нет активных заказов на сегодня и позже.",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("🏠 Главное меню", callback_data="acstop")]
            ]),
        )

    rows = [
        [InlineKeyboardButton(
            f"{_date_label(o.target_date)} — {o.quantity} порц.",
            callback_data=f"acdate:{emp['id']}:{o.target_date.isoformat()}",
        )]
        for o in orders
    ]
    rows.append([InlineKeyboardButton("❌ Отмена", callback_data="acstop")])

    text = (
        f"{emp['full_name']}\n\n"
        "Активные заказы — выберите, какой отменяем:"
    )
    return text, InlineKeyboardMarkup(rows)


# ------------------------------------------------------------------
# Шаги диалога
# ------------------------------------------------------------------

async def admin_cancel_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Точка входа: главный админ нажал «Отменить заказ сотрудника»."""
    _reset(context)
    await update.message.reply_text(
        "Введите ФИО сотрудника, чей заказ отменяем "
        "(или его Bitrix ID).\n\n"
        "Покажем даты, на которые у него есть заказ.",
        reply_markup=ReplyKeyboardMarkup(
            [["❌ Отмена", "🏠 Главное меню"]], resize_keyboard=True),
    )
    return ADMIN_CANCEL_SEARCH


async def admin_cancel_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ввод ФИО или Bitrix ID сотрудника."""
    text = (update.message.text or "").strip()

    if text in ("❌ Отмена", "Отмена", "🏠 Главное меню"):
        return await admin_cancel_finish(update, context)

    employees = find_employees(text, db.session, limit=10)

    if not employees:
        await update.message.reply_text(
            f"❌ По запросу «{text}» сотрудник не найден.\n\n"
            "Проверьте ФИО или введите Bitrix ID."
        )
        return ADMIN_CANCEL_SEARCH

    if len(employees) == 1:
        emp = employees[0]
        _set(context, employee=emp)
        step_text, kb = _date_step(context, emp)
        await update.message.reply_text(step_text, reply_markup=kb)
        return ADMIN_CANCEL_DATE

    keyboard = [
        [InlineKeyboardButton(_employee_label(e), callback_data=f"acemp:{e['id']}")]
        for e in employees
    ]
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="acstop")])
    await update.message.reply_text(
        f"Найдено сотрудников: {len(employees)}. Выберите нужного:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return ADMIN_CANCEL_EMPLOYEE


async def admin_cancel_employee(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выбор сотрудника из списка совпадений."""
    query = update.callback_query
    await query.answer()
    if not _is_master(update):
        return ConversationHandler.END

    try:
        emp_id = int(query.data.split(":", 1)[1])
    except (ValueError, IndexError):
        return ADMIN_CANCEL_EMPLOYEE

    emp = _employee_by_id(emp_id)
    if not emp:
        await query.edit_message_text("❌ Сотрудник не найден в базе.")
        return ADMIN_CANCEL_EMPLOYEE

    _set(context, employee=emp)
    step_text, kb = _date_step(context, emp)
    await query.edit_message_text(step_text, reply_markup=kb)
    return ADMIN_CANCEL_DATE


async def admin_cancel_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выбор заказа — показываем подтверждение."""
    query = update.callback_query
    await query.answer()
    if not _is_master(update):
        return ConversationHandler.END

    try:
        _, emp_id, date_iso = query.data.split(":", 2)
        emp_id = int(emp_id)
        target_date = datetime.strptime(date_iso, "%Y-%m-%d").date()
    except (ValueError, IndexError):
        return ADMIN_CANCEL_DATE

    emp = _employee_by_id(emp_id)
    if not emp:
        await query.edit_message_text("❌ Сотрудник не найден в базе.")
        return ConversationHandler.END

    order = get_order_for_date(emp_id, target_date, db.session)
    if not order:
        await query.edit_message_text(
            "❌ Заказ не найден — возможно, его уже отменили."
        )
        return ConversationHandler.END

    _set(context, employee=emp, date=target_date)

    kind = "🕵️ инспектору" if order.is_for_inspector else "🍽 сотруднику"
    text = (
        "Отменить заказ?\n\n"
        f"Сотрудник: {emp['full_name']}\n"
        f"Дата: {_date_label(target_date)}\n"
        f"Порций: {order.quantity}\n"
        f"Тип: {kind}\n"
        f"Создан в: {order.order_time}"
    )
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Да, отменить", callback_data="acdo")],
        [InlineKeyboardButton("❌ Нет", callback_data="acstop")],
    ])
    await query.edit_message_text(text, reply_markup=keyboard)
    return ADMIN_CANCEL_CONFIRM


async def admin_cancel_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена заказа."""
    query = update.callback_query
    await query.answer()
    if not _is_master(update):
        return ConversationHandler.END

    data = _flow(context)
    emp = data.get("employee")
    target_date = data.get("date")

    if not emp or not target_date:
        _reset(context)
        await query.edit_message_text("❌ Данные заказа не найдены. Начните заново.")
        return ConversationHandler.END

    order, err = cancel_order(emp["id"], target_date, db.session)
    if err:
        _reset(context)
        await query.edit_message_text(f"❌ {err}")
        await query.message.reply_text("Главное меню:", reply_markup=_admin_kb())
        return ConversationHandler.END

    qty = order.quantity
    db.session.commit()
    _reset(context)

    logger.info(
        "MASTER ADMIN %s: отменён заказ — %s, %s, %s порц.",
        update.effective_user.id, emp["full_name"], target_date.isoformat(), qty,
    )

    await query.edit_message_text(
        f"✅ Заказ отменён — {_date_label(target_date)}, {qty} порц.\n"
        f"Сотрудник: {emp['full_name']}"
    )
    await query.message.reply_text("Главное меню:", reply_markup=_admin_kb())
    return ConversationHandler.END


async def admin_cancel_finish(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выход из диалога без отмены."""
    _reset(context)

    if update.callback_query:
        await update.callback_query.answer()
        try:
            await update.callback_query.edit_message_text("❌ Отмена заказа не выполнена.")
        except Exception:
            logger.debug("Не удалось отредактировать сообщение при выходе из диалога")
        await update.callback_query.message.reply_text("Главное меню:", reply_markup=_admin_kb())
        return ConversationHandler.END

    await update.message.reply_text("Главное меню:", reply_markup=_admin_kb())
    return ConversationHandler.END


# ------------------------------------------------------------------
# Список заказов на дату
# ------------------------------------------------------------------

def _view_dates():
    """Даты для просмотра: сегодня и следующие 6 дней."""
    today = datetime.now(TIME_CONFIG.TIMEZONE).date()
    result = []
    for i in range(7):
        d = today + timedelta(days=i)
        if i == 0:
            label = "Сегодня"
        elif i == 1:
            label = "Завтра"
        else:
            label = DAYS_RU[d.weekday()]
        result.append((d, f"{label} {d.strftime('%d.%m')}"))
    return result


def _plural(n: int, one: str, few: str, many: str) -> str:
    """Согласование существительного с числом: 1 заказ / 2 заказа / 5 заказов."""
    if n % 10 == 1 and n % 100 != 11:
        return one
    if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        return few
    return many


def _orders_for_date(target_date):
    """Активные заказы на дату вместе с сотрудниками."""
    return (db.session.query(Order, User)
            .join(User, Order.user_id == User.id)
            .filter(
                Order.target_date == target_date,
                Order.is_cancelled == False,
            )
            .order_by(User.full_name)
            .all())


def _orders_list_view(target_date):
    """Текст и клавиатура со списком заказов на дату."""
    rows = _orders_for_date(target_date)
    date_label = f"{DAYS_RU[target_date.weekday()]} ({target_date.strftime('%d.%m')})"

    if not rows:
        text = f"📋 Заказы на {date_label}\n\nЗаказов нет."
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Обновить", callback_data=f"aodate:{target_date.isoformat()}")],
            [InlineKeyboardButton("🏠 Главное меню", callback_data="acstop")],
        ])
        return text, keyboard

    lines = [f"📋 Заказы на {date_label}", ""]
    buttons = []
    total_portions = 0
    for order, user in rows:
        total_portions += order.quantity or 0
        mark = ""
        if order.is_for_inspector:
            mark = " 🕵️"
        elif order.is_sent_to_bitrix:
            mark = " 📤 уже в Б24"
        lines.append(
            f"👤 {user.full_name} — {order.quantity} порц."
            f"{mark}  ·  {order.order_time}"
        )
        if order.is_from_bitrix:
            # Заказ из формы Битрикс24 — локально его не отменить.
            continue
        if order.is_sent_to_bitrix:
            # Уже отправлен поставщику — локальная отмена ничего не изменит.
            continue
        buttons.append([InlineKeyboardButton(
            f"❌ {user.full_name}",
            callback_data=f"aocancel:{order.id}",
        )])

    lines.append("")
    lines.append(
        f"Всего: {len(rows)} {_plural(len(rows), 'заказ', 'заказа', 'заказов')}, "
        f"{total_portions} {_plural(total_portions, 'порция', 'порции', 'порций')}."
    )

    buttons.append([InlineKeyboardButton(
        "🔄 Обновить", callback_data=f"aodate:{target_date.isoformat()}")])
    buttons.append([InlineKeyboardButton("🏠 Главное меню", callback_data="acstop")])

    return "\n".join(lines), InlineKeyboardMarkup(buttons)


async def admin_orders_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Точка входа: главный админ нажал «Заказы на дату»."""
    _reset(context)
    rows = [
        [InlineKeyboardButton(label, callback_data=f"aodate:{d.isoformat()}")]
        for d, label in _view_dates()
    ]
    rows.append([InlineKeyboardButton("🏠 Главное меню", callback_data="acstop")])
    await update.message.reply_text(
        "Выберите дату — покажу список заказавших и дам отменить заказ одним нажатием.",
        reply_markup=InlineKeyboardMarkup(rows),
    )
    return ADMIN_ORDERS_DATE


async def admin_orders_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показали заказы на выбранную дату."""
    query = update.callback_query
    await query.answer()
    if not _is_master(update):
        return ConversationHandler.END

    try:
        target_date = datetime.strptime(query.data.split(":", 1)[1], "%Y-%m-%d").date()
    except (ValueError, IndexError):
        return ADMIN_ORDERS_DATE

    text, keyboard = _orders_list_view(target_date)
    await query.edit_message_text(text, reply_markup=keyboard)
    return ADMIN_ORDERS_LIST


async def admin_orders_cancel_item(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена заказа прямо из списка."""
    query = update.callback_query
    await query.answer()
    if not _is_master(update):
        return ConversationHandler.END

    try:
        order_id = int(query.data.split(":", 1)[1])
    except (ValueError, IndexError):
        return ADMIN_ORDERS_LIST

    order = db.session.query(Order).filter(Order.id == order_id).first()
    if not order:
        await query.answer("❌ Заказ не найден", show_alert=True)
        return ADMIN_ORDERS_LIST

    target_date = order.target_date
    emp = _employee_by_id(order.user_id)

    _, err = cancel_order(order.user_id, target_date, db.session)
    if err:
        await query.answer(f"❌ {err}", show_alert=True)
        return ADMIN_ORDERS_LIST

    db.session.commit()

    logger.info(
        "MASTER ADMIN %s: отменён заказ #%s — %s, %s, %s порц.",
        update.effective_user.id, order.id,
        (emp or {}).get("full_name", "?"), target_date.isoformat(), order.quantity,
    )

    await query.answer("✅ Заказ отменён", show_alert=True)
    text, keyboard = _orders_list_view(target_date)
    try:
        await query.edit_message_text(text, reply_markup=keyboard)
    except Exception:
        logger.debug("Не удалось обновить список заказов после отмены")
    return ADMIN_ORDERS_LIST


async def admin_orders_finish(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выход из списка."""
    _reset(context)
    if update.callback_query:
        await update.callback_query.answer()
        try:
            await update.callback_query.edit_message_text("✅ Возврат в главное меню.")
        except Exception:
            logger.debug("Не удалось отредактировать сообщение при выходе из списка")
        await update.callback_query.message.reply_text("Главное меню:", reply_markup=_admin_kb())
        return ConversationHandler.END

    await update.message.reply_text("Главное меню:", reply_markup=_admin_kb())
    return ConversationHandler.END


# ------------------------------------------------------------------
# Регистрация
# ------------------------------------------------------------------

def setup_admin_cancel_handlers(application):
    """Регистрация диалога отмены заказа (только для главного админа)."""
    from decorators import master_admin_filter

    conv = ConversationHandler(
        entry_points=[
            MessageHandler(
                filters.Regex("^" + re.escape(ADMIN_CANCEL_BUTTON) + "$") & master_admin_filter,
                admin_cancel_start,
            ),
        ],
        states={
            ADMIN_CANCEL_SEARCH: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_cancel_search),
            ],
            ADMIN_CANCEL_EMPLOYEE: [
                CallbackQueryHandler(pattern=r"^acemp:", callback=admin_cancel_employee),
            ],
            ADMIN_CANCEL_DATE: [
                CallbackQueryHandler(pattern=r"^acdate:", callback=admin_cancel_date),
            ],
            ADMIN_CANCEL_CONFIRM: [
                CallbackQueryHandler(pattern=r"^acdo", callback=admin_cancel_confirm),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", admin_cancel_finish),
            MessageHandler(
                filters.Regex(r"^(❌ Отмена|Отмена|🏠 Главное меню)$"),
                admin_cancel_finish,
            ),
            CallbackQueryHandler(pattern=r"^acstop", callback=admin_cancel_finish),
        ],
        allow_reentry=True,
    )
    application.add_handler(conv)
    return conv


def setup_admin_orders_handlers(application):
    """Список заказов на дату (только для главного админа)."""
    from decorators import master_admin_filter

    conv = ConversationHandler(
        entry_points=[
            MessageHandler(
                filters.Regex("^" + re.escape(ADMIN_ORDERS_BUTTON) + "$") & master_admin_filter,
                admin_orders_start,
            ),
        ],
        states={
            ADMIN_ORDERS_DATE: [
                CallbackQueryHandler(pattern=r"^aodate:", callback=admin_orders_date),
            ],
            ADMIN_ORDERS_LIST: [
                CallbackQueryHandler(pattern=r"^aocancel:", callback=admin_orders_cancel_item),
                CallbackQueryHandler(pattern=r"^aodate:", callback=admin_orders_date),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", admin_orders_finish),
            MessageHandler(
                filters.Regex(r"^(❌ Отмена|Отмена|🏠 Главное меню)$"),
                admin_orders_finish,
            ),
            CallbackQueryHandler(pattern=r"^acstop", callback=admin_orders_finish),
        ],
        allow_reentry=True,
    )
    application.add_handler(conv)
    return conv
