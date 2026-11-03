# ##handlers/admin_cancel_handlers.py
"""Отмена заказа сотрудника — только для главного админа (MASTER_ADMIN_ID).

Зачем: админ оформил заказ за сотрудника («👨‍💼 Заказать за сотрудника»),
и если что-то пошло не так — ошиблись сотрудником, датой или количеством —
отменить этот заказ было нечем. Сотрудник отменяет заказ сам, обычному
админу такая возможность не нужна, поэтому кнопка есть только у главного.
"""
import logging
import re
from datetime import datetime

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

from bot_keyboards import create_admin_keyboard
from config import CONFIG
from constants import (
    ADMIN_CANCEL_CONFIRM,
    ADMIN_CANCEL_DATE,
    ADMIN_CANCEL_EMPLOYEE,
    ADMIN_CANCEL_SEARCH,
)
from database import db
from handlers.admin_order_handlers import _employee_by_id
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
