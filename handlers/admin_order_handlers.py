# ##handlers/admin_order_handlers.py
"""
Админский заказ: оформление заказа за другого сотрудника или для инспектора.

Зачем это нужно:
  * сотрудник забыл заказать обед, а поставщику количество порций ещё не
    отправляли — админ оформляет заказ за него;
  * обед инспектору (расходы компании) — админ может оформить такой же
    заказ, как сотрудники из INSPECTOR_ALLOWED_BITRIX_IDS.

Отличия от обычного заказа сотрудника:
  * не нужно ждать закрытия окна приёма (15:00 предыдущего рабочего дня);
  * на сегодня — только до выгрузки в Bitrix24 (09:30), дальше заказ уже
    не попадёт к поставщику;
  * количество порций выбирает админ.

Заказ создаётся на сотрудника: сотрудник видит его у себя в «Мои заказы»,
а в выгрузке поставщику уходит на его Bitrix ID. Для заказа инспектору
CRM ID подменяется на инспектора (is_for_inspector=True).
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
    ADMIN_ORDER_CONFIRM,
    ADMIN_ORDER_DATE,
    ADMIN_ORDER_EMPLOYEE,
    ADMIN_ORDER_QTY,
    ADMIN_ORDER_SEARCH,
    ADMIN_ORDER_TYPE,
)
from database import db
from models import User
from services.menu_service import DAYS_RU
from services.order_service import create_order, find_employees, get_order_for_date
from services.time_service import (
    admin_can_order_date,
    admin_order_dates,
    admin_today_note,
)
from time_config import TIME_CONFIG

logger = logging.getLogger(__name__)

ADMIN_ORDER_BUTTON = "👨‍💼 Заказать за сотрудника"

KIND_NORMAL = "normal"
KIND_INSPECTOR = "inspector"

_FLOW_KEY = "admin_order"


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


def _is_admin(update) -> bool:
    return update.effective_user.id in CONFIG.admin_ids


def _employee_by_id(emp_id: int):
    """Данные сотрудника по внутреннему id."""
    row = db.session.query(User).filter(User.id == emp_id).first()
    if not row:
        return None
    return {
        "id": row.id,
        "full_name": row.full_name,
        "position": row.position,
        "department": row.department,
        "location": row.location,
        "bitrix_id": row.bitrix_id,
    }


def _employee_label(emp: dict) -> str:
    """Подпись сотрудника в кнопке выбора."""
    label = emp.get("full_name") or "Без имени"
    detail = emp.get("position") or emp.get("location")
    if detail:
        label += f" — {detail}"
    if emp.get("bitrix_id"):
        label += f" (B24: {emp['bitrix_id']})"
    else:
        label += " ⚠️ нет Bitrix ID"
    return label


def _kind_label(kind: str) -> str:
    if kind == KIND_INSPECTOR:
        return "🕵️ Заказ инспектору"
    return "🍽 Заказ сотруднику"


def _date_label(target_date, today=None) -> str:
    if today is None:
        today = datetime.now(TIME_CONFIG.TIMEZONE).date()
    if target_date == today:
        return f"Сегодня {target_date.strftime('%d.%m')}"
    return f"{DAYS_RU[target_date.weekday()]} {target_date.strftime('%d.%m')}"


def _date_step(context, emp: dict):
    """Текст и клавиатура выбора даты."""
    kind = _flow(context).get("kind", KIND_NORMAL)
    dates = admin_order_dates()

    rows = [
        [InlineKeyboardButton(
            _date_label(d),
            callback_data=f"admdate:{d.isoformat()}",
        )]
        for d in dates
    ]
    rows.append([InlineKeyboardButton("❌ Отмена", callback_data="admcancel")])

    text = (
        f"{_kind_label(kind)}\n"
        f"Сотрудник: {emp['full_name']}\n\n"
        "Выберите дату обеда:"
    )
    note = admin_today_note()
    if note:
        text += f"\n\n{note}"

    return text, InlineKeyboardMarkup(rows)


def _confirm_text(context) -> str:
    data = _flow(context)
    emp = data.get("employee") or {}
    kind = data.get("kind", KIND_NORMAL)
    target_date = data.get("date")
    qty = data.get("qty", 1)

    lines = []
    if kind == KIND_INSPECTOR:
        lines.append("🕵️ Заказ для инспектора (расходы компании)")
        lines.append(f"Организатор: {emp.get('full_name')}")
    else:
        lines.append("🍽 Заказ сотруднику")
        lines.append(f"Сотрудник: {emp.get('full_name')}")
    lines.append(f"Дата: {_date_label(target_date)}")
    lines.append(f"Порций: {qty}")
    lines.append("")
    lines.append("Всё верно?")
    return "\n".join(lines)


# ------------------------------------------------------------------
# Шаги диалога
# ------------------------------------------------------------------

async def admin_order_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Точка входа: админ нажал «Заказать за сотрудника»."""
    _reset(context)
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🍽 Заказ сотруднику", callback_data=f"admtype:{KIND_NORMAL}")],
        [InlineKeyboardButton("🕵️ Заказ инспектору", callback_data=f"admtype:{KIND_INSPECTOR}")],
        [InlineKeyboardButton("❌ Отмена", callback_data="admcancel")],
    ])
    await update.message.reply_text("Что оформляем?", reply_markup=keyboard)
    return ADMIN_ORDER_TYPE


async def admin_order_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выбран тип заказа — спрашиваем, для кого оформляем."""
    query = update.callback_query
    await query.answer()
    if not _is_admin(update):
        return ConversationHandler.END

    kind = query.data.split(":", 1)[1] if ":" in query.data else ""
    if kind not in (KIND_NORMAL, KIND_INSPECTOR):
        return ADMIN_ORDER_TYPE

    _set(context, kind=kind)

    if kind == KIND_INSPECTOR:
        prompt = (
            "Введите ФИО сотрудника-организатора заказа инспектору "
            "(или его Bitrix ID).\n\n"
            "Этот сотрудник будет указан заказчиком в Битрикс24."
        )
    else:
        prompt = (
            "Введите ФИО сотрудника, которому оформляется обед "
            "(или его Bitrix ID)."
        )

    await query.edit_message_reply_markup(reply_markup=None)
    await query.message.reply_text(
        prompt,
        reply_markup=ReplyKeyboardMarkup(
            [["❌ Отмена", "🏠 Главное меню"]], resize_keyboard=True),
    )
    return ADMIN_ORDER_SEARCH


async def admin_order_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ввод ФИО или Bitrix ID сотрудника."""
    text = (update.message.text or "").strip()

    if text in ("❌ Отмена", "Отмена"):
        return await admin_order_cancel(update, context)
    if text == "🏠 Главное меню":
        return await admin_order_cancel(update, context)

    employees = find_employees(text, db.session, limit=10)

    if not employees:
        await update.message.reply_text(
            f"❌ По запросу «{text}» сотрудник не найден.\n\n"
            "Проверьте ФИО или введите Bitrix ID. Если сотрудника нет в базе — "
            "дождитесь синхронизации с Битрикс24 (или добавьте его через "
            "«➕ Добавить сотрудника»)."
        )
        return ADMIN_ORDER_SEARCH

    if len(employees) == 1:
        emp = employees[0]
        if not emp.get("bitrix_id"):
            await update.message.reply_text(
                f"❌ У сотрудника {emp['full_name']} нет Bitrix ID — такой заказ "
                "не попадёт в выгрузку поставщику.\n\n"
                "Обновите сотрудников в Битрикс24 и попробуйте снова."
            )
            return ADMIN_ORDER_SEARCH

        _set(context, employee=emp)
        step_text, kb = _date_step(context, emp)
        await update.message.reply_text(step_text, reply_markup=kb)
        return ADMIN_ORDER_DATE

    keyboard = [
        [InlineKeyboardButton(_employee_label(e), callback_data=f"admemp:{e['id']}")]
        for e in employees
    ]
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="admcancel")])
    await update.message.reply_text(
        f"Найдено сотрудников: {len(employees)}. Выберите нужного:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return ADMIN_ORDER_EMPLOYEE


async def admin_order_employee(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выбор сотрудника из списка совпадений."""
    query = update.callback_query
    await query.answer()
    if not _is_admin(update):
        return ConversationHandler.END

    try:
        emp_id = int(query.data.split(":", 1)[1])
    except (ValueError, IndexError):
        return ADMIN_ORDER_EMPLOYEE

    emp = _employee_by_id(emp_id)
    if not emp:
        await query.edit_message_text("❌ Сотрудник не найден в базе.")
        return ADMIN_ORDER_EMPLOYEE

    if not emp.get("bitrix_id"):
        await query.edit_message_text(
            f"❌ У сотрудника {emp['full_name']} нет Bitrix ID — такой заказ "
            "не попадёт в выгрузку поставщику.\n\n"
            "Обновите сотрудников в Битрикс24 и попробуйте снова."
        )
        return ADMIN_ORDER_EMPLOYEE

    _set(context, employee=emp)
    step_text, kb = _date_step(context, emp)
    await query.edit_message_text(step_text, reply_markup=kb)
    return ADMIN_ORDER_DATE


async def admin_order_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выбор даты обеда."""
    query = update.callback_query
    await query.answer()
    if not _is_admin(update):
        return ConversationHandler.END

    try:
        target_date = datetime.strptime(query.data.split(":", 1)[1], "%Y-%m-%d").date()
    except (ValueError, IndexError):
        return ADMIN_ORDER_DATE

    if not admin_can_order_date(target_date):
        await query.edit_message_text(
            f"⏳ На {target_date.strftime('%d.%m')} заказ уже нельзя оформить."
        )
        return ADMIN_ORDER_DATE

    _set(context, date=target_date)

    rows = [[
        InlineKeyboardButton(str(i), callback_data=f"admqty:{i}")
        for i in range(1, TIME_CONFIG.MAX_PORTIONS + 1)
    ]]
    rows.append([InlineKeyboardButton("❌ Отмена", callback_data="admcancel")])
    await query.edit_message_text(
        "Сколько порций заказать?", reply_markup=InlineKeyboardMarkup(rows))
    return ADMIN_ORDER_QTY


async def admin_order_qty(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выбор количества порций."""
    query = update.callback_query
    await query.answer()
    if not _is_admin(update):
        return ConversationHandler.END

    try:
        qty = int(query.data.split(":", 1)[1])
    except (ValueError, IndexError):
        return ADMIN_ORDER_QTY

    if qty < 1 or qty > TIME_CONFIG.MAX_PORTIONS:
        return ADMIN_ORDER_QTY

    _set(context, qty=qty)
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Оформить заказ", callback_data="admcreate")],
        [InlineKeyboardButton("❌ Отмена", callback_data="admcancel")],
    ])
    await query.edit_message_text(_confirm_text(context), reply_markup=keyboard)
    return ADMIN_ORDER_CONFIRM


async def admin_order_create(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Создание заказа."""
    query = update.callback_query
    await query.answer()
    if not _is_admin(update):
        return ConversationHandler.END

    data = _flow(context)
    emp = data.get("employee")
    kind = data.get("kind", KIND_NORMAL)
    target_date = data.get("date")
    qty = int(data.get("qty", 1))

    if not emp or not target_date:
        _reset(context)
        await query.edit_message_text("❌ Данные заказа не найдены. Начните заново.")
        return ConversationHandler.END

    if not admin_can_order_date(target_date):
        await query.edit_message_text(
            f"⏳ На {target_date.strftime('%d.%m')} заказ уже нельзя оформить."
        )
        return ConversationHandler.END

    existing = get_order_for_date(emp["id"], target_date, db.session)
    if existing:
        _reset(context)
        await query.edit_message_text(
            f"ℹ️ У {emp['full_name']} уже есть заказ на "
            f"{target_date.strftime('%d.%m')} — {existing.quantity} порц. "
            "Дубликат не создаём."
        )
        await query.message.reply_text(
            "Главное меню:", reply_markup=create_admin_keyboard())
        return ConversationHandler.END

    order, err = create_order(
        emp["id"],
        target_date,
        db.session,
        quantity=qty,
        is_for_inspector=(kind == KIND_INSPECTOR),
    )
    if err:
        _reset(context)
        await query.edit_message_text(f"❌ {err}")
        await query.message.reply_text(
            "Главное меню:", reply_markup=create_admin_keyboard())
        return ConversationHandler.END

    db.session.commit()
    _reset(context)

    logger.info(
        "ADMIN %s: оформлен заказ — %s, %s, %s порц., инспектор=%s",
        update.effective_user.id, emp["full_name"],
        target_date.isoformat(), qty, kind == KIND_INSPECTOR,
    )

    if kind == KIND_INSPECTOR:
        result = (
            f"✅ Заказ для инспектора оформлен — "
            f"{_date_label(target_date)}, {qty} порц.\n"
            f"Организатор: {emp['full_name']}\n"
            "⚠️ Расходы компании: в Битрикс24 заказ уйдёт на инспектора."
        )
    else:
        result = (
            f"✅ Заказ оформлен — {_date_label(target_date)}, {qty} порц.\n"
            f"Сотрудник: {emp['full_name']}"
        )

    await query.edit_message_text(result)
    await query.message.reply_text("Главное меню:", reply_markup=create_admin_keyboard())
    return ConversationHandler.END


async def admin_order_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена админского заказа."""
    _reset(context)

    if update.callback_query:
        await update.callback_query.answer()
        try:
            await update.callback_query.edit_message_text("❌ Оформление заказа отменено.")
        except Exception:
            logger.debug("Не удалось отредактировать сообщение при отмене заказа")
        await update.callback_query.message.reply_text(
            "Главное меню:", reply_markup=create_admin_keyboard())
        return ConversationHandler.END

    await update.message.reply_text("❌ Оформление заказа отменено.")
    from handlers.common import show_main_menu
    await show_main_menu(update, update.effective_user.id)
    return ConversationHandler.END


# ------------------------------------------------------------------
# Регистрация
# ------------------------------------------------------------------

def setup_admin_order_handlers(application):
    """Регистрация диалога админского заказа."""
    from decorators import admin_filter

    conv = ConversationHandler(
        entry_points=[
            MessageHandler(
                filters.Regex("^" + re.escape(ADMIN_ORDER_BUTTON) + "$") & admin_filter,
                admin_order_start,
            ),
        ],
        states={
            ADMIN_ORDER_TYPE: [
                CallbackQueryHandler(pattern=r"^admtype:", callback=admin_order_type),
            ],
            ADMIN_ORDER_SEARCH: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_order_search),
            ],
            ADMIN_ORDER_EMPLOYEE: [
                CallbackQueryHandler(pattern=r"^admemp:", callback=admin_order_employee),
            ],
            ADMIN_ORDER_DATE: [
                CallbackQueryHandler(pattern=r"^admdate:", callback=admin_order_date),
            ],
            ADMIN_ORDER_QTY: [
                CallbackQueryHandler(pattern=r"^admqty:", callback=admin_order_qty),
            ],
            ADMIN_ORDER_CONFIRM: [
                CallbackQueryHandler(pattern=r"^admcreate", callback=admin_order_create),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", admin_order_cancel),
            MessageHandler(
                filters.Regex(r"^(❌ Отмена|Отмена|🏠 Главное меню)$"),
                admin_order_cancel,
            ),
            CallbackQueryHandler(pattern=r"^admcancel", callback=admin_order_cancel),
        ],
        allow_reentry=True,
    )
    application.add_handler(conv)
    return conv
