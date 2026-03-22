"""Common handlers for Max bot: view orders, statistics, location change."""
import logging
from datetime import datetime

from maxapi import Router, F
from maxapi.types import MessageCreated, MessageCallback
from maxapi.fsm import MemoryContext

from database import db
from config import CONFIG
from time_config import TIME_CONFIG
from services.order_service import get_active_orders, get_user_monthly_stats
from services.menu_service import DAYS_RU
from services.user_service import get_verified_user, MESSENGER_MAX
from max_bot.keyboards import main_menu_keyboard, location_keyboard
from max_bot.states import Registration
from maxapi.types import CallbackButton
from maxapi.utils.inline_keyboard import InlineKeyboardBuilder

logger = logging.getLogger(__name__)
router = Router()


def _get_user_db_id(max_user_id, session):
    user = get_verified_user(max_user_id, MESSENGER_MAX, session)
    return user.id if user else None


@router.message_created(F.message.body.text == "Просмотреть заказы")
async def view_orders(event: MessageCreated):
    """Show active orders with cancel buttons."""
    user_id = event.user.user_id
    today = datetime.now(CONFIG.timezone).date()

    with db.get_session() as session:
        user_db_id = _get_user_db_id(user_id, session)
        if not user_db_id:
            await event.message.answer("❌ Вы не зарегистрированы. Отправьте /start")
            return

        orders = get_active_orders(user_db_id, today, session)

        if not orders:
            await event.message.answer("ℹ️ У вас нет активных заказов.")
            return

        text = "📦 <b>Ваши активные заказы:</b>\n\n"

        builder = InlineKeyboardBuilder()
        for order in orders:
            day_name = DAYS_RU[order.target_date.weekday()]
            date_str = order.target_date.strftime('%d.%m')
            qty = order.quantity
            status = " (предв.)" if order.is_preliminary else ""

            text += f"• {day_name} {date_str} — {qty} порц.{status}\n"
            builder.row(CallbackButton(
                text=f"❌ Отменить {day_name} {date_str}",
                payload=f"cancel_order_{order.target_date.strftime('%Y-%m-%d')}"
            ))

    await event.message.answer(text, attachments=[builder.as_markup()], format="html")


@router.message_created(F.message.body.text == "Статистика за месяц")
async def monthly_stats(event: MessageCreated):
    """Show user's monthly order statistics."""
    user_id = event.user.user_id
    now = datetime.now(CONFIG.timezone)
    start_date = now.replace(day=1).date()
    end_date = now.date()

    with db.get_session() as session:
        user_db_id = _get_user_db_id(user_id, session)
        if not user_db_id:
            await event.message.answer("❌ Вы не зарегистрированы. Отправьте /start")
            return

        stats = get_user_monthly_stats(user_db_id, start_date, end_date, session)

    month_names = {
        1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
        5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
        9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь"
    }

    text = (
        f"📊 Статистика за {month_names[now.month]} {now.year}:\n\n"
        f"🍽 Всего обедов: {stats['total']}\n"
        f"✅ Выполненные: {stats['completed']}\n"
        f"⏳ Предстоящие: {stats['upcoming']}"
    )
    await event.message.answer(text)


@router.message_created(F.message.body.text == "📍 Изменить локацию")
async def change_location(event: MessageCreated):
    """Start location change flow."""
    user_id = event.user.user_id

    with db.get_session() as session:
        user = get_verified_user(user_id, MESSENGER_MAX, session)
        if not user:
            await event.message.answer("❌ Вы не зарегистрированы. Отправьте /start")
            return

        current = user.location or "не установлена"

    await event.message.answer(
        f"Текущая локация: {current}\nВыберите новый объект:",
        attachments=[location_keyboard()]
    )
