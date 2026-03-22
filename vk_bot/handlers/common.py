"""Common handlers for VK bot: view orders, statistics, location change."""
import logging
from datetime import datetime

from vkbottle.bot import BotLabeler, Message
from vkbottle import Keyboard, Callback, KeyboardButtonColor

from database import db
from config import CONFIG
from time_config import TIME_CONFIG
from services.order_service import get_active_orders, get_user_monthly_stats
from services.menu_service import DAYS_RU
from services.user_service import get_verified_user, MESSENGER_VK
from vk_bot.keyboards import main_menu_keyboard, location_keyboard

logger = logging.getLogger(__name__)
common_labeler = BotLabeler()


def _get_user_db_id(vk_user_id, session):
    user = get_verified_user(vk_user_id, MESSENGER_VK, session)
    return user.id if user else None


@common_labeler.private_message(text="Просмотреть заказы")
async def view_orders(message: Message):
    """Show active orders with cancel buttons."""
    user_id = message.from_id
    today = datetime.now(CONFIG.timezone).date()

    with db.get_session() as session:
        user_db_id = _get_user_db_id(user_id, session)
        if not user_db_id:
            await message.answer("❌ Вы не зарегистрированы. Напишите /start")
            return

        orders = get_active_orders(user_db_id, today, session)

        if not orders:
            await message.answer("ℹ️ У вас нет активных заказов.")
            return

        text = "📦 Ваши активные заказы:\n\n"

        kb = Keyboard(inline=True)
        for i, order in enumerate(orders):
            day_name = DAYS_RU[order.target_date.weekday()]
            date_str = order.target_date.strftime('%d.%m')
            qty = order.quantity
            status = " (предв.)" if order.is_preliminary else ""

            text += f"• {day_name} {date_str} — {qty} порц.{status}\n"
            if i > 0:
                kb.row()
            kb.add(
                Callback(
                    f"❌ Отменить {day_name} {date_str}",
                    payload={"cmd": "cancel_order", "date": order.target_date.strftime('%Y-%m-%d')}
                ),
                color=KeyboardButtonColor.NEGATIVE,
            )

    await message.answer(text, keyboard=kb.get_json())


@common_labeler.private_message(text="Статистика за месяц")
async def monthly_stats(message: Message):
    """Show user's monthly order statistics."""
    user_id = message.from_id
    now = datetime.now(CONFIG.timezone)
    start_date = now.replace(day=1).date()
    end_date = now.date()

    with db.get_session() as session:
        user_db_id = _get_user_db_id(user_id, session)
        if not user_db_id:
            await message.answer("❌ Вы не зарегистрированы. Напишите /start")
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
    await message.answer(text)


@common_labeler.private_message(text="📍 Изменить локацию")
async def change_location(message: Message):
    """Start location change flow."""
    user_id = message.from_id

    with db.get_session() as session:
        user = get_verified_user(user_id, MESSENGER_VK, session)
        if not user:
            await message.answer("❌ Вы не зарегистрированы. Напишите /start")
            return

        current = user.location or "не установлена"

    await message.answer(
        f"Текущая локация: {current}\nВыберите новый объект:",
        keyboard=location_keyboard()
    )
