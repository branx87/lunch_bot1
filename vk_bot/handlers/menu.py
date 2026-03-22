"""Menu display handlers for VK bot."""
import logging
from datetime import datetime

from vkbottle.bot import BotLabeler, Message

from database import db
from config import CONFIG
from time_config import TIME_CONFIG
from services.menu_service import get_menu_for_day, format_menu_text, get_week_menus
from services.order_service import get_order_for_date
from services.time_service import can_modify_order
from services.user_service import get_verified_user, MESSENGER_VK
from vk_bot.keyboards import order_buttons

logger = logging.getLogger(__name__)
menu_labeler = BotLabeler()


def _get_user_db_id(vk_user_id, session):
    user = get_verified_user(vk_user_id, MESSENGER_VK, session)
    return user.id if user else None


@menu_labeler.private_message(text="Меню на сегодня")
async def show_today_menu(message: Message):
    """Show today's menu with order buttons."""
    user_id = message.from_id

    with db.get_session() as session:
        user_db_id = _get_user_db_id(user_id, session)
        if not user_db_id:
            await message.answer("❌ Вы не зарегистрированы. Напишите /start")
            return

        menu, day_name, target_date = get_menu_for_day(0, CONFIG)

        if not menu:
            await message.answer(f"На {day_name} меню не предусмотрено (выходной/праздник).")
            return

        text = format_menu_text(menu, day_name, target_date)

        order = get_order_for_date(user_db_id, target_date, session)
        has_order = order is not None
        can_mod = can_modify_order(target_date, CONFIG.are_orders_accepted_now())

        if has_order:
            text += f"\n\n🛒 Ваш заказ: {order.quantity} порций"

    await message.answer(text, keyboard=order_buttons(0, has_order, can_mod))


@menu_labeler.private_message(text="Меню на неделю")
async def show_week_menu(message: Message):
    """Show weekly menu with order buttons per day."""
    user_id = message.from_id

    with db.get_session() as session:
        user_db_id = _get_user_db_id(user_id, session)
        if not user_db_id:
            await message.answer("❌ Вы не зарегистрированы. Напишите /start")
            return

        week = get_week_menus(CONFIG)

        for day_info in week:
            if day_info['is_weekend']:
                continue
            if day_info['is_holiday']:
                await message.answer(
                    f"🎉 {day_info['day_name']} ({day_info['target_date'].strftime('%d.%m')}) — "
                    f"{day_info['holiday_name']}"
                )
                continue

            menu = day_info['menu']
            if not menu:
                continue

            text = format_menu_text(menu, day_info['day_name'], day_info['target_date'])
            offset = day_info['day_offset']

            order = get_order_for_date(user_db_id, day_info['target_date'], session)
            has_order = order is not None
            can_mod = can_modify_order(day_info['target_date'], CONFIG.are_orders_accepted_now())

            if has_order:
                text += f"\n\n🛒 Ваш заказ: {order.quantity} порций"

            await message.answer(text, keyboard=order_buttons(offset, has_order, can_mod))
