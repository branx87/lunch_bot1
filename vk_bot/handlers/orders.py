"""Order callback handlers for VK bot."""
import logging
from datetime import datetime, timedelta

from vkbottle.bot import BotLabeler, Message, MessageEvent, rules
from vkbottle import GroupEventType

from database import db
from config import CONFIG
from time_config import TIME_CONFIG
from services.order_service import create_order, cancel_order, modify_quantity, get_order_for_date
from services.time_service import can_modify_order, order_deadline_message, is_new_supplier_date, next_working_day
from services.menu_service import get_menu_for_day, format_menu_text, DAYS_RU
from services.user_service import get_user_db_id, MESSENGER_VK
from vk_bot.keyboards import order_buttons, quantity_buttons
from bitrix.ops import cleanup_cancelled

logger = logging.getLogger(__name__)
orders_labeler = BotLabeler()


def _build_day_view(day_offset, user_db_id, session):
    """Build menu text and keyboard for a day. Returns (text, keyboard) or (None, None)."""
    now = datetime.now(TIME_CONFIG.TIMEZONE)
    target_date = (now + timedelta(days=day_offset)).date()
    menu, day_name, _ = get_menu_for_day(day_offset, CONFIG)

    if not menu:
        return f"На {day_name} меню не предусмотрено.", None

    text = format_menu_text(menu, day_name, target_date)
    order = get_order_for_date(user_db_id, target_date, session)
    has_order = order is not None
    can_mod = can_modify_order(target_date, CONFIG.are_orders_accepted_now())

    if not CONFIG.are_orders_accepted_now():
        text += f"\n\n{CONFIG.get_orders_status_message()}"
    if has_order:
        text += f"\n\n🛒 Ваш заказ: {order.quantity} порций"

    return text, order_buttons(day_offset, has_order, can_mod)


async def _refresh_day_view(api, peer_id, day_offset, user_db_id, session):
    """Send updated menu view as a NEW message."""
    text, keyboard = _build_day_view(day_offset, user_db_id, session)
    kwargs = {"peer_id": peer_id, "message": text, "random_id": 0}
    if keyboard:
        kwargs["keyboard"] = keyboard
    await api.messages.send(**kwargs)


async def _edit_day_view(event, day_offset, user_db_id, session):
    """Edit the current message with updated menu view."""
    text, keyboard = _build_day_view(day_offset, user_db_id, session)
    if keyboard:
        await event.edit_message(text, keyboard=keyboard)
    else:
        await event.edit_message(text)


# --- Quick order ---

@orders_labeler.private_message(text="✅ Быстрый заказ")
async def quick_order(message: Message):
    """Quick order (1 portion).

    У старого поставщика — на сегодня, у нового — на ближайший рабочий день.
    """
    user_id = message.from_id
    now = datetime.now(TIME_CONFIG.TIMEZONE)
    today = now.date()

    if is_new_supplier_date(today):
        # Новый поставщик: заказ на сегодня не принимается
        target_date = next_working_day(today, CONFIG.holidays)
    else:
        target_date = today

    if not CONFIG.are_orders_accepted_now():
        await message.answer(CONFIG.get_orders_status_message())
        return

    if not can_modify_order(target_date, CONFIG.are_orders_accepted_now()):
        await message.answer(order_deadline_message(target_date))
        return

    with db.get_session() as session:
        user_db_id = get_user_db_id(user_id, MESSENGER_VK, session)
        if not user_db_id:
            await message.answer("❌ Вы не зарегистрированы. Напишите /start")
            return

        order, error = create_order(user_db_id, target_date, session)
        if error:
            await message.answer(f"ℹ️ {error}")
            return

        session.commit()
        await message.answer(f"✅ Заказ на {target_date.strftime('%d.%m')} оформлен (1 порция)")
        # Показываем меню того дня, на который оформлен заказ,
        # а не «сегодня» — на сегодня заказы не принимаются.
        await _refresh_day_view(
            message.ctx_api, message.peer_id, (target_date - today).days, user_db_id, session)


# --- All inline callback events ---

ORDER_CMDS = {"order", "change", "inc", "dec", "cancel", "confirm", "cancel_order", "noop"}


@orders_labeler.raw_event(
    GroupEventType.MESSAGE_EVENT,
    MessageEvent,
    rules.FuncRule(lambda e: e.object.payload.get("cmd") in ORDER_CMDS),
)
async def on_order_callback(event: MessageEvent):
    """Handle all order-related inline button callbacks."""
    payload = event.object.payload
    cmd = payload.get("cmd")
    day_offset = payload.get("d", 0)
    user_id = event.object.peer_id
    now = datetime.now(TIME_CONFIG.TIMEZONE)
    target_date = (now + timedelta(days=day_offset)).date()

    # --- NOOP ---
    if cmd == "noop":
        await event.send_empty_answer()
        return

    # --- ORDER ---
    if cmd == "order":
        if not CONFIG.are_orders_accepted_now():
            await event.show_snackbar(CONFIG.get_orders_status_message()[:90])
            return

        if not can_modify_order(target_date, CONFIG.are_orders_accepted_now()):
            await event.show_snackbar(order_deadline_message(target_date)[:120])
            return

        with db.get_session() as session:
            user_db_id = get_user_db_id(user_id, MESSENGER_VK, session)
            if not user_db_id:
                await event.show_snackbar("Вы не зарегистрированы")
                return

            order, error = create_order(user_db_id, target_date, session, is_preliminary=(day_offset > 0))
            if error:
                await event.show_snackbar(error[:90])
                return

            session.commit()
            await event.show_snackbar("✅ Заказ оформлен")
            await _edit_day_view(event, day_offset, user_db_id, session)
        return

    # --- CHANGE ---
    if cmd == "change":
        if not can_modify_order(target_date, CONFIG.are_orders_accepted_now()):
            await event.show_snackbar(order_deadline_message(target_date, action='modify'))
            return

        with db.get_session() as session:
            user_db_id = get_user_db_id(user_id, MESSENGER_VK, session)
            if not user_db_id:
                return

            order = get_order_for_date(user_db_id, target_date, session)
            if not order:
                await event.show_snackbar("Заказ не найден")
                return

            day_name = DAYS_RU[target_date.weekday()]
            menu = CONFIG.menu.get(day_name)
            if not menu:
                await event.show_snackbar("Меню не найдено")
                return

            text = format_menu_text(menu, day_name, target_date)
            text += f"\n\n🛒 Текущий заказ: {order.quantity} порции"

        await event.edit_message(text, keyboard=quantity_buttons(day_offset))
        return

    # --- INC / DEC ---
    if cmd in ("inc", "dec"):
        delta = 1 if cmd == "inc" else -1

        if not can_modify_order(target_date, CONFIG.are_orders_accepted_now()):
            await event.show_snackbar(order_deadline_message(target_date, action='modify'))
            return

        with db.get_session() as session:
            user_db_id = get_user_db_id(user_id, MESSENGER_VK, session)
            if not user_db_id:
                return

            new_qty, order, error = modify_quantity(user_db_id, target_date, delta, session)

            if error:
                await event.show_snackbar(error[:90])
                return

            if new_qty == 0:
                order.is_cancelled = True
                order.order_time = now.strftime("%H:%M:%S")
                session.commit()
                await event.show_snackbar("✅ Заказ отменён")
                await _edit_day_view(event, day_offset, user_db_id, session)
                return

            session.commit()
            await event.show_snackbar(f"Установлено: {new_qty} порции")

            day_name = DAYS_RU[target_date.weekday()]
            menu = CONFIG.menu.get(day_name)
            if menu:
                text = format_menu_text(menu, day_name, target_date)
                text += f"\n\n🛒 Текущий заказ: {new_qty} порции"
                await event.edit_message(text, keyboard=quantity_buttons(day_offset))
        return

    # --- CANCEL ---
    if cmd == "cancel":
        # Отмена доступна всегда, даже при выключенном приёме заказов:
        # заблокировав приём, нельзя оставить человека без возможности отменить свой заказ.
        if not can_modify_order(target_date, True):
            await event.show_snackbar(order_deadline_message(target_date, action='cancel'))
            return

        with db.get_session() as session:
            user_db_id = get_user_db_id(user_id, MESSENGER_VK, session)
            if not user_db_id:
                return

            order, error = cancel_order(user_db_id, target_date, session)
            if error:
                await event.show_snackbar(error[:90])
                return

            session.commit()

            await cleanup_cancelled(order.id)

            await event.show_snackbar("✅ Заказ отменён")
            await _edit_day_view(event, day_offset, user_db_id, session)
        return

    # --- CONFIRM ---
    if cmd == "confirm":
        with db.get_session() as session:
            user_db_id = get_user_db_id(user_id, MESSENGER_VK, session)
            if not user_db_id:
                return
            await _edit_day_view(event, day_offset, user_db_id, session)
        await event.show_snackbar("✅ Заказ подтверждён")
        return

    # --- CANCEL from orders list (cancel_order_YYYY-MM-DD) ---
    if cmd == "cancel_order":
        date_str = payload.get("date", "")
        try:
            from datetime import datetime as dt
            target_date = dt.strptime(date_str, "%Y-%m-%d").date()
            day_offset = (target_date - now.date()).days
        except ValueError:
            await event.show_snackbar("Ошибка формата даты")
            return

        # Отмена из списка заказов — глобальный статус заказов тут не учитывается,
        # отменить свой заказ можно всегда.
        if not can_modify_order(target_date, True):
            await event.show_snackbar(order_deadline_message(target_date, action='cancel'))
            return

        with db.get_session() as session:
            user_db_id = get_user_db_id(user_id, MESSENGER_VK, session)
            if not user_db_id:
                return

            order, error = cancel_order(user_db_id, target_date, session)
            if error:
                await event.show_snackbar(error[:90])
                return

            session.commit()
            await event.show_snackbar("✅ Заказ отменён")
            await event.edit_message("✅ Заказ отменён")
        return
