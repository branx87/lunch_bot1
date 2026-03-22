"""Order callback handlers for Max bot: create, modify, cancel."""
import logging
from datetime import datetime, timedelta

from maxapi import Router, F
from maxapi.types import MessageCallback

from database import db
from config import CONFIG
from time_config import TIME_CONFIG
from services.order_service import (
    create_order, cancel_order, modify_quantity, get_order_for_date
)
from services.time_service import can_modify_order
from services.menu_service import get_menu_for_day, format_menu_text, DAYS_RU
from services.user_service import get_verified_user, MESSENGER_MAX
from max_bot.keyboards import order_buttons, quantity_buttons, main_menu_keyboard

logger = logging.getLogger(__name__)
router = Router()


def _parse_day_offset(payload):
    """Extract day_offset from payload like 'order_2' -> 2."""
    parts = payload.split("_", 1)
    return int(parts[1]) if len(parts) > 1 else 0


def _get_user_db_id(max_user_id, session):
    user = get_verified_user(max_user_id, MESSENGER_MAX, session)
    return user.id if user else None


async def _refresh_day_view(event, day_offset, user_db_id, session):
    """Re-send menu view with updated order status."""
    now = datetime.now(TIME_CONFIG.TIMEZONE)
    target_date = (now + timedelta(days=day_offset)).date()
    menu, day_name, _ = get_menu_for_day(day_offset, CONFIG)

    if not menu:
        await event.message.answer(f"На {day_name} меню не предусмотрено.")
        return

    text = format_menu_text(menu, day_name, target_date)
    order = get_order_for_date(user_db_id, target_date, session)
    has_order = order is not None
    can_mod = can_modify_order(target_date, CONFIG.are_orders_accepted_now())

    if has_order:
        text += f"\n\n🛒 Ваш заказ: {order.quantity} порций"

    await event.message.answer(text, attachments=[order_buttons(day_offset, has_order, can_mod)])


# --- Quick order ---

@router.message_created(F.message.body.text == "✅ Быстрый заказ")
async def quick_order(event):
    """Quick order for today (1 portion)."""
    user_id = event.user.user_id
    now = datetime.now(TIME_CONFIG.TIMEZONE)
    target_date = now.date()

    if not CONFIG.are_orders_accepted_now():
        await event.message.answer(CONFIG.get_orders_status_message())
        return

    if now.time() >= TIME_CONFIG.ORDER_DEADLINE:
        await event.message.answer(
            f"ℹ️ Приём заказов на сегодня завершён в {TIME_CONFIG.ORDER_DEADLINE.strftime('%H:%M')}"
        )
        return

    with db.get_session() as session:
        user_db_id = _get_user_db_id(user_id, session)
        if not user_db_id:
            await event.message.answer("❌ Вы не зарегистрированы. Отправьте /start")
            return

        order, error = create_order(user_db_id, target_date, session)
        if error:
            await event.message.answer(f"ℹ️ {error}")
            return

        session.commit()
        await event.message.answer("✅ Заказ на сегодня оформлен (1 порция)")
        await _refresh_day_view(event, 0, user_db_id, session)


# --- Order callback ---

@router.message_callback(F.callback.payload.startswith("order_"))
async def on_order(event: MessageCallback):
    """Create order from menu button."""
    user_id = event.user.user_id
    day_offset = _parse_day_offset(event.callback.payload)
    now = datetime.now(TIME_CONFIG.TIMEZONE)
    target_date = (now + timedelta(days=day_offset)).date()

    if not CONFIG.are_orders_accepted_now():
        await event.answer(notification=CONFIG.get_orders_status_message())
        return

    if day_offset == 0 and now.time() >= TIME_CONFIG.ORDER_DEADLINE:
        await event.answer(
            notification=f"ℹ️ Приём заказов на сегодня завершён в {TIME_CONFIG.ORDER_DEADLINE.strftime('%H:%M')}"
        )
        return

    with db.get_session() as session:
        user_db_id = _get_user_db_id(user_id, session)
        if not user_db_id:
            await event.answer(notification="❌ Вы не зарегистрированы")
            return

        order, error = create_order(user_db_id, target_date, session, is_preliminary=(day_offset > 0))
        if error:
            await event.answer(notification=f"ℹ️ {error}")
            return

        session.commit()
        await event.answer(notification="✅ Заказ оформлен")
        await _refresh_day_view(event, day_offset, user_db_id, session)


# --- Change quantity ---

@router.message_callback(F.callback.payload.startswith("change_"))
async def on_change(event: MessageCallback):
    """Show quantity modification interface."""
    user_id = event.user.user_id
    day_offset = _parse_day_offset(event.callback.payload)
    now = datetime.now(TIME_CONFIG.TIMEZONE)
    target_date = (now + timedelta(days=day_offset)).date()

    if not can_modify_order(target_date, CONFIG.are_orders_accepted_now()):
        await event.answer(
            notification=f"ℹ️ Изменение невозможно после {TIME_CONFIG.MODIFICATION_DEADLINE.strftime('%H:%M')}"
        )
        return

    with db.get_session() as session:
        user_db_id = _get_user_db_id(user_id, session)
        if not user_db_id:
            return

        order = get_order_for_date(user_db_id, target_date, session)
        if not order:
            await event.answer(notification="ℹ️ Заказ не найден")
            return

        day_name = DAYS_RU[target_date.weekday()]
        menu = CONFIG.menu.get(day_name)
        if not menu:
            await event.answer(notification="⚠️ Меню не найдено")
            return

        text = format_menu_text(menu, day_name, target_date)
        text += f"\n\n🛒 Текущий заказ: {order.quantity} порции"

    await event.message.answer(text, attachments=[quantity_buttons(day_offset)])
    await event.answer()


# --- Increase/decrease quantity ---

@router.message_callback(F.callback.payload.startswith("inc_"))
async def on_increase(event: MessageCallback):
    await _modify(event, +1)


@router.message_callback(F.callback.payload.startswith("dec_"))
async def on_decrease(event: MessageCallback):
    await _modify(event, -1)


async def _modify(event: MessageCallback, delta: int):
    user_id = event.user.user_id
    day_offset = _parse_day_offset(event.callback.payload)
    now = datetime.now(TIME_CONFIG.TIMEZONE)
    target_date = (now + timedelta(days=day_offset)).date()

    if not can_modify_order(target_date, CONFIG.are_orders_accepted_now()):
        await event.answer(
            notification=f"ℹ️ Изменение невозможно после {TIME_CONFIG.MODIFICATION_DEADLINE.strftime('%H:%M')}"
        )
        return

    with db.get_session() as session:
        user_db_id = _get_user_db_id(user_id, session)
        if not user_db_id:
            return

        new_qty, order, error = modify_quantity(user_db_id, target_date, delta, session)

        if error:
            await event.answer(notification=f"ℹ️ {error}")
            return

        if new_qty == 0:
            # Quantity went below 1 — cancel
            order.is_cancelled = True
            order.order_time = now.strftime("%H:%M:%S")
            session.commit()
            await event.answer(notification="✅ Заказ отменён")
            await _refresh_day_view(event, day_offset, user_db_id, session)
            return

        session.commit()
        await event.answer(notification=f"Установлено: {new_qty} порции")

        # Re-show quantity interface
        day_name = DAYS_RU[target_date.weekday()]
        menu = CONFIG.menu.get(day_name)
        if menu:
            text = format_menu_text(menu, day_name, target_date)
            text += f"\n\n🛒 Текущий заказ: {new_qty} порции"
            await event.message.answer(text, attachments=[quantity_buttons(day_offset)])


# --- Cancel ---

@router.message_callback(F.callback.payload.startswith("cancel_"))
async def on_cancel(event: MessageCallback):
    """Cancel an order."""
    user_id = event.user.user_id
    payload = event.callback.payload

    # Parse: cancel_2 or cancel_order_2025-06-23
    parts = payload.split("_")
    now = datetime.now(TIME_CONFIG.TIMEZONE)

    if len(parts) > 2 and parts[1] == "order":
        from datetime import datetime as dt
        date_str = "_".join(parts[2:])
        target_date = dt.strptime(date_str, "%Y-%m-%d").date()
        day_offset = (target_date - now.date()).days
    else:
        day_offset = int(parts[1])
        target_date = (now + timedelta(days=day_offset)).date()

    if not can_modify_order(target_date, CONFIG.are_orders_accepted_now()):
        await event.answer(
            notification=f"ℹ️ Отмена невозможна после {TIME_CONFIG.MODIFICATION_DEADLINE.strftime('%H:%M')}"
        )
        return

    with db.get_session() as session:
        user_db_id = _get_user_db_id(user_id, session)
        if not user_db_id:
            return

        order, error = cancel_order(user_db_id, target_date, session)
        if error:
            await event.answer(notification=f"ℹ️ {error}")
            return

        session.commit()

        # Trigger Bitrix cleanup
        try:
            from bitrix.sync import BitrixSync
            sync = BitrixSync()
            await sync.cancel_order_immediate_cleanup(order.id)
        except Exception as e:
            logger.warning(f"Bitrix cleanup failed: {e}")

        await event.answer(notification="✅ Заказ отменён")
        await _refresh_day_view(event, day_offset, user_db_id, session)


# --- Confirm ---

@router.message_callback(F.callback.payload.startswith("confirm_"))
async def on_confirm(event: MessageCallback):
    """Confirm quantity changes — just refresh the day view."""
    user_id = event.user.user_id
    day_offset = _parse_day_offset(event.callback.payload)

    with db.get_session() as session:
        user_db_id = _get_user_db_id(user_id, session)
        if not user_db_id:
            return
        await _refresh_day_view(event, day_offset, user_db_id, session)
    await event.answer(notification="✅ Заказ подтверждён")


# --- Noop ---

@router.message_callback(F.callback.payload == "noop")
async def on_noop(event: MessageCallback):
    await event.answer()
