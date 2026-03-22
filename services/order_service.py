"""
Order-related business logic: create, cancel, modify, list.
Messenger-agnostic — used by both Telegram and Max bots.
"""
import logging
from datetime import datetime, date, timedelta
from models import Order
from time_config import TIME_CONFIG

logger = logging.getLogger(__name__)

# Bitrix quantity mappings
QUANTITY_MAP = {1: '821', 2: '822', 3: '823', 4: '824', 5: '825'}
BITRIX_QUANTITY_MAP = {'821': 1, '822': 2, '823': 3, '824': 4, '825': 5}


def get_order_for_date(user_db_id, target_date, session):
    """Get active (non-cancelled) order for a user on a specific date."""
    return session.query(Order).filter(
        Order.user_id == user_db_id,
        Order.target_date == target_date,
        Order.is_cancelled == False
    ).first()


def get_active_orders(user_db_id, from_date, session):
    """Get all active orders for a user from from_date onwards, ordered by date."""
    return session.query(Order).filter(
        Order.user_id == user_db_id,
        Order.is_cancelled == False,
        Order.target_date >= from_date
    ).order_by(Order.target_date).all()


def create_order(user_db_id, target_date, session, is_preliminary=False):
    """
    Create a new order. Does NOT commit.

    Returns (order, error_message). If error, order is None.
    """
    now = datetime.now(TIME_CONFIG.TIMEZONE)

    # Check weekend
    if target_date.weekday() in TIME_CONFIG.WEEKEND_DAYS:
        return None, "Заказы на выходные не принимаются"

    # Check existing order
    existing = get_order_for_date(user_db_id, target_date, session)
    if existing:
        return None, f"У вас уже заказано {existing.quantity} порций"

    quantity = 1
    order = Order(
        user_id=user_db_id,
        target_date=target_date,
        order_time=now.strftime("%H:%M:%S"),
        quantity=quantity,
        bitrix_quantity_id=QUANTITY_MAP[quantity],
        is_active=True,
        is_preliminary=is_preliminary,
        created_at=now.replace(tzinfo=None),
    )
    session.add(order)
    return order, None


def cancel_order(user_db_id, target_date, session):
    """
    Cancel an order. Does NOT commit.

    Returns (order, error_message). If error, order is None.
    """
    now = datetime.now(TIME_CONFIG.TIMEZONE)

    order = get_order_for_date(user_db_id, target_date, session)
    if not order:
        return None, "Заказ не найден"

    if order.is_from_bitrix == 1:
        return None, "Заказ создан в Битрикс, отмена невозможна"

    order.is_cancelled = True
    order.order_time = now.strftime("%H:%M:%S")
    return order, None


def modify_quantity(user_db_id, target_date, delta, session):
    """
    Change order quantity by delta (+1 or -1).
    Does NOT commit.

    Returns (new_quantity, order, error_message).
    If quantity would go below 1, returns (0, order, None) — caller should cancel.
    """
    order = get_order_for_date(user_db_id, target_date, session)
    if not order:
        return None, None, "Заказ не найден"

    if order.is_from_bitrix == 1:
        return None, None, "Заказ создан в Битрикс, изменение невозможно"

    new_qty = order.quantity + delta

    if new_qty < 1:
        return 0, order, None  # Signal to cancel

    if new_qty > TIME_CONFIG.MAX_PORTIONS:
        return None, None, f"Максимум {TIME_CONFIG.MAX_PORTIONS} порций"

    order.quantity = new_qty
    order.bitrix_quantity_id = QUANTITY_MAP.get(new_qty, '821')
    order.updated_at = datetime.now()
    return new_qty, order, None


def get_user_monthly_stats(user_db_id, start_date, end_date, session):
    """
    Get user's order statistics for a period.
    Returns dict with counts.
    """
    from sqlalchemy import func

    now = datetime.now(TIME_CONFIG.TIMEZONE).date()

    total = session.query(func.sum(Order.quantity)).filter(
        Order.user_id == user_db_id,
        Order.is_cancelled == False,
        Order.target_date >= start_date,
        Order.target_date <= end_date
    ).scalar() or 0

    completed = session.query(func.sum(Order.quantity)).filter(
        Order.user_id == user_db_id,
        Order.is_cancelled == False,
        Order.target_date >= start_date,
        Order.target_date <= end_date,
        Order.target_date < now
    ).scalar() or 0

    upcoming = session.query(func.sum(Order.quantity)).filter(
        Order.user_id == user_db_id,
        Order.is_cancelled == False,
        Order.target_date >= start_date,
        Order.target_date <= end_date,
        Order.target_date >= now
    ).scalar() or 0

    return {
        'total': total,
        'completed': completed,
        'upcoming': upcoming,
    }
