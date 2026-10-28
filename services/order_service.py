"""
Order-related business logic: create, cancel, modify, list.
Messenger-agnostic — used by both Telegram and Max bots.
"""
import logging
from datetime import datetime, date, timedelta
from models import Order, User
from time_config import TIME_CONFIG
from services.time_service import is_new_supplier_date

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


def find_employees(query, session, limit=10):
    """Поиск сотрудников по ФИО или Bitrix ID.

    Нужно администратору, чтобы оформить заказ за другого сотрудника
    (кто-то забыл заказать) или заказать обед инспектору.

    Сравнение по ФИО делается в Python: ILIKE в PostgreSQL зависит от локали
    БД, а lower() в SQLite не работает с кириллицей — на обеих СУБД
    результат должен быть одинаковым. Активных сотрудников несколько
    сотен, такой перебор занимает единицы миллисекунд.

    Возвращает список словарей: id, full_name, position, department,
    location, bitrix_id.
    """
    q = (query or "").strip()
    if not q:
        return []

    needle = q.lower()
    candidates = session.query(User).filter(
        User.is_employee == True,
        User.is_deleted == False,
    ).order_by(User.full_name).all()

    matched = []
    for row in candidates:
        name = (row.full_name or "").lower()
        if needle in name:
            matched.append(row)
            continue
        if q.isdigit() and row.bitrix_id and str(row.bitrix_id) == q:
            matched.append(row)

    return [
        {
            "id": r.id,
            "full_name": r.full_name,
            "position": r.position,
            "department": r.department,
            "location": r.location,
            "bitrix_id": r.bitrix_id,
        }
        for r in matched[:limit]
    ]


def create_order(user_db_id, target_date, session, is_preliminary=False,
                 quantity=1, is_for_inspector=False):
    """
    Create a new order. Does NOT commit.

    quantity — количество порций (1..MAX_PORTIONS).
    is_for_inspector — заказ для инспектора (расходы компании):
    в Bitrix24 такой заказ уходит на CRM ID инспектора.

    Returns (order, error_message). If error, order is None.
    """
    now = datetime.now(TIME_CONFIG.TIMEZONE)

    # Check weekend
    if target_date.weekday() in TIME_CONFIG.WEEKEND_DAYS:
        return None, "Заказы на выходные не принимаются"

    # Check quantity
    if quantity < 1 or quantity > TIME_CONFIG.MAX_PORTIONS:
        return None, f"Количество порций должно быть от 1 до {TIME_CONFIG.MAX_PORTIONS}"

    # Check existing order
    # flush нужен, чтобы увидеть заказ, добавленный в этой же сессии:
    # сессии создаются с autoflush=False, иначе дубль прошёл бы незамеченным.
    session.flush()
    existing = get_order_for_date(user_db_id, target_date, session)
    if existing:
        return None, f"У вас уже заказано {existing.quantity} порций"

    # У нового поставщика заказ на будущий день — обычный заказ:
    # окно приёма закрывается в 15:00 накануне, подтверждать его не нужно.
    if is_new_supplier_date(target_date):
        is_preliminary = False

    order = Order(
        user_id=user_db_id,
        target_date=target_date,
        order_time=now.strftime("%H:%M:%S"),
        quantity=quantity,
        bitrix_quantity_id=QUANTITY_MAP[quantity],
        is_active=True,
        is_preliminary=is_preliminary,
        is_for_inspector=is_for_inspector,
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
