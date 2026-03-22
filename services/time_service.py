"""
Time-related business logic: order deadlines, workday calculations.
Messenger-agnostic — used by both Telegram and Max bots.
"""
from datetime import datetime, date, timedelta
from time_config import TIME_CONFIG


def get_next_workday(from_date=None):
    """Returns the next working day after from_date."""
    if from_date is None:
        from_date = datetime.now(TIME_CONFIG.TIMEZONE)

    days_to_add = 1
    if from_date.weekday() == 4:  # Friday -> Monday
        days_to_add = 3
    elif from_date.weekday() == 5:  # Saturday -> Monday
        days_to_add = 2

    return from_date + timedelta(days=days_to_add)


def is_weekend(target_date):
    """Check if target_date falls on a weekend."""
    if isinstance(target_date, datetime):
        target_date = target_date.date()
    return target_date.weekday() in TIME_CONFIG.WEEKEND_DAYS


def is_holiday(target_date, holidays):
    """Check if target_date is a holiday."""
    if isinstance(target_date, datetime):
        target_date = target_date.date()
    return target_date.strftime("%Y-%m-%d") in holidays


def can_modify_order(target_date, orders_enabled=True):
    """
    Check if an order for target_date can be created/modified/cancelled.
    Consolidates the two duplicate implementations from utils.py and order_callbacks.py.

    Args:
        target_date: date or str (YYYY-MM-DD)
        orders_enabled: whether orders are globally enabled (from CONFIG)

    Returns:
        bool
    """
    if not orders_enabled:
        return False

    now = datetime.now(TIME_CONFIG.TIMEZONE)

    # Parse string dates
    if isinstance(target_date, str):
        try:
            target_date = datetime.strptime(target_date, "%Y-%m-%d").date()
        except ValueError:
            return False

    if isinstance(target_date, datetime):
        target_date = target_date.date()

    # No orders on weekends
    if target_date.weekday() in TIME_CONFIG.WEEKEND_DAYS:
        return False

    # Future dates (pre-orders) — always allowed
    if target_date > now.date():
        return True

    # Today — only before MODIFICATION_DEADLINE
    if target_date == now.date():
        return now.time() < TIME_CONFIG.MODIFICATION_DEADLINE

    # Past dates — never
    return False
