"""
Menu-related business logic: retrieval and formatting.
Messenger-agnostic — used by both Telegram and Max bots.
"""
from datetime import datetime, timedelta
from time_config import TIME_CONFIG

DAYS_RU = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]


def get_menu_for_day(day_offset, config):
    """
    Get menu for a day at offset from today.
    Returns (menu_dict_or_None, day_name, target_date).
    """
    now = datetime.now(TIME_CONFIG.TIMEZONE)
    target_date = (now + timedelta(days=day_offset)).date()
    day_name = DAYS_RU[target_date.weekday()]

    # Holiday check
    if target_date.strftime("%Y-%m-%d") in config.holidays:
        return None, day_name, target_date

    return config.menu.get(day_name), day_name, target_date


def format_menu_text(menu, day_name, target_date):
    """
    Format menu as human-readable text with emojis.
    Returns str.
    """
    if not menu:
        return f"На {day_name} выходной! Меню не предусмотрено."

    date_str = target_date.strftime("%d.%m")
    return (
        f"🍽 Меню на {day_name} ({date_str}):\n"
        f"1. 🍲 Первое: {menu['first']}\n"
        f"2. 🍛 Основное блюдо: {menu['main']}\n"
        f"3. 🥗 Салат: {menu['salad']}"
    )


def get_week_menus(config):
    """
    Get menu data for 7 days starting from today.
    Returns list of dicts:
        {day_offset, target_date, day_name, menu, is_holiday, is_weekend, holiday_name}
    """
    now = datetime.now(TIME_CONFIG.TIMEZONE)
    result = []

    for day_offset in range(7):
        target_date = (now + timedelta(days=day_offset)).date()
        day_name = DAYS_RU[target_date.weekday()]
        date_iso = target_date.strftime("%Y-%m-%d")

        is_weekend = target_date.weekday() in TIME_CONFIG.WEEKEND_DAYS
        holiday_name = config.holidays.get(date_iso)
        is_holiday = holiday_name is not None
        menu = config.menu.get(day_name) if not is_holiday and not is_weekend else None

        result.append({
            'day_offset': day_offset,
            'target_date': target_date,
            'day_name': day_name,
            'menu': menu,
            'is_holiday': is_holiday,
            'is_weekend': is_weekend,
            'holiday_name': holiday_name,
        })

    return result
