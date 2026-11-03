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
    return [_day_info(offset, (now + timedelta(days=offset)).date(), config)
            for offset in range(7)]


def get_day_info(day_offset, config):
    """
    Данные одного дня — тот же формат, что элемент get_week_menus.
    """
    now = datetime.now(TIME_CONFIG.TIMEZONE)
    return _day_info(day_offset, (now + timedelta(days=day_offset)).date(), config)


def _day_info(day_offset, target_date, config) -> dict:
    day_name = DAYS_RU[target_date.weekday()]
    date_iso = target_date.strftime("%Y-%m-%d")

    is_weekend = target_date.weekday() in TIME_CONFIG.WEEKEND_DAYS
    holiday_name = config.holidays.get(date_iso)
    is_holiday = holiday_name is not None
    menu = config.menu.get(day_name) if not is_holiday and not is_weekend else None

    return {
        'day_offset': day_offset,
        'target_date': target_date,
        'day_name': day_name,
        'menu': menu,
        'is_holiday': is_holiday,
        'is_weekend': is_weekend,
        'holiday_name': holiday_name,
    }


def format_day_text(day_info) -> str:
    """
    Текст дня: праздник / выходной / меню.

    Единый формат для всех мессенджеров (Telegram, ВК, Б24, Max): праздник
    называется явно. Раньше в Б24 такой день просто выпадал из меню — было
    непонятно, почему на него нельзя заказать, и никакой информации о
    празднике не было.
    """
    day_name = day_info['day_name']
    date_str = day_info['target_date'].strftime('%d.%m')

    if day_info.get('is_holiday'):
        return f"🎉 {day_name} ({date_str}) — {day_info['holiday_name']}! Меню не предусмотрено."
    if day_info.get('is_weekend'):
        return f"⏳ {day_name} ({date_str}) — Выходной! Меню не предусмотрено."
    return format_menu_text(day_info.get('menu'), day_name, day_info['target_date'])
