"""VK keyboard builders using vkbottle."""
import json
from vkbottle import Keyboard, KeyboardButtonColor, Text, Callback, EMPTY_KEYBOARD

from config import CONFIG
from services.user_service import get_user_role, MESSENGER_VK


def main_menu_keyboard(vk_user_id=None):
    """Build main menu keyboard for VK."""
    kb = Keyboard(one_time=False, inline=False)
    kb.add(Text("Меню на сегодня"), color=KeyboardButtonColor.PRIMARY)
    kb.add(Text("Меню на неделю"), color=KeyboardButtonColor.PRIMARY)
    kb.row()
    kb.add(Text("✅ Быстрый заказ"), color=KeyboardButtonColor.POSITIVE)
    kb.row()
    kb.add(Text("Просмотреть заказы"))
    kb.add(Text("Статистика за месяц"))
    kb.row()
    kb.add(Text("📍 Изменить локацию"))

    role = get_user_role(vk_user_id, MESSENGER_VK, CONFIG) if vk_user_id else None

    if role in ('provider', 'admin', 'accountant'):
        kb.row()
        kb.add(Text("📊 Отчет за день"))
        kb.add(Text("📅 Отчет за месяц"))

    return kb.get_json()


def order_buttons(day_offset, has_order=False, can_modify=True):
    """Build inline order buttons for a menu day."""
    kb = Keyboard(inline=True)

    if not can_modify:
        kb.add(Callback("⏳ Прием заказов завершен", payload={"cmd": "noop"}))
        return kb.get_json()

    if has_order:
        kb.add(Callback("✏️ Изменить количество", payload={"cmd": "change", "d": day_offset}))
        kb.row()
        kb.add(Callback("❌ Отменить заказ", payload={"cmd": "cancel", "d": day_offset}),
               color=KeyboardButtonColor.NEGATIVE)
    else:
        kb.add(Callback("✅ Заказать", payload={"cmd": "order", "d": day_offset}),
               color=KeyboardButtonColor.POSITIVE)
    return kb.get_json()


def quantity_buttons(day_offset):
    """Build quantity modification inline buttons."""
    kb = Keyboard(inline=True)
    kb.add(Callback("➖ Уменьшить", payload={"cmd": "dec", "d": day_offset}))
    kb.add(Callback("➕ Увеличить", payload={"cmd": "inc", "d": day_offset}))
    kb.row()
    kb.add(Callback("✔️ Подтвердить", payload={"cmd": "confirm", "d": day_offset}),
           color=KeyboardButtonColor.POSITIVE)
    kb.row()
    kb.add(Callback("❌ Отменить заказ", payload={"cmd": "cancel", "d": day_offset}),
           color=KeyboardButtonColor.NEGATIVE)
    return kb.get_json()


def location_keyboard():
    """Keyboard with location buttons."""
    kb = Keyboard(inline=True)
    for loc in CONFIG.locations:
        kb.add(Callback(loc, payload={"cmd": "location", "loc": loc}))
        kb.row()
    return kb.get_json()


def month_selection_keyboard():
    """Month selection inline keyboard."""
    kb = Keyboard(inline=True)
    kb.add(Callback("Текущий месяц", payload={"cmd": "month", "p": "current"}))
    kb.add(Callback("Прошлый месяц", payload={"cmd": "month", "p": "previous"}))
    return kb.get_json()


def report_type_keyboard():
    """Report type selection inline keyboard."""
    kb = Keyboard(inline=True)
    kb.add(Callback("💰 Бухгалтерский", payload={"cmd": "report", "t": "accounting"}))
    kb.row()
    kb.add(Callback("📦 Поставщика", payload={"cmd": "report", "t": "provider"}))
    kb.row()
    kb.add(Callback("👨‍💼 Админский", payload={"cmd": "report", "t": "admin"}))
    return kb.get_json()
