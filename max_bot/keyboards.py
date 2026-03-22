"""Max messenger keyboard builders."""
from maxapi.types import (
    CallbackButton,
    RequestContactButton,
    MessageButton,
)
from maxapi.utils.inline_keyboard import InlineKeyboardBuilder

from config import CONFIG
from services.user_service import get_user_role, MESSENGER_MAX


def main_menu_keyboard(max_user_id=None):
    """Build main menu keyboard for Max."""
    builder = InlineKeyboardBuilder()

    builder.row(MessageButton(text="Меню на сегодня"), MessageButton(text="Меню на неделю"))
    builder.row(MessageButton(text="✅ Быстрый заказ"))
    builder.row(MessageButton(text="Просмотреть заказы"), MessageButton(text="Статистика за месяц"))
    builder.row(MessageButton(text="📍 Изменить локацию"))

    role = get_user_role(max_user_id, MESSENGER_MAX, CONFIG) if max_user_id else None

    if role in ('provider', 'admin'):
        builder.row(MessageButton(text="📊 Отчет за день"), MessageButton(text="📅 Отчет за месяц"))
    elif role == 'accountant':
        builder.row(MessageButton(text="📊 Отчет за день"), MessageButton(text="📅 Отчет за месяц"))

    return builder.as_markup()


def phone_request_keyboard():
    """Keyboard with phone request button."""
    builder = InlineKeyboardBuilder()
    builder.row(RequestContactButton(text="📱 Отправить номер телефона"))
    return builder.as_markup()


def location_keyboard():
    """Keyboard with location selection buttons."""
    builder = InlineKeyboardBuilder()
    for loc in CONFIG.locations:
        builder.row(CallbackButton(text=loc, payload=f"location_{loc}"))
    return builder.as_markup()


def order_buttons(day_offset, has_order=False, can_modify=True):
    """Build inline order buttons for a menu day."""
    builder = InlineKeyboardBuilder()

    if not can_modify:
        builder.row(CallbackButton(text="⏳ Прием заказов завершен", payload="noop"))
        return builder.as_markup()

    if has_order:
        builder.row(
            CallbackButton(text="✏️ Изменить количество", payload=f"change_{day_offset}"),
        )
        builder.row(
            CallbackButton(text="❌ Отменить заказ", payload=f"cancel_{day_offset}"),
        )
    else:
        builder.row(
            CallbackButton(text="✅ Заказать", payload=f"order_{day_offset}"),
        )
    return builder.as_markup()


def quantity_buttons(day_offset):
    """Build quantity modification buttons."""
    builder = InlineKeyboardBuilder()
    builder.row(
        CallbackButton(text="➖ Уменьшить", payload=f"dec_{day_offset}"),
        CallbackButton(text="➕ Увеличить", payload=f"inc_{day_offset}"),
    )
    builder.row(CallbackButton(text="✔️ Подтвердить", payload=f"confirm_{day_offset}"))
    builder.row(CallbackButton(text="❌ Отменить заказ", payload=f"cancel_{day_offset}"))
    return builder.as_markup()


def month_selection_keyboard():
    """Build month selection buttons for reports."""
    builder = InlineKeyboardBuilder()
    builder.row(
        CallbackButton(text="Текущий месяц", payload="month_current"),
        CallbackButton(text="Прошлый месяц", payload="month_previous"),
    )
    return builder.as_markup()


def report_type_keyboard():
    """Build report type selection buttons."""
    builder = InlineKeyboardBuilder()
    builder.row(CallbackButton(text="💰 Бухгалтерский", payload="report_accounting"))
    builder.row(CallbackButton(text="📦 Поставщика", payload="report_provider"))
    builder.row(CallbackButton(text="👨‍💼 Админский", payload="report_admin"))
    return builder.as_markup()


def retry_keyboard():
    """Keyboard for unverified users."""
    builder = InlineKeyboardBuilder()
    builder.row(CallbackButton(text="Попробовать снова", payload="retry_registration"))
    return builder.as_markup()
