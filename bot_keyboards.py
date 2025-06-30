# ##bot_keyboards.py нужно добавить админу все отчеты
from asyncio.log import logger
from typing import Optional
from telegram import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardButton, InlineKeyboardMarkup
from db import CONFIG
from db import db
from settings import SETTINGS_CONFIG

LOCATIONS = SETTINGS_CONFIG["LOCATIONS"]

def create_unverified_user_keyboard():
    return ReplyKeyboardMarkup([
        ["Попробовать снова"],
        ["Написать администратору"]
    ], resize_keyboard=True)

def create_main_menu_keyboard(user_id=None):
    """Главное меню с разными кнопками для разных ролей"""
    if not user_id:
        return ReplyKeyboardMarkup([
            ["Меню на сегодня", "Меню на неделю"],
            ["Просмотреть заказы", "Статистика за месяц"],
            ["Написать администратору"]
        ], resize_keyboard=True)

    role = get_user_role(user_id)

    if role == 'employee':
        return ReplyKeyboardMarkup([
            ["Меню на сегодня", "Меню на неделю"],
            ["Просмотреть заказы", "Статистика за месяц"],
            ["Написать администратору"]
        ], resize_keyboard=True)

    elif role == 'provider':
        return ReplyKeyboardMarkup([
            ["Меню на сегодня", "Меню на неделю"],
            ["✏️ Изменить меню", "Статистика за месяц"],
            ["📊 Отчет за день", "📅 Отчет за месяц"],
            ["Написать администратору"]
        ], resize_keyboard=True)

    elif role == 'accountant':
        return ReplyKeyboardMarkup([
            ["Меню на сегодня", "Меню на неделю"],
            ["📊 Отчет за день", "📅 Отчет за месяц"],
            ["Написать администратору"]
        ], resize_keyboard=True)

    elif role == 'admin':
        return create_admin_keyboard()

    return ReplyKeyboardMarkup([
        ["Меню на сегодня", "Меню на неделю"],
        ["Просмотреть заказы", "Статистика за месяц"],
        ["Написать администратору"],
        ["🏠 Главное меню"]
    ], resize_keyboard=True)

def create_month_selection_keyboard():
    """Обновленная клавиатура выбора месяца (кнопки рядом)"""
    return ReplyKeyboardMarkup([
        ["Текущий месяц", "Прошлый месяц"],
        ["🔙 Назад"]
    ], resize_keyboard=True)

def create_admin_reports_menu():
    """Меню выбора типа отчета для админа"""
    return ReplyKeyboardMarkup([
        ["📊 Отчет за день", "📅 Отчет за месяц"],
        ["🏠 Главное меню"]
    ], resize_keyboard=True)

def create_report_type_menu():
    """Меню выбора типа отчета (бухгалтерский/поставщика/админский)"""
    return ReplyKeyboardMarkup([
        ["💰 Бухгалтерский", "📦 Поставщика", "👨‍💼 Админский"],
        ["🔙 Назад"]
    ], resize_keyboard=True)

def create_order_keyboard(has_order):
    if has_order:
        return [
            [InlineKeyboardButton("✏️ Изменить количество", callback_data="change")],
            [InlineKeyboardButton("❌ Отменить заказ", callback_data="cancel")]
        ]
    return [[InlineKeyboardButton("✅ Заказать", callback_data="order")]]

def create_admin_keyboard():
    """Основная клавиатура админа"""
    return ReplyKeyboardMarkup([
        ["Меню на сегодня", "Меню на неделю"],
        ["Просмотреть заказы", "Статистика за месяц"],
        ["📊 Отчеты", "⚙️ Управление конфигурацией"],
        ["📢 Сделать рассылку", "✉️ Написать пользователю"],
        ["📜 История сообщений", "🔒 Вкл/Выкл заказы"],
        ["🏠 Главное меню"]
    ], resize_keyboard=True)

def create_admin_config_keyboard():
    return ReplyKeyboardMarkup([
        ["➕ Добавить администратора", "➖ Удалить администратора"],
        ["➕ Добавить поставщика", "➖ Удалить поставщика"],
        ["➕ Добавить бухгалтера", "➖ Удалить бухгалтера"],
        ["➕ Добавить сотрудника", "➖ Удалить сотрудника"],
        ["➕ Добавить праздник", "➖ Удалить праздник"],
        ["🏠 Главное меню"]
    ], resize_keyboard=True)

def create_provider_menu_keyboard():
    return ReplyKeyboardMarkup([
        ["✏️ Изменить меню"],
        ["🏠 Главное меню"]
    ], resize_keyboard=True)

# === Добавленная функция для универсальной кнопки "Отмена" ===
def get_cancel_button():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("❌ Отмена", callback_data="cancel_delete")
    ]])
    
def get_user_role(user_id: int) -> Optional[str]:
    """
    Определяет роль пользователя по его Telegram ID.
    Возвращает одно из: 'admin', 'provider', 'accountant', 'employee' или None.
    """
    try:
        # Проверяем кэшированные роли из конфига
        if user_id in CONFIG.admin_ids:
            logger.debug(f"User {user_id} identified as admin")
            return 'admin'
        if user_id in CONFIG.provider_ids:
            logger.debug(f"User {user_id} identified as provider")
            return 'provider'
        if user_id in CONFIG.accounting_ids:
            logger.debug(f"User {user_id} identified as accountant")
            return 'accountant'

        # Проверяем, является ли пользователь сотрудником
        db.cursor.execute(
            "SELECT id FROM users WHERE telegram_id = ? AND is_employee = TRUE AND is_deleted = FALSE",
            (user_id,)  # Важно: передаем как кортеж
        )
        result = db.cursor.fetchone()
        
        if result:
            logger.debug(f"User {user_id} identified as employee")
            return 'employee'
        
        logger.debug(f"User {user_id} has no recognized role")
        return None

    except Exception as e:
        logger.error(f"Error determining role for user {user_id}: {e}")
        return None