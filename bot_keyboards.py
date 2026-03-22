# ##bot_keyboards.py нужно добавить админу все отчеты
from asyncio.log import logger
from typing import Optional
from telegram import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardButton, InlineKeyboardMarkup
from database import db
from config import CONFIG
from settings import SETTINGS_CONFIG
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__) 

LOCATIONS = SETTINGS_CONFIG["LOCATIONS"]

def create_unverified_user_keyboard():
    return ReplyKeyboardMarkup([
        ["Попробовать снова"],
        ["Написать администратору"]
    ], resize_keyboard=True)

# Добавить в функцию create_main_menu_keyboard
def create_main_menu_keyboard(user_id=None):
    """Главное меню с разными кнопками для разных ролей"""
    role = get_user_role(user_id) if user_id else None
    
    # Базовые кнопки для всех
    base_buttons = [
        ["Меню на сегодня", "Меню на неделю", "✅ Быстрый заказ"],  # Добавлена кнопка быстрого заказа
        ["Просмотреть заказы", "Статистика за месяц"],
        ["📍 Изменить локацию", "Написать администратору"]
    ]
    
    # Дополнительные кнопки для разных ролей
    if role == 'provider':
        base_buttons.insert(1, ["✏️ Изменить меню", "📊 Отчет за день", "📅 Отчет за месяц"])
    elif role == 'accountant':
        base_buttons.insert(1, ["📊 Отчет за день", "📅 Отчет за месяц"])
    elif role == 'admin':
        # Для админа используем специальную клавиатуру
        return create_admin_keyboard()
    
    return ReplyKeyboardMarkup(base_buttons, resize_keyboard=True)

def create_month_selection_keyboard():
    """Обновленная клавиатура выбора месяца (кнопки рядом)"""
    return ReplyKeyboardMarkup([
        ["Текущий месяц", "Прошлый месяц"],
        ["🏠 Главное меню"]
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
        ["🏠 Главное меню"]
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
        ["Меню на сегодня", "Меню на неделю", "✅ Быстрый заказ"],
        ["Просмотреть заказы", "✏️ Изменить меню"],  # Добавлена кнопка изменения меню
        ["Статистика за месяц", "📊 Отчеты"],
        ["⚙️ Управление конфигурацией", "✉️ Написать пользователю", "📢 Сделать рассылку"],
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
    from services.user_service import get_user_role as _get_role, MESSENGER_TELEGRAM
    return _get_role(user_id, MESSENGER_TELEGRAM, CONFIG)