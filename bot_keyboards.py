# ##bot_keyboards.py
from typing import Optional, List
from telegram import (
    ReplyKeyboardMarkup, 
    KeyboardButton, 
    InlineKeyboardButton, 
    InlineKeyboardMarkup
)
from config import CONFIG
import logging

logger = logging.getLogger(__name__)

def create_unverified_user_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура для неподтвержденных пользователей"""
    return ReplyKeyboardMarkup([
        ["Попробовать снова"],
        ["Написать администратору"]
    ], resize_keyboard=True)

def create_main_menu_keyboard(user_id: Optional[int] = None, db_connection = None) -> ReplyKeyboardMarkup:
    """
    Главное меню с разными кнопками для разных ролей
    Args:
        user_id: Telegram ID пользователя
        db_connection: Соединение с БД (опционально, только если нужно проверить роль)
    """
    menu = []

    if not user_id:
        # Базовое меню для незарегистрированных
        return ReplyKeyboardMarkup([
            ["Меню на сегодня", "Меню на неделю"],
            ["Просмотреть заказы", "Статистика за месяц"],
            ["Написать администратору"]
        ], resize_keyboard=True)

    role = get_user_role(user_id, db_connection) if db_connection else None

    if role == 'employee':
        menu.extend([
            ["Меню на сегодня", "Меню на неделю"],
            ["Просмотреть заказы", "Статистика за месяц"],
            ["Написать администратору"]
        ])

    elif role == 'provider':
        menu.extend([
            ["Меню на сегодня", "Меню на неделю"],
            ["✏️ Изменить меню", "Статистика за месяц"],
            ["📊 Отчет за день", "📅 Отчет за месяц"],
            ["Написать администратору"]
        ])

    elif role == 'accountant':
        menu.extend([
            ["Меню на сегодня", "Меню на неделю"],
            ["📊 Отчет за день", "📅 Отчет за месяц"],
            ["Написать администратору"]
        ])

    elif role == 'admin':
        reports_menu = []
        if hasattr(CONFIG, 'admin_ids') and user_id in CONFIG.admin_ids:
            reports_menu.append("📊 Отчет за день")
        reports_menu.append("📅 Отчет за месяц")
        menu.insert(0, reports_menu)

        admin_menu = [
            "⚙️ Управление конфигурацией",
            "📢 Сделать рассылку",
            "✉️ Написать пользователю"
        ]
        menu.append(admin_menu)

        menu.extend([
            ["Меню на сегодня", "Меню на неделю"],
            ["Просмотреть заказы", "Статистика за месяц"],
            ["Обновить меню", "🏠 Главное меню"]
        ])

    else:
        menu.extend([
            ["Меню на сегодня", "Меню на неделю"],
            ["Просмотреть заказы", "Статистика за месяц"],
            ["Написать администратору"],
            ["🏠 Главное меню"]
        ])

    return ReplyKeyboardMarkup(menu, resize_keyboard=True)

def create_month_selection_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура выбора месяца для отчетов"""
    return ReplyKeyboardMarkup([
        ["Текущий месяц"],
        ["Прошлый месяц"],
        ["Вернуться в главное меню"]
    ], resize_keyboard=True)

def create_order_keyboard(has_order: bool) -> List[List[InlineKeyboardButton]]:
    """Клавиатура действий с заказом"""
    if has_order:
        return [
            [InlineKeyboardButton("✏️ Изменить количество", callback_data="change")],
            [InlineKeyboardButton("❌ Отменить заказ", callback_data="cancel")]
        ]
    return [[InlineKeyboardButton("✅ Заказать", callback_data="order")]]

def create_admin_keyboard() -> ReplyKeyboardMarkup:
    """Основная клавиатура админа"""
    return ReplyKeyboardMarkup([
        ["✉️ Написать пользователю", "📢 Сделать рассылку"],
        ["⚙️ Управление конфигурацией", "📜 История сообщений"],
        ["🏠 Главное меню"]
    ], resize_keyboard=True)

def create_admin_config_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура управления конфигурацией"""
    return ReplyKeyboardMarkup([
        ["➕ Добавить администратора", "➖ Удалить администратора"],
        ["➕ Добавить поставщика", "➖ Удалить поставщика"],
        ["➕ Добавить бухгалтера", "➖ Удалить бухгалтера"],
        ["➕ Добавить сотрудника", "➖ Удалить сотрудника"],
        ["➕ Добавить праздник", "➖ Удалить праздник"],
        ["🏠 Главное меню"]
    ], resize_keyboard=True)

def create_provider_menu_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура поставщика"""
    return ReplyKeyboardMarkup([
        ["✏️ Изменить меню"],
        ["🏠 Главное меню"]
    ], resize_keyboard=True)

def get_cancel_button() -> InlineKeyboardMarkup:
    """Универсальная кнопка 'Отмена'"""
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("❌ Отмена", callback_data="cancel_delete")
    ]])

def get_user_role(user_id: int, db_connection) -> Optional[str]:
    """
    Определяет роль пользователя по его Telegram ID.
    Args:
        user_id: Telegram ID пользователя
        db_connection: Соединение с БД
    Returns:
        Одно из: 'admin', 'provider', 'accountant', 'employee' или None
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
        if db_connection:
            db_connection.cursor.execute(
                "SELECT id FROM users WHERE telegram_id = ? AND is_employee = TRUE AND is_deleted = FALSE",
                (user_id,)  # Важно: передаем как кортеж
            )
            result = db_connection.cursor.fetchone()
            
            if result:
                logger.debug(f"User {user_id} identified as employee")
                return 'employee'
        
        logger.debug(f"User {user_id} has no recognized role")
        return None

    except Exception as e:
        logger.error(f"Error determining role for user {user_id}: {e}")
        return None
    
def create_history_keyboard(current_page: int = 0, has_next: bool = True) -> InlineKeyboardMarkup:
    """
    Создает клавиатуру для навигации по истории сообщений
    Args:
        current_page: Текущая страница
        has_next: Есть ли следующая страница
    Returns:
        InlineKeyboardMarkup с кнопками навигации
    """
    buttons = []
    if current_page > 0:
        buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"history_prev_{current_page}"))
    if has_next:
        buttons.append(InlineKeyboardButton("Вперед ➡️", callback_data=f"history_next_{current_page}"))
    
    return InlineKeyboardMarkup([buttons])