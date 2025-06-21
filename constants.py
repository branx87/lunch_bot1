# ##constants.py

# Базовые состояния диалога
PHONE = "PHONE"
FULL_NAME = "FULL_NAME"
LOCATION = "LOCATION"
MAIN_MENU = "MAIN_MENU"
ORDER_ACTION = "ORDER_ACTION"
ORDER_CONFIRMATION = "ORDER_CONFIRMATION"
SELECT_MONTH_RANGE = "SELECT_MONTH_RANGE"
BROADCAST_MESSAGE = "BROADCAST_MESSAGE"
AWAIT_MESSAGE_TEXT = "AWAIT_MESSAGE_TEXT"
ADMIN_MESSAGE = "ADMIN_MESSAGE"
AWAIT_USER_SELECTION = "AWAIT_USER_SELECTION"
SELECT_MONTH_RANGE_STATS = "SELECT_MONTH_RANGE_STATS"
SELECT_MONTH_RANGE = "SELECT_MONTH_RANGE"

# Состояния для управления конфигурацией
CONFIG_MENU = "CONFIG_MENU"
ADD_ADMIN = "ADD_ADMIN"
ADD_PROVIDER = "ADD_PROVIDER"
ADD_ACCOUNTANT = "ADD_ACCOUNTANT"
ADD_STAFF = "ADD_STAFF"
ADD_HOLIDAY_DATE = "ADD_HOLIDAY_DATE"
ADD_HOLIDAY_NAME = "ADD_HOLIDAY_NAME"
DELETE_ADMIN = "DELETE_ADMIN"
DELETE_PROVIDER = "DELETE_PROVIDER"
DELETE_ACCOUNTANT = "DELETE_ACCOUNTANT"
DELETE_STAFF = "DELETE_STAFF"
SEARCH_STAFF = "SEARCH_STAFF"
DELETE_HOLIDAY = "DELETE_HOLIDAY"

# Состояния для редактирования меню
EDIT_MENU_DAY = "EDIT_MENU_DAY"
EDIT_MENU_FIRST = "EDIT_MENU_FIRST"
EDIT_MENU_MAIN = "EDIT_MENU_MAIN"
EDIT_MENU_SALAD = "EDIT_MENU_SALAD"

# Дополнительные состояния для работы с заказами
CONFIRM_ORDER = "CONFIRM_ORDER"
CANCEL_ORDER = "CANCEL_ORDER"
MODIFY_PORTION_COUNT = "MODIFY_PORTION_COUNT"
CHANGE_ORDER = "CHANGE_ORDER"
EDIT_MENU_ITEMS = "EDIT_MENU_ITEMS"
MENU_EDIT_FIRST = "MENU_EDIT_FIRST"
MENU_EDIT_DAY = "MENU_EDIT_DAY"

# Временные ограничения
from datetime import time
ORDER_MODIFICATION_DEADLINE = time(9, 30)  # Крайний срок изменения заказов (9:30 утра)

# Константы для callback-действий
ACTION_ORDER = "order"
ACTION_INC = "inc"
ACTION_DEC = "dec"
ACTION_CHANGE = "change"
ACTION_CANCEL = "cancel"
ACTION_CONFIRM = "confirm"
ACTION_BACK = "back"
ACTION_REFRESH = "refresh"
ACTION_NOOP = "noop"
PAGE_SIZE = 20  # Количество элементов на странице

# Команды главного меню
MAIN_MENU_COMMANDS = {
    "Меню на сегодня",
    "Меню на неделю",
    "Просмотреть заказы",
    "Статистика за месяц",
    "🏠 Главное меню"
}

# Команды админ-панели
ADMIN_MENU_COMMANDS = {
    "⚙️ Управление конфигурацией",
    "📢 Сделать рассылку",
    "📅 Отчет за месяц",
    "📊 Отчет за день",
    "➕ Добавить администратора",
    "➖ Удалить администратора",
    "➕ Добавить поставщика", 
    "➖ Удалить поставщика",
    "➕ Добавить бухгалтера",
    "➖ Удалить бухгалтера",
    "➕ Добавить сотрудника",
    "➖ Удалить сотрудника",
    "➕ Добавить праздник",
    "➖ Удалить праздник"
}

# Команды поставщика
PROVIDER_MENU_COMMANDS = {
    "📦 Отчет поставщика",
    "📅 График поставок"
}