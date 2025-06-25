# # ##keyboards.py
# from telegram import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardButton, InlineKeyboardMarkup
# from config import CONFIG
# from settings import SETTINGS_CONFIG
# LOCATIONS = SETTINGS_CONFIG["LOCATIONS"]


# def create_unverified_user_keyboard():
#     return ReplyKeyboardMarkup([
#         ["Попробовать снова"],
#         ["Написать администратору"]
#     ], resize_keyboard=True)

# def create_main_menu_keyboard(user_id=None):
#     """Улучшенная клавиатура с полным набором кнопок"""
#     menu = [
#         ["Меню на сегодня", "Меню на неделю"],
#         ["Просмотреть заказы", "Статистика за месяц"],
#         ["Написать администратору"]
#     ]

#     # Роли
#     is_admin = hasattr(CONFIG, 'admin_ids') and user_id in CONFIG.admin_ids
#     is_provider = hasattr(CONFIG, 'provider_ids') and user_id in CONFIG.provider_ids
#     is_accounting = hasattr(CONFIG, 'accounting_ids') and user_id in CONFIG.accounting_ids

#     # Кнопки отчетов для админов, поставщиков и бухгалтеров
#     if is_admin or is_provider or is_accounting:
#         reports_menu = []
#         if is_admin or is_accounting:
#             reports_menu.append("📊 Отчет за день")
#         reports_menu.append("📅 Отчет за месяц")
#         menu.insert(0, reports_menu)

#     # Админские функции
#     if is_admin:
#         admin_menu = [
#             "⚙️ Управление конфигурацией",
#             "📢 Сделать рассылку",
#             "✉️ Написать пользователю"
#         ]
#         menu.append(admin_menu)

#     # Функции поставщика
#     if is_provider:
#         menu.append(["✏️ Изменить меню"])

#     # Общие кнопки
#     menu.append(["Обновить меню", "🏠 Главное меню"])

#     return ReplyKeyboardMarkup(menu, resize_keyboard=True)

# def create_month_selection_keyboard():
#     return ReplyKeyboardMarkup([
#         ["Текущий месяц"],
#         ["Прошлый месяц"],
#         ["Вернуться в главное меню"]
#     ], resize_keyboard=True)

# def create_order_keyboard(has_order):
#     if has_order:
#         return [
#             [InlineKeyboardButton("✏️ Изменить количество", callback_data="change")],
#             [InlineKeyboardButton("❌ Отменить заказ", callback_data="cancel")]
#         ]
#     return [[InlineKeyboardButton("✅ Заказать", callback_data="order")]]

# def create_admin_keyboard():
#     """Основная клавиатура админа"""
#     return ReplyKeyboardMarkup([
#         ["✉️ Написать пользователю", "📢 Сделать рассылку"],
#         ["⚙️ Управление конфигурацией", "📜 История сообщений"],
#         ["🏠 Главное меню"]
#     ], resize_keyboard=True)

# def create_admin_config_keyboard():
#     return ReplyKeyboardMarkup([
#         ["➕ Добавить администратора", "➖ Удалить администратора"],
#         ["➕ Добавить поставщика", "➖ Удалить поставщика"],
#         ["➕ Добавить бухгалтера", "➖ Удалить бухгалтера"],
#         ["➕ Добавить сотрудника", "➖ Удалить сотрудника"],
#         ["➕ Добавить праздник", "➖ Удалить праздник"],
#         ["🏠 Главное меню"]
#     ], resize_keyboard=True)

# def create_provider_menu_keyboard():
#     return ReplyKeyboardMarkup([
#         ["✏️ Изменить меню"],
#         ["🏠 Главное меню"]
#     ], resize_keyboard=True)