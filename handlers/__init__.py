# ##handlers/__init__.py
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    filters,
    ContextTypes
)

from admin import message_history
from config import CONFIG
from constants import (
    AWAIT_MESSAGE_TEXT, FULL_NAME, LOCATION, MAIN_MENU, 
    ORDER_ACTION, ORDER_CONFIRMATION, PHONE, 
    SELECT_MONTH_RANGE, SELECT_MONTH_RANGE_STATS
)
from handlers.admin_config_handlers import handle_deletion, setup_admin_config_handlers
from handlers.admin_handlers import handle_admin_choice
from handlers.base_handlers import error_handler, handle_registered_user, handle_text_message, main_menu, start, test_connection
from handlers.callback_handlers import handle_cancel_order
from handlers.common import show_main_menu
from handlers.common_handlers import view_orders
from handlers.common_report_handlers import select_month_range
from handlers.menu_handlers import (
    handle_cancel_from_view, 
    handle_order_confirmation, 
    monthly_stats, 
    monthly_stats_selected,
    show_today_menu,
    show_week_menu
)
from handlers.message_handlers import handle_broadcast_command, process_broadcast_message, start_user_to_admin_message
from handlers.order_callbacks import callback_handler, setup_order_callbacks
from handlers.provider_handlers import setup_provider_handlers
from handlers.registration_handlers import get_full_name, get_location, get_phone

def setup_handlers(application):
    """Настройка всех обработчиков в правильном порядке"""
    
    # 1. Специальные команды (тестовые, служебные)
    async def test(update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("✅ Бот работает! Получена команда /test")
    application.add_handler(CommandHandler('test', test_connection))
    
    # 2. Обработчики рассылки (должны быть в начале)
    broadcast_handler = ConversationHandler(
        entry_points=[MessageHandler(
            filters.Regex("^📢 Сделать рассылку$") & 
            filters.User(user_id=CONFIG.admin_ids),
            handle_broadcast_command
        )],
        states={
            AWAIT_MESSAGE_TEXT: [MessageHandler(
                filters.TEXT & ~filters.COMMAND,
                process_broadcast_message
            )]
        },
        fallbacks=[
            CommandHandler('cancel', lambda u, c: show_main_menu(u, u.effective_user.id)),
            MessageHandler(filters.Regex("^(❌ Отмена|Отмена)$"),
            lambda u, c: show_main_menu(u, u.effective_user.id))
        ],
        allow_reentry=True
    )
    application.add_handler(broadcast_handler)
    
    # 3. Обработчики конфигурации админа и провайдеров
    setup_admin_config_handlers(application)
    setup_provider_handlers(application)
    
    # 4. Обработчики заказов
    setup_order_callbacks(application)
    
    # 5. Явные обработчики команд (главное меню, история сообщений)
    application.add_handler(MessageHandler(
        filters.Regex(r'^(🏠 Главное меню|Вернуться в главное меню)$'),
        lambda update, context: show_main_menu(update, update.effective_user.id)
    ))
    
    application.add_handler(MessageHandler(
        filters.Regex("^📜 История сообщений$") & filters.User(user_id=CONFIG.admin_ids),
        message_history
    ))

    # 6. Основные обработчики сообщений
    from handlers.message_handlers import setup_message_handlers
    setup_message_handlers(application)
    
    # 7. Главный ConversationHandler (регистрация, меню, заказы)
    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler('start', start),
            MessageHandler(filters.CONTACT, get_phone),
            MessageHandler(filters.Regex("^Меню на сегодня$"), show_today_menu),
            MessageHandler(filters.Regex("^Меню на неделю$"), show_week_menu),
            MessageHandler(filters.Regex("^Просмотреть заказы$"), view_orders),
            MessageHandler(filters.Regex("^Статистика за месяц$"), monthly_stats),
            MessageHandler(filters.Regex("^Админ-панель$"), handle_admin_choice),
            MessageHandler(filters.Regex("^Написать администратору$"), start_user_to_admin_message),
        ],
        states={
            SELECT_MONTH_RANGE_STATS: [
                MessageHandler(
                    filters.Regex("^(Текущий месяц|Прошлый месяц|Вернуться в главное меню)$"),
                    monthly_stats_selected
                )
            ],
            SELECT_MONTH_RANGE: [
                MessageHandler(
                    filters.Regex(r'^(Текущий месяц|Прошлый месяц)$'),
                    select_month_range
                ),
                MessageHandler(filters.Regex(r'^Вернуться в главное меню$'), show_main_menu)
            ],
            PHONE: [
                MessageHandler(filters.CONTACT, get_phone),
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_phone)
            ],
            FULL_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_full_name)],
            LOCATION: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_location)],
            MAIN_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, main_menu)],
            ORDER_ACTION: [CallbackQueryHandler(callback_handler)],
            ORDER_CONFIRMATION: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_order_confirmation)]
        },
        fallbacks=[
            CommandHandler('cancel', lambda u, c: show_main_menu(u, u.effective_user.id)),
            MessageHandler(filters.Regex(r'^(❌ Отмена|Отмена|Вернуться в главное меню|🏠 Главное меню)$'), 
            lambda u, c: show_main_menu(u, u.effective_user.id))
        ],
        per_chat=True,
        per_user=True,
        allow_reentry=True
    )
    application.add_handler(conv_handler)
    
    # 8. Обработчик для зарегистрированных пользователей (только отчеты)
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.Regex(
            r'^(💰 Бухгалтерский отчет|📦 Отчет поставщика|'
            r'📊 Отчет за день|📅 Отчет за месяц|Обновить меню)$'
        ),
        handle_registered_user
    ))

    # 9. Общий обработчик callback-запросов (должен быть ПОСЛЕДНИМ)
    application.add_handler(CallbackQueryHandler(callback_handler))

    # 10. Обработчик всех текстовых сообщений (должен быть самым последним)
    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            handle_text_message
        )
    )

    # 11. Обработчик ошибок
    application.add_error_handler(error_handler)