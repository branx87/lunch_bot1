# ##handlers/__init__.py
from bitrix_checker import BitrixChecker
from datetime import datetime, timedelta
from time_config import TIME_CONFIG
from handlers.registration_handlers import get_full_name, get_location, get_phone, change_location
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

from handlers.common import show_main_menu
from admin import message_history
from database import db
from config import CONFIG, logger
from decorators import admin_filter, provider_or_admin_filter
from constants import (
    ADMIN_REPORTS_MENU, AWAIT_MESSAGE_TEXT, FULL_NAME, LOCATION, MAIN_MENU, 
    ORDER_ACTION, ORDER_CONFIRMATION, PHONE, 
    SELECT_MONTH_RANGE, SELECT_MONTH_RANGE_STATS, SELECT_REPORT_TYPE
)
from handlers.admin_config_handlers import handle_deletion, setup_admin_config_handlers
from handlers.admin_handlers import handle_admin_choice
from handlers.base_handlers import (
    admin_reports_menu, error_handler, handle_admin_reports_menu, 
    handle_registered_user, handle_report_type_selection, handle_text_message, 
    main_menu, start, test_connection
)
from handlers.callback_handlers import handle_cancel_order
from handlers.common import show_main_menu
from handlers.common_handlers import view_orders
from handlers.common_report_handlers import select_month_range
from handlers.menu_handlers import (
    handle_cancel_from_view, 
    handle_order_confirmation, 
    monthly_stats, 
    monthly_stats_selected,
    quick_order,
    show_today_menu,
    show_week_menu
)
from handlers.message_handlers import (
    handle_broadcast_command, 
    process_broadcast_message, 
    start_user_to_admin_message,
    setup_message_handlers
)
from handlers.order_callbacks import callback_handler, setup_order_callbacks
from handlers.provider_handlers import setup_provider_handlers
from handlers.registration_handlers import get_full_name, get_location, get_phone

async def manual_push_orders_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ручной отправки заказов"""
    query = update.callback_query
    user_id = query.from_user.id
    
    from config import logger  # 🔥 Импортируем здесь
    
    # Проверка прав
    if not CONFIG.master_admin_id or user_id != CONFIG.master_admin_id:
        await query.answer("❌ У вас нет прав для этой операции", show_alert=True)
        return
    
    await query.answer("🔄 Запускаю отправку заказов...")
    
    try:
        from bitrix.sync import BitrixSync
        from time_config import TIME_CONFIG  # 🔥 Импортируем здесь
        
        # Получаем информацию о неотправленных заказах
        async with BitrixSync() as sync:
            pending_info = await sync.get_pending_orders_info()
            
            if pending_info['count'] == 0:
                await query.edit_message_text(
                    "✅ Все заказы уже отправлены!\n\n"
                    f"Дата: {pending_info['date']}"
                )
                return
            
            # Запускаем отправку
            await query.edit_message_text(
                f"🔄 Отправляю {pending_info['count']} заказов в Bitrix...\n"
                "⏳ Это может занять некоторое время..."
            )
            
            success = await sync._push_to_bitrix()
            
            if success:
                result_msg = (
                    f"✅ УСПЕШНО!\n\n"
                    f"📤 Отправлено заказов: {pending_info['count']}\n"
                    f"📅 Дата: {pending_info['date']}\n"
                    f"⏰ Время: {datetime.now(TIME_CONFIG.TIMEZONE).strftime('%H:%M:%S')}"
                )
            else:
                # Получаем обновленную информацию
                new_pending = await sync.get_pending_orders_info()
                result_msg = (
                    f"⚠️ ЧАСТИЧНО ВЫПОЛНЕНО\n\n"
                    f"✅ Отправлено: {pending_info['count'] - new_pending['count']}\n"
                    f"❌ Не отправлено: {new_pending['count']}\n"
                    f"📅 Дата: {pending_info['date']}\n\n"
                    "Проверьте логи для деталей ошибок."
                )
            
            await query.edit_message_text(result_msg)
        
    except Exception as e:
        logger.error(f"Ошибка ручной отправки заказов: {e}", exc_info=True)
        await query.edit_message_text(
            f"❌ Ошибка при отправке заказов:\n\n{str(e)}\n\n"
            "Проверьте логи для подробностей."
        )

def setup_handlers(application):
    """Настройка всех обработчиков в правильном порядке"""
    
    # Временная функция прямо здесь
    async def check_system_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда для проверки системы на СЕГОДНЯ"""
        user_id = update.effective_user.id
        
        if user_id not in CONFIG.admin_ids:
            await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
            return
        
        from config import logger
        logger.info(f"Админ {user_id} запустил проверку системы на сегодня")
        
        await update.message.reply_text("🔍 Запускаю проверку системы на сегодня...")
        
        try:
            checker = BitrixChecker()
            success, info_issues, orders_count = await checker.run_all_checks()
            
            if success:
                message = "✅ СИСТЕМА ГОТОВА К ОТПРАВКЕ В 9:25!\n\n"
                
                if info_issues:
                    message += "💡 Технические замечания (не влияют на работу):\n"
                    for issue in info_issues:
                        message += f"• {issue}\n"
                    message += "\n"
                
                if orders_count > 0:
                    message += f"🎯 В 9:25 будет отправлено {orders_count} заказов в Bitrix"
                else:
                    message += "ℹ️ На сегодня нет заказов для отправки"
                    
                await update.message.reply_text(message)
            else:
                await update.message.reply_text(
                    "❌ ЕСТЬ КРИТИЧЕСКИЕ ПРОБЛЕМЫ!\n\n"
                    "Система НЕ ГОТОВА к отправке в 9:25.\n"
                    "Проверьте логи для деталей и срочно исправьте проблемы."
                )
                
        except Exception as e:
            logger.error(f"Ошибка при проверке системы: {e}", exc_info=True)
            await update.message.reply_text("❌ Произошла ошибка при проверке системы")

    async def check_safety_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда для проверки безопасности отправки заказов"""
        user_id = update.effective_user.id
        
        if user_id not in CONFIG.admin_ids:
            await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
            return
        
        from config import logger
        logger.info(f"Админ {user_id} запустил проверку безопасности")
        
        await update.message.reply_text("🔒 Запускаю проверку безопасности отправки...")
        
        try:
            checker = BitrixChecker()
            is_safe, orders, issues = await checker.check_send_orders_safety()
            
            if is_safe:
                message = (
                    f"✅ ОТПРАВКА БЕЗОПАСНА!\n\n"
                    f"📦 Заказов для отправки: {len(orders)}\n"
                    f"🔒 Риск дублей: НЕТ\n"
                    f"🌐 Подключение к Bitrix: РАБОТАЕТ\n\n"
                    f"Можно запускать отправку в 9:25!"
                )
            else:
                message = "🚨 ОПАСНОСТЬ ДУБЛЕЙ!\n\nПРОБЛЕМЫ:\n"
                for issue in issues:
                    message += f"• {issue}\n"
                message += "\n❌ НЕ отправлять заказы до исправления!"
                
            await update.message.reply_text(message)
                
        except Exception as e:
            logger.error(f"Ошибка при проверке безопасности: {e}", exc_info=True)
            await update.message.reply_text("❌ Произошла ошибка при проверке безопасности")

    async def check_config_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда для проверки текущей конфигурации"""
        user = update.effective_user
        
        # Принудительно перезагружаем конфиг
        CONFIG.reload()
        
        # Импортируем TIME_CONFIG
        from time_config import TIME_CONFIG
        
        message = (
            f"🔧 ТЕКУЩАЯ КОНФИГУРАЦИЯ:\n"
            f"👑 Админы: {CONFIG.admin_ids}\n"
            f"📦 Поставщики: {CONFIG.provider_ids}\n"
            f"💰 Бухгалтеры: {CONFIG.accounting_ids}\n"
            f"🔑 Ваш ID: {user.id}\n"
            f"✅ Вы админ: {user.id in CONFIG.admin_ids}\n"
            f"🔄 Заказы включены: {CONFIG.orders_enabled}\n\n"
            f"⏰ НАСТРОЙКИ ВРЕМЕНИ:\n"
            f"🕘 Прием заказов до: {TIME_CONFIG.ORDER_DEADLINE.strftime('%H:%M')}\n"
            f"✏️ Изменение до: {TIME_CONFIG.MODIFICATION_DEADLINE.strftime('%H:%M')}\n"
            f"🔄 Синхронизация с: {TIME_CONFIG.IMMEDIATE_SYNC_TIME.strftime('%H:%M')}\n"
            f"📅 Выходные дни: {[['Пн','Вт','Ср','Чт','Пт','Сб','Вс'][i] for i in TIME_CONFIG.WEEKEND_DAYS]}\n"
            f"🏢 Рабочие дни: {[['Пн','Вт','Ср','Чт','Пт','Сб','Вс'][i] for i in TIME_CONFIG.WORK_DAYS]}\n"
            f"⏰ Часовой пояс: {TIME_CONFIG.TIMEZONE}\n\n"
            f"🔔 CRON ЗАДАЧИ:\n"
            f"⏰ Напоминания: {TIME_CONFIG.MORNING_REMINDER_TIME.strftime('%H:%M')}\n"
            f"📊 Отчеты: {TIME_CONFIG.MORNING_REPORTS_TIME.strftime('%H:%M')}\n"
            f"💰 Бух. отчет: {TIME_CONFIG.ACCOUNTING_REPORT_TIME.strftime('%H:%M')}\n"
            f"🔄 Синхр. сотр.: {TIME_CONFIG.SYNC_EMPLOYEES_TIME.strftime('%H:%M')}"
        )
        
        await update.message.reply_text(message)
    
    # 1. Специальные команды (тестовые, служебные)
    application.add_handler(CommandHandler('test', test_connection))
    application.add_handler(CommandHandler('check_system', check_system_command))
    application.add_handler(CommandHandler('check_safety', check_safety_command))
    application.add_handler(CommandHandler('config_check', check_config_command))
    
    # 2. Обработчики рассылки
    broadcast_handler = ConversationHandler(
        entry_points=[MessageHandler(
            filters.Regex("^📢 Сделать рассылку$") &
            admin_filter,
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
    
    # 3. Обработчики конфигурации
    setup_admin_config_handlers(application)
    setup_provider_handlers(application)
    
    # 4. Обработчики заказов
    setup_order_callbacks(application)
    
    # 5. Обработчик отчетов админа
    admin_reports_conv = ConversationHandler(
        entry_points=[
            MessageHandler(
                filters.Regex("^📊 Отчеты$") & admin_filter,
                admin_reports_menu
            )
        ],
        states={
            ADMIN_REPORTS_MENU: [
                MessageHandler(
                    filters.Regex("^(📊 Отчет за день|📅 Отчет за месяц)$"),
                    handle_admin_reports_menu
                )
            ],
            SELECT_REPORT_TYPE: [
                MessageHandler(
                    filters.Regex("^(💰 Бухгалтерский|📦 Поставщика|👨‍💼 Админский)$"),
                    handle_report_type_selection
                )
            ],
            SELECT_MONTH_RANGE: [
                MessageHandler(
                    filters.Regex(r'^(Текущий месяц|Прошлый месяц)$'),
                    select_month_range
                ),
                MessageHandler(filters.Regex(r'^🏠 Главное меню$'), admin_reports_menu)
            ]
        },
        fallbacks=[
            CommandHandler('cancel', lambda u, c: show_main_menu(u, u.effective_user.id)),
            MessageHandler(filters.Regex(r'^(🏠 Главное меню|Отмена)$'), 
                lambda u, c: show_main_menu(u, u.effective_user.id))
        ],
        allow_reentry=True
    )
    application.add_handler(admin_reports_conv)
    
    # 6. Обработчик изменения локации (ДОБАВЛЯЕМ ЭТОТ НОВЫЙ HANDLER)
    change_location_conv = ConversationHandler(
        entry_points=[
            MessageHandler(
                filters.Regex("^📍 Изменить локацию$"),
                change_location
            )
        ],
        states={
            LOCATION: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_location)
            ]
        },
        fallbacks=[
            CommandHandler('cancel', lambda u, c: show_main_menu(u, u.effective_user.id)),
            MessageHandler(filters.Regex(r'^(❌ Отмена|Отмена|Вернуться в главное меню|🏠 Главное меню)$'), 
                lambda u, c: show_main_menu(u, u.effective_user.id))
        ],
        allow_reentry=True
    )
    application.add_handler(change_location_conv)
    
    # 7. Явные обработчики команд
    application.add_handler(MessageHandler(
        filters.Regex(r'^(🏠 Главное меню|Вернуться в главное меню)$'),
        lambda update, context: show_main_menu(update, update.effective_user.id)
    ))

    application.add_handler(MessageHandler(
        filters.Regex("^📜 История сообщений$") & admin_filter,
        message_history
    ))

    application.add_handler(MessageHandler(
        filters.Regex("^🔒 Вкл/Выкл заказы$") & admin_filter,
        handle_admin_choice
    ))

    # 8. Основные обработчики сообщений
    setup_message_handlers(application)
    
    # 9. Главный ConversationHandler (регистрация, меню, заказы)
    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler('start', start),
            MessageHandler(filters.CONTACT, get_phone),
            MessageHandler(filters.Regex("^Меню на сегодня$"), show_today_menu),
            MessageHandler(filters.Regex("^Меню на неделю$"), show_week_menu),
            MessageHandler(filters.Regex("^✅ Быстрый заказ$"), quick_order),
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
                ),
                MessageHandler(filters.Regex("^🏠 Главное меню$"), monthly_stats)
            ],
            SELECT_MONTH_RANGE: [
                MessageHandler(
                    filters.Regex(r'^(Текущий месяц|Прошлый месяц)$'),
                    select_month_range
                ),
                MessageHandler(filters.Regex(r'^🏠 Главное меню$'), admin_reports_menu),
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
    
    # 🔥 ОБРАБОТЧИК ДЛЯ CALLBACK РУЧНОЙ ОТПРАВКИ (до общего CallbackQueryHandler!)
    application.add_handler(
        CallbackQueryHandler(
            manual_push_orders_callback, 
            pattern="^manual_push_orders$"
        )
    )
    
    # 🔥 КОМАНДА РУЧНОЙ ОТПРАВКИ ЗАКАЗОВ
    from handlers.commands import manual_sync_command
    application.add_handler(CommandHandler('manual_sync', manual_sync_command))
    
    # 10. Обработчик для зарегистрированных пользователей (только отчеты)
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.Regex(
            r'^(💰 Бухгалтерский отчет|📦 Отчет поставщика|'
            r'📊 Отчет за день|📅 Отчет за месяц|Обновить меню)$'
        ),
        handle_registered_user
    ))

    # 11. Общий обработчик callback-запросов
    application.add_handler(CallbackQueryHandler(callback_handler))

    # 12. Обработчик всех остальных текстовых сообщений (должен быть самым последним)
    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            handle_text_message
        )
    )

    # 13. Обработчик ошибок
    application.add_error_handler(error_handler)