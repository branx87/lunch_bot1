# ##handlers/base_handlers.py
from email import message
import logging
import asyncio
from telegram import Update, ReplyKeyboardRemove, KeyboardButton, ReplyKeyboardMarkup
from telegram.ext import ConversationHandler
from telegram.ext import ContextTypes
from telegram.error import NetworkError, TimedOut
from datetime import datetime, timedelta

from bot_keyboards import create_admin_reports_menu, create_main_menu_keyboard, create_report_type_menu, create_month_selection_keyboard
from database import db
from models import User
from config import CONFIG
from constants import FULL_NAME, PHONE, SELECT_MONTH_RANGE
from handlers.common import show_main_menu
from handlers.common_handlers import view_orders
from handlers.common_report_handlers import select_month_range
from handlers.menu_handlers import monthly_stats, show_today_menu, show_week_menu
from report_generators import export_accounting_report, export_daily_admin_report, export_daily_orders_for_provider, export_orders_for_provider
from utils import check_registration, handle_unregistered
from bot_keyboards import get_user_role

logger = logging.getLogger(__name__)

__all__ = ['start', 'error_handler', 'test_connection', 'main_menu', 'handle_text_message']

ADMIN_REPORTS_MENU = "ADMIN_REPORTS_MENU"
SELECT_REPORT_TYPE = "SELECT_REPORT_TYPE"

def get_user_role(user_id):
    """Определяет роль пользователя на основе ID"""
    try:
        user_id = str(user_id)
        roles = []
        
        if user_id in [str(id) for id in CONFIG.admin_ids]:
            roles.append("Администратор")
        if user_id in [str(id) for id in CONFIG.provider_ids]:
            roles.append("Поставщик")
        if user_id in [str(id) for id in CONFIG.accounting_ids]:
            roles.append("Бухгалтер")
        
        return ", ".join(roles) if roles else "Пользователь"
    except Exception as e:
        logger.error(f"Ошибка определения роли для пользователя {user_id}: {e}")
        return "Пользователь"

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обработчик команды /start. Проверяет регистрацию пользователя:
    - Для новых пользователей начинает регистрацию с кнопки отправки номера
    - Для незавершивших регистрацию предлагает продолжить
    - Для зарегистрированных показывает главное меню
    """
    await update.message.reply_text("Обновляю меню...", reply_markup=ReplyKeyboardRemove())
    user = update.effective_user

    try:
        # Проверяем, является ли пользователь админом, поставщиком или бухгалтером
        if user.id in CONFIG.admin_ids or \
           user.id in CONFIG.provider_ids or \
           user.id in CONFIG.accounting_ids:
            
            # Пропускаем регистрацию и сразу показываем главное меню
            return await show_main_menu(update, user.id)
        
        # Проверяем регистрацию пользователя через SQLAlchemy
        user_data = db.session.query(User).filter(
            User.telegram_id == user.id
        ).first()

        if user_data:
            if user_data.is_deleted:
                await update.message.reply_text(
                    "❌ Ваш аккаунт деактивирован. Обратитесь к администратору.",
                    reply_markup=ReplyKeyboardRemove()
                )
                return ConversationHandler.END

            if user_data.is_verified:
                await show_main_menu(update, user.id)
                return ConversationHandler.END

            # ДОБАВЛЯЕМ СИНХРОНИЗАЦИЮ ДЛЯ НЕВЕРИФИЦИРОВАННЫХ ОБЫЧНЫХ ПОЛЬЗОВАТЕЛЕЙ
            from bitrix.sync import BitrixSync
            bitrix_sync = BitrixSync()
            await bitrix_sync.sync_employees()
            logger.info(f"Синхронизация сотрудников выполнена для неверифицированного пользователя {user.id}")

        else:
            # ДОБАВЛЯЕМ СИНХРОНИЗАЦИЮ ДЛЯ НОВЫХ ПОЛЬЗОВАТЕЛЕЙ (КОТОРЫХ НЕТ В БАЗЕ)
            from bitrix.sync import BitrixSync
            bitrix_sync = BitrixSync()
            await bitrix_sync.sync_employees()
            logger.info(f"Синхронизация сотрудников выполнена для нового пользователя {user.id}")

        # Показываем кнопку для отправки номера телефона
        keyboard = [[KeyboardButton("📱 Отправить номер телефона", request_contact=True)]]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)
        await update.message.reply_text(
            "Для регистрации отправьте свой номер телефона, используя кнопку ниже:",
            reply_markup=reply_markup
        )
        return PHONE

    except Exception as e:
        logger.error(f"Ошибка в start: {e}")
        await update.message.reply_text("Произошла ошибка. Попробуйте снова.")
        return await show_main_menu(update, user.id)

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Глобальный обработчик ошибок бота. Логирует ошибку и:
    - Отправляет уведомление администраторам
    - Информирует пользователя о проблеме
    Обрабатывает как ошибки в обработчиках, так и системные ошибки
    """
    # Временные сетевые ошибки — не спамим админам, бот сам восстановится
    if isinstance(context.error, (NetworkError, TimedOut)):
        logger.warning(f"Временная сетевая ошибка: {context.error.__class__.__name__}")
        return

    error = str(context.error)
    logger.error(f"Ошибка: {error}", exc_info=context.error)

    for admin_id in CONFIG.admin_ids:
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=f"⚠️ Ошибка в боте:\n\n{error}\n\n"
                     f"Update: {update if update else 'Нет данных'}"
            )
        except Exception as e:
            logger.error(f"Не удалось отправить сообщение админу {admin_id}: {e}")
    
    if update and isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text("⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")
        except Exception as e:
            logger.error(f"Не удалось отправить сообщение пользователю: {e}")

async def test_connection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        # Удаляем саму команду /test (если есть права)
        try:
            await update.message.delete()
        except Exception as delete_error:
            logger.warning(f"Не удалось удалить команду: {delete_error}")
            
        user = update.effective_user
        bot_info = await context.bot.get_me()
        
        # Получаем данные пользователя из БД через SQLAlchemy
        user_data = db.session.query(User).filter(
            User.telegram_id == user.id
        ).first()
        
        if user_data:
            user_status = "❌ Удален/заблокирован" if user_data.is_deleted else (
                "🟡 Ожидает верификации" if not user_data.is_verified else "✅ Активен"
            )
        else:
            user_status = "❌ Не зарегистрирован"
        
        response = (
            "✅ Система работает\n\n"
            f"👤 Ваш профиль:\n"
            f"Имя: {user_data.full_name if user_data else 'не указано'}\n"
            f"Телефон: {user_data.phone if user_data and user_data.phone else 'не указан'}\n"
            f"ID: {user.id}\n"
            f"Роль: {get_user_role(user.id)}\n"
            f"Логин: @{user.username if user.username else 'не установлен'}\n"
            f"Локация: {user_data.location if user_data and user_data.location else 'не указана'}\n"
            f"Статус: {user_status}\n\n"
            f"🤖 Бот:\n"
            f"ID: {bot_info.id}\n"
            f"Имя: @{bot_info.username}\n"
            f"Версия: 3.0.0\n"
            f"Статус: активен"
        )
        
        msg = await update.message.reply_text(response)
        await asyncio.sleep(10)
        await msg.delete()
        
    except Exception as e:
        logger.error(f"Ошибка соединения: {e}")
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")

async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Основной обработчик текстовых сообщений. Выполняет:
    - Обработку сообщений от незарегистрированных пользователей
    - Проверку регистрации пользователя
    - Перенаправление команд в соответствующие обработчики
    - Обработку запросов отчетов по месяцам
    """
    user = update.effective_user
    text = update.message.text
    logger.info(f"Получено сообщение: '{text}' от {user.id}")

    try:
        # Обработка сообщения от незарегистрированного пользователя
        if text == "Написать администратору":
            # Проверяем, есть ли пользователь в списке сотрудников через SQLAlchemy
            employee_data = db.session.query(User).filter(
                User.full_name.ilike(f"%{user.full_name}%"),
                User.is_employee == True,
                User.is_deleted == False
            ).first()

            if not employee_data:
                await update.message.reply_text(
                    "❌ Вы не найдены в списке сотрудников",
                    reply_markup=ReplyKeyboardRemove()
                )
                return ConversationHandler.END

            # Формируем правильное сообщение для администратора
            admin_message = (
                f"⚠️ Незарегистрированный пользователь пытается использовать бота:\n"
                f"🆔 ID: {user.id}\n"
                f"👤 Username: @{user.username if user.username else 'нет'}\n"
                f"📝 Имя: {user.full_name}"
            )

            for admin_id in CONFIG.admin_ids:
                try:
                    await context.bot.send_message(chat_id=admin_id, text=admin_message)
                except Exception as e:
                    logger.error(f"Не удалось отправить сообщение админу {admin_id}: {e}")

            await update.message.reply_text(
                "✅ Ваше сообщение отправлено администратору. Ожидайте ответа.",
                reply_markup=ReplyKeyboardMarkup([["Попробовать снова"]], resize_keyboard=True)
            )
            return FULL_NAME

        # Проверяем регистрацию только если это не админ/поставщик/бухгалтер
        if user.id not in CONFIG.admin_ids and \
           user.id not in CONFIG.provider_ids and \
           user.id not in CONFIG.accounting_ids:
            if not await check_employee_registration(update, context):
                return await handle_unregistered(update, context)

        if text in ["Текущий месяц", "Прошлый месяц"] and context.user_data.get('report_type'):
            return await select_month_range(update, context)

        # Все остальные команды передаем в main_menu
        return await main_menu(update, context)

    except Exception as e:
        logger.error(f"Ошибка в handle_text_message: {e}", exc_info=True)
        await update.message.reply_text(
            "⚠️ Произошла ошибка. Попробуйте снова или используйте /start",
            reply_markup=ReplyKeyboardRemove()
        )
        return await show_main_menu(update, user.id)

async def main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Главный обработчик команд основного меню. Обеспечивает:
    - Навигацию по разделам меню (дневное/недельное меню, заказы)
    - Формирование отчетов (дневных/месячных) с проверкой прав доступа
    - Обработку команды обновления меню
    - Перенаправление неизвестных команд
    """
    logger.info(f"Получена команда: '{update.message.text}' от пользователя {update.effective_user.id}")
    
    try:
        user = update.effective_user
        text = update.message.text
        
        # Основные команды меню
        if text == "Меню на сегодня":
            return await show_today_menu(update, context)
        
        elif text == "Меню на неделю":
            return await show_week_menu(update, context)
        
        elif text == "Просмотреть заказы":
            return await view_orders(update, context)
        
        elif text == "Статистика за месяц":
            return await monthly_stats(update, context)
        
        elif text == "✏️ Изменить меню":
            # Проверяем, является ли пользователь поставщиком или админом
            if user.id in CONFIG.provider_ids or user.id in CONFIG.admin_ids:
                # Перенаправляем в обработчик поставщика
                from handlers.provider_handlers import edit_menu
                return await edit_menu(update, context)
            else:
                await update.message.reply_text("❌ У вас нет прав для изменения меню")
                return await show_main_menu(update, user.id)
        
        elif text == "📅 Отчет за месяц":
            # Устанавливаем тип отчета в зависимости от прав пользователя
            if user.id in getattr(CONFIG, 'admin_ids', []):
                context.user_data['report_type'] = 'admin_monthly'
            elif user.id in getattr(CONFIG, 'provider_ids', []):
                context.user_data['report_type'] = 'provider_monthly'
            elif user.id in getattr(CONFIG, 'accounting_ids', []):
                context.user_data['report_type'] = 'accounting_monthly'
            else:
                await update.message.reply_text("❌ У вас нет прав для просмотра отчетов")
                return await show_main_menu(update, user.id)

            # Запрашиваем период
            await update.message.reply_text(
                "Выберите период:",
                reply_markup=ReplyKeyboardMarkup([
                    ["Текущий месяц"],
                    ["Прошлый месяц"],
                    ["Вернуться в главное меню"]
                ], resize_keyboard=True)
            )
            return SELECT_MONTH_RANGE

        elif text == "📊 Отчет за день":
            today = datetime.now(CONFIG.timezone).date()
            if user.id in getattr(CONFIG, 'admin_ids', []):
                context.user_data['report_type'] = 'admin_daily'
                await export_daily_admin_report(update, context, today)
            elif user.id in getattr(CONFIG, 'provider_ids', []):
                context.user_data['report_type'] = 'provider_daily'
                await export_daily_orders_for_provider(update, context, today)
            elif user.id in getattr(CONFIG, 'accounting_ids', []):
                context.user_data['report_type'] = 'accounting_daily'
                await export_accounting_report(update, context, today, today)
            else:
                await update.message.reply_text("❌ Нет прав доступа")
            return await show_main_menu(update, user.id)
        
        elif text == "Вернуться в главное меню":
            return await show_main_menu(update, user.id)
        
        elif text == "Обновить меню":
            await update.message.reply_text("Обновляю меню...", reply_markup=ReplyKeyboardRemove())
            return await show_main_menu(update, user.id)

        # Обработка неизвестной команды
        else:
            await update.message.reply_text(
                "Неизвестная команда. Попробуйте обновить меню или используйте /start",
                reply_markup=ReplyKeyboardRemove()
            )
            return await show_main_menu(update, user.id)

    except Exception as e:
        logger.error(f"Ошибка в main_menu: {e}", exc_info=True)
        await update.message.reply_text(
            "⚠️ Произошла ошибка. Попробуйте снова.",
            reply_markup=create_main_menu_keyboard(user.id) if user else ReplyKeyboardRemove()
        )
        return ConversationHandler.END
    
async def handle_registered_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Специализированный обработчик для зарегистрированных пользователей.
    Проверяет права доступа и предоставляет функционал:
    - Формирование бухгалтерских отчетов
    - Генерацию отчетов для поставщиков
    - Перенаправление остальных команд в main_menu
    """
    try:
        user = update.effective_user
        text = update.message.text

        # Проверяем, является ли пользователь админом, поставщиком или бухгалтером
        role = get_user_role(user.id)

        if not role:
            # Если роль не определена — пользователь должен быть сотрудником
            # Иначе — показываем ошибку
            await update.message.reply_text(
                "❌ У вас нет доступа к этому функционалу",
                reply_markup=ReplyKeyboardRemove()
            )
            return ConversationHandler.END

        # Обработка отчётов
        if text == "💰 Бухгалтерский отчет":
            if role in ['accountant', 'admin']:
                context.user_data['report_type'] = 'accounting'
                await update.message.reply_text(
                    "Выберите период:",
                    reply_markup=ReplyKeyboardMarkup([
                        ["Текущий месяц", "Прошлый месяц"],
                        ["Вернуться в главное меню"]
                    ], resize_keyboard=True)
                )
                return SELECT_MONTH_RANGE

        elif text == "📦 Отчет поставщика":
            if role in ['provider', 'admin']:
                context.user_data['report_type'] = 'provider'
                await update.message.reply_text(
                    "Выберите период:",
                    reply_markup=ReplyKeyboardMarkup([
                        ["Текущий месяц", "Прошлый месяц"],
                        ["Вернуться в главное меню"]
                    ], resize_keyboard=True)
                )
                return SELECT_MONTH_RANGE

        elif text in ["📊 Отчет за день", "📅 Отчет за месяц"]:
            if role in ['provider', 'accountant', 'admin']:
                # Устанавливаем report_type в зависимости от роли и типа отчета
                if text == "📊 Отчет за день":
                    if role == 'provider':
                        context.user_data['report_type'] = 'provider_daily'
                    elif role == 'accountant':
                        context.user_data['report_type'] = 'accounting_daily'
                    elif role == 'admin':
                        context.user_data['report_type'] = 'admin_daily'
                else:  # "📅 Отчет за месяц"
                    if role == 'provider':
                        context.user_data['report_type'] = 'provider_monthly'
                    elif role == 'accountant':
                        context.user_data['report_type'] = 'accounting_monthly'
                    elif role == 'admin':
                        context.user_data['report_type'] = 'admin_monthly'
                
                # Для дневных отчетов сразу генерируем отчет
                if text == "📊 Отчет за день":
                    today = datetime.now(CONFIG.timezone).date()
                    if role == 'admin':
                        await export_daily_admin_report(update, context, today)
                    elif role == 'provider':
                        await export_daily_orders_for_provider(update, context, today)
                    elif role == 'accountant':
                        await export_accounting_report(update, context, today, today)
                    return await show_main_menu(update, user.id)
                # Для месячных отчетов запрашиваем период
                else:
                    await update.message.reply_text(
                        "Выберите период:",
                        reply_markup=ReplyKeyboardMarkup([
                            ["Текущий месяц"],
                            ["Прошлый месяц"],
                            ["Вернуться в главное меню"]
                        ], resize_keyboard=True)
                    )
                    return SELECT_MONTH_RANGE
            else:
                await update.message.reply_text("❌ У вас нет прав на этот отчёт")
                return

    except Exception as e:
        logger.error(f"Ошибка в handle_registered_user: {e}", exc_info=True)
        await update.message.reply_text(
            "⚠️ Произошла ошибка. Попробуйте снова.",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

async def check_employee_registration(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Проверяет, является ли пользователь сотрудником или имеет роль provider/accountant/admin"""
    user = update.effective_user

    # Админы, поставщики и бухгалтеры из .env получают доступ без регистрации
    if user.id in CONFIG.admin_ids or user.id in CONFIG.provider_ids or user.id in CONFIG.accounting_ids:
        return True

    try:
        # Используем SQLAlchemy для проверки регистрации
        user_data = db.session.query(User).filter(
            User.telegram_id == user.id,
            User.is_deleted == False
        ).first()
        
        if not user_data:
            return False

        return bool(user_data.is_employee and user_data.is_verified)

    except Exception as e:
        logger.error(f"Ошибка при проверке регистрации: {e}")
        return False
    
async def admin_reports_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик меню отчетов админа с фоновой синхронизацией"""
    user = update.effective_user
    if user.id not in CONFIG.admin_ids:
        await update.message.reply_text("❌ У вас нет прав доступа")
        return await show_main_menu(update, user.id)
    
    # 🔥 ФОНОВАЯ СИНХРОНИЗАЦИЯ В ОТДЕЛЬНОЙ ЗАДАЧЕ
    async def background_sync():
        try:
            from bitrix.sync import BitrixSync
            from datetime import datetime, timedelta
            
            sync = BitrixSync()
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
            
            await sync.sync_orders(start_date, end_date, incremental=True)
            logger.info(f"✅ Фоновая синхронизация выполнена для пользователя {user.id}")
            
        except Exception as e:
            logger.error(f"Ошибка фоновой синхронизации: {e}")
    
    # Запускаем в фоне без ожидания
    import asyncio
    asyncio.create_task(background_sync())
    
    # Сразу показываем меню
    await update.message.reply_text(
        "📊 Выберите период для отчета:",
        reply_markup=create_admin_reports_menu()
    )
    return ADMIN_REPORTS_MENU

async def handle_admin_reports_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик выбора в меню отчетов админа"""
    user = update.effective_user
    text = update.message.text
    
    if text == "📊 Отчет за день":
        context.user_data['report_period'] = 'daily'
        await update.message.reply_text(
            "📋 Выберите тип отчета:",
            reply_markup=create_report_type_menu()
        )
        return SELECT_REPORT_TYPE
        
    elif text == "📅 Отчет за месяц":
        context.user_data['report_period'] = 'monthly'
        await update.message.reply_text(
            "📋 Выберите тип отчета:",
            reply_markup=create_report_type_menu()
        )
        return SELECT_REPORT_TYPE
        
    elif text == "🏠 Главное меню":
        return await show_main_menu(update, user.id)
        
    else:
        await update.message.reply_text("Неизвестная команда")
        return await admin_reports_menu(update, context)

async def handle_report_type_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик выбора типа отчета"""
    user = update.effective_user
    text = update.message.text
    
    if text == "🔙 Назад":
        return await admin_reports_menu(update, context)
        
    # Сохраняем тип отчета
    if text == "💰 Бухгалтерский":
        context.user_data['report_type'] = 'accounting'
    elif text == "📦 Поставщика":
        context.user_data['report_type'] = 'provider'
    elif text == "👨‍💼 Админский":
        context.user_data['report_type'] = 'admin'
    else:
        await update.message.reply_text("Неизвестный тип отчета")
        return await handle_report_type_selection(update, context)
    
    # Для дневных отчетов - сразу генерируем
    if context.user_data['report_period'] == 'daily':
        today = datetime.now(CONFIG.timezone).date()
        
        if context.user_data['report_type'] == 'accounting':
            await export_accounting_report(update, context, today, today)
        elif context.user_data['report_type'] == 'provider':
            await export_orders_for_provider(update, context, today, today)
        elif context.user_data['report_type'] == 'admin':
            await export_daily_admin_report(update, context, today)
            
        return await show_main_menu(update, user.id)
    # Для месячных - запрашиваем месяц
    else:
        await update.message.reply_text(
            "📅 Выберите месяц:",
            reply_markup=create_month_selection_keyboard()
        )
        return SELECT_MONTH_RANGE