# ##utils.py
from telegram import Update, KeyboardButton, ReplyKeyboardMarkup
from telegram.ext import ContextTypes
import logging
from datetime import datetime, timedelta, date, time
import pytz

from database import db
from config import CONFIG
from handlers.common import show_main_menu
from settings import SETTINGS_CONFIG
from models import User, Order
from time_config import TIME_CONFIG

# Re-export from services for backwards compatibility
from services.time_service import get_next_workday, can_modify_order as _can_modify_order
from services.menu_service import get_menu_for_day as _get_menu_for_day, format_menu_text, DAYS_RU

logger = logging.getLogger(__name__)

# Состояния диалога (для handle_unregistered)
PHONE = 0


def can_modify_order(target_date):
    """Wrapper that passes orders_enabled from CONFIG."""
    return _can_modify_order(target_date, CONFIG.are_orders_accepted_now())


def is_order_time_expired():
    return not can_modify_order(datetime.now(CONFIG.timezone).date())


def get_order_time_restriction():
    from services.time_service import is_weekend
    now = datetime.now(TIME_CONFIG.TIMEZONE)

    if is_weekend(now):
        next_workday = get_next_workday(now)
        return f"⏳ Сегодня выходной. Вы можете оформить предварительный заказ на {next_workday.strftime('%d.%m')} (понедельник)"

    if now.hour >= 10:
        next_workday = get_next_workday(now)
        return f"⏳ Прием заказов на сегодня завершен в 10:00. Вы можете оформить предварительный заказ на {next_workday.strftime('%d.%m')}"

    return None


def is_employee(full_name):
    normalized_input = ' '.join(full_name.strip().split()).lower()
    return normalized_input in CONFIG.staff_names


def get_menu_for_day(day_offset=0):
    """Legacy wrapper — returns (menu, day_name) for backward compat."""
    menu, day_name, target_date = _get_menu_for_day(day_offset, CONFIG)
    return menu, day_name


def format_menu(menu, day_name, is_tomorrow=False):
    """Legacy wrapper — computes target_date from day_name."""
    if not menu:
        return f"На {day_name} выходной! Меню не предусмотрено."

    now = datetime.now(TIME_CONFIG.TIMEZONE)
    current_day_index = now.weekday()
    target_day_index = DAYS_RU.index(day_name)

    if target_day_index > current_day_index:
        days_diff = target_day_index - current_day_index
    else:
        days_diff = 7 - (current_day_index - target_day_index)

    target_date = (now + timedelta(days=days_diff)).date()
    return format_menu_text(menu, day_name, target_date)

async def check_registration(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Универсальная проверка доступа — используется в некоторых других частях бота
    """
    user = update.effective_user
    
    if user.id in CONFIG.admin_ids:
        return True

    with db.get_session() as session:
        user_data = session.query(User).filter(
            User.telegram_id == user.id,
            User.is_deleted == False
        ).first()
        
        if not user_data:
            return False

        return bool(user_data.is_employee and user_data.is_verified)

async def handle_unregistered(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обработчик для незарегистрированных пользователей.
    Теперь предлагает регистрацию ВСЕМ, включая админов и бухгалтеров.
    """
    user = update.effective_user
    user_id = user.id

    logger.info(f"Незарегистрированный пользователь {user_id} взаимодействует с ботом")

    # Уведомляем администраторов (для всех незарегистрированных)
    await notify_admins_about_unregistered(context.bot, user.id, user.username, user.full_name)

    # Предлагаем зарегистрироваться ВСЕМ
    keyboard = [[KeyboardButton("📱 Отправить номер телефона", request_contact=True)]]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

    await update.message.reply_text(
        "Для доступа к боту необходимо пройти регистрацию. Пожалуйста, отправьте свой номер телефона:",
        reply_markup=reply_markup
    )
    return PHONE

def is_order_cancelled(user_id: int, target_date_str: str, context=None) -> bool:
    """Проверяет, отменён ли заказ (из БД или временного хранилища)"""
    try:
        with db.get_session() as session:
            # Проверка из базы данных
            order = session.query(Order).join(
                User, Order.user_id == User.id
            ).filter(
                User.telegram_id == user_id,
                Order.target_date == target_date_str
            ).first()
            
            if order and order.is_cancelled:
                return True
                
        # Резервная проверка из контекста
        if context and context.user_data.get('cancelled_orders'):
            return target_date_str in context.user_data['cancelled_orders']
            
        return False
        
    except Exception as e:
        logger.error(f"Ошибка проверки статуса отмены: {e}")
        return False
    
async def notify_admins_about_unregistered(bot, user_id, username, full_name):
    """Отправляет уведомление администраторам о незарегистрированном пользователе"""
    message = (
        f"⚠️ Незарегистрированный пользователь пытается использовать бота:\n"
        f"🆔 ID: {user_id}\n"
        f"👤 Username: @{username if username else 'нет'}\n"
        f"📝 Имя: {full_name}"
    )
    
    for admin_id in CONFIG.admin_ids:
        try:
            await bot.send_message(chat_id=admin_id, text=message)
        except Exception as e:
            logger.error(f"Не удалось отправить сообщение админу {admin_id}: {e}")