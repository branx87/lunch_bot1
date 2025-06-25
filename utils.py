# ##utils.py
from telegram import Update, KeyboardButton, ReplyKeyboardMarkup
from telegram.ext import ContextTypes
import logging
from datetime import datetime, timedelta, date, time
import pytz

from config import CONFIG, MENU, TIMEZONE
from handlers.common import show_main_menu
from settings import SETTINGS_CONFIG
from db import Database

logger = logging.getLogger(__name__)

# Состояния диалога (для handle_unregistered)
PHONE = 0

def is_weekday(date=None):
    if date is None:
        date = datetime.now(TIMEZONE)
    return date.weekday() < 5  # 0-4 = пн-пт

def get_next_workday(date=None):
    if date is None:
        date = datetime.now(TIMEZONE)
    
    days_to_add = 1
    if date.weekday() == 4:  # Пятница
        days_to_add = 3  # Понедельник
    elif date.weekday() == 5:  # Суббота
        days_to_add = 2  # Понедельник
    
    return date + timedelta(days=days_to_add)

def can_modify_order(target_date):
    """Проверяет, можно ли изменять заказ на указанную дату"""
    now = datetime.now(TIMEZONE)
    
    # Если target_date - строка, преобразуем в дату
    if isinstance(target_date, str):
        try:
            target_date = datetime.strptime(target_date, "%Y-%m-%d").date()
        except ValueError:
            logger.error(f"Неверный формат даты: {target_date}")
            return False
    
    # Заказы на выходные невозможны
    if target_date.weekday() >= 5:  # 5-6 = суббота-воскресенье
        return False
    
    # Заказы на будущие дни (предзаказы) можно менять в любое время
    if target_date > now.date():
        return True
    
    # Заказы на сегодня можно менять только до 9:30
    if target_date == now.date():
        return now.time() < time(9, 30)
    
    # Заказы на прошедшие дни нельзя менять
    return False

def is_order_time_expired():
    """Старая функция, оставляем для совместимости, но теперь она использует новую функцию"""
    return not can_modify_order(datetime.now(TIMEZONE).date())

def get_order_time_restriction():
    now = datetime.now(TIMEZONE)
    current_hour = now.hour
    
    if not is_weekday(now):
        next_workday = get_next_workday(now)
        return f"⏳ Сегодня выходной. Вы можете оформить предварительный заказ на {next_workday.strftime('%d.%m')} (понедельник)"
    
    if current_hour >= 10:
        next_workday = get_next_workday(now)
        return f"⏳ Прием заказов на сегодня завершен в 10:00. Вы можете оформить предварительный заказ на {next_workday.strftime('%d.%m')}"
    
    return None

def is_employee(full_name):
    normalized_input = ' '.join(full_name.strip().split()).lower()
    return normalized_input in CONFIG.staff_names

def get_menu_for_day(day_offset=0):
    now = datetime.now(TIMEZONE)
    days = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
    target_date = (now + timedelta(days=day_offset)).date()
    day_name = days[target_date.weekday()]
    
    # Проверяем, является ли день праздником
    if target_date.strftime("%Y-%m-%d") in CONFIG.get('holidays', {}):
        return None, day_name
    
    return MENU.get(day_name), day_name

def format_menu(menu, day_name, is_tomorrow=False):
    if not menu:
        return f"На {day_name} выходной! Меню не предусмотрено."
    
    # Получаем текущую дату и вычисляем дату для отображения
    now = datetime.now(TIMEZONE)
    days_ru = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
    
    # Находим индекс текущего дня и вычисляем дату
    current_day_index = now.weekday()
    target_day_index = days_ru.index(day_name)
    
    # Если день в будущем (например, среда при текущем понедельнике)
    if target_day_index > current_day_index:
        days_diff = target_day_index - current_day_index
    # Если день в прошлом (например, понедельник при текущей среде)
    else:
        days_diff = 7 - (current_day_index - target_day_index)
    
    target_date = (now + timedelta(days=days_diff)).date()
    date_str = target_date.strftime("%d.%m")
    
    return (
        f"🍽 Меню на {day_name} ({date_str}):\n"
        f"1. 🍲 Первое: {menu['first']}\n"
        f"2. 🍛 Основное блюдо: {menu['main']}\n"
        f"3. 🥗 Салат: {menu['salad']}"
    )

async def check_registration(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Универсальная проверка доступа — используется в некоторых других частях бота
    """
    user = update.effective_user
    
    if user.id in CONFIG.admin_ids:
        return True

    db.cursor.execute("""
        SELECT is_verified, is_employee 
        FROM users 
        WHERE telegram_id = ? AND is_deleted = FALSE
    """, (user.id,))
    
    result = db.cursor.fetchone()
    if not result:
        return False

    is_verified, is_employee = result
    return bool(is_employee and is_verified)

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
        # Проверка из базы данных
        db.cursor.execute("""
            SELECT is_cancelled FROM orders 
            WHERE user_id = (SELECT id FROM users WHERE telegram_id = ?)
            AND target_date = ?
        """, (user_id, target_date_str))
        
        result = db.cursor.fetchone()
        if result and result[0]:
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