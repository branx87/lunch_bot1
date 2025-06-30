# ##handlers/registration_handlers.py
import sqlite3
import logging
from telegram import Update, KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import ConversationHandler
from telegram.ext import ContextTypes
from datetime import datetime, timedelta

from config import CONFIG
from constants import AWAIT_MESSAGE_TEXT, FULL_NAME, LOCATION, PHONE
from db import db
from handlers.common import show_main_menu
from handlers.message_handlers import handle_admin_message, start_user_to_admin_message

logger = logging.getLogger(__name__)

async def get_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Получает номер телефона ТОЛЬКО через кнопку "📱 Отправить номер телефона"
    """
    user = update.effective_user
    logger.info(f"Получен запрос номера телефона от пользователя {user.id}")

    # Если пользователь не отправил контакт, а написал текст
    if not update.message.contact:
        # Повторно показываем кнопку для отправки номера
        keyboard = [[KeyboardButton("📱 Отправить номер телефона", request_contact=True)]]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)
        await update.message.reply_text(
            "❌ Пожалуйста, используйте кнопку '📱 Отправить номер телефона' для отправки вашего номера",
            reply_markup=reply_markup
        )
        return PHONE

    try:
        # Получаем номер из контакта
        phone = update.message.contact.phone_number
        
        if not phone:
            await update.message.reply_text("❌ Не удалось получить номер телефона.")
            return PHONE

        # Нормализуем номер телефона (убираем все нецифровые символы кроме + в начале)
        normalized_phone = normalize_phone(phone)
        if not normalized_phone:
            await update.message.reply_text("❌ Неверный формат номера телефона.")
            return PHONE

        # Сохраняем в контекст
        context.user_data['phone'] = normalized_phone

        # Обновляем запись в БД
        with db.conn:
            db.cursor.execute("""
                UPDATE users 
                SET phone = ?, updated_at = CURRENT_TIMESTAMP
                WHERE telegram_id = ?
            """, (normalized_phone, user.id))

        # Проверяем, что запись действительно изменилась
        db.cursor.execute("SELECT phone FROM users WHERE telegram_id = ?", (user.id,))
        result = db.cursor.fetchone()
        logger.info(f"После обновления телефон в БД: {result[0] if result else None}")

        # Переход к имени
        await update.message.reply_text(
            "Введите ваше фамилию и имя:",
            reply_markup=ReplyKeyboardRemove()  # Убираем клавиатуру после успешного ввода
        )
        return FULL_NAME

    except Exception as e:
        logger.error(f"Ошибка в get_phone: {e}", exc_info=True)
        await update.message.reply_text("⚠️ Ошибка при сохранении номера")
        return PHONE


def is_valid_phone(phone: str) -> bool:
    """Проверяет, является ли строка валидным номером телефона"""
    if not phone:
        return False
    
    # Удаляем все символы, кроме цифр и +
    cleaned = ''.join(c for c in phone if c.isdigit() or c == '+')
    
    # Проверяем длину (минимум 10 цифр для российских номеров)
    digits = [c for c in cleaned if c.isdigit()]
    return len(digits) >= 10


def normalize_phone(phone: str) -> str:
    """Нормализует номер телефона к стандартному формату"""
    if not phone:
        return ""
    
    # Оставляем только цифры и +
    cleaned = ''.join(c for c in phone if c.isdigit() or c == '+')
    
    # Если номер начинается с 8, заменяем на +7
    if cleaned.startswith('8'):
        cleaned = '+7' + cleaned[1:]
    # Если номер начинается с 7, добавляем +
    elif cleaned.startswith('7') and not cleaned.startswith('+7'):
        cleaned = '+' + cleaned
    
    return cleaned

async def get_full_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обрабатывает ввод ФИО при регистрации.
    Ищет совпадение в БД по full_name.
    Если нашёл — обновляет telegram_id и username у существующей записи.
    Не создаёт новых пользователей — этим занимается администратор.
    """
    try:
        user = update.effective_user  # Сначала получаем пользователя

        # Теперь можно безопасно логировать
        db.cursor.execute("SELECT id, phone FROM users WHERE telegram_id = ?", (user.id,))
        record_before = db.cursor.fetchone()
        logger.info(f"Запись в БД перед get_full_name: {record_before}")

        user_input = update.message.text.strip()
        logger.info(f"Получено имя: '{user_input}' от пользователя {user.id}")

        # Обработка специальных команд
        if user_input == "Написать администратору":
            # Сразу переходим к вводу сообщения, минуя проверки
            await update.message.reply_text(
                "✍️ Введите ваше сообщение администратору:",
                reply_markup=ReplyKeyboardMarkup([["❌ Отменить"]], resize_keyboard=True)
            )
            context.user_data['user_name'] = user_input  # Сохраняем то, что ввел пользователь
            context.user_data['is_registered'] = False
            return AWAIT_MESSAGE_TEXT

        if user_input == "Попробовать снова":
            await update.message.reply_text("Введите ваше фамилию и имя:")
            return FULL_NAME

        # Проверяем формат имени
        name_parts = user_input.split()
        if len(name_parts) < 2:
            await update.message.reply_text("❌ Пожалуйста, введите фамилию и имя полностью.\nПример: Иванов Иван")
            return FULL_NAME

        full_name = ' '.join(name_parts)
        context.user_data['full_name'] = full_name

        # Проверяем, есть ли такой сотрудник в списке сотрудников
        # Используй прямой SQL-запрос:
        db.cursor.execute("SELECT id FROM users WHERE telegram_id = ? AND is_employee = TRUE", (user.id,))
        result = db.cursor.fetchone()
        if result:
            # Пользователь является сотрудником
            context.user_data['unverified_name'] = full_name
            reply_markup = ReplyKeyboardMarkup(
                [["Попробовать снова"], ["Написать администратору"]],
                resize_keyboard=True
            )
            await update.message.reply_text(
                "❌ Вас нет в списке сотрудников.",
                reply_markup=reply_markup
            )
            return FULL_NAME

        # Проверяем, зарегистрирован ли уже по telegram_id
        db.cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (user.id,))
        existing_by_telegram = db.cursor.fetchone()

        if existing_by_telegram:
            await update.message.reply_text("⚠️ Вы уже зарегистрированы.")
            return ConversationHandler.END

        # Ищем пользователя по полному совпадению full_name
        db.cursor.execute("""
            SELECT id, full_name 
            FROM users 
            WHERE full_name = ? AND telegram_id IS NULL
        """, (full_name,))

        existing_by_name = db.cursor.fetchone()

        if not existing_by_name:
            # Нет подходящей записи для привязки
            reply_markup = ReplyKeyboardMarkup(
                [["Попробовать снова"], ["Написать администратору"]],
                resize_keyboard=True
            )
            await update.message.reply_text(
                "❌ Такого сотрудника нет в системе или он уже зарегистрирован.",
                reply_markup=reply_markup
            )
            return FULL_NAME

        # Получаем телефон из контекста
        phone = context.user_data.get('phone')

        # Обновляем существующую запись — добавляем telegram_id, username и телефон
        with db.conn:
            db.cursor.execute("""
                UPDATE users
                SET telegram_id = ?, 
                    username = ?,
                    phone = ?
                WHERE id = ?
            """, (user.id, user.username, phone, existing_by_name[0]))

        logger.info(f"Обновлена запись пользователя ID: {existing_by_name[0]}")

        # Логируем после обновления
        db.cursor.execute("SELECT id, phone FROM users WHERE telegram_id = ?", (user.id,))
        record_after = db.cursor.fetchone()
        logger.info(f"Запись в БД после get_full_name: {record_after}")

        # Переход к выбору локации
        keyboard = [[loc] for loc in CONFIG.locations]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        await update.message.reply_text("Выберите ваш объект:", reply_markup=reply_markup)
        return LOCATION

    except Exception as e:
        logger.error(f"Ошибка в get_full_name: {e}", exc_info=True)
        await update.message.reply_text("⚠️ Произошла ошибка. Попробуйте снова.")
        return ConversationHandler.END

async def get_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    location = update.message.text

    if location not in CONFIG.locations:
        await update.message.reply_text("❌ Пожалуйста, выберите объект из списка.")
        return LOCATION

    try:
        # Обновляем запись пользователя
        db.cursor.execute("""
            UPDATE users
            SET location = ?, is_verified = TRUE
            WHERE telegram_id = ?
        """, (location, user.id))
        db.conn.commit()

        # После обновления записи
        db.cursor.execute("SELECT is_verified FROM users WHERE telegram_id = ?", (user.id,))
        verified_status = db.cursor.fetchone()[0]
        logger.info(f"Текущий статус верификации: {verified_status}")

        await show_main_menu(update, user.id)
        return ConversationHandler.END

    except Exception as e:
        logger.error(f"Ошибка сохранения локации: {e}")
        await update.message.reply_text("⚠️ Ошибка при сохранении данных")
        return LOCATION