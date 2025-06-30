# ##handlers/registration_handlers.py
import sqlite3
import logging
from telegram import Update, KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import ConversationHandler
from telegram.ext import ContextTypes
from datetime import datetime, timedelta

from db import CONFIG
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
    Понимает разные варианты написания одного и того же имени (например, "Гребеньков Иван" и "Иван Гребеньков").
    """
    try:
        user = update.effective_user
        user_input = update.message.text.strip()
        logger.info(f"Получено имя: '{user_input}' от пользователя {user.id}")

        # Проверка существующей записи пользователя
        db.cursor.execute("SELECT id, phone FROM users WHERE telegram_id = ?", (user.id,))
        record_before = db.cursor.fetchone()
        logger.info(f"Запись в БД перед get_full_name: {record_before}")

        # Обработка специальных команд
        if user_input == "Написать администратору":
            await update.message.reply_text(
                "✍️ Введите ваше сообщение администратору:",
                reply_markup=ReplyKeyboardMarkup([["❌ Отменить"]], resize_keyboard=True)
            )
            context.user_data['user_name'] = user_input
            context.user_data['is_registered'] = False
            return AWAIT_MESSAGE_TEXT

        if user_input == "Попробовать снова":
            await update.message.reply_text("Введите ваше фамилию и имя:")
            return FULL_NAME

        # Проверяем формат имени
        name_parts = [part for part in user_input.split() if part]
        if len(name_parts) < 2:
            await update.message.reply_text(
                "❌ Пожалуйста, введите фамилию и имя полностью.\nПример: Иванов Иван",
                reply_markup=ReplyKeyboardMarkup(
                    [["Попробовать снова"], ["Написать администратору"]],
                    resize_keyboard=True
                )
            )
            return FULL_NAME

        # Нормализуем ввод пользователя
        normalized_input = ' '.join(name_parts).lower()

        # Ищем совпадения в базе данных среди незарегистрированных пользователей
        db.cursor.execute("""
            SELECT id, full_name 
            FROM users 
            WHERE telegram_id IS NULL AND is_employee = TRUE
        """)
        all_users = db.cursor.fetchall()

        matched_user = None
        for db_user in all_users:
            user_id, db_full_name = db_user
            if not db_full_name:
                continue

            # Нормализуем имя из базы и сравниваем наборы слов
            db_parts = {part.lower() for part in db_full_name.split()}
            input_parts = {part.lower() for part in name_parts}
            
            if db_parts == input_parts:
                matched_user = (user_id, db_full_name)
                break

        if not matched_user:
            reply_markup = ReplyKeyboardMarkup(
                [["Попробовать снова"], ["Написать администратору"]],
                resize_keyboard=True
            )
            await update.message.reply_text(
                "❌ Такого сотрудника нет в системе или он уже зарегистрирован.",
                reply_markup=reply_markup
            )
            return FULL_NAME

        user_id, db_full_name = matched_user
        context.user_data['full_name'] = db_full_name  # Сохраняем оригинальное имя из базы

        # Обновляем запись в БД
        phone = context.user_data.get('phone')
        with db.conn:
            db.cursor.execute("""
                UPDATE users
                SET telegram_id = ?, 
                    username = ?,
                    phone = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (user.id, user.username, phone, user_id))

        logger.info(f"Обновлена запись пользователя ID: {user_id}")

        # Переход к выбору локации
        keyboard = [[loc] for loc in CONFIG.locations]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        await update.message.reply_text("Выберите ваш объект:", reply_markup=reply_markup)
        return LOCATION

    except Exception as e:
        logger.error(f"Ошибка в get_full_name: {e}", exc_info=True)
        await update.message.reply_text(
            "⚠️ Произошла ошибка. Попробуйте снова.",
            reply_markup=ReplyKeyboardMarkup(
                [["Попробовать снова"], ["Написать администратору"]],
                resize_keyboard=True
            )
        )
        return FULL_NAME

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