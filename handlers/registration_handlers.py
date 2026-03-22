# ##handlers/registration_handlers.py
import logging
from telegram import Update, KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import ConversationHandler
from telegram.ext import ContextTypes
from datetime import datetime, timedelta

from database import db
from models import User
from config import CONFIG
from constants import AWAIT_MESSAGE_TEXT, FULL_NAME, LOCATION, PHONE
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

        # Обновляем запись в БД через SQLAlchemy
        user_record = db.session.query(User).filter(User.telegram_id == user.id).first()
        if user_record:
            user_record.phone = normalized_phone
            user_record.updated_at = datetime.now()
            db.session.commit()

        # Проверяем, что запись действительно изменилась
        updated_user = db.session.query(User).filter(User.telegram_id == user.id).first()
        logger.info(f"После обновления телефон в БД: {updated_user.phone if updated_user else None}")

        # Переход к имени
        await update.message.reply_text(
            "Введите ваше фамилию имя и отчество:",
            reply_markup=ReplyKeyboardRemove()  # Убираем клавиатуру после успешного ввода
        )
        return FULL_NAME

    except Exception as e:
        logger.error(f"Ошибка в get_phone: {e}", exc_info=True)
        await update.message.reply_text("⚠️ Ошибка при сохранении номера")
        return PHONE


# Re-export from services
from services.user_service import is_valid_phone, normalize_phone

async def get_full_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обрабатывает ввод ФИО при регистрации.
    Понимает разные варианты написания одного и того же имени (например, "Гребеньков Иван" и "Иван Гребеньков").
    """
    try:
        user = update.effective_user
        user_input = update.message.text.strip()
        logger.info(f"Получено имя: '{user_input}' от пользователя {user.id}")

        # Проверка существующей записи пользователя через SQLAlchemy
        user_record = db.session.query(User).filter(User.telegram_id == user.id).first()
        logger.info(f"Запись в БД перед get_full_name: {user_record.id if user_record else None}")

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
            await update.message.reply_text("Введите ваше фамилию имя и отчество:")
            return FULL_NAME

        # Проверяем формат имени
        name_parts = [part for part in user_input.split() if part]
        if len(name_parts) < 2:
            await update.message.reply_text(
                "❌ Пожалуйста, введите фамилию имя и отчество полностью.\nПример: Иванов Иван Иванович",
                reply_markup=ReplyKeyboardMarkup(
                    [["Попробовать снова"], ["Написать администратору"]],
                    resize_keyboard=True
                )
            )
            return FULL_NAME

        # Нормализуем ввод пользователя
        normalized_input = ' '.join(name_parts).lower()

        # Ищем совпадения в базе данных среди незарегистрированных пользователей через SQLAlchemy
        unregistered_users = db.session.query(User).filter(
            User.telegram_id == None,
            User.is_employee == True
        ).all()

        matched_user = None
        for db_user in unregistered_users:
            if not db_user.full_name:
                continue

            # Нормализуем имя из базы и сравниваем наборы слов
            db_parts = {part.lower() for part in db_user.full_name.split()}
            input_parts = {part.lower() for part in name_parts}
            
            if db_parts == input_parts:
                matched_user = db_user
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

        context.user_data['full_name'] = matched_user.full_name  # Сохраняем оригинальное имя из базы

        # Обновляем запись в БД через SQLAlchemy
        phone = context.user_data.get('phone')
        matched_user.telegram_id = user.id
        matched_user.username = user.username
        matched_user.phone = phone
        matched_user.updated_at = datetime.now()
        db.session.commit()

        logger.info(f"Обновлена запись пользователя ID: {matched_user.id}")

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
        # Обновляем запись пользователя через SQLAlchemy
        user_record = db.session.query(User).filter(User.telegram_id == user.id).first()
        if user_record:
            user_record.location = location
            user_record.is_verified = True
            user_record.updated_at = datetime.now()
            db.session.commit()

        # Проверяем, что запись обновилась
        updated_user = db.session.query(User).filter(User.telegram_id == user.id).first()
        logger.info(f"Локация обновлена: {updated_user.location}, статус верификации: {updated_user.is_verified}")

        await update.message.reply_text(
            f"✅ Локация успешно изменена на: {location}",
            reply_markup=ReplyKeyboardRemove()
        )
        
        await show_main_menu(update, user.id)
        return ConversationHandler.END

    except Exception as e:
        logger.error(f"Ошибка сохранения локации: {e}")
        await update.message.reply_text("⚠️ Ошибка при сохранении данных")
        return LOCATION
    
# Добавить новую функцию в конец файла
async def change_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обрабатывает запрос на изменение локации пользователя
    """
    user = update.effective_user

    try:
        # Получаем текущую локацию пользователя через SQLAlchemy
        user_record = db.session.query(User).filter(User.telegram_id == user.id).first()
        current_location = user_record.location if user_record and user_record.location else "не установлена"
        
        logger.info(f"Пользователь {user.id} запросил изменение локации. Текущая локация: '{current_location}'")

        # Удаляем текущую локацию из базы данных через SQLAlchemy
        if user_record:
            user_record.location = None
            user_record.updated_at = datetime.now()
            db.session.commit()

        logger.info(f"Локация пользователя {user.id} удалена из БД (была: '{current_location}')")

        # Предлагаем выбрать новую локацию с информацией о текущей
        keyboard = [[loc] for loc in CONFIG.locations]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        location_info = f" (текущая: {current_location})" if current_location != "не установлена" else ""
        await update.message.reply_text(
            f"Выберите новый объект{location_info}:",
            reply_markup=reply_markup
        )
        return LOCATION

    except Exception as e:
        logger.error(f"Ошибка при изменении локации: {e}", exc_info=True)
        await update.message.reply_text(
            "⚠️ Произошла ошибка при изменении локации. Попробуйте позже."
        )
        await show_main_menu(update, user.id)
        return ConversationHandler.END