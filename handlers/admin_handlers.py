# ##handlers/admin_handlers.py
import logging
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import ConversationHandler, MessageHandler
from telegram.ext import ContextTypes
import asyncio

from config import CONFIG
from constants import BROADCAST_MESSAGE
from bot_keyboards import create_admin_keyboard

logger = logging.getLogger(__name__)

async def handle_admin_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Основной обработчик выбора в админ-меню. 
    Проверяет права пользователя и перенаправляет на соответствующие действия:
    - Рассылка сообщений
    - Генерация отчетов (закомментировано)
    - Обработка отмены операций
    Возвращает состояние для продолжения диалога или завершает его.
    """
    user = update.effective_user  # Добавляем получение пользователя
    text = update.message.text.strip().lower()  # Добавляем получение текста
    
    # Переносим лог после определения переменных
    logger.info(f"User {user.id} with text '{text}'. Admin IDs: {CONFIG.get('admin_ids')}, Accounting IDs: {CONFIG.get('accounting_ids')}")

    # Проверка прав администратора
    if user.id not in CONFIG.get('admin_ids', []) and user.id not in CONFIG.get('accounting_ids', []):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды.")
        return ConversationHandler.END

    if text == "📢 сделать рассылку":  # Приводим к нижнему регистру для сравнения
        await update.message.reply_text(
            "Введите сообщение для рассылки:",
            reply_markup=ReplyKeyboardMarkup([["Отмена"]], resize_keyboard=True)
        )
        return BROADCAST_MESSAGE

    elif text == "отмена":
        await update.message.reply_text(
            "Действие отменено",
            reply_markup=create_admin_keyboard()
        )
        return ConversationHandler.END

    # Неизвестная команда
    await update.message.reply_text(
        "Неизвестная команда. Пожалуйста, используйте кнопки меню.",
        reply_markup=create_admin_keyboard()
    )
    return ConversationHandler.END