# ##handlers/common.py
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import ConversationHandler
from telegram.ext import ContextTypes
import logging

from database import db
from models import User
from config import CONFIG
from constants import MAIN_MENU
from bot_keyboards import create_main_menu_keyboard, create_provider_menu_keyboard, create_unverified_user_keyboard

logger = logging.getLogger(__name__)

async def show_main_menu(update: Update, user_id: int):
    """Общая функция для показа главного меню"""
    try:
        # Поставщики, бухгалтеры и админы — всегда могут видеть главное меню
        if user_id in CONFIG.admin_ids or user_id in CONFIG.provider_ids or user_id in CONFIG.accounting_ids:
            reply_markup = create_main_menu_keyboard(user_id)
        else:
            # Остальные должны быть сотрудниками - проверяем через SQLAlchemy
            user = db.session.query(User).filter(
                User.telegram_id == user_id
            ).first()
            
            if user and user.is_verified:
                reply_markup = create_main_menu_keyboard(user_id)
            else:
                reply_markup = create_unverified_user_keyboard()

        if isinstance(update, Update) and update.message:
            await update.message.reply_text("Главное меню:", reply_markup=reply_markup)
        return MAIN_MENU

    except Exception as e:
        logger.error(f"Ошибка в show_main_menu: {e}", exc_info=True)
        if update.message:
            await update.message.reply_text(
                "⚠️ Ошибка при отображении меню",
                reply_markup=create_unverified_user_keyboard()
            )
        return ConversationHandler.END

async def cancel_edit_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Изменение меню отменено",
        reply_markup=create_provider_menu_keyboard()
    )
    return ConversationHandler.END