# ##handlers/common.py
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import ConversationHandler
from telegram.ext import ContextTypes  # вместо ContextType
import logging

from config import CONFIG
from constants import MAIN_MENU
from bot_keyboards import create_main_menu_keyboard, create_provider_menu_keyboard, create_unverified_user_keyboard


logger = logging.getLogger(__name__)

async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not update:
            logger.error("Update object is None")
            return ConversationHandler.END
            
        if hasattr(update, 'callback_query') and update.callback_query:
            message = update.callback_query.message
            user = update.callback_query.from_user
        elif hasattr(update, 'message') and update.message:
            message = update.message
            user = update.effective_user
        else:
            logger.error("Неизвестный тип update в show_main_menu")
            return ConversationHandler.END

        if not user:
            logger.error("Пользователь не определен в show_main_menu")
            return ConversationHandler.END

        db = context.bot_data['db']
        user_id = user.id

        # Проверяем права пользователя
        is_admin = user_id in CONFIG.admin_ids
        is_provider = user_id in CONFIG.provider_ids
        is_accountant = user_id in CONFIG.accounting_ids
        is_verified = False

        # Для обычных пользователей проверяем верификацию в БД
        if not (is_admin or is_provider or is_accountant):
            try:
                db.cursor.execute("SELECT is_verified FROM users WHERE telegram_id = ?", (user_id,))
                result = db.cursor.fetchone()
                is_verified = result and result['is_verified']
            except Exception as e:
                logger.error(f"Ошибка проверки верификации пользователя: {e}")

        # Формируем соответствующую клавиатуру
        if is_admin or is_provider or is_accountant or is_verified:
            reply_markup = create_main_menu_keyboard(user_id)
            menu_text = "Главное меню:"
        else:
            reply_markup = create_unverified_user_keyboard()
            menu_text = "Ваш аккаунт ожидает подтверждения"

        # Отправляем меню
        try:
            await message.reply_text(
                text=menu_text,
                reply_markup=reply_markup
            )
            return MAIN_MENU
        except Exception as e:
            logger.error(f"Ошибка отправки меню: {e}")
            return ConversationHandler.END

    except Exception as e:
        logger.error(f"Критическая ошибка в show_main_menu: {e}", exc_info=True)
        try:
            if 'message' in locals():
                await message.reply_text(
                    "⚠️ Ошибка при отображении меню",
                    reply_markup=create_unverified_user_keyboard()
                )
        except Exception as inner_e:
            logger.error(f"Ошибка при обработке ошибки: {inner_e}")
        
        return ConversationHandler.END

from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler

async def cancel_edit_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Изменение меню отменено",
        reply_markup=create_provider_menu_keyboard()
    )
    return ConversationHandler.END

