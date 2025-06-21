# ##middleware.py
from telegram import Update
from telegram.ext import BaseHandler, ContextTypes
import logging
from db import db

logger = logging.getLogger(__name__)

class AccessControlHandler(BaseHandler):
    def __init__(self):
        super().__init__(self._handle_update)
        self.priority = -1

    def check_update(self, update: object) -> bool:
        return isinstance(update, Update)

    async def _handle_update(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        if context.user_data.get('is_verified'):
            return True  # Пропускаем проверку, если уже верифицирован в текущей сессии
        
        try:
            user = update.effective_user
            if not user:
                return False

            if user.id in context.application.bot_data.get('admin_ids', []):
                return True

            db.cursor.execute("""
                SELECT is_verified, is_deleted 
                FROM users 
                WHERE telegram_id = ?
            """, (user.id,))
            result = db.cursor.fetchone()
            
            if not result:
                logger.info(f"Незарегистрированный пользователь {user.id}")
                return False
                
            if not result[0] or result[1]:
                logger.info(f"Доступ запрещен для пользователя {user.id} (verified={result[0]}, deleted={result[1]})")
                return False
                
            return True
        except Exception as e:
            logger.error(f"Ошибка проверки доступа: {e}", exc_info=True)
            return False

    async def _deny_access(self, update: Update):
        """Уведомление об отказе только для пользователя"""
        try:
            if update.callback_query:
                await update.callback_query.answer(
                    "⛔ Доступ запрещён. Аккаунт неактивен", 
                    show_alert=True
                )
            elif update.message:
                await update.message.reply_text(
                    "⛔ Ваш аккаунт деактивирован\nОбратитесь к администратору"
                )
        except Exception as e:
            logger.error(f"Error showing access denied: {e}")

async def check_user_access(user_id: int, application=None) -> bool:
    """Проверка доступа без уведомлений администраторам"""
    try:
        if application and user_id in application.bot_data.get('admin_ids', []):
            return True
            
        db.cursor.execute("SELECT is_verified, is_deleted FROM users WHERE telegram_id = ?", (user_id,))
        result = db.cursor.fetchone()
        
        return bool(result and result[0] and not result[1])
    except Exception as e:
        logger.error(f"Ошибка проверки доступа: {e}")
        return False