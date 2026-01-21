# ##middleware.py
from telegram import Update
from telegram.ext import BaseHandler, ContextTypes
import logging
from database import db
from models import User

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

            with db.get_session() as session:
                user_data = session.query(User).filter(
                    User.telegram_id == user.id
                ).first()
                
                if not user_data:
                    logger.info(f"Незарегистрированный пользователь {user.id}")
                    return False
                    
                if not user_data.is_verified or user_data.is_deleted:
                    logger.info(f"Доступ запрещен для пользователя {user.id} (verified={user_data.is_verified}, deleted={user_data.is_deleted})")
                    return False
                    
                return True
        except Exception as e:
            logger.error(f"Ошибка проверки доступа: {e}", exc_info=True)
            return False

    # async def _deny_access(self, update: Update):
    #     """Уведомление об отказе только для пользователя"""
    #     try:
    #         if update.callback_query:
    #             await update.callback_query.answer(
    #                 "⛔ Доступ запрещён. Аккаунт неактивен", 
    #                 show_alert=True
    #             )
    #         elif update.message:
    #             await update.message.reply_text(
    #                 "⛔ Ваш аккаунт деактивирован\nОбратитесь к администратору"
    #             )
    #     except Exception as e:
    #         logger.error(f"Error showing access denied: {e}")

    async def _deny_access(self, update: Update):
        try:
            message = "🔧 Бот временно на обслуживании. Приносим извинения за неудобства!"
            if update.callback_query:
                await update.callback_query.answer(message, show_alert=True)
            elif update.message:
                await update.message.reply_text(message)
        except Exception as e:
            logger.error(f"Error showing maintenance message: {e}")

async def check_user_access(user_id: int, application=None) -> bool:
    """Проверка доступа без уведомлений администраторам"""
    try:
        if application and user_id in application.bot_data.get('admin_ids', []):
            return True
            
        with db.get_session() as session:
            user_data = session.query(User).filter(
                User.telegram_id == user_id
            ).first()
            
            return bool(user_data and user_data.is_verified and not user_data.is_deleted)
    except Exception as e:
        logger.error(f"Ошибка проверки доступа: {e}")
        return False