# ##middleware.py
from telegram import Update
from telegram.ext import BaseHandler, ContextTypes
import logging

logger = logging.getLogger(__name__)

class AccessControlHandler(BaseHandler):
    def __init__(self, db_connection):
        super().__init__(self._handle_update)
        self.db = db_connection
        self.priority = -1

    def check_update(self, update: object) -> bool:
        return isinstance(update, Update)

    async def _handle_update(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        if not user:
            return False

        # Проверяем кэш в user_data
        if context.user_data.get('is_verified'):
            return True

        try:
            # Проверяем админов через конфиг
            if user.id in context.application.bot_data.get('admin_ids', []):
                context.user_data['is_verified'] = True
                return True

            # Проверяем обычных пользователей через БД
            user_data = self.db.get_user(user.id)
            if user_data and user_data.get('is_verified') and not user_data.get('is_deleted'):
                context.user_data['is_verified'] = True
                return True

            logger.info(f"Доступ запрещен для пользователя {user.id}")
            await self._deny_access(update)
            return False

        except Exception as e:
            logger.error(f"Ошибка проверки доступа: {e}", exc_info=True)
            return False

    async def _deny_access(self, update: Update):
        try:
            if update.callback_query:
                await update.callback_query.answer(
                    "⛔ Доступ запрещён. Обратитесь к администратору", 
                    show_alert=True
                )
            elif update.message:
                await update.message.reply_text(
                    "⛔ Ваш аккаунт не верифицирован\nОбратитесь к администратору"
                )
        except Exception as e:
            logger.error(f"Ошибка при отображении отказа: {e}")

async def check_user_access(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Проверка доступа без уведомлений администраторам"""
    try:
        # Получаем необходимые данные из контекста
        db = context.bot_data['db']
        admin_ids = context.bot_data.get('admin_ids', [])
        
        if user_id in admin_ids:
            return True
            
        # Проверяем пользователя в БД
        db.cursor.execute("""
            SELECT is_verified, is_deleted 
            FROM users 
            WHERE telegram_id = ?
        """, (user_id,))
        user_data = db.cursor.fetchone()
        
        if user_data:
            is_verified, is_deleted = user_data
            return bool(is_verified and not is_deleted)
        return False
        
    except Exception as e:
        logger.error(f"Ошибка проверки доступа: {e}", exc_info=True)
        return False