# ##handlers/admin_handlers.py
import logging
from telegram import Update
from telegram.ext import ContextTypes
from bot_keyboards import create_admin_keyboard
from db import CONFIG

logger = logging.getLogger(__name__)

async def handle_admin_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Основной обработчик выбора в админ-меню. 
    Проверяет права пользователя и перенаправляет на соответствующие действия:
    - Управление приемом заказов (вкл/выкл)
    - Рассылка сообщений
    - История сообщений
    - Обработка отмены операций
    """
    user = update.effective_user
    text = update.message.text
    
    if user.id not in CONFIG.admin_ids:
        await update.message.reply_text("❌ Нет прав доступа")
        return

    if text == "🔒 Вкл/Выкл заказы":
        # Переключаем статус заказов через CONFIG
        new_status = not CONFIG.orders_enabled
        CONFIG.toggle_orders(new_status)
        
        status = "разрешены ✅" if new_status else "запрещены ❌"
        await update.message.reply_text(
            f"Прием заказов теперь {status}",
            reply_markup=create_admin_keyboard()
        )