# ##handlers/admin_handlers.py
import logging
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler
from bot_keyboards import create_admin_keyboard
from database import db
from config import CONFIG

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
        hint = (
            "Заказы принимаются во всех мессенджерах."
            if new_status else
            "Заказы не принимаются во всех мессенджерах (Telegram, VK, Bitrix24).\n"
            "Утренние напоминания отключены.\n"
            "Действие вступит в силу в течение 5 секунд."
        )
        await update.message.reply_text(
            f"Прием заказов теперь {status}\n\n{hint}",
            reply_markup=create_admin_keyboard()
        )

async def manual_sync(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ручная синхронизация с Bitrix24"""
    if update.effective_user.id not in CONFIG.admin_ids:
        await update.message.reply_text("⛔ У вас нет прав для этой команды")
        return
    
    try:
        from bitrix.sync import BitrixSync
        async with BitrixSync() as sync:
            await update.message.reply_text("🔄 Начата синхронизация с Bitrix24...")
            await sync._push_to_bitrix()
        await update.message.reply_text("✅ Синхронизация завершена")
    except Exception as e:
        logger.error(f"Ошибка ручной синхронизации: {e}", exc_info=True)
        await update.message.reply_text("⚠️ Ошибка синхронизации. Проверьте логи.")

async def toggle_orders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id not in CONFIG.admin_ids:
        await update.message.reply_text("❌ Нет прав доступа")
        return

    new_status = not CONFIG.orders_enabled
    CONFIG.toggle_orders(new_status)

    status = "разрешены ✅" if new_status else "запрещены ❌"
    status_msg = CONFIG.get_orders_status_message()
    
    await update.message.reply_text(
        f"Глобальный статус заказов: {status}\n"
        f"Текущий статус: {status_msg}"
    )

# В конец файла добавьте регистрацию команды:
def setup_admin_handlers(application):
    application.add_handler(CommandHandler("sync_bitrix", manual_sync))
