# ##handlers/commands.py
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler
from db import db

async def notifications_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Включение уведомлений"""
    db.cursor.execute(
        "UPDATE users SET notifications_enabled = TRUE WHERE telegram_id = ?",
        (update.effective_user.id,)
    )
    db.conn.commit()
    await update.message.reply_text("🔔 Уведомления включены!")

async def notifications_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отключение уведомлений"""
    db.cursor.execute(
        "UPDATE users SET notifications_enabled = FALSE WHERE telegram_id = ?",
        (update.effective_user.id,)
    )
    db.conn.commit()
    await update.message.reply_text("🔕 Уведомления отключены!")

def setup(application):
    application.add_handler(CommandHandler("notifications_on", notifications_on))
    application.add_handler(CommandHandler("notifications_off", notifications_off))