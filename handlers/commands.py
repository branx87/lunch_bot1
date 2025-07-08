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
    
async def check_sync(update: Update, context: ContextTypes.DEFAULT_TYPE):
    synced_users = db.execute('''
        SELECT u.id, u.full_name, b.bitrix_id
        FROM users u
        JOIN bitrix_mapping b ON u.id = b.local_id AND b.local_type = 'user'
    ''')
    
    if not synced_users:
        await update.message.reply_text("Нет синхронизированных пользователей.")
        return
    
    msg = "Сопоставленные сотрудники:\n" + "\n".join(
        f"{u[0]}: {u[1]} → Bitrix ID {u[2]}" for u in synced_users
    )
    await update.message.reply_text(msg)

def setup_commands(app):
    app.add_handler(CommandHandler("checksync", check_sync))