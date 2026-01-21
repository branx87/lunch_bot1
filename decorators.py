# ##decorators.py
from functools import wraps
from telegram import Update
from telegram.ext import ContextTypes

from database import db
from config import CONFIG

def admin_required(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id not in CONFIG.admin_ids:
            await update.message.reply_text("❌ У вас нет прав для этой команды.")
            return
        return await func(update, context)
    return wrapper
