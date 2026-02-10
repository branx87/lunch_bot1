# ##decorators.py
from functools import wraps
from telegram import Update
from telegram.ext import ContextTypes
from telegram.ext import filters

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

# Динамические фильтры, которые проверяют права при каждом запросе
class DynamicAdminFilter(filters.MessageFilter):
    """Динамический фильтр для проверки прав администратора"""
    def filter(self, message):
        return message.from_user.id in CONFIG.admin_ids

class DynamicProviderFilter(filters.MessageFilter):
    """Динамический фильтр для проверки прав поставщика"""
    def filter(self, message):
        return message.from_user.id in CONFIG.provider_ids

class DynamicAccountingFilter(filters.MessageFilter):
    """Динамический фильтр для проверки прав бухгалтера"""
    def filter(self, message):
        return message.from_user.id in CONFIG.accounting_ids

class DynamicProviderOrAdminFilter(filters.MessageFilter):
    """Динамический фильтр для проверки прав поставщика или администратора"""
    def filter(self, message):
        return message.from_user.id in CONFIG.provider_ids or message.from_user.id in CONFIG.admin_ids

# Создаем экземпляры фильтров для использования в обработчиках
admin_filter = DynamicAdminFilter()
provider_filter = DynamicProviderFilter()
accounting_filter = DynamicAccountingFilter()
provider_or_admin_filter = DynamicProviderOrAdminFilter()
