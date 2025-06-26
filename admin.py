# ##admin.py
from datetime import datetime, date, timedelta
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext, ContextTypes, MessageHandler, filters, CallbackQueryHandler
import logging
import matplotlib
matplotlib.use('Agg')

from config import CONFIG
from constants import ADMIN_MESSAGE, MAIN_MENU, SELECT_MONTH_RANGE
from bot_keyboards import create_admin_keyboard, create_history_keyboard
from pathlib import Path
from typing import Optional, List, Dict, Any
from telegram import Update
from telegram.ext import ContextTypes
import logging

logger = logging.getLogger(__name__)

def setup_admin_handlers(application, db_connection):
    """Регистрация обработчиков администратора"""
    from . import message_history  # Локальный импорт
    
    # Создаем обертку для передачи db
    async def wrapped_message_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
        return await message_history(update, context, db_connection)
    
    # Регистрируем обработчик с оберткой
    application.add_handler(MessageHandler(
        filters.Regex("^📜 История сообщений$") & filters.User(user_id=CONFIG.admin_ids),
        wrapped_message_history
    ))
    
    application.add_handler(CallbackQueryHandler(
        lambda update, context: handle_history_pagination(update, context, db_connection),
        pattern="^history_(prev|next)_\\d+$"
    ))
    
def ensure_reports_dir(report_type: str = 'accounting') -> Path:
    """Создает папку для отчетов если ее нет и возвращает путь к ней"""
    reports_dir = CONFIG.reports_dir / report_type
    reports_dir.mkdir(parents=True, exist_ok=True)
    
    # Очищаем старые отчеты (оставляем 5 последних)
    report_files = sorted(
        [f for f in reports_dir.glob('*.xlsx')],
        key=lambda x: x.stat().st_mtime,
        reverse=True
    )
    for old_file in report_files[5:]:
        try:
            old_file.unlink()
        except Exception as e:
            logger.error(f"Ошибка удаления старого отчета {old_file}: {e}")
    
    return reports_dir

async def message_history(update: Update, context: ContextTypes.DEFAULT_TYPE, db):
    """Показ истории сообщений админам"""
    user = update.effective_user
    logger.info(f"Запрос истории сообщений от {user.id}")

    if user.id not in CONFIG.admin_ids:
        await update.message.reply_text(
            "❌ У вас нет прав для просмотра истории сообщений.",
            reply_markup=create_admin_keyboard()
        )
        return ADMIN_MESSAGE

    try:
        db = context.bot_data['db']  # Добавляем эту строку
        page = context.user_data.get('history_page', 0)
        offset = page * 20
        
        db.cursor.execute("""
            SELECT 
                m.sent_at, 
                a.full_name AS admin_name,
                u.full_name AS user_name,
                m.message_text,
                CASE WHEN m.admin_id IS NOT NULL THEN 'admin_to_user' ELSE 'user_to_admin' END AS direction
            FROM admin_messages m
            LEFT JOIN users a ON m.admin_id = a.telegram_id
            LEFT JOIN users u ON m.user_id = u.telegram_id
            ORDER BY m.sent_at DESC 
            LIMIT 20 OFFSET ?
        """, (offset,))
        messages = db.cursor.fetchall()

        if not messages:
            await update.message.reply_text(
                "📭 В истории больше нет сообщений",
                reply_markup=create_history_keyboard(page)
            )
            return ADMIN_MESSAGE

        response = ["📜 История сообщений (страница {page+1}):\n\n"]
        
        for msg in messages:
            sent_at, admin_name, user_name, message_text, direction = msg
            msg_text = (
                f"📅 {sent_at}\n"
                f'{"👨‍💼 Админ" if direction == "admin_to_user" else "👤 Пользователь"}: '
                f"{admin_name if direction == 'admin_to_user' else user_name}\n"
                f"✉️: {message_text}\n"
                "━━━━━━━━━━━━━━\n"
            )
            response.append(msg_text)

        await update.message.reply_text(
            "".join(response),
            reply_markup=create_history_keyboard(page, len(messages) == 20),
            parse_mode="HTML"
        )
            
    except Exception as e:
        logger.error(f"Ошибка при выводе истории: {e}", exc_info=True)
        await update.message.reply_text(
            f"❌ Ошибка: {str(e)}",
            reply_markup=create_admin_keyboard()
        )
    
    return ADMIN_MESSAGE

async def handle_export_orders_for_month(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатия кнопки 'Выгрузить заказы за месяц'"""
    if update.effective_user.id not in CONFIG.provider_ids:
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды.")
        return MAIN_MENU

    keyboard = [["Текущий месяц"], ["Прошлый месяц"], ["Вернуться в главное меню"]]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text("Выберите период для выгрузки:", reply_markup=reply_markup)
    return SELECT_MONTH_RANGE

async def handle_history_pagination(update: Update, context: ContextTypes.DEFAULT_TYPE, db):
    query = update.callback_query
    await query.answer()
    
    action, _, page = query.data.split('_')
    page = int(page)
    
    if action == "prev":
        page = max(0, page - 1)
    else:
        page += 1
    
    context.user_data['history_page'] = page
    await message_history(update, context, db)