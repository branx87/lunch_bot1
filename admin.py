# ##admin.py
from datetime import datetime, date, timedelta
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext
from telegram.ext import ContextTypes
import logging
from handlers.common import show_main_menu
# import matplotlib
# matplotlib.use('Agg')  # Используем non-GUI бэкенд
# import matplotlib.pyplot as plt

from database import db
from config import CONFIG
from constants import ADMIN_MESSAGE, MAIN_MENU, SELECT_MONTH_RANGE
from models import User, AdminMessage
from bot_keyboards import create_admin_keyboard
from sqlalchemy import text

try:
    from openpyxl.styles import Font
except RuntimeError:  # Для окружений без GUI
    class Font:
        def __init__(self, bold=False):
            self.bold = bold
import os
from openpyxl import Workbook
from openpyxl.styles import Font

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='bot.log'
)
logger = logging.getLogger(__name__)

def ensure_reports_dir(report_type: str = 'accounting') -> str:
    """Создает папку для отчетов если ее нет и возвращает путь к ней"""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    if report_type == 'provider':
        reports_dir = os.path.join(base_dir, 'data', 'reports', 'provider_reports')
    elif report_type == 'admin':
        reports_dir = os.path.join(base_dir, 'data', 'reports', 'admin_reports')
    else:
        reports_dir = os.path.join(base_dir, 'data', 'reports', 'accounting_reports')
    
    os.makedirs(reports_dir, exist_ok=True)
    
    # Очищаем старые отчеты (оставляем 5 последних)
    report_files = sorted(
        [f for f in os.listdir(reports_dir) if f.endswith('.xlsx')],
        key=lambda x: os.path.getmtime(os.path.join(reports_dir, x)),
        reverse=True
    )
    for old_file in report_files[5:]:
        try:
            os.remove(os.path.join(reports_dir, old_file))
        except Exception as e:
            logger.error(f"Ошибка удаления старого отчета {old_file}: {e}")
    
    return reports_dir

async def message_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показ истории сообщений админам"""
    user = update.effective_user
    logger.info(f"Запрос истории сообщений от {user.id}")

    # Исправленная проверка прав администратора
    if not hasattr(CONFIG, 'admin_ids') or user.id not in CONFIG.admin_ids:
        await update.message.reply_text(
            "❌ У вас нет прав для просмотра истории сообщений.",
            reply_markup=create_admin_keyboard()
        )
        return ADMIN_MESSAGE

    try:
        # Получаем последние 20 сообщений с пагинацией
        page = context.user_data.get('history_page', 0)
        offset = page * 20
        
        with db.get_session() as session:
            # ИСПРАВЛЕННЫЙ ЗАПРОС - используем created_at и правильные JOIN
            messages = session.execute(text("""
                SELECT 
                    m.created_at, 
                    a.full_name AS admin_name,
                    u.full_name AS user_name,
                    m.message_text,
                    CASE 
                        WHEN m.admin_id IS NOT NULL OR m.admin_telegram_id IS NOT NULL THEN 'admin_to_user' 
                        ELSE 'user_to_admin' 
                    END AS direction,
                    m.is_unregistered,
                    m.admin_telegram_id,
                    m.user_telegram_id
                FROM admin_messages m
                LEFT JOIN users a ON m.admin_id = a.id  -- JOIN по id таблицы users
                LEFT JOIN users u ON m.user_id = u.id   -- JOIN по id таблицы users
                ORDER BY m.created_at DESC 
                LIMIT 20 OFFSET :offset
            """), {'offset': offset}).fetchall()

        if not messages:
            await update.message.reply_text(
                "📭 В истории больше нет сообщений",
                reply_markup=create_history_keyboard(page)
            )
            return ADMIN_MESSAGE

        # Формируем ответ
        response = [f"📜 История сообщений (страница {page+1}):\n\n"]
        
        for msg in messages:
            created_at, admin_name, user_name, message_text, direction, is_unregistered, admin_tg_id, user_tg_id = msg
            
            # Форматируем дату
            if isinstance(created_at, datetime):
                date_str = created_at.strftime("%d.%m.%Y %H:%M")
            else:
                date_str = str(created_at)
            
            # Определяем отправителя и получателя
            if direction == 'admin_to_user':
                sender_name = admin_name or f"Админ (ID: {admin_tg_id})" if admin_tg_id else "Админ"
                receiver_name = user_name or f"Пользователь (ID: {user_tg_id})" if user_tg_id else "Пользователь"
                if is_unregistered:
                    receiver_name = f"👤 {receiver_name} (незарегистрированный)"
                else:
                    receiver_name = f"👤 {receiver_name}"
                sender_prefix = "👨‍💼 Админ"
            else:
                sender_name = user_name or f"Пользователь (ID: {user_tg_id})" if user_tg_id else "Пользователь"
                receiver_name = "Администраторам"
                if is_unregistered:
                    sender_name = f"👤 {sender_name} (незарегистрированный)"
                else:
                    sender_name = f"👤 {sender_name}"
                sender_prefix = "👤 Пользователь"
            
            msg_text = (
                f"📅 {date_str}\n"
                f"{sender_prefix}: {sender_name}\n"
                f"➡️ Получатель: {receiver_name}\n"
                f"✉️: {message_text}\n"
                "━━━━━━━━━━━━━━\n"
            )
            response.append(msg_text)

        # Добавляем кнопки навигации
        keyboard = create_history_keyboard(page, len(messages) == 20)
        
        await update.message.reply_text(
            "".join(response),
            reply_markup=keyboard,
            parse_mode="HTML"
        )
            
    except Exception as e:
        logger.error(f"Ошибка при выводе истории: {e}", exc_info=True)
        await update.message.reply_text(
            f"❌ Ошибка при загрузке истории: {str(e)}",
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
    
def _check_access(user_id: int, report_type: str) -> bool:
    """Проверка прав доступа к отчету"""
    if report_type == 'admin' and user_id in CONFIG.admin_ids:
        return True
    if report_type == 'provider' and user_id in CONFIG.provider_ids:
        return True
    if report_type == 'accounting' and user_id in CONFIG.accounting_ids:
        return True
    return False

def create_history_keyboard(current_page=0, has_next=False):
    buttons = []
    if current_page > 0:
        buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"history_prev_{current_page}"))
    if has_next:
        buttons.append(InlineKeyboardButton("Вперёд ➡️", callback_data=f"history_next_{current_page}"))
    
    buttons.append(InlineKeyboardButton("🔙 В меню", callback_data="back_to_menu"))
    
    # ДОБАВЬТЕ ЛОГ ДЛЯ ПРОВЕРКИ
    logger.info(f"Создана клавиатура истории: page={current_page}, has_next={has_next}")
    
    return InlineKeyboardMarkup([buttons])

async def handle_history_pagination(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    data_parts = query.data.split('_')
    if len(data_parts) >= 3:
        action = data_parts[1]
        page = int(data_parts[2])
        
        if action == "prev":
            page = max(0, page - 1)
        else:
            page += 1
        
        context.user_data['history_page'] = page
        await message_history(update, context)
    
async def handle_sync_bitrix(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ручной запуск синхронизации с Bitrix (/sync_bitrix)"""
    user = update.effective_user
    if user.id not in CONFIG.admin_ids:
        await update.message.reply_text("❌ У вас нет прав для этой команды")
        return

    msg = await update.message.reply_text("🔄 Начинаю синхронизацию с Bitrix...")
    
    try:
        from bitrix.sync import BitrixSync
        sync = BitrixSync()
        
        # 1. Синхронизация сотрудников
        await msg.edit_text("🔄 Синхронизирую сотрудников...")
        emp_stats = await sync.sync_employees()
        
        # 2. Синхронизация заказов (текущий месяц)
        await msg.edit_text("🔄 Синхронизирую заказы...")
        today = datetime.now().date()
        start_date = today.replace(day=1).strftime('%Y-%m-%d')
        end_date = today.strftime('%Y-%m-%d')
        order_stats = await sync.sync_orders(start_date, end_date)
        
        # Формируем отчет
        report = (
            "✅ Синхронизация завершена\n\n"
            "👥 Сотрудники:\n"
            f"• Всего: {emp_stats['total']}\n"
            f"• Обновлено: {emp_stats['updated']}\n"
            f"• Добавлено: {emp_stats['added']}\n"
            f"• Ошибок: {emp_stats['errors']}\n\n"
            "🍽 Заказы:\n"
            f"• Всего: {order_stats['processed']}\n"
            f"• Добавлено: {order_stats['added']}\n"
            f"• Обновлено: {order_stats['updated']}\n"
            f"• Ошибок: {order_stats['errors']}"
        )
        
        await msg.edit_text(report)
        
    except Exception as e:
        logger.error(f"Ошибка синхронизации: {e}", exc_info=True)
        await msg.edit_text(f"❌ Ошибка синхронизации: {str(e)}")