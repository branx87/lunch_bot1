# ##handlers/callback_handlers.py
import logging
from telegram.ext import ContextTypes
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardRemove
from datetime import datetime, time, timedelta
from config import TIMEZONE
from db import db
from telegram.ext import CommandHandler, MessageHandler, CallbackQueryHandler, ConversationHandler, filters
import sqlite3


from handlers.common import show_main_menu
from handlers.common_handlers import view_orders
from handlers.order_callbacks import handle_cancel_callback, handle_change_callback, handle_confirm_callback, handle_order_callback, modify_portion_count
from utils import can_modify_order
from view_utils import refresh_day_view

logger = logging.getLogger(__name__)

# async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     query = update.callback_query
#     if not query:
#         return
        
#     await query.answer()
    
#     # Если это callback удаления, пропускаем (он уже обработан)
#     if query.data.startswith(('del_admin_', 'del_provider_', 'del_staff_', 'cancel_delete')):
#         return

async def handle_quantity_change(query, now, user, context):
    """
    Обработчик изменения количества порций в заказе.
    Выполняет:
    - Увеличение/уменьшение количества порций с проверкой лимитов
    - Автоматическую отмену заказа при уменьшении до 0 порций
    - Проверку временного окна для изменений (до 9:30)
    - Обновление представления дня через refresh_day_view
    """
    try:
        action, day_offset_str = query.data.split("_", 1)
        day_offset = int(day_offset_str)
        target_date = (now + timedelta(days=day_offset)).date()
        max_portions = 3

        if not can_modify_order(target_date):
            await query.answer("ℹ️ Изменение невозможно после 9:30", show_alert=True)
            return

        db.cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (user.id,))
        user_record = db.cursor.fetchone()
        if not user_record:
            await query.answer("❌ Пользователь не найден", show_alert=True)
            return
        user_db_id = user_record[0]

        db.cursor.execute("""
            SELECT quantity 
            FROM orders 
            WHERE user_id = ?
              AND target_date = ?
              AND is_cancelled = FALSE
        """, (user_db_id, target_date.isoformat()))
        result = db.cursor.fetchone()
        if not result:
            await query.answer("❌ Заказ не найден", show_alert=True)
            return
        current_quantity = result[0]

        if action == "increase":
            if current_quantity >= max_portions:
                await query.answer("ℹ️ Максимальное количество порций (3) достигнуто", show_alert=True)
                return
            new_quantity = current_quantity + 1
            feedback = f"✅ Увеличено до {new_quantity} порций"
            
        elif action == "decrease":
            if current_quantity <= 1:
                # Отмена заказа при уменьшении до 0
                await cancel_order(query, user_db_id, target_date, now)
                return
                
            new_quantity = current_quantity - 1
            feedback = f"✅ Уменьшено до {new_quantity} порций"
            
        else:
            await query.answer("⚠️ Неизвестное действие")
            return

        with db.conn:
            db.cursor.execute("""
                UPDATE orders
                SET quantity = ?,
                    order_time = ?
                WHERE user_id = ?
                  AND target_date = ?
                  AND is_cancelled = FALSE
            """, (new_quantity, now.strftime("%H:%M:%S"), user_db_id, target_date.isoformat()))

        await refresh_day_view(query, day_offset, user_db_id, now)
        await query.answer(feedback)

    except Exception as e:
        logger.error(f"Ошибка изменения количества ({action}): {e}")
        await query.answer("⚠️ Ошибка изменения. Попробуйте позже", show_alert=True)

# --- Callback для отмены заказа ---

async def cancel_order(query, user_db_id, target_date, now):
    """
    Общая функция отмены заказа
    """
    with db.conn:
        db.cursor.execute("""
            UPDATE orders
            SET is_cancelled = TRUE,
                order_time = ?
            WHERE user_id = ?
              AND target_date = ?
              AND is_cancelled = FALSE
        """, (now.strftime("%H:%M:%S"), user_db_id, target_date.isoformat()))
    
    # После отмены просто обновляем представление дня
    day_offset = (target_date - now.date()).days
    await refresh_day_view(query, day_offset, user_db_id, now)
    await query.answer("ℹ️ Заказ отменён")

async def handle_cancel_order(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обработчик отмены конкретного заказа. Выполняет:
    - Проверку возможности отмены (временное окно до 9:30)
    - Обновление статуса заказа в базе данных
    - Визуальное подтверждение отмены
    - Обновление списка заказов пользователя
    """
    query = update.callback_query
    await query.answer()
    
    try:
        # Получаем данные из callback
        _, day_offset_str = query.data.split("_", 1)
        day_offset = int(day_offset_str)
        now = datetime.now(TIMEZONE)
        target_date = (now + timedelta(days=day_offset)).date()
        
        if not can_modify_order(target_date):
            await query.answer("ℹ️ Отмена невозможна после 9:30", show_alert=True)
            return

        # Получаем ID пользователя в БД
        db.cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (query.from_user.id,))
        user_record = db.cursor.fetchone()
        if not user_record:
            await query.answer("❌ Пользователь не найден", show_alert=True)
            return
        user_db_id = user_record[0]

        # Отменяем заказ
        with db.conn:
            db.cursor.execute("""
                UPDATE orders
                SET is_cancelled = TRUE,
                    order_time = ?
                WHERE user_id = ?
                  AND target_date = ?
                  AND is_cancelled = FALSE
            """, (now.strftime("%H:%M:%S"), user_db_id, target_date.isoformat()))

        # Обновляем представление дня (а не показываем список заказов)
        await refresh_day_view(query, day_offset, user_db_id, now)
        await query.answer("✅ Заказ отменён")

    except Exception as e:
        logger.error(f"Ошибка при отмене заказа: {e}")
        await query.answer("⚠️ Ошибка при отмене заказа", show_alert=True)

# Перенес в handlers\order_callbacks.py
# async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     """
#     Главный обработчик callback-запросов. Распределяет обработку по типам действий:
#     - Изменение количества порций (увеличение/уменьшение)
#     - Изменение, отмена, подтверждение заказов
#     - Создание новых заказов
#     - Навигационные команды (возврат в меню, обновление)
#     Логирует неизвестные callback-запросы
#     """
#     query = update.callback_query
#     await query.answer()
#     now = datetime.now(TIMEZONE)
#     user = update.effective_user
    
#     try:
#         if query.data.startswith("inc_"):
#             await modify_portion_count(query, now, user, context, +1)
#         elif query.data.startswith("dec_"):
#             await modify_portion_count(query, now, user, context, -1)
#         elif query.data.startswith("change_"):
#             await handle_change_callback(query, now, user, context)
#         elif query.data.startswith("cancel_"):
#             await handle_cancel_callback(query, now, user, context)
#         elif query.data.startswith("confirm_"):
#             await handle_confirm_callback(query, now, user, context)
#         elif query.data.startswith("order_"):
#             await handle_order_callback(query, now, user, context)
#         elif query.data == "back_to_menu":
#             await show_main_menu(query.message, user.id)
#         elif query.data == "noop":
#             await query.answer()  # Пустое действие
#         elif query.data == "refresh":
#             pass  # Логика обновления, если нужно
#         else:
#             logger.warning(f"Неизвестный callback: {query.data}")
#             await query.answer("⚠️ Неизвестная команда")

#     except Exception as e:
#         logger.error(f"Ошибка в callback_handler: {e}", exc_info=True)
#         try:
#             await query.answer("⚠️ Произошла ошибка. Попробуйте позже")
#         except Exception as inner_e:
#             logger.error(f"Ошибка при обработке callback: {inner_e}")

async def handle_back_callback(query, now, user, context):
    """
    Обработчик кнопки 'Назад'. 
    Обновляет представление дня через refresh_day_view,
    возвращая пользователя к предыдущему состоянию интерфейса.
    """
    try:
        _, day_offset_str = query.data.split("_", 1)
        day_offset = int(day_offset_str)
        await refresh_day_view(query, day_offset, context.user_data['user_db_id'], now)
        await query.answer("Возврат к меню")
    except Exception as e:
        logger.error(f"Ошибка в handle_back_callback: {e}")
        await query.answer("⚠️ Ошибка возврата", show_alert=True)
