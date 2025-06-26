# ##handlers/callback_handlers.py
import logging
from telegram.ext import ContextTypes
from telegram import Update
from datetime import datetime, timedelta

from utils import can_modify_order
from view_utils import refresh_day_view

logger = logging.getLogger(__name__)

async def handle_quantity_change(query, now, user, context: ContextTypes.DEFAULT_TYPE):
    """
    Обработчик изменения количества порций в заказе.
    """
    try:
        action, day_offset_str = query.data.split("_", 1)
        day_offset = int(day_offset_str)
        target_date = (now + timedelta(days=day_offset)).date()
        max_portions = 3

        db = context.bot_data['db']
        timezone = context.bot_data['timezone']

        if not await can_modify_order(target_date, timezone):
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
                await cancel_order(query, user_db_id, target_date, now, context)
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

        await refresh_day_view(query, day_offset, user_db_id, context)
        await query.answer(feedback)

    except Exception as e:
        logger.error(f"Ошибка изменения количества ({action}): {e}")
        await query.answer("⚠️ Ошибка изменения. Попробуйте позже", show_alert=True)

async def cancel_order(query, user_db_id, target_date, now, context: ContextTypes.DEFAULT_TYPE):
    """
    Общая функция отмены заказа
    """
    db = context.bot_data['db']
    with db.conn:
        db.cursor.execute("""
            UPDATE orders
            SET is_cancelled = TRUE,
                order_time = ?
            WHERE user_id = ?
              AND target_date = ?
              AND is_cancelled = FALSE
        """, (now.strftime("%H:%M:%S"), user_db_id, target_date.isoformat()))
    
    day_offset = (target_date - now.date()).days
    await refresh_day_view(query, day_offset, user_db_id, context)
    await query.answer("ℹ️ Заказ отменён")

async def handle_cancel_order(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обработчик отмены конкретного заказа.
    """
    query = update.callback_query
    await query.answer()
    
    try:
        _, day_offset_str = query.data.split("_", 1)
        day_offset = int(day_offset_str)
        now = datetime.now(context.bot_data['timezone'])
        target_date = (now + timedelta(days=day_offset)).date()
        
        if not await can_modify_order(target_date, context.bot_data['timezone']):
            await query.answer("ℹ️ Отмена невозможна после 9:30", show_alert=True)
            return

        db = context.bot_data['db']
        db.cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (query.from_user.id,))
        user_record = db.cursor.fetchone()
        if not user_record:
            await query.answer("❌ Пользователь не найден", show_alert=True)
            return
        user_db_id = user_record[0]

        with db.conn:
            db.cursor.execute("""
                UPDATE orders
                SET is_cancelled = TRUE,
                    order_time = ?
                WHERE user_id = ?
                  AND target_date = ?
                  AND is_cancelled = FALSE
            """, (now.strftime("%H:%M:%S"), user_db_id, target_date.isoformat()))

        await refresh_day_view(query, day_offset, user_db_id, context)
        await query.answer("✅ Заказ отменён")

    except Exception as e:
        logger.error(f"Ошибка при отмене заказа: {e}")
        await query.answer("⚠️ Ошибка при отмене заказа", show_alert=True)

async def handle_back_callback(query, now, user, context: ContextTypes.DEFAULT_TYPE):
    """
    Обработчик кнопки 'Назад'.
    """
    try:
        _, day_offset_str = query.data.split("_", 1)
        day_offset = int(day_offset_str)
        db = context.bot_data['db']
        db.cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (user.id,))
        user_record = db.cursor.fetchone()
        if user_record:
            await refresh_day_view(query, day_offset, user_record[0], context)
            await query.answer("Возврат к меню")
    except Exception as e:
        logger.error(f"Ошибка в handle_back_callback: {e}")
        await query.answer("⚠️ Ошибка возврата", show_alert=True)