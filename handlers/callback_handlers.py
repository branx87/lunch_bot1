# ##handlers/callback_handlers.py
import logging
from telegram.ext import ContextTypes
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardRemove
from datetime import datetime, time, timedelta
from database import db, User, Order
from config import CONFIG
from telegram.ext import CommandHandler, MessageHandler, CallbackQueryHandler, ConversationHandler, filters

from handlers.common import show_main_menu
from handlers.common_handlers import view_orders
from handlers.order_callbacks import handle_cancel_callback, handle_change_callback, handle_confirm_callback, handle_order_callback, modify_portion_count
from utils import can_modify_order
from view_utils import refresh_day_view

logger = logging.getLogger(__name__)

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

        # Получаем пользователя через SQLAlchemy
        user_record = db.session.query(User).filter(
            User.telegram_id == user.id
        ).first()
        
        if not user_record:
            await query.answer("❌ Пользователь не найден", show_alert=True)
            return
        user_db_id = user_record.id

        # Получаем заказ через SQLAlchemy
        order = db.session.query(Order).filter(
            Order.user_id == user_db_id,
            Order.target_date == target_date,
            Order.is_cancelled == False
        ).first()
        
        if not order:
            await query.answer("❌ Заказ не найден", show_alert=True)
            return
        
        current_quantity = order.quantity

        if action == "increase":
            if current_quantity >= max_portions:
                await query.answer("ℹ️ Максимальное количество порций (3) достигнуто", show_alert=True)
                return
            order.quantity = current_quantity + 1
            feedback = f"✅ Увеличено до {order.quantity} порций"
            
        elif action == "decrease":
            if current_quantity <= 1:
                # Отмена заказа при уменьшении до 0
                await cancel_order(query, user_db_id, target_date, now)
                return
                
            order.quantity = current_quantity - 1
            feedback = f"✅ Уменьшено до {order.quantity} порций"
            
        else:
            await query.answer("⚠️ Неизвестное действие")
            return

        # Обновляем время заказа и сохраняем
        order.order_time = now.strftime("%H:%M:%S")
        db.session.commit()

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
    # Находим и обновляем заказ через SQLAlchemy
    order = db.session.query(Order).filter(
        Order.user_id == user_db_id,
        Order.target_date == target_date,
        Order.is_cancelled == False
    ).first()
    
    if order:
        order.is_cancelled = True
        order.order_time = now.strftime("%H:%M:%S")
        db.session.commit()
    
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
        now = datetime.now(CONFIG.timezone)
        target_date = (now + timedelta(days=day_offset)).date()
        
        if not can_modify_order(target_date):
            await query.answer("ℹ️ Отмена невозможна после 9:30", show_alert=True)
            return

        # Получаем пользователя через SQLAlchemy
        user_record = db.session.query(User).filter(
            User.telegram_id == query.from_user.id
        ).first()
        
        if not user_record:
            await query.answer("❌ Пользователь не найден", show_alert=True)
            return
        user_db_id = user_record.id

        # Отменяем заказ через SQLAlchemy
        order = db.session.query(Order).filter(
            Order.user_id == user_db_id,
            Order.target_date == target_date,
            Order.is_cancelled == False
        ).first()
        
        if order:
            order.is_cancelled = True
            order.order_time = now.strftime("%H:%M:%S")
            db.session.commit()

        # Обновляем представление дня (а не показываем список заказов)
        await refresh_day_view(query, day_offset, user_db_id, now)
        await query.answer("✅ Заказ отменён")

    except Exception as e:
        logger.error(f"Ошибка при отмене заказа: {e}")
        await query.answer("⚠️ Ошибка при отмене заказа", show_alert=True)

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