# ##view_utils.py
import logging
from telegram import InlineKeyboardButton, Update, InlineKeyboardMarkup
from datetime import datetime, timedelta

from database import db
from config import CONFIG
from models import User, Order
from handlers.common import show_main_menu
from utils import can_modify_order

logger = logging.getLogger(__name__)

def _can_order_for_inspector_by_bitrix_id(user_db_id: int) -> bool:
    """Проверяет по bitrix_id пользователя, может ли он заказывать для инспектора"""
    try:
        with db.get_session() as session:
            user = session.query(User.bitrix_id).filter(User.id == user_db_id).first()
            if user and user.bitrix_id:
                return user.bitrix_id in CONFIG.inspector_allowed_bitrix_ids
    except Exception as e:
        logger.error(f"Ошибка проверки прав инспектора: {e}")
    return False
    
async def refresh_day_view(query, day_offset, user_db_id, now, is_order=False):
    """
    Обновляет интерфейс меню дня с информацией о заказе.
    
    Args:
        query: CallbackQuery от Telegram.
        day_offset: Смещение дней от текущей даты.
        user_db_id: ID пользователя в БД.
        now: Текущая дата и время.
        is_order: Флаг, указывающий на действие заказа.
    """
    try:
        days_ru = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
        target_date = (now + timedelta(days=day_offset)).date()
        day_name = days_ru[target_date.weekday()]
        date_str = target_date.strftime("%d.%m")
        menu = CONFIG.menu.get(day_name)

        # Формируем текст сообщения
        if not menu:
            response_text = f"📅 {day_name} ({date_str}) - выходной! Меню не предусмотрено."
        else:
            response_text = (
                f"🍽 Меню на {day_name} ({date_str}):\n"
                f"1. 🍲 Первое: {menu['first']}\n"
                f"2. 🍛 Основное блюдо: {menu['main']}\n"
                f"3. 🥗 Салат: {menu['salad']}"
            )

        # Проверяем заказ пользователя
        with db.get_session() as session:
            order = session.query(Order.quantity, Order.is_preliminary, Order.is_for_inspector).filter(
                Order.user_id == user_db_id,
                Order.target_date == target_date.isoformat(),
                Order.is_cancelled == False
            ).first()

        # Добавляем информацию о заказе
        keyboard = []
        if order:
            qty, is_preliminary, is_for_inspector = order
            order_type = "Предзаказ" if is_preliminary else "Заказ"
            if is_for_inspector:
                order_type = "🕵️ Заказ для инспектора"
            response_text += f"\n\n✅ {order_type}: {qty} порции"
            
            if can_modify_order(target_date):
                keyboard.append([InlineKeyboardButton("✏️ Изменить", callback_data=f"change_{day_offset}")])
                keyboard.append([InlineKeyboardButton("❌ Отменить", callback_data=f"cancel_{day_offset}")])
            else:
                response_text += "\n⏳ Изменение невозможно (время истекло)"
        elif can_modify_order(target_date):
            keyboard.append([InlineKeyboardButton("✅ Заказать", callback_data=f"order_{day_offset}")])
            # 🔥 Кнопка для заказа инспектору (проверка по bitrix_id)
            if _can_order_for_inspector_by_bitrix_id(user_db_id):
                keyboard.append([InlineKeyboardButton("🕵️ Заказать инспектору", callback_data=f"inspector_{day_offset}")])
        else:
            response_text += "\n⏳ Приём заказов завершён"

        # Отправляем обновлённое сообщение
        await query.edit_message_text(
            text=response_text,
            reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
            parse_mode="Markdown"
        )

    except Exception as e:
        logger.error(f"Ошибка обновления дня: {e}", exc_info=True)
        await query.answer("⚠️ Ошибка обновления. Попробуйте позже", show_alert=True)
        
async def refresh_orders_view(query, context, user_id, now, days_ru):
    """Обновляет список заказов после изменения количества"""
    try:
        with db.get_session() as session:
            active_orders = session.query(
                Order.target_date, 
                Order.quantity, 
                Order.is_preliminary
            ).join(
                User, Order.user_id == User.id
            ).filter(
                User.telegram_id == user_id,
                Order.is_cancelled == False,
                Order.target_date >= now.date().isoformat()
            ).order_by(Order.target_date).all()

        if not active_orders:
            await query.edit_message_text("ℹ️ У вас нет активных заказов.")
            return await show_main_menu(query.message, user_id)

        response = "📦 Ваши активные заказы:\n"
        keyboard = []

        for order in active_orders:
            target_date = datetime.strptime(order[0], "%Y-%m-%d").date()
            day_name = days_ru[target_date.weekday()]
            date_str = target_date.strftime('%d.%m')
            qty = order[1]
            status = " (предварительный)" if order[2] else ""

            response += f"📅 {day_name} ({date_str}) - {qty} порций{status}\n"
            keyboard.append([
                InlineKeyboardButton(f"✏️ Изменить {date_str}", callback_data=f"change_{target_date.strftime('%Y-%m-%d')}"),
                InlineKeyboardButton(f"✕ Отменить {date_str}", callback_data=f"cancel_{target_date.strftime('%Y-%m-%d')}")
            ])

        keyboard.append([InlineKeyboardButton("✔ В главное меню", callback_data="back_to_menu")])

        await query.edit_message_text(
            response,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )

    except Exception as e:
        logger.error(f"Ошибка обновления списка: {e}")
        await query.edit_message_text("⚠️ Ошибка загрузки заказов")