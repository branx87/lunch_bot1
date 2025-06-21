# ##view_utils.py
import logging
from telegram import InlineKeyboardButton, Update, InlineKeyboardMarkup
from datetime import datetime, timedelta

from config import MENU
from db import db
from handlers.common import show_main_menu
from utils import can_modify_order

logger = logging.getLogger(__name__)
    
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
        menu = MENU.get(day_name)

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
        db.cursor.execute("""
            SELECT quantity, is_preliminary 
            FROM orders 
            WHERE user_id = ? 
              AND target_date = ?
              AND is_cancelled = FALSE
        """, (user_db_id, target_date.isoformat()))
        order = db.cursor.fetchone()

        # Добавляем информацию о заказе
        keyboard = []
        if order:
            qty, is_preliminary = order
            order_type = "Предзаказ" if is_preliminary else "Заказ"
            response_text += f"\n\n✅ {order_type}: {qty} порции"
            
            if can_modify_order(target_date):
                keyboard.append([InlineKeyboardButton("✏️ Изменить", callback_data=f"change_{day_offset}")])
                keyboard.append([InlineKeyboardButton("❌ Отменить", callback_data=f"cancel_{day_offset}")])
            else:
                response_text += "\n⏳ Изменение невозможно (время истекло)"
        elif can_modify_order(target_date):
            keyboard.append([InlineKeyboardButton("✅ Заказать", callback_data=f"order_{day_offset}")])
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
        db.cursor.execute("""
            SELECT o.target_date, o.quantity, o.is_preliminary
            FROM orders o
            JOIN users u ON o.user_id = u.id
            WHERE u.telegram_id = ?
              AND o.is_cancelled = FALSE
              AND o.target_date >= ?
            ORDER BY o.target_date
        """, (user_id, now.date().isoformat()))

        active_orders = db.cursor.fetchall()

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