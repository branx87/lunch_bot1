# ##view_utils.py
import logging
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from datetime import datetime, timedelta
from telegram.ext import ContextTypes

from config import CONFIG, TIMEZONE
from handlers.common import show_main_menu
from utils import can_modify_order

logger = logging.getLogger(__name__)
    
async def refresh_day_view(query, day_offset, user_db_id, context: ContextTypes.DEFAULT_TYPE):
    """
    Обновляет интерфейс меню дня с информацией о заказе.
    """
    try:
        db = context.bot_data['db']
        now = datetime.now(CONFIG.timezone)  # Используем CONFIG.timezone
        
        days_ru = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
        target_date = (now + timedelta(days=day_offset)).date()
        day_name = days_ru[target_date.weekday()]
        date_str = target_date.strftime("%d.%m")

        # Проверяем выходной или праздник
        if target_date.weekday() >= 5:  # Суббота или воскресенье
            await query.edit_message_text(
                text=f"⏳ {day_name} ({date_str}) - выходной! Меню не предусмотрено.",
                reply_markup=None
            )
            return

        # Используем CONFIG вместо context.bot_data['config']
        holiday_name = CONFIG.holidays.get(target_date.isoformat())
        if holiday_name:
            await query.edit_message_text(
                text=f"🎉 {day_name} ({date_str}) - {holiday_name}! Меню не предусмотрено.",
                reply_markup=None
            )
            return

        # Получаем меню из базы данных
        db.cursor.execute("""
            SELECT first_course, main_course, salad 
            FROM menu 
            WHERE day = ?
        """, (day_name,))
        menu_data = db.cursor.fetchone()

        if not menu_data:
            logger.error(f"Меню не найдено для дня {day_name}. Полный список дней в базе:")
            db.cursor.execute("SELECT day FROM menu")
            for row in db.cursor.fetchall():
                logger.error(f"День в базе: {row['day']}")
            await query.edit_message_text(
                text=f"⚠️ Меню на {day_name} ({date_str}) не найдено",
                reply_markup=None
            )
            return

        menu = {
            'first': menu_data['first_course'],
            'main': menu_data['main_course'],
            'salad': menu_data['salad']
        }

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

        keyboard = []
        if order:
            qty, is_preliminary = order['quantity'], order['is_preliminary']
            order_type = "Предзаказ" if is_preliminary else "Заказ"
            response_text += f"\n\n✅ {order_type}: {qty} порции"
            
            if await can_modify_order(target_date, TIMEZONE):
                keyboard.append([InlineKeyboardButton("✏️ Изменить", callback_data=f"change_{day_offset}")])
                keyboard.append([InlineKeyboardButton("❌ Отменить", callback_data=f"cancel_{day_offset}")])
            else:
                response_text += "\n⏳ Изменение невозможно (время истекло)"
        elif await can_modify_order(target_date, TIMEZONE):
            keyboard.append([InlineKeyboardButton("✅ Заказать", callback_data=f"order_{day_offset}")])
        else:
            response_text += "\n⏳ Приём заказов завершён"

        await query.edit_message_text(
            text=response_text,
            reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
            parse_mode="Markdown"
        )

    except Exception as e:
        logger.error(f"Ошибка обновления дня: {e}", exc_info=True)
        await query.answer("⚠️ Ошибка обновления. Попробуйте позже", show_alert=True)
        
async def refresh_orders_view(query, context: ContextTypes.DEFAULT_TYPE, is_cancellation=False):
    """Обновляет список заказов после изменения количества"""
    try:
        db = context.bot_data['db']
        user = query.from_user
        now = datetime.now(context.bot_data.get('timezone'))
        days_ru = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]

        # Получаем ID пользователя в БД
        with db.conn:
            db.cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (user.id,))
            user_record = db.cursor.fetchone()
            if not user_record:
                await query.answer("❌ Пользователь не найден", show_alert=True)
                return
            user_db_id = user_record[0]

            # Получаем активные заказы
            db.cursor.execute("""
                SELECT o.target_date, o.quantity, o.is_preliminary
                FROM orders o
                WHERE o.user_id = ?
                  AND o.is_cancelled = FALSE
                  AND o.target_date >= ?
                ORDER BY o.target_date
            """, (user_db_id, now.date().isoformat()))
            active_orders = db.cursor.fetchall()

        if not active_orders:
            await query.edit_message_text("ℹ️ У вас нет активных заказов.")
            return await show_main_menu(query.message, context)

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
                InlineKeyboardButton(f"✏️ Изменить {date_str}", 
                                   callback_data=f"change_{target_date.strftime('%Y-%m-%d')}"),
                InlineKeyboardButton(f"✕ Отменить {date_str}", 
                                   callback_data=f"cancel_{target_date.strftime('%Y-%m-%d')}")
            ])

        keyboard.append([InlineKeyboardButton("✔ В главное меню", callback_data="back_to_menu")])

        await query.edit_message_text(
            response,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )

    except Exception as e:
        logger.error(f"Ошибка обновления списка: {e}", exc_info=True)
        await query.edit_message_text("⚠️ Ошибка загрузки заказов")
        await query.answer("Произошла ошибка", show_alert=True)