# ##handlers/common_handlers.py
from asyncio.log import logger
from datetime import datetime
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes
from config import TIMEZONE
from db import db
from handlers.common import show_main_menu
from utils import can_modify_order

# --- Просмотр заказов ---
async def view_orders(update: Update, context: ContextTypes.DEFAULT_TYPE, is_cancellation=False):
    """
    Отображает список активных заказов пользователя
    Параметры:
    - update: Объект Update от Telegram
    - context: Контекст обработчика
    - is_cancellation: Флаг, указывающий что вызов произошел после отмены заказа
    
    Функционал:
    - Получает активные заказы из БД (не отмененные и на будущие даты)
    - Формирует интерактивное сообщение с кнопками отмены для каждого заказа
    - Обрабатывает случаи отсутствия заказов
    - Поддерживает как вызов из сообщения, так и из callback-запроса
    """
    logger.info("=== Начало обработки view_orders ===")
    try:
        # Определяем источник вызова
        query = update.callback_query if hasattr(update, 'callback_query') else None
        message = query.message if query else update.message
        user = query.from_user if query else update.effective_user
        
        logger.info(f"Источник вызова: {'callback' if query else 'message'}")
        logger.info(f"Пользователь ID: {user.id if user else 'не определен'}")

        if not message or not user:
            logger.error("Не удалось определить сообщение или пользователя")
            return

        user_id = user.id
        today = datetime.now(TIMEZONE).date()
        today_str = today.isoformat()

        # Получаем активные заказы
        db.cursor.execute("""
            SELECT target_date, quantity, is_preliminary
            FROM orders 
            WHERE user_id = (SELECT id FROM users WHERE telegram_id = ?)
            AND is_cancelled = FALSE
            AND target_date >= ?
            ORDER BY target_date
        """, (user_id, today_str))
        active_orders = db.cursor.fetchall()
        logger.info(f"Найдено активных заказов: {len(active_orders)}")
        if active_orders:
            logger.info(f"Пример заказа: {active_orders[0]}")

        # Если заказов нет
        if not active_orders:
            text = "✅ Все заказы отменены." if is_cancellation else "ℹ️ У вас нет активных заказов."
            if query:
                try:
                    await query.edit_message_text(text)
                except:
                    await query.message.reply_text(text)
            else:
                await message.reply_text(text)
            return await show_main_menu(message, user_id)

        # Формируем сообщение
        response = "📦 <b>Ваши активные заказы:</b>\n\n"
        response += "<i>Нажмите на заказ, чтобы отменить его</i>\n\n"
        
        keyboard = []
        days_ru = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]

        for order in active_orders:
            target_date = datetime.strptime(order[0], "%Y-%m-%d").date()
            day_name = days_ru[target_date.weekday()]
            date_str = target_date.strftime('%d.%m')
            qty = order[1]
            status = " (предв.)" if order[2] else ""

            keyboard.append([
                InlineKeyboardButton(
                    f"{day_name} {date_str} - {qty} порц.{status}",
                    callback_data=f"cancel_order_{target_date.strftime('%Y-%m-%d')}"
                )
            ])

        # Добавляем кнопку возврата в меню
        keyboard.append([
            InlineKeyboardButton("🔙 Назад", callback_data="back_to_main_menu")
        ])

        reply_markup = InlineKeyboardMarkup(keyboard)

        # Отправляем или обновляем сообщение
        if query:
            try:
                await query.edit_message_text(
                    text=response,
                    reply_markup=reply_markup,
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"Ошибка редактирования: {e}")
                await query.message.reply_text(
                    text=response,
                    reply_markup=reply_markup,
                    parse_mode="HTML"
                )
        else:
            await message.reply_text(
                text=response,
                reply_markup=reply_markup,
                parse_mode="HTML"
            )

    except Exception as e:
        logger.error(f"Критическая ошибка в view_orders: {str(e)}", exc_info=True)
        error_msg = "⚠️ Ошибка загрузки заказов"
        if query:
            await query.answer(error_msg)
        await show_main_menu(message, user_id)