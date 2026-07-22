# ##handlers/common_handlers.py
from asyncio.log import logger
from datetime import datetime
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes
from database import db
from models import User, Order
from config import CONFIG
from handlers.common import show_main_menu
from utils import can_modify_order
from services.menu_service import DAYS_RU

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
        today = datetime.now(CONFIG.timezone).date()

        # Получаем пользователя и его активные заказы через SQLAlchemy
        user_record = db.session.query(User).filter(
            User.telegram_id == user_id
        ).first()

        if not user_record:
            logger.error(f"Пользователь с telegram_id {user_id} не найден")
            return

        # Получаем активные заказы через SQLAlchemy
        active_orders = db.session.query(Order).filter(
            Order.user_id == user_record.id,
            Order.is_cancelled == False,
            Order.target_date >= today
        ).order_by(Order.target_date).all()
        
        logger.info(f"Найдено активных заказов: {len(active_orders)}")
        if active_orders:
            logger.info(f"Пример заказа: {active_orders[0].target_date} - {active_orders[0].quantity} порций")

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

        for order in active_orders:
            day_name = DAYS_RU[order.target_date.weekday()]
            date_str = order.target_date.strftime('%d.%m')
            qty = order.quantity
            status = " (предв.)" if order.is_preliminary else ""

            keyboard.append([
                InlineKeyboardButton(
                    f"{day_name} {date_str} - {qty} порц.{status}",
                    callback_data=f"cancel_order_{order.target_date.strftime('%Y-%m-%d')}"
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