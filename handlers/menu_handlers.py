# ##handlers/menu_handlers.py
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import ContextTypes
from datetime import datetime, time, timedelta, date

from bot_keyboards import create_main_menu_keyboard
from database import db
from models import User, Order
from config import CONFIG
from time_config import TIME_CONFIG  # ← ДОБАВИТЬ ИМПОРТ
from constants import SELECT_MONTH_RANGE_STATS
from handlers.common import show_main_menu
from handlers.common_handlers import view_orders
from utils import can_modify_order, check_registration, format_menu, handle_unregistered
from view_utils import refresh_orders_view

logger = logging.getLogger(__name__)

async def show_today_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отображает меню на текущий день с учетом праздников"""
    user_id = update.effective_user.id
    now = datetime.now(TIME_CONFIG.TIMEZONE)  # ← ИСПРАВИТЬ
    today = now.date()
    days_ru = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
    day_name = days_ru[today.weekday()]
    date_str = today.strftime("%d.%m")
    
    # Проверяем праздник
    holiday_name = CONFIG.holidays.get(today.isoformat())
    if holiday_name:
        await update.message.reply_text(f"🎉 Сегодня ({date_str}) праздник - {holiday_name}! Меню не предусмотрено.")
        return await show_main_menu(update, user_id)
    
    # Проверяем выходной
    if today.weekday() in TIME_CONFIG.WEEKEND_DAYS:  # ← ИСПРАВИТЬ
        await update.message.reply_text(f"⏳ Сегодня ({day_name}, {date_str}) выходной! Меню не предусмотрено.")
        return await show_main_menu(update, user_id)
    
    menu = CONFIG.menu.get(day_name)
    if not menu:
        await update.message.reply_text(f"⏳ На сегодня ({date_str}) меню не загружено.")
        return await show_main_menu(update, user_id)
    
    # Формируем сообщение
    message = f"🍽 Меню на {day_name} ({date_str}):\n"
    message += f"1. 🍲 Первое: {menu['first']}\n"
    message += f"2. 🍛 Основное блюдо: {menu['main']}\n"
    message += f"3. 🥗 Салат: {menu['salad']}"
    
    # Проверяем есть ли активный заказ через SQLAlchemy
    user_record = db.session.query(User).filter(User.telegram_id == user_id).first()
    if user_record:
        order = db.session.query(Order).filter(
            Order.user_id == user_record.id,
            Order.target_date == today,
            Order.is_cancelled == False
        ).first()
        
        has_active_order = order is not None
        order_quantity = order.quantity if order else 0
    else:
        has_active_order = False
        order_quantity = 0

    can_modify = can_modify_order(today)  # ← ИСПРАВИТЬ
    
    keyboard = []
    if not CONFIG.are_orders_accepted_now():
        # Заказы отключены по времени
        status_msg = CONFIG.get_orders_status_message()
        message += f"\n\n{status_msg}"
        keyboard.append([InlineKeyboardButton("⏳ Заказы принимаются через Битрикс", callback_data="noop")])
    elif has_active_order:
        # Есть активный заказ
        message += f"\n\n✅ Заказ: {order_quantity} порции"
        if can_modify:
            keyboard.append([InlineKeyboardButton("✏️ Изменить количество", callback_data="change_0")])
            keyboard.append([InlineKeyboardButton("❌ Отменить заказ", callback_data="cancel_0")])
        else:
            keyboard.append([InlineKeyboardButton("ℹ️ Заказ оформлен (изменение невозможно)", callback_data="noop")])
    else:
        # Нет активного заказа
        message += "\n\n🛒 Заказ: не оформлен"
        if can_modify:
            keyboard.append([InlineKeyboardButton("✅ Заказать", callback_data="order_0")])
        else:
            keyboard.append([InlineKeyboardButton("⏳ Время для заказов истекло", callback_data="noop")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(message, reply_markup=reply_markup)
    return await show_main_menu(update, user_id)

async def show_week_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Отображает меню на всю неделю (7 дней) с учетом выходных и праздников.
    Для каждого дня показывает:
    - Состав меню
    - Статус заказа пользователя (если есть)
    - Кнопки для заказа/изменения (если разрешено временными рамками)
    Обрабатывает случаи отсутствия меню на определенные дни.
    """
    try:
        user = update.effective_user
        now = datetime.now(TIME_CONFIG.TIMEZONE)  # ← ИСПРАВИТЬ
        today = now.date()
        days_ru = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
        
        sent_days = 0
        
        # Инициализируем message ДО использования
        message = ""
        
        # Получаем пользователя один раз
        user_record = db.session.query(User).filter(User.telegram_id == user.id).first()
        if not user_record:
            await update.message.reply_text("❌ Пользователь не найден")
            return await show_main_menu(update, user.id)
        
        for day_offset in range(7):
            day_date = today + timedelta(days=day_offset)
            day_name = days_ru[day_date.weekday()]
            date_str = day_date.strftime("%d.%m")
            date_iso = day_date.isoformat()
            
            # Проверяем праздники
            holiday_name = CONFIG.holidays.get(date_iso)
            if holiday_name:
                await update.message.reply_text(
                    f"🎉 {day_name} ({date_str}) — {holiday_name}! Меню не предусмотрено."
                )
                continue
            
            # Проверяем выходные
            if day_date.weekday() in TIME_CONFIG.WEEKEND_DAYS:  # ← ИСПРАВИТЬ
                await update.message.reply_text(
                    f"⏳ {day_name} ({date_str}) — Выходной! Меню не предусмотрено."
                )
                continue
            
            menu = CONFIG.menu.get(day_name)
            if not menu:
                logger.warning(f"Меню для {day_name} не найдено")
                continue
            
            menu_text = f"🍽 Меню на {day_name} ({date_str}):\n"
            menu_text += f"1. 🍲 Первое: {menu['first']}\n"
            menu_text += f"2. 🍛 Основное блюдо: {menu['main']}\n"
            menu_text += f"3. 🥗 Салат: {menu['salad']}"
            
            # Проверка заказа пользователя через SQLAlchemy
            order = db.session.query(Order).filter(
                Order.user_id == user_record.id,
                Order.target_date == day_date,
                Order.is_cancelled == False
            ).first()
            
            keyboard = []
            if not CONFIG.are_orders_accepted_now():
                # Заказы отключены по времени
                status_msg = CONFIG.get_orders_status_message()
                message += f"\n\n{status_msg}"
                keyboard.append([InlineKeyboardButton("⏳ Заказы принимаются через Битрикс", callback_data="noop")])
            else:
                # Логика для включенных заказов
                if order:
                    menu_text += f"\n\n✅ Заказ: {order.quantity} порции"
                    if can_modify_order(day_date):
                        keyboard.append([InlineKeyboardButton("✏️ Изменить", callback_data=f"change_{day_offset}")])
                elif can_modify_order(day_date):
                    keyboard.append([InlineKeyboardButton("✅ Заказать", callback_data=f"order_{day_offset}")])

            await update.message.reply_text(
                menu_text,
                reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
                parse_mode="Markdown"
            )
            sent_days += 1
        
        if sent_days == 0:
            await update.message.reply_text("ℹ️ На эту неделю меню не загружено")
            
    except Exception as e:
        logger.error(f"Ошибка в show_week_menu: {e}", exc_info=True)
        await update.message.reply_text(
            "⚠️ Ошибка при загрузке меню. Попробуйте позже.",
            reply_markup=create_main_menu_keyboard(user.id)
        )
        
async def show_day_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, day_offset=0):
    """
    Отображает меню на конкретный день (сегодня/завтра/другой день).
    Параметры:
    - day_offset: смещение в днях от текущей даты (0 - сегодня, 1 - завтра и т.д.)
    Формирует сообщение с:
    - Подробным описанием меню
    - Информацией о текущем заказе (если есть)
    - Кнопками действий (заказ/изменение/отмена)
    """
    try:
        user = update.effective_user
        now = datetime.now(TIME_CONFIG.TIMEZONE)  # ← ИСПРАВИТЬ
        days_ru = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
        target_date = now.date() + timedelta(days=day_offset)
        day_name = days_ru[target_date.weekday()]
        date_iso = target_date.isoformat()

        # Проверяем праздник
        if holiday_name := CONFIG.holidays.get(date_iso):
            await update.message.reply_text(f"🎉 {day_name} ({target_date.strftime('%d.%m')}) — {holiday_name}! Меню не предусмотрено.")
            return

        # Проверяем выходной
        if target_date.weekday() in TIME_CONFIG.WEEKEND_DAYS:  # ← ИСПРАВИТЬ
            await update.message.reply_text(f"⏳ {day_name} ({target_date.strftime('%d.%m')}) — Выходной! Меню не предусмотрено.")
            return

        # Получаем меню
        if not (menu := CONFIG.menu.get(day_name)):
            await update.message.reply_text(f"⏳ На {day_name} меню не загружено")
            return

        # Формируем сообщение
        message = format_menu(menu, day_name, is_tomorrow=day_offset == 1)

        # Получаем пользователя через SQLAlchemy
        user_record = db.session.query(User).filter(User.telegram_id == user.id).first()
        if not user_record:
            await update.message.reply_text("❌ Пользователь не найден")
            return
        user_db_id = user_record.id

        # Проверяем существующий заказ через SQLAlchemy
        order = db.session.query(Order).filter(
            Order.user_id == user_db_id,
            Order.target_date == target_date,
            Order.is_cancelled == False
        ).first()

        # Формируем клавиатуру
        keyboard = []
        if not CONFIG.are_orders_accepted_now():
            # Заказы отключены по времени
            status_msg = CONFIG.get_orders_status_message()
            message += f"\n\n{status_msg}"
            keyboard.append([InlineKeyboardButton("⏳ Заказы принимаются через Битрикс", callback_data="noop")])
        else:
            # Существующая логика для включенных заказов
            can_modify = can_modify_order(target_date)
            if order:  # Если заказ уже есть
                message += f"\n\n✅ {'Предзаказ' if day_offset > 0 else 'Заказ'}: {order.quantity} порции"
                if can_modify:
                    keyboard.append([InlineKeyboardButton("✏️ Изменить количество", callback_data=f"change_{day_offset}")])
                keyboard.append([InlineKeyboardButton("❌ Отменить заказ", callback_data=f"cancel_{day_offset}")])
            else:  # Если заказа нет
                if can_modify:
                    keyboard.append([InlineKeyboardButton("✅ Заказать", callback_data=f"order_{day_offset}")])
                else:
                    keyboard.append([InlineKeyboardButton("⏳ Время для заказов истекло", callback_data="noop")])
        
        await update.message.reply_text(
            message,
            reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
            parse_mode="Markdown"
        )

    except Exception as e:
        logger.error(f"Ошибка в show_day_menu: {e}", exc_info=True)
        await update.message.reply_text("⚠️ Ошибка загрузки меню")

async def order_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Центральный обработчик действий с заказами. Разбирает callback-запросы и:
    - Отменяет заказы (проверяя временные ограничения)
    - Изменяет количество порций (заглушка)
    - Подтверждает заказы (заглушка)
    Обновляет интерфейс после выполнения действий.
    """
    try:
        query = update.callback_query
        await query.answer()
        
        user_id = query.from_user.id
        logger.info(f"USER {user_id}: вызвал действие {query.data}")
        
        # Добавляем глобальную проверку
        if not CONFIG.orders_enabled and query.data.startswith(('order_', 'change_', 'confirm_')):
            logger.warning(f"USER {user_id}: попытка заказа при отключенных заказах")
            await query.answer("❌ Приём заказов временно приостановлен", show_alert=True)
            return

        if query.data.startswith("cancel_"):
            try:
                _, date_part = query.data.split("_", 1)
                user_id = query.from_user.id

                # Определяем дату
                now = datetime.now(TIME_CONFIG.TIMEZONE)  # ← ИСПРАВИТЬ
                if '-' in date_part:
                    target_date = datetime.strptime(date_part, "%Y-%m-%d").date()
                elif date_part.isdigit():
                    day_offset = int(date_part)
                    target_date = (now + timedelta(days=day_offset)).date()
                else:
                    raise ValueError(f"Неверный формат даты: {date_part}")

                logger.info(f"USER {user_id}: пытается отменить заказ на {target_date}")

                # Проверяем возможность отмены
                if not can_modify_order(target_date):
                    logger.warning(f"USER {user_id}: отмена невозможна для {target_date} (время истекло)")
                    await query.answer(f"ℹ️ Отмена невозможна после {TIME_CONFIG.MODIFICATION_DEADLINE.strftime('%H:%M')}", show_alert=True)  # ← ИСПРАВИТЬ
                    return

                # Получаем пользователя через SQLAlchemy
                user_record = db.session.query(User).filter(User.telegram_id == user_id).first()
                if not user_record:
                    logger.error(f"USER {user_id}: не найден в базе данных")
                    await query.answer("❌ Пользователь не найден", show_alert=True)
                    return

                user_db_id = user_record.id

                # Отменяем заказ в БД через SQLAlchemy
                order = db.session.query(Order).filter(
                    Order.user_id == user_db_id,
                    Order.target_date == target_date,
                    Order.is_cancelled == False
                ).first()

                if not order:
                    logger.warning(f"USER {user_id}: заказ на {target_date} не найден для отмены")
                    await query.answer("❌ Заказ не найден", show_alert=True)
                    return

                order.is_cancelled = True
                order.order_time = now.strftime("%H:%M:%S")
                db.session.commit()

                logger.info(f"USER {user_id}: успешно отменил заказ на {target_date}")

                # Обновляем интерфейс
                days_ru = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
                if "Меню на" in query.message.text:
                    day_name = days_ru[target_date.weekday()]
                    menu = CONFIG.menu.get(day_name)
                    await query.edit_message_text(
                        text=f"~~{format_menu(menu, day_name)}~~\n❌ Заказ отменён",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("✅ Заказать", callback_data=f"order_{target_date.isoformat()}")]
                        ]),
                        parse_mode="Markdown"
                    )
                else:
                    await refresh_orders_view(query, context, user_id, now, days_ru)

                await query.answer("✅ Заказ отменён")

            except Exception as e:
                logger.error(f"USER {user_id}: ошибка при отмене заказа: {e}")
                await query.answer("⚠️ Ошибка отмены", show_alert=True)

        elif query.data.startswith("change_"):
            await query.answer("🔄 Изменение количества порций временно недоступно")
            return

        elif query.data.startswith("confirm_"):
            await query.answer("✅ Заказ подтверждён")
            return

        else:
            logger.warning(f"USER {user_id}: неизвестный callback: {query.data}")
            await query.answer("⚠️ Неизвестное действие", show_alert=True)

    except Exception as e:
        logger.error(f"USER {user_id}: критическая ошибка в order_action: {e}", exc_info=True)
        await query.answer("⚠️ Серверная ошибка", show_alert=True)

async def monthly_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Инициирует процесс просмотра статистики заказов.
    Предлагает пользователю выбрать период (текущий/прошлый месяц)
    и переходит в состояние ожидания выбора.
    """
    try:
        user = update.effective_user
        reply_markup = ReplyKeyboardMarkup(
            [["Текущий месяц", "Прошлый месяц"], ["Вернуться в главное меню"]],
            resize_keyboard=True,
            one_time_keyboard=True
        )
        await update.message.reply_text(
            "📅 Выберите месяц для статистики:",
            reply_markup=reply_markup
        )
        return SELECT_MONTH_RANGE_STATS
    except Exception as e:
        logger.error(f"Ошибка при запуске monthly_stats: {e}")
        await update.message.reply_text("❌ Произошла ошибка. Попробуйте позже.")
        return await show_main_menu(update, user.id)

async def monthly_stats_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Показывает статистику с разделением на:
    - ✅ Выполненные (заказы, дата которых уже прошла)
    - 🍽 Сегодня (заказы на сегодня, если время > 9:30)
    - ⏳ Предстоящие (будущие заказы, которые можно отменить)
    - ❌ Отмененные (по желанию)
    """
    try:
        user = update.effective_user
        text = update.message.text.strip()
        now = datetime.now(TIME_CONFIG.TIMEZONE)  # ← ИСПРАВИТЬ
        today = now.date()
        current_time = now.time()

        # Обработка кнопки "🔙 Назад"
        if text == "🏠 Главное меню":
            return await monthly_stats(update, context)

        if text == "Вернуться в главное меню":
            return await show_main_menu(update, user.id)

        # Определяем период
        if text == "Текущий месяц":
            start_date = today.replace(day=1)
            month_name = start_date.strftime("%B %Y")
        elif text == "Прошлый месяц":
            start_date = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
            month_name = start_date.strftime("%B %Y")
        else:
            await update.message.reply_text("❌ Неизвестный период.")
            return SELECT_MONTH_RANGE_STATS

        end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)

        # Получаем пользователя через SQLAlchemy
        user_record = db.session.query(User).filter(User.telegram_id == user.id).first()
        if not user_record:
            await update.message.reply_text("❌ Пользователь не найден.")
            return await show_main_menu(update, user.id)

        user_db_id = user_record.id

        # 🔥 ИСПРАВЛЕНИЕ: Используем правильные типы данных для сравнения
        from sqlalchemy import func, case, and_
        
        # Получаем статистику через отдельные запросы для надежности
        completed = db.session.query(func.sum(Order.quantity)).filter(
            Order.user_id == user_db_id,
            Order.target_date < today,
            Order.target_date.between(start_date, end_date),
            Order.is_cancelled == False
        ).scalar() or 0

        today_orders = db.session.query(func.sum(Order.quantity)).filter(
            Order.user_id == user_db_id,
            Order.target_date == today,
            Order.is_cancelled == False
        ).scalar() or 0

        upcoming = db.session.query(func.sum(Order.quantity)).filter(
            Order.user_id == user_db_id,
            Order.target_date > today,
            Order.target_date.between(start_date, end_date),
            Order.is_cancelled == False
        ).scalar() or 0

        cancelled = db.session.query(func.sum(Order.quantity)).filter(
            Order.user_id == user_db_id,
            Order.target_date.between(start_date, end_date),
            Order.is_cancelled == True
        ).scalar() or 0

        # Формируем сообщение
        message_lines = [
            f"📊 Статистика за {month_name}:",
            f"━━━━━━━━━━━━━━━━━━━━",
            f"🍽 Всего порций: *{completed + today_orders + upcoming}*",
            "",
            f"✅ Выполненные: *{completed}*",
        ]

        # Добавляем строку про сегодняшние заказы, если время > ORDER_DEADLINE и есть заказы
        if current_time > TIME_CONFIG.ORDER_DEADLINE and today_orders > 0:  # ← ИСПРАВИТЬ
            message_lines.append(f"🍽 Сегодня: *{today_orders}*")

        message_lines.append(f"⏳ Предстоящие: *{upcoming}*")

        if cancelled > 0:
            message_lines.append(f"❌ Отмененные: *{cancelled}*")

        # Дополнительная информация о предстоящих заказах
        if upcoming > 0:
            order_count = db.session.query(Order).filter(
                Order.user_id == user_db_id,
                Order.target_date > today,
                Order.target_date.between(start_date, end_date),
                Order.is_cancelled == False
            ).count()
            
            next_order = db.session.query(Order).filter(
                Order.user_id == user_db_id,
                Order.target_date > today,
                Order.target_date.between(start_date, end_date),
                Order.is_cancelled == False
            ).order_by(Order.target_date).first()
            
            if next_order:
                next_order_date = next_order.target_date.strftime("%d.%m.%Y")
                message_lines.extend([
                    "",
                    f"Ближайший заказ: *{next_order_date}*",
                    f"Всего предстоящих дней с заказами: *{order_count}*"
                ])

        await update.message.reply_text(
            "\n".join(message_lines),
            parse_mode="Markdown"
        )

    except Exception as e:
        logger.error(f"Ошибка статистики: {e}", exc_info=True)
        await update.message.reply_text("❌ Ошибка при получении статистики.")

    return await show_main_menu(update, user.id)
    
async def handle_order_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обрабатывает подтверждение предварительного заказа.
    Создает запись в БД с флагом is_preliminary=True.
    Особые случаи:
    - В пятницу заказ создается на понедельник
    - Подтверждение только при ответе "Да"
    """
    try:
        text = update.message.text
        user = update.effective_user
        
        if text == "Да":
            now = datetime.now(TIME_CONFIG.TIMEZONE)  # ← ИСПРАВИТЬ
            target_date = now + timedelta(days=1)
            if now.weekday() == 4:  # Пятница -> понедельник
                target_date += timedelta(days=2)
            
            # Получаем пользователя через SQLAlchemy
            user_record = db.session.query(User).filter(User.telegram_id == user.id).first()
            if user_record:
                new_order = Order(
                    user_id=user_record.id,
                    target_date=target_date.date(),
                    order_time=now.strftime("%H:%M:%S"),
                    quantity=1,
                    is_preliminary=True
                )
                db.session.add(new_order)
                db.session.commit()
                await update.message.reply_text(f"✅ Предзаказ на {target_date.strftime('%d.%m')} оформлен!")
            else:
                await update.message.reply_text("❌ Пользователь не найден.")
        else:
            await update.message.reply_text("❌ Заказ отменен.")
        
        return await show_main_menu(update, user.id)
    except Exception as e:
        logger.error(f"Ошибка в handle_order_confirmation: {e}")
        await update.message.reply_text("⚠️ Произошла ошибка. Попробуйте снова.")
        return await show_main_menu(update, user.id)

async def handle_cancel_from_view(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обрабатывает отмену заказа из списка заказов.
    Проверяет возможность отмены (временные ограничения),
    обновляет статус в БД и обновляет интерфейс списка заказов.
    """
    query = update.callback_query
    await query.answer()
    
    try:
        # Получаем дату из callback_data
        target_date_str = query.data.split('_')[-1]
        target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
        
        # Проверяем возможность отмены
        if not can_modify_order(target_date):
            await query.answer(f"ℹ️ Отмена невозможна после {TIME_CONFIG.MODIFICATION_DEADLINE.strftime('%H:%M')}", show_alert=True)  # ← ИСПРАВИТЬ
            return

        # Отменяем заказ
        user_id = query.from_user.id
        now = datetime.now(TIME_CONFIG.TIMEZONE)  # ← ИСПРАВИТЬ
        
        # Находим заказ через SQLAlchemy
        order = db.session.query(Order).join(User).filter(
            User.telegram_id == user_id,
            Order.target_date == target_date,
            Order.is_cancelled == False
        ).first()
        
        if order:
            order.is_cancelled = True
            order.order_time = now.isoformat()
            db.session.commit()
            
            # 🔥 НЕМЕДЛЕННОЕ УДАЛЕНИЕ
            from bitrix.sync import BitrixSync
            sync = BitrixSync()
            await sync.cancel_order_immediate_cleanup(order.id)

            logger.info(f"Пользователь {user_id} отменил заказ на {target_date_str}")
            
            # Обновляем список заказов
            await view_orders(update, context, is_cancellation=True)
            await query.answer(f"✅ Заказ на {target_date.strftime('%d.%m')} отменён")
        else:
            await query.answer("❌ Заказ не найден или уже отменен", show_alert=True)

    except Exception as e:
        logger.error(f"Ошибка при отмене заказа: {e}")
        await query.answer("⚠️ Ошибка при отмене заказа", show_alert=True)

async def quick_order(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Быстрый заказ на 1 порцию на сегодня"""
    try:
        user = update.effective_user
        user_id = user.id
        now = datetime.now(TIME_CONFIG.TIMEZONE)  # ← ИСПРАВИТЬ
        today = now.date()
        
        logger.info(f"USER {user_id}: начат быстрый заказ (username: {user.username or 'N/A'}, first_name: {user.first_name or 'N/A'})")
        
        # Проверяем выходной
        if today.weekday() in TIME_CONFIG.WEEKEND_DAYS:  # ← ИСПРАВИТЬ
            logger.info(f"USER {user_id}: выходной день - быстрый заказ недоступен")
            await update.message.reply_text("⏳ Сегодня выходной! Быстрый заказ недоступен.")
            return await show_main_menu(update, user_id)
        
        # Проверяем праздник
        holiday_name = CONFIG.holidays.get(today.isoformat())
        if holiday_name:
            logger.info(f"USER {user_id}: праздник {holiday_name} - быстрый заказ недоступен")
            await update.message.reply_text(f"🎉 Сегодня праздник - {holiday_name}! Быстрый заказ недоступен.")
            return await show_main_menu(update, user_id)
        
        # Проверяем время (до ORDER_DEADLINE)
        if now.time() >= TIME_CONFIG.ORDER_DEADLINE:  # ← ИСПРАВИТЬ
            logger.info(f"USER {user_id}: время заказа истекло ({now.time()})")
            await update.message.reply_text(f"⏳ Время для заказов истекло (после {TIME_CONFIG.ORDER_DEADLINE.strftime('%H:%M')}).")  # ← ИСПРАВИТЬ
            return await show_main_menu(update, user_id)
        
        # Проверяем, доступны ли заказы в текущее время
        if not CONFIG.are_orders_accepted_now():
            status_msg = CONFIG.get_orders_status_message()  # Получаем правильное сообщение
            logger.info(f"USER {user_id}: заказы недоступны - {status_msg}")
            await update.message.reply_text(status_msg)
            return await show_main_menu(update, user_id)
        
        # Получаем пользователя через SQLAlchemy
        user_record = db.session.query(User).filter(User.telegram_id == user_id).first()
        if not user_record:
            logger.warning(f"USER {user_id}: пользователь не найден в БД")
            await update.message.reply_text("❌ Пользователь не найден.")
            return await show_main_menu(update, user_id)
        
        user_db_id = user_record.id
        logger.info(f"USER {user_id}: найден в БД с ID {user_db_id}")
        
        # Проверяем существующий заказ через SQLAlchemy
        existing_order = db.session.query(Order).filter(
            Order.user_id == user_db_id,
            Order.target_date == today,
            Order.is_cancelled == False
        ).first()
        
        if existing_order:
            # Если заказ уже есть - показываем его
            logger.info(f"USER {user_id}: уже есть заказ на {today} - {existing_order.quantity} порции")
            await update.message.reply_text(
                f"✅ У вас уже есть заказ на сегодня: {existing_order.quantity} порции\n\n"
                f"Чтобы изменить количество, используйте 'Меню на сегодня'."
            )
            return await show_main_menu(update, user_id)
        
        logger.info(f"USER {user_id}: создаю новый быстрый заказ на 1 порцию")
        
        # Создаем новый заказ на 1 порцию через SQLAlchemy
        new_order = Order(
            user_id=user_db_id,
            target_date=today,
            order_time=now.strftime("%H:%M:%S"),
            quantity=1,
            bitrix_quantity_id='821',  # ID для 1 порции в Битрикс
            is_active=True,
            is_preliminary=False,
            created_at=datetime.now()
        )
        db.session.add(new_order)
        db.session.commit()
        order_id = new_order.id
        logger.info(f"USER {user_id}: заказ создан с ID {order_id}")

        # # 🔥 НЕМЕДЛЕННАЯ СИНХРОНИЗАЦИЯ только для быстрых заказов на СЕГОДНЯ после IMMEDIATE_SYNC_TIME
        # if now.time() >= TIME_CONFIG.IMMEDIATE_SYNC_TIME:  # ← ИСПРАВИТЬ
        #     logger.info(f"USER {user_id}: быстрый заказ на сегодня - немедленная синхронизация (время {now.time()})")
        #     try:
        #         from bitrix.sync import BitrixSync
        #         sync = BitrixSync()
        #         success = await sync._push_to_bitrix()
        #         if success:
        #             logger.info(f"USER {user_id}: быстрый заказ на сегодня ID {order_id} синхронизирован с Битрикс")
        #         else:
        #             logger.warning(f"USER {user_id}: ошибка синхронизации быстрого заказа на сегодня ID {order_id}")
        #     except Exception as sync_error:
        #         logger.error(f"USER {user_id}: ошибка синхронизации быстрого заказа на сегодня ID {order_id}: {sync_error}")

        logger.info(f"USER {user_id}: быстрый заказ успешно оформлен - 1 порция на {today}")
        await update.message.reply_text(
            "✅ Быстрый заказ оформлен!\n"
            "• Порции: 1\n" 
            "• Дата: сегодня\n"
            "• Статус: активен"
        )
        
    except Exception as e:
        logger.error(f"USER {user_id}: ошибка в быстром заказе - {e}", exc_info=True)
        await update.message.reply_text("⚠️ Ошибка при оформлении заказа.")
    
    return await show_main_menu(update, user_id)