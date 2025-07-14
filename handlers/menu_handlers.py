# ##handlers/menu_handlers.py
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import ContextTypes
from datetime import datetime, timedelta, date

from bot_keyboards import create_main_menu_keyboard
from db import CONFIG
from constants import SELECT_MONTH_RANGE_STATS
from db import db
from handlers.common import show_main_menu
from handlers.common_handlers import view_orders
from utils import can_modify_order, check_registration, format_menu, handle_unregistered
from view_utils import refresh_orders_view


logger = logging.getLogger(__name__)

async def show_today_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отображает меню на текущий день с учетом праздников"""
    user_id = update.effective_user.id
    now = datetime.now(CONFIG.timezone)
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
    if today.weekday() >= 5:
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
    
    # Проверяем есть ли активный заказ
    db.cursor.execute(
        "SELECT quantity FROM orders WHERE user_id = "
        "(SELECT id FROM users WHERE telegram_id = ?) AND target_date = ? AND is_cancelled = FALSE",
        (user_id, today.isoformat())
    )
    has_active_order = db.cursor.fetchone() is not None

    can_modify = can_modify_order(today)
    
    keyboard = []
    if not CONFIG.orders_enabled:
        # Заказы отключены глобально
        message += "\n\n⚠️ Приём заказов временно приостановлен"
        keyboard.append([InlineKeyboardButton("⏳ Заказы принимаются через Битрикс", callback_data="noop")])
    elif has_active_order:
        # Замените строку с ошибкой на:
        db.cursor.execute("""
            SELECT quantity FROM orders 
            WHERE user_id = ? AND target_date = ? AND is_cancelled = FALSE
        """, (user_id, today.isoformat()))
        order = db.cursor.fetchone()
        message += f"\n\n✅ Заказ: {order[0]} порции" if order else "\n\n🛒 Заказ: не оформлен"
        if can_modify:
            keyboard.append([InlineKeyboardButton("✏️ Изменить количество", callback_data="change_0")])
            keyboard.append([InlineKeyboardButton("❌ Отменить заказ", callback_data="cancel_0")])
        else:
            keyboard.append([InlineKeyboardButton("ℹ️ Заказ оформлен (изменение невозможно)", callback_data="noop")])
    else:
        # Нет активного заказа
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
        now = datetime.now(CONFIG.timezone)
        today = now.date()
        days_ru = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
        
        sent_days = 0
        
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
            if day_date.weekday() >= 5:
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
            
            # Проверка заказа пользователя
            db.cursor.execute("""
                SELECT quantity FROM orders 
                WHERE user_id = (SELECT id FROM users WHERE telegram_id = ?)
                AND target_date = ?
                AND is_cancelled = FALSE
            """, (user.id, date_iso))
            order = db.cursor.fetchone()
            
            keyboard = []
            if not CONFIG.orders_enabled:
                # Добавляем информационную кнопку, если заказы отключены
                menu_text += "\n\n⚠️ Приём заказов временно приостановлен"
                keyboard.append([InlineKeyboardButton("⏳ Заказы принимаются через Битрикс", callback_data="noop")])
            else:
                # Логика для включенных заказов
                if order:
                    menu_text += f"\n✅ Заказ: {order[0]} порции"
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
            
    except Exception as e:
        logger.error(f"Ошибка в show_week_menu: {e}", exc_info=True)
        # from bot_keyboards import create_main_menu_keyboard  # Добавляем импорт
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
        now = datetime.now(CONFIG.timezone)
        days_ru = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
        target_date = now.date() + timedelta(days=day_offset)
        day_name = days_ru[target_date.weekday()]
        date_iso = target_date.isoformat()

        # Проверяем праздник
        if holiday_name := CONFIG.holidays.get(date_iso):
            await update.message.reply_text(f"🎉 {day_name} ({target_date.strftime('%d.%m')}) — {holiday_name}! Меню не предусмотрено.")
            return

        # Проверяем выходной
        if target_date.weekday() >= 5:
            await update.message.reply_text(f"⏳ {day_name} ({target_date.strftime('%d.%m')}) — Выходной! Меню не предусмотрено.")
            return

        # Получаем меню
        if not (menu := CONFIG.menu.get(day_name)):
            await update.message.reply_text(f"⏳ На {day_name} меню не загружено")
            return

        # Формируем сообщение
        message = format_menu(menu, day_name, is_tomorrow=day_offset == 1)

        # Получаем ID пользователя
        if not (user_record := db.cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (user.id,)).fetchone()):
            await update.message.reply_text("❌ Пользователь не найден")
            return
        user_db_id = user_record[0]

        # Проверяем существующий заказ
        order = db.cursor.execute("""
            SELECT quantity FROM orders 
            WHERE user_id = ? AND target_date = ? AND is_cancelled = FALSE
        """, (user_db_id, date_iso)).fetchone()

        # Формируем клавиатуру
        keyboard = []
        if not CONFIG.orders_enabled:
            # Добавляем информационную кнопку, если заказы отключены
            message += "\n\n⚠️ Приём заказов временно приостановлен"
            keyboard.append([InlineKeyboardButton("⏳ Заказы принимаются через Битрикс", callback_data="noop")])
        else:
            # Существующая логика для включенных заказов
            can_modify = can_modify_order(target_date)
            if order:  # Если заказ уже есть
                message += f"\n\n✅ {'Предзаказ' if day_offset > 0 else 'Заказ'}: {order[0]} порции"
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
        
        # Добавляем глобальную проверку
        if not CONFIG.orders_enabled and query.data.startswith(('order_', 'change_', 'confirm_')):
            await query.answer("❌ Приём заказов временно приостановлен", show_alert=True)
            return

        logger.info(f"Получен callback: {query.data} от пользователя {query.from_user.id}")

        if query.data.startswith("cancel_"):
            # Извлекаем дату из callback_data
            try:
                _, date_part = query.data.split("_", 1)

                # Определяем дату
                now = datetime.now(CONFIG.timezone)
                if '-' in date_part:
                    target_date = datetime.strptime(date_part, "%Y-%m-%d").date()
                elif date_part.isdigit():
                    day_offset = int(date_part)
                    target_date = (now + timedelta(days=day_offset)).date()
                else:
                    raise ValueError(f"Неверный формат даты: {date_part}")

                # Проверяем возможность отмены
                if not can_modify_order(target_date):
                    await query.answer("ℹ️ Отмена невозможна после 9:30", show_alert=True)
                    return

                # Получаем ID пользователя из БД
                db.cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (query.from_user.id,))
                user_record = db.cursor.fetchone()
                if not user_record:
                    await query.answer("❌ Пользователь не найден", show_alert=True)
                    return

                user_db_id = user_record[0]

                # Отменяем заказ в БД
                with db.conn:
                    db.cursor.execute("""
                        UPDATE orders 
                        SET is_cancelled = TRUE,
                            order_time = ?
                        WHERE user_id = ?
                          AND target_date = ?
                          AND is_cancelled = FALSE
                    """, (now.strftime("%H:%M:%S"), user_db_id, target_date.isoformat()))

                    if db.cursor.rowcount == 0:
                        await query.answer("❌ Заказ не найден", show_alert=True)
                        return

                # Обновляем интерфейс
                days_ru = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
                if "Меню на" in query.message.text:
                    # Отмена из меню дня
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
                    # Отмена из списка заказов
                    await refresh_orders_view(query, context, query.from_user.id, now, days_ru)

                await query.answer("✅ Заказ отменён")

            except Exception as e:
                logger.error(f"Ошибка при отмене заказа: {e}")
                await query.answer("⚠️ Ошибка отмены", show_alert=True)

        elif query.data.startswith("change_"):
            # Логика изменения количества порций (заглушка)
            await query.answer("🔄 Изменение количества порций временно недоступно")
            return

        elif query.data.startswith("confirm_"):
            # Логика подтверждения заказа (заглушка)
            await query.answer("✅ Заказ подтверждён")
            return

        else:
            # Неизвестное действие
            logger.warning(f"Неизвестный callback: {query.data}")
            await query.answer("⚠️ Неизвестное действие", show_alert=True)

    except Exception as e:
        logger.error(f"Критическая ошибка в order_action: {e}", exc_info=True)
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
    - ⏳ Предстоящие (будущие заказы, которые можно отменить)
    - ❌ Отмененные (по желанию)
    """
    try:
        user = update.effective_user
        text = update.message.text.strip()
        today = datetime.now(CONFIG.timezone).date()

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

        # Получаем ID пользователя
        db.cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (user.id,))
        user_record = db.cursor.fetchone()
        if not user_record:
            await update.message.reply_text("❌ Пользователь не найден.")
            return await show_main_menu(update, user.id)

        user_db_id = user_record[0]

        # Получаем статистику
        db.cursor.execute("""
            SELECT 
                -- Выполненные заказы (дата прошла + не отменены)
                SUM(CASE WHEN target_date < ? AND is_cancelled = FALSE THEN quantity ELSE 0 END) as completed,
                
                -- Предстоящие заказы (дата в будущем + не отменены)
                SUM(CASE WHEN target_date >= ? AND is_cancelled = FALSE THEN quantity ELSE 0 END) as upcoming,
                
                -- Отмененные заказы
                SUM(CASE WHEN is_cancelled = TRUE THEN quantity ELSE 0 END) as cancelled
            FROM orders
            WHERE user_id = ?
              AND target_date BETWEEN ? AND ?
        """, (today, today, user_db_id, start_date.isoformat(), end_date.isoformat()))

        stats = db.cursor.fetchone()
        completed = stats[0] or 0
        upcoming = stats[1] or 0
        cancelled = stats[2] or 0

        # Формируем сообщение
        message_lines = [
            f"📊 Статистика за {month_name}:",
            f"━━━━━━━━━━━━━━━━━━━━",
            f"🍽 Всего порций: *{completed + upcoming}*",
            "",
            f"✅ Выполненные: *{completed}*",
            f"⏳ Предстоящие: *{upcoming}*"
        ]

        if cancelled > 0:
            message_lines.append(f"❌ Отмененные: *{cancelled}*")

        # Дополнительная информация о предстоящих заказах
        if upcoming > 0:
            db.cursor.execute("""
                SELECT COUNT(*) 
                FROM orders 
                WHERE user_id = ? 
                  AND target_date >= ? 
                  AND is_cancelled = FALSE
            """, (user_db_id, today))
            order_count = db.cursor.fetchone()[0]
            
            next_order_date = None
            db.cursor.execute("""
                SELECT MIN(target_date)
                FROM orders
                WHERE user_id = ?
                  AND target_date >= ?
                  AND is_cancelled = FALSE
            """, (user_db_id, today))
            next_date = db.cursor.fetchone()[0]
            
            if next_date:
                next_order_date = datetime.strptime(next_date, "%Y-%m-%d").strftime("%d.%m.%Y")
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
            now = datetime.now(CONFIG.timezone)
            target_date = now + timedelta(days=1)
            if now.weekday() == 4:  # Пятница -> понедельник
                target_date += timedelta(days=2)
            
            db.cursor.execute(
                "INSERT INTO orders (user_id, target_date, order_time, quantity, is_preliminary) "
                "SELECT id, ?, ?, 1, TRUE FROM users WHERE telegram_id = ?",
                (target_date.date().isoformat(), now.strftime("%H:%M:%S"), user.id)
            )
            db.conn.commit()
            await update.message.reply_text(f"✅ Предзаказ на {target_date.strftime('%d.%m')} оформлен!")
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
            await query.answer("ℹ️ Отмена невозможна после 9:30", show_alert=True)
            return

        # Отменяем заказ
        user_id = query.from_user.id
        now = datetime.now(CONFIG.timezone)
        
        db.cursor.execute("""
            UPDATE orders
            SET is_cancelled = TRUE,
                order_time = ?
            WHERE user_id = (SELECT id FROM users WHERE telegram_id = ?)
            AND target_date = ?
            AND is_cancelled = FALSE
        """, (now.isoformat(), user_id, target_date_str))
        db.conn.commit()

        if db.cursor.rowcount == 0:
            await query.answer("❌ Заказ не найден или уже отменен", show_alert=True)
            return

        logger.info(f"Пользователь {user_id} отменил заказ на {target_date_str}")
        
        # Обновляем список заказов
        await view_orders(update, context, is_cancellation=True)
        await query.answer(f"✅ Заказ на {target_date.strftime('%d.%m')} отменён")

    except Exception as e:
        logger.error(f"Ошибка при отмене заказа: {e}")
        await query.answer("⚠️ Ошибка при отмене заказа", show_alert=True)
