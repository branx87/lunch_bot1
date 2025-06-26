# ##handlers/order_callbacks.py
import sqlite3
from turtle import update
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackQueryHandler
from telegram.ext import ContextTypes
from datetime import datetime, date, time, timedelta
import logging

from config import CONFIG, MENU, TIMEZONE
from db import Database
from handlers.common import show_main_menu
from middleware import check_user_access
from utils import can_modify_order
from view_utils import refresh_day_view
from notifications import show_access_denied

logger = logging.getLogger(__name__)
    
async def handle_order_callback(query, now, user, context):
    """
    Обработчик оформления нового заказа. Выполняет:
    - Проверку допустимости заказа (выходные, временные ограничения)
    - Создание записи заказа в базе данных
    - Обновление интерфейса через refresh_day_view
    - Обработку всех возможных ошибок
    """
    try:
        db = context.bot_data['db']
        _, day_offset_str = query.data.split("_", 1)
        day_offset = int(day_offset_str)
        target_date = (now + timedelta(days=day_offset)).date()
        
        # Проверки на выходные и время
        if target_date.weekday() >= 5:
            await query.answer("ℹ️ Заказы на выходные не принимаются", show_alert=True)
            return

        if day_offset > 0 and target_date <= now.date():
            await query.answer("❌ Предзаказ можно сделать только на будущие даты", show_alert=True)
            return

        if day_offset == 0 and now.time() >= time(9, 30):
            await query.answer("ℹ️ Приём заказов на сегодня завершён в 9:30", show_alert=True)
            return

        # Получаем ID пользователя
        db.cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (user.id,))
        user_record = db.cursor.fetchone()
        if not user_record:
            await query.answer("❌ Пользователь не найден", show_alert=True)
            return
        user_db_id = user_record[0]

        # Проверяем существующий заказ
        db.cursor.execute("""
            SELECT quantity FROM orders 
            WHERE user_id = ? 
              AND target_date = ?
              AND is_cancelled = FALSE
        """, (user_db_id, target_date.isoformat()))
        existing_order = db.cursor.fetchone()

        if existing_order:
            await query.answer(f"ℹ️ У вас уже заказано {existing_order[0]} порций", show_alert=True)
            return

        # Создаём новый заказ
        with db.conn:
            db.cursor.execute("""
                INSERT INTO orders (
                    user_id,
                    target_date,
                    order_time,
                    quantity,
                    is_preliminary
                ) VALUES (?, ?, ?, ?, ?)
            """, (
                user_db_id,
                target_date.isoformat(),
                now.strftime("%H:%M:%S"),
                1,
                day_offset > 0
            ))

        # Обновляем интерфейс (убрали is_order=True)
        await refresh_day_view(query, day_offset, user_db_id, context)
        await query.answer("✅ Заказ успешно оформлен")
    except Exception as e:
        logger.error(f"Ошибка при оформлении заказа: {e}", exc_info=True)
        await query.answer("⚠️ Произошла ошибка. Попробуйте позже", show_alert=True)
        
async def handle_change_callback(query, now, user, context):
    """
    Обработчик изменения существующего заказа. Предоставляет:
    - Интерфейс изменения количества порций (+/-)
    - Кнопки подтверждения/отмены заказа
    - Сохранение контекста меню при изменении
    - Проверку временных ограничений на изменения
    """
    # Добавляем проверку доступа в начале функции
    if not await check_user_access(user.id, context):
        await show_access_denied(update)
        return
    
    try:
        # Парсим данные callback
        try:
            _, day_offset_str = query.data.split("_", 1)
            day_offset = int(day_offset_str)
        except (ValueError, AttributeError) as e:
            logger.error(f"Неверный формат callback данных: {query.data}", exc_info=True)
            await query.answer("⚠️ Ошибка в запросе", show_alert=True)
            return

        target_date = (now + timedelta(days=day_offset)).date()
        
        # Проверка возможности изменения
        if not can_modify_order(target_date):
            await query.answer("ℹ️ Изменение невозможно после 9:30", show_alert=True)
            if 'user_db_id' in context.user_data:
                await refresh_day_view(query, day_offset, context.user_data['user_db_id'], now)
            return

        # Получаем ID пользователя
        try:
            db = context.bot_data['db']
            if 'user_db_id' not in context.user_data:
                db.cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (user.id,))
                user_record = db.cursor.fetchone()
                if not user_record:
                    await query.answer("❌ Пользователь не найден", show_alert=True)
                    return
                context.user_data['user_db_id'] = user_record[0]
            
            user_db_id = context.user_data['user_db_id']
        except sqlite3.Error as e:
            logger.error(f"Ошибка БД при получении user_id: {e}", exc_info=True)
            await query.answer("⚠️ Ошибка базы данных", show_alert=True)
            return

        # Получаем текущий заказ
        try:
            db.cursor.execute("""
                SELECT quantity FROM orders 
                WHERE user_id = ? AND target_date = ? AND is_cancelled = FALSE
            """, (user_db_id, target_date.isoformat()))
            order_record = db.cursor.fetchone()
            if not order_record:
                await query.answer("ℹ️ Заказ не найден", show_alert=True)
                return
            
            current_qty = order_record[0]
        except sqlite3.Error as e:
            logger.error(f"Ошибка БД при получении заказа: {e}", exc_info=True)
            await query.answer("⚠️ Ошибка базы данных", show_alert=True)
            return

        # Получаем меню на выбранный день
        try:
            days_ru = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
            day_name = days_ru[target_date.weekday()]
            menu = MENU.get(day_name)
            if not menu:
                logger.error(f"Меню не найдено для дня {day_name}")
                await query.answer("⚠️ Меню не найдено", show_alert=True)
                return
        except Exception as e:
            logger.error(f"Ошибка получения меню: {e}", exc_info=True)
            await query.answer("⚠️ Ошибка получения меню", show_alert=True)
            return

        # Сохраняем контекст
        context.user_data['current_day_offset'] = day_offset

        # Формируем интерфейс
        menu_text = (
            f"🍽 Меню на {day_name} ({target_date.strftime('%d.%m')}):\n"
            f"1. 🍲 Первое: {menu['first']}\n"
            f"2. 🍛 Основное блюдо: {menu['main']}\n"
            f"3. 🥗 Салат: {menu['salad']}\n\n"
            f"🛒 Текущий заказ: {current_qty} порции"
        )

        keyboard = [
            [
                InlineKeyboardButton("➖ Уменьшить", callback_data=f"dec_{day_offset}"),
                InlineKeyboardButton("➕ Увеличить", callback_data=f"inc_{day_offset}")
            ],
            [InlineKeyboardButton("✔️ Подтвердить", callback_data=f"confirm_{day_offset}")],
            [InlineKeyboardButton("❌ Отменить заказ", callback_data=f"cancel_{day_offset}")]
        ]

        # Обновляем сообщение
        try:
            await query.edit_message_text(
                text=menu_text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="Markdown"
            )
            await query.answer()
        except Exception as e:
            logger.error(f"Ошибка обновления сообщения: {e}", exc_info=True)
            await query.answer("⚠️ Ошибка обновления", show_alert=True)

    except Exception as e:
        logger.error(f"Критическая ошибка в handle_change_callback: {e}", exc_info=True)
        await query.answer("⚠️ Произошла ошибка. Попробуйте позже", show_alert=True)
        
async def handle_cancel_callback(query, now, user, context):
    """
    Обработчик отмены заказа с улучшенной безопасностью:
    - Проверка формата и валидности callback-данных
    - Проверка временных ограничений на отмену
    - Надежное обновление статуса в базе данных
    - Логирование действий и обработка ошибок интерфейса
    """
    try:
        db = context.bot_data['db']
        parts = query.data.split("_")
        
        # Парсим дату
        if len(parts) > 2 and parts[1] == "order":
            date_part = "_".join(parts[2:])
            is_from_orders = True
        else:
            date_part = parts[1]
            is_from_orders = False

        if '-' in date_part:
            target_date = datetime.strptime(date_part, "%Y-%m-%d").date()
            day_offset = (target_date - now.date()).days
        elif date_part.isdigit():
            day_offset = int(date_part)
            target_date = (now + timedelta(days=day_offset)).date()
        else:
            raise ValueError("Неизвестный формат даты")

        # Проверяем возможность отмены
        if not can_modify_order(target_date):
            await query.answer("ℹ️ Отмена невозможна после 9:30", show_alert=True)
            return

        # Получаем ID пользователя
        db.cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (user.id,))
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
            
            if db.cursor.rowcount == 0:
                await query.answer("❌ Заказ не найден", show_alert=True)
                return

        logger.info(f"Пользователь {user.id} отменил заказ на {target_date}")

        # Обновляем интерфейс
        if is_from_orders:
            from handlers.common_handlers import view_orders
            await view_orders(query, context, is_cancellation=True)
        else:
            await refresh_day_view(query, day_offset, user_db_id, context)

        await query.answer("✅ Заказ отменён")

    except Exception as e:
        logger.error(f"Критическая ошибка в handle_cancel_callback: {e}", exc_info=True)
        await query.answer("⚠️ Произошла ошибка. Попробуйте снова.", show_alert=True)
        
async def handle_confirm_callback(query, now, user, context):
    """
    Обработчик подтверждения изменений заказа:
    - Обновляет интерфейс через refresh_day_view
    - Сохраняет контекст текущего дня из user_data
    - Обрабатывает возможные ошибки подтверждения
    """
    if not await check_user_access(user.id, context):
        await show_access_denied(update)
        return
    
    try:
        day_offset = context.user_data['current_day_offset']
        await refresh_day_view(query, day_offset, context.user_data['user_db_id'], now)
        await query.answer("✅ Заказ подтверждён")
    except Exception as e:
        logger.error(f"Ошибка подтверждения: {e}")
        await query.answer("⚠️ Ошибка подтверждения", show_alert=True)
        
async def modify_portion_count(query, now, user, context, delta):
    """
    Изменяет количество порций в заказе:
    - Обрабатывает увеличение/уменьшение количества
    - Проверяет граничные значения (1-3 порции)
    - При уменьшении до 0 автоматически отменяет заказ
    - Обновляет интерфейс через handle_change_callback
    """
    if not await check_user_access(user.id, context):
        await show_access_denied(update)
        return
    
    try:
        db = context.bot_data['db']
        day_offset = context.user_data['current_day_offset']
        target_date = (now + timedelta(days=day_offset)).date()
        user_db_id = context.user_data['user_db_id']
        
        # Получаем текущее количество
        db.cursor.execute("""
            SELECT quantity FROM orders 
            WHERE user_id = ? AND target_date = ? AND is_cancelled = FALSE
        """, (user_db_id, target_date.isoformat()))
        current_qty = db.cursor.fetchone()[0]
        new_qty = current_qty + delta

        # Проверка границ
        if new_qty < 1:
            return await handle_cancel_callback(query, now, user, context)
        if new_qty > 3:
            await query.answer("ℹ️ Максимум 3 порции")
            return

        # Обновляем количество
        with db.conn:
            db.cursor.execute("""
                UPDATE orders SET quantity = ? 
                WHERE user_id = ? AND target_date = ? AND is_cancelled = FALSE
            """, (new_qty, user_db_id, target_date.isoformat()))

        # Обновляем интерфейс без возврата в меню
        await handle_change_callback(query, now, user, context)
        await query.answer(f"Установлено: {new_qty} порции")

    except Exception as e:
        logger.error(f"Ошибка изменения количества: {e}")
        await query.answer("⚠️ Ошибка изменения", show_alert=True)
        
def setup_order_callbacks(application):
    """
    Настраивает и добавляет обработчики callback-запросов:
    - Оформление заказов (order_*)
    - Изменение количества порций (inc_*, dec_*)
    - Изменение заказа (change_*)
    - Отмена заказа (cancel_*)
    - Подтверждение заказа (confirm_*)
    """
    application.add_handler(CallbackQueryHandler(
        callback_handler,
        pattern=r'^(order|inc|dec|change|cancel|confirm)_'
    ))
    
    # Альтернативный вариант с раздельными обработчиками для каждого типа callback:
    # handlers = [
    #     CallbackQueryHandler(handle_order_callback, pattern=r'^order_'),
    #     CallbackQueryHandler(modify_portion_count, pattern=r'^inc_'),
    #     CallbackQueryHandler(modify_portion_count, pattern=r'^dec_'),
    #     CallbackQueryHandler(handle_change_callback, pattern=r'^change_'),
    #     CallbackQueryHandler(handle_cancel_callback, pattern=r'^cancel_'),
    #     CallbackQueryHandler(handle_confirm_callback, pattern=r'^confirm_')
    # ]
    # for handler in handlers:
    #     application.add_handler(handler)

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    
    try:
        user = update.effective_user
        if not user:
            return
            
        # Исправленный вызов check_user_access
        if not await check_user_access(user.id, context):
            await show_access_denied(update)
            return
        
        now = datetime.now(TIMEZONE)
        
        # Обработка кнопки "Назад"
        if query.data == "back_to_main_menu":
            await show_main_menu(query.message, user.id)
            return
        
        # Обработка пагинации
        if query.data.startswith(('admin_', 'provider_', 'accountant_', 'staff_', 'holiday_')) and ('_prev_' in query.data or '_next_' in query.data):
            from handlers.admin_config_handlers import handle_pagination
            return await handle_pagination(update, context)
            
        # Остальная обработка callback'ов
        user = update.effective_user
        now = datetime.now(TIMEZONE)
        
        if query.data.startswith("inc_"):
            await modify_portion_count(query, now, user, context, +1)
        elif query.data.startswith("dec_"):
            await modify_portion_count(query, now, user, context, -1)
        elif query.data.startswith("change_"):
            await handle_change_callback(query, now, user, context)
        elif query.data.startswith("cancel_"):
            await handle_cancel_callback(query, now, user, context)
        elif query.data.startswith("confirm_"):
            await handle_confirm_callback(query, now, user, context)
        elif query.data.startswith("order_"):
            await handle_order_callback(query, now, user, context)
        elif query.data.startswith("del_"):  # Обработка удалений
            from handlers.admin_config_handlers import handle_deletion
            await handle_deletion(update, context)
        elif query.data == "back_to_menu":
            await show_main_menu(query.message, user.id)
        elif query.data == "noop":
            await query.answer()
        elif query.data == "refresh":
            pass
        else:
            logger.warning(f"Неизвестный callback: {query.data}")
            await query.answer("⚠️ Неизвестная команда")

    except Exception as e:
        logger.error(f"Ошибка в callback_handler: {e}", exc_info=True)
        await query.answer("⚠️ Произошла ошибка. Попробуйте позже")

async def get_user_db_id(context: ContextTypes.DEFAULT_TYPE, telegram_id: int = None):
    """
    Получает ID пользователя в БД по telegram_id
    Args:
        context: Контекст бота (для доступа к БД)
        telegram_id: ID пользователя в Telegram (если None, берется из update)
    Returns:
        int: ID пользователя в БД или None если не найден
    """
    try:
        # Получаем доступ к БД из контекста
        db = context.bot_data['db']
        
        # Если telegram_id не передан, пытаемся получить из контекста
        if telegram_id is None and hasattr(context, 'user_data') and 'telegram_id' in context.user_data:
            telegram_id = context.user_data['telegram_id']
        
        if telegram_id is None:
            logger.warning("Не удалось получить telegram_id для поиска пользователя")
            return None
        
        # Ищем пользователя в БД
        db.cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
        result = db.cursor.fetchone()
        
        if result:
            return result[0]
        logger.warning(f"Пользователь с telegram_id={telegram_id} не найден в БД")
        return None
        
    except Exception as e:
        logger.error(f"Ошибка в get_user_db_id: {e}", exc_info=True)
        return None