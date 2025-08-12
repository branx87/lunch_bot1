# ##handlers/order_callbacks.py
import sqlite3
from turtle import update
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackQueryHandler
from telegram.ext import ContextTypes
from datetime import datetime, date, time, timedelta
import logging

from bitrix.sync import BitrixSync
from db import CONFIG
from db import db
from handlers.common import show_main_menu
from middleware import check_user_access
from utils import can_modify_order
from view_utils import refresh_day_view
from notifications import show_access_denied

logger = logging.getLogger(__name__)

# В начале файла добавьте:
QUANTITY_MAP = {
    1: '821',
    2: '822',
    3: '823',
    4: '824',
    5: '825'
}

# И обратное преобразование (если нужно):
BITRIX_QUANTITY_MAP = {
    '821': 1,
    '822': 2,
    '823': 3,
    '824': 4,
    '825': 5
}
    
async def handle_order_callback(query, now, user, context):
    """Обработчик оформления заказа с проверкой доступности заказов"""
    # Проверяем, разрешены ли заказы
    if not CONFIG.orders_enabled:
        await query.answer("❌ Приём заказов временно приостановлен", show_alert=True)
        await query.edit_message_reply_markup(reply_markup=None)  # Убираем кнопки
        return

    # Проверяем "возраст" кнопки (если нужно ограничить время жизни)
    if '_ts_' in query.data:  # Если в callback_data есть timestamp
        _, day_offset_str, timestamp_str = query.data.split("_", 2)
        request_time = datetime.fromtimestamp(int(timestamp_str))
        if (now - request_time) > timedelta(minutes=30):  # Кнопка "просрочена"
            await query.answer("⏳ Время действия кнопки истекло", show_alert=True)
            await query.edit_message_reply_markup(reply_markup=None)
            return
    else:
        day_offset_str = query.data.split("_", 1)[1]

    # Остальная логика обработки заказа...
    day_offset = int(day_offset_str)
    target_date = (now + timedelta(days=day_offset)).date()
    
    # Добавляем проверку доступа в начале функции
    if not await check_user_access(user.id, context.application):
        await show_access_denied(update)
        return

    logger.info(f"Получен callback: {query.data}")
    try:
        # Парсим параметры из callback
        _, day_offset_str = query.data.split("_", 1)
        day_offset = int(day_offset_str)
        target_date = (now + timedelta(days=day_offset)).date()
        
        # Ручная проверка 1: Заказы на выходные не принимаются
        if target_date.weekday() >= 5:  # 5-6 = суббота-воскресенье
            await query.answer("ℹ️ Заказы на выходные не принимаются", show_alert=True)
            return

        # Ручная проверка 2: Предзаказы только на будущие даты
        if day_offset > 0 and target_date <= now.date():
            await query.answer("❌ Предзаказ можно сделать только на будущие даты", show_alert=True)
            return

        # Ручная проверка 3: Обычные заказы только на сегодня и до 9:30
        if day_offset == 0:
            if now.time() >= time(9, 30):
                await query.answer("ℹ️ Приём заказов на сегодня завершён в 9:30", show_alert=True)
                return

        # Получаем ID пользователя из БД
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

        # Маппинг количества порций на bitrix_quantity_id
        quantity_map = {
            1: '821',
            2: '822',
            3: '823',
            4: '824',
            5: '825'
        }
        initial_quantity = 1  # По умолчанию 1 порция
        bitrix_quantity_id = quantity_map[initial_quantity]

        # Создаём новый заказ
        with db.conn:
            db.cursor.execute("""
                INSERT INTO orders (
                    user_id, target_date, order_time, 
                    quantity, bitrix_quantity_id, is_active,
                    is_preliminary, created_at
                ) VALUES (?, ?, ?, ?, ?, TRUE, ?, ?)
            """, (
                user_db_id,
                target_date.isoformat(),
                now.strftime("%H:%M:%S"),
                initial_quantity,
                bitrix_quantity_id,
                day_offset > 0,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ))

        # Если время после 9:29 - немедленная синхронизация
        if now.time() >= time(9, 29):
            sync = BitrixSync()
            await sync._push_to_bitrix()  # Немедленная синхронизация

        # Обновляем интерфейс
        await refresh_day_view(query, day_offset, user_db_id, now, is_order=True)
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
    if not await check_user_access(user.id, context.application):
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
                SELECT quantity, bitrix_quantity_id FROM orders 
                WHERE user_id = ? AND target_date = ? AND is_cancelled = FALSE
            """, (user_db_id, target_date.isoformat()))
            order_record = db.cursor.fetchone()
            if not order_record:
                await query.answer("ℹ️ Заказ не найден", show_alert=True)
                return
            
            current_qty = order_record[0]
            bitrix_quantity_id = order_record[1]
            
        except sqlite3.Error as e:
            logger.error(f"Ошибка БД при получении заказа: {e}", exc_info=True)
            await query.answer("⚠️ Ошибка базы данных", show_alert=True)
            return

        # Получаем меню на выбранный день
        try:
            days_ru = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
            day_name = days_ru[target_date.weekday()]
            menu = CONFIG.menu.get(day_name)
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
    - Проверка временных ограничений на отмену (до 9:20)
    - Проверка, что заказ не создан в Битрикс (is_from_bitrix != 1)
    - Надежное обновление статуса в базе данных
    - Логирование действий и обработка ошибок интерфейса
    """
    if not await check_user_access(user.id, context.application):
        await show_access_denied(update)
        return
    
    try:
        logger.info(f"Получен callback: {query.data}")
        
        # Проверяем наличие пользователя
        if not user:
            await query.answer("❌ Пользователь не определен")
            return

        # Разбираем callback данные
        try:
            parts = query.data.split("_")
            if len(parts) < 2:
                raise ValueError("Недостаточно частей в callback")
            
            # Определяем тип callback (из меню или из списка заказов)
            if len(parts) > 2 and parts[1] == "order":
                # Формат: cancel_order_2025-06-23
                date_part = "_".join(parts[2:])
                is_from_orders = True
            else:
                # Формат: cancel_2025-06-23 или cancel_3
                date_part = parts[1]
                is_from_orders = False

            # Парсим дату
            if '-' in date_part:  # Формат YYYY-MM-DD
                target_date = datetime.strptime(date_part, "%Y-%m-%d").date()
                day_offset = (target_date - now.date()).days
            elif date_part.isdigit():  # Числовое смещение (cancel_3)
                day_offset = int(date_part)
                target_date = (now + timedelta(days=day_offset)).date()
            else:
                raise ValueError("Неизвестный формат даты")
                
        except Exception as e:
            logger.error(f"Ошибка парсинга callback: {query.data}. Ошибка: {str(e)}")
            await query.answer("⚠️ Ошибка в запросе")
            return

        # Получаем ID пользователя в БД
        user_db_id = await get_user_db_id(user.id)
        if not user_db_id:
            await query.answer("❌ Пользователь не найден", show_alert=True)
            return

        # Проверяем можно ли отменять заказ
        if not can_modify_order(target_date):
            await query.answer("ℹ️ Отмена невозможна после 9:20", show_alert=True)
            return

        # Проверяем, не создан ли заказ в Битрикс
        db.cursor.execute("""
            SELECT is_from_bitrix FROM orders 
            WHERE user_id = ? AND target_date = ? AND is_cancelled = FALSE
        """, (user_db_id, target_date.isoformat()))
        order_record = db.cursor.fetchone()
        
        if not order_record:
            await query.answer("❌ Заказ не найден", show_alert=True)
            return
            
        if order_record[0] == 1:  # is_from_bitrix = 1
            await query.answer("❌ Заказ создан в Битрикс, отмена невозможна", show_alert=True)
            return

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

        # Логируем успешную отмену
        logger.info(f"Пользователь {user.id} отменил заказ на {target_date}")

        # Разное поведение после отмены
        if is_from_orders:
            # Обновляем список заказов
            from handlers.common_handlers import view_orders
            await view_orders(update=Update(0, callback_query=query), 
                             context=context, 
                             is_cancellation=True)
        else:
            # Возвращаем меню дня
            await refresh_day_view(query, day_offset, user_db_id, now)

        await query.answer("✅ Заказ отменён")

    except Exception as e:
        logger.error(f"Критическая ошибка в handle_cancel_callback: {e}", exc_info=True)
        await query.answer("⚠️ Произошла ошибка. Попробуйте снова.", show_alert=True)

def can_modify_order(target_date):
    """
    Проверяет возможность изменения/отмены заказа:
    - Для сегодняшней даты: до 9:20
    - Для будущих дат: всегда можно
    - Для заказов из Битрикс: нельзя
    """
    now = datetime.now(CONFIG.timezone)
    
    # Если заказ на сегодня
    if target_date == now.date():
        return now.time() < time(9, 20)
    
    # Если заказ на будущее
    return True
        
async def handle_confirm_callback(query, now, user, context):
    """
    Обработчик подтверждения изменений заказа:
    - Обновляет интерфейс через refresh_day_view
    - Сохраняет контекст текущего дня из user_data
    - Обрабатывает возможные ошибки подтверждения
    """
    if not await check_user_access(user.id, context.application):
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
    - Проверяет граничные значения (1-5 порций)
    - Проверяет возможность изменения (до 9:20 и не из Битрикс)
    - Обновляет bitrix_quantity_id в соответствии с количеством
    """
    if not await check_user_access(user.id, context.application):
        await show_access_denied(update)
        return
    
    try:
        day_offset = context.user_data['current_day_offset']
        target_date = (now + timedelta(days=day_offset)).date()
        user_db_id = context.user_data['user_db_id']
        
        # Проверяем можно ли изменять заказ
        if not can_modify_order(target_date):
            await query.answer("ℹ️ Изменение невозможно после 9:20", show_alert=True)
            return

        # Получаем текущий заказ
        db.cursor.execute("""
            SELECT quantity, bitrix_quantity_id, is_from_bitrix FROM orders 
            WHERE user_id = ? AND target_date = ? AND is_cancelled = FALSE
        """, (user_db_id, target_date.isoformat()))
        current_order = db.cursor.fetchone()
        
        if not current_order:
            await query.answer("ℹ️ Заказ не найден")
            return
            
        # Проверяем, не создан ли заказ в Битрикс
        if current_order[2] == 1:  # is_from_bitrix = 1
            await query.answer("❌ Заказ создан в Битрикс, изменение невозможно", show_alert=True)
            return
            
        current_qty = current_order[0]
        new_qty = current_qty + delta

        # Проверка границ
        if new_qty < 1:
            return await handle_cancel_callback(query, now, user, context)
        if new_qty > 5:  # Максимум 5 порций
            await query.answer("ℹ️ Максимум 5 порций")
            return

        # Маппинг количества порций на bitrix_quantity_id
        new_bitrix_quantity_id = QUANTITY_MAP.get(new_qty, '821')

        # Обновляем заказ
        with db.conn:
            db.cursor.execute("""
                UPDATE orders 
                SET quantity = ?,
                    bitrix_quantity_id = ?,
                    updated_at = datetime('now')
                WHERE user_id = ? AND target_date = ? AND is_cancelled = FALSE
            """, (new_qty, new_bitrix_quantity_id, user_db_id, target_date.isoformat()))

        # Обновляем интерфейс
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
            
        if not await check_user_access(user.id, context.application):
            await show_access_denied(update)  # Передаем весь update объект
            return
        
        user = update.effective_user
        now = datetime.now(CONFIG.timezone)
        
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
        now = datetime.now(CONFIG.timezone)
        
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

async def get_user_db_id(telegram_id):
    """Получает ID пользователя в БД"""
    db.cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
    result = db.cursor.fetchone()
    return result[0] if result else None