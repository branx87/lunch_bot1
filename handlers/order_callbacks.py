# ##handlers/order_callbacks.py
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackQueryHandler
from telegram.ext import ContextTypes
from datetime import datetime, date, time, timedelta
import logging
from bot_keyboards import create_main_menu_keyboard, create_unverified_user_keyboard

from bitrix.sync import BitrixSync
from database import db
from models import User, Order
from config import CONFIG
from handlers.common import show_main_menu
from middleware import check_user_access
from utils import can_modify_order
from view_utils import refresh_day_view
from notifications import show_access_denied
from time_config import TIME_CONFIG

logger = logging.getLogger(__name__)

# Re-export from services
from services.order_service import QUANTITY_MAP, BITRIX_QUANTITY_MAP
    
async def handle_order_callback(query, now, user, context):
    """Обработчик оформления заказа с проверкой доступности заказов"""
    user_id = user.id
    logger.info(f"USER {user_id}: начинает оформление заказа: {query.data}")
    
    # Проверяем, разрешены ли заказы
    if not CONFIG.are_orders_accepted_now():
        status_msg = CONFIG.get_orders_status_message()
        logger.warning(f"USER {user_id}: попытка заказа вне времени приема: {status_msg}")
        await query.answer(status_msg, show_alert=True)
        await query.edit_message_reply_markup(reply_markup=None)
        return

    # Проверяем "возраст" кнопки (если нужно ограничить время жизни)
    if '_ts_' in query.data:  # Если в callback_data есть timestamp
        _, day_offset_str, timestamp_str = query.data.split("_", 2)
        request_time = datetime.fromtimestamp(int(timestamp_str))
        if (now - request_time) > timedelta(minutes=30):  # Кнопка "просрочена"
            logger.warning(f"USER {user_id}: просроченная кнопка заказа")
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
        logger.warning(f"USER {user_id}: доступ запрещен")
        await show_access_denied(update)
        return

    logger.info(f"USER {user_id}: обрабатывает callback: {query.data}")
    try:
        # Парсим параметры из callback
        _, day_offset_str = query.data.split("_", 1)
        day_offset = int(day_offset_str)
        target_date = (now + timedelta(days=day_offset)).date()
        
        logger.info(f"USER {user_id}: оформляет заказ на {target_date}")
        
        # Ручная проверка 1: Заказы на выходные не принимаются
        if target_date.weekday() in TIME_CONFIG.WEEKEND_DAYS:
            logger.warning(f"USER {user_id}: попытка заказа на выходной {target_date}")
            await query.answer("ℹ️ Заказы на выходные не принимаются", show_alert=True)
            return

        # Ручная проверка 2: Предзаказы только на будущие даты
        if day_offset > 0 and target_date <= now.date():
            logger.warning(f"USER {user_id}: попытка предзаказа на прошедшую дату {target_date}")
            await query.answer("❌ Предзаказ можно сделать только на будущие даты", show_alert=True)
            return

        # Ручная проверка 3: Обычные заказы только на сегодня и до ORDER_DEADLINE
        if day_offset == 0:
            if now.time() >= TIME_CONFIG.ORDER_DEADLINE:
                logger.warning(f"USER {user_id}: попытка заказа на сегодня после {TIME_CONFIG.ORDER_DEADLINE}")
                await query.answer(f"ℹ️ Приём заказов на сегодня завершён в {TIME_CONFIG.ORDER_DEADLINE.strftime('%H:%M')}", show_alert=True)
                return

        # Получаем пользователя через SQLAlchemy
        user_record = db.session.query(User).filter(User.telegram_id == user.id).first()
        if not user_record:
            logger.error(f"USER {user_id}: не найден в базе данных")
            await query.answer("❌ Пользователь не найден", show_alert=True)
            return
        user_db_id = user_record.id

        # Проверяем существующий заказ через SQLAlchemy
        existing_order = db.session.query(Order).filter(
            Order.user_id == user_db_id,
            Order.target_date == target_date,
            Order.is_cancelled == False
        ).first()

        if existing_order:
            logger.info(f"USER {user_id}: уже имеет заказ на {target_date} - {existing_order.quantity} порций")
            await query.answer(f"ℹ️ У вас уже заказано {existing_order.quantity} порций", show_alert=True)
            return

        initial_quantity = 1  # По умолчанию 1 порция
        bitrix_quantity_id = QUANTITY_MAP[initial_quantity]

        # Создаём новый заказ через SQLAlchemy
        new_order = Order(
            user_id=user_db_id,
            target_date=target_date,
            order_time=now.strftime("%H:%M:%S"),
            quantity=initial_quantity,
            bitrix_quantity_id=bitrix_quantity_id,
            is_active=True,
            is_preliminary=day_offset > 0,
            created_at=datetime.now()
        )
        db.session.add(new_order)
        db.session.commit()

        logger.info(f"USER {user_id}: успешно создал заказ на {target_date}, {initial_quantity} порция(й)")

        # # 🔥 НЕМЕДЛЕННАЯ СИНХРОНИЗАЦИЯ только для заказов на СЕГОДНЯ после IMMEDIATE_SYNC_TIME
        # if day_offset == 0 and now.time() >= TIME_CONFIG.IMMEDIATE_SYNC_TIME:
        #     logger.info(f"USER {user_id}: выполняется немедленная синхронизация заказа на сегодня (время после {TIME_CONFIG.IMMEDIATE_SYNC_TIME.strftime('%H:%M')})")
        #     try:
        #         sync = BitrixSync()
        #         success = await sync._push_to_bitrix()
        #         if success:
        #             logger.info(f"USER {user_id}: заказ на сегодня синхронизирован немедленно")
        #         else:
        #             logger.warning(f"USER {user_id}: ошибка немедленной синхронизации заказа на сегодня")
        #     except Exception as sync_error:
        #         logger.error(f"USER {user_id}: ошибка при немедленной синхронизации заказа на сегодня: {sync_error}")

        # Обновляем интерфейс
        await refresh_day_view(query, day_offset, user_db_id, now, is_order=True)
        await query.answer("✅ Заказ успешно оформлен")

    except Exception as e:
        logger.error(f"USER {user_id}: ошибка при оформлении заказа: {e}", exc_info=True)
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
            await query.answer(f"ℹ️ Изменение невозможно после {TIME_CONFIG.MODIFICATION_DEADLINE.strftime('%H:%M')}", show_alert=True)
            if 'user_db_id' in context.user_data:
                await refresh_day_view(query, day_offset, context.user_data['user_db_id'], now)
            return

        # Получаем ID пользователя через SQLAlchemy
        try:
            if 'user_db_id' not in context.user_data:
                user_record = db.session.query(User).filter(User.telegram_id == user.id).first()
                if not user_record:
                    await query.answer("❌ Пользователь не найден", show_alert=True)
                    return
                context.user_data['user_db_id'] = user_record.id
            
            user_db_id = context.user_data['user_db_id']
        except Exception as e:
            logger.error(f"Ошибка БД при получении user_id: {e}", exc_info=True)
            await query.answer("⚠️ Ошибка базы данных", show_alert=True)
            return

        # Получаем текущий заказ через SQLAlchemy
        try:
            order = db.session.query(Order).filter(
                Order.user_id == user_db_id,
                Order.target_date == target_date,
                Order.is_cancelled == False
            ).first()
            
            if not order:
                await query.answer("ℹ️ Заказ не найден", show_alert=True)
                return
            
            current_qty = order.quantity
            bitrix_quantity_id = order.bitrix_quantity_id
            
        except Exception as e:
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
    user_id = user.id
    logger.info(f"USER {user_id}: вызывает отмену заказа: {query.data}")
    
    if not await check_user_access(user.id, context.application):
        logger.warning(f"USER {user_id}: доступ запрещен")
        await show_access_denied(update)
        return
    
    try:
        logger.info(f"USER {user_id}: получен callback: {query.data}")
        
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
            logger.error(f"USER {user_id}: ошибка парсинга callback: {query.data}. Ошибка: {str(e)}")
            await query.answer("⚠️ Ошибка в запросе")
            return

        logger.info(f"USER {user_id}: пытается отменить заказ на {target_date}")

        # Получаем ID пользователя в БД через SQLAlchemy
        user_record = db.session.query(User).filter(User.telegram_id == user.id).first()
        if not user_record:
            logger.error(f"USER {user_id}: не найден в базе данных")
            await query.answer("❌ Пользователь не найден", show_alert=True)
            return
        user_db_id = user_record.id

        # Проверяем можно ли отменять заказ
        if not can_modify_order(target_date):
            logger.warning(f"USER {user_id}: отмена невозможна для {target_date} (время истекло)")
            await query.answer(f"ℹ️ Отмена невозможна после {TIME_CONFIG.MODIFICATION_DEADLINE.strftime('%H:%M')}", show_alert=True)
            return

        # Проверяем, не создан ли заказ в Битрикс через SQLAlchemy
        order = db.session.query(Order).filter(
            Order.user_id == user_db_id,
            Order.target_date == target_date,
            Order.is_cancelled == False
        ).first()
        
        if not order:
            logger.warning(f"USER {user_id}: заказ на {target_date} не найден")
            await query.answer("❌ Заказ не найден", show_alert=True)
            return
            
        if order.is_from_bitrix == 1:
            logger.warning(f"USER {user_id}: попытка отменить заказ из Битрикс на {target_date}")
            await query.answer("❌ Заказ создан в Битрикс, отмена невозможна", show_alert=True)
            return

        # Отменяем заказ через SQLAlchemy
        order.is_cancelled = True
        order.order_time = now.strftime("%H:%M:%S")
        db.session.commit()

        # 🔥 НЕМЕДЛЕННОЕ УДАЛЕНИЕ если условия подходят
        sync = BitrixSync()
        await sync.cancel_order_immediate_cleanup(order.id)

        # Логируем успешную отмену
        logger.info(f"USER {user_id}: успешно отменил заказ на {target_date}")

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
        logger.error(f"USER {user_id}: критическая ошибка в handle_cancel_callback: {e}", exc_info=True)
        await query.answer("⚠️ Произошла ошибка. Попробуйте снова.", show_alert=True)

# can_modify_order removed — use utils.can_modify_order (delegates to services.time_service)
        
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
        
        # Проверка возможности изменения
        if not can_modify_order(target_date):
            await query.answer(f"ℹ️ Изменение невозможно после {TIME_CONFIG.MODIFICATION_DEADLINE.strftime('%H:%M')}", show_alert=True)
            if 'user_db_id' in context.user_data:
                await refresh_day_view(query, day_offset, context.user_data['user_db_id'], now)
            return

        # Получаем текущий заказ через SQLAlchemy
        order = db.session.query(Order).filter(
            Order.user_id == user_db_id,
            Order.target_date == target_date,
            Order.is_cancelled == False
        ).first()
        
        if not order:
            await query.answer("ℹ️ Заказ не найден")
            return
            
        # Проверяем, не создан ли заказ в Битрикс
        if order.is_from_bitrix == 1:
            await query.answer("❌ Заказ создан в Битрикс, изменение невозможно", show_alert=True)
            return
            
        current_qty = order.quantity
        new_qty = current_qty + delta

        # Проверка границ
        if new_qty < 1:
            return await handle_cancel_callback(query, now, user, context)
        if new_qty > TIME_CONFIG.MAX_PORTIONS:
            await query.answer(f"ℹ️ Максимум {TIME_CONFIG.MAX_PORTIONS} порций")
            return

        # Маппинг количества порций на bitrix_quantity_id
        new_bitrix_quantity_id = QUANTITY_MAP.get(new_qty, '821')

        # Обновляем заказ через SQLAlchemy
        order.quantity = new_qty
        order.bitrix_quantity_id = new_bitrix_quantity_id
        order.updated_at = datetime.now()
        db.session.commit()

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
    
async def show_main_menu_from_callback(update: Update, user_id: int):
    """Показ главного меню из callback"""
    query = update.callback_query
    try:
        # Закрываем callback
        await query.answer()
        
        # Отправляем новое сообщение с главным меню
        if user_id in CONFIG.admin_ids or user_id in CONFIG.provider_ids or user_id in CONFIG.accounting_ids:
            reply_markup = create_main_menu_keyboard(user_id)
        else:
            user = db.session.query(User).filter(
                User.telegram_id == user_id
            ).first()
            
            if user and user.is_verified:
                reply_markup = create_main_menu_keyboard(user_id)
            else:
                reply_markup = create_unverified_user_keyboard()

        await query.message.reply_text("Главное меню:", reply_markup=reply_markup)
        
    except Exception as e:
        logger.error(f"Ошибка в show_main_menu_from_callback: {e}", exc_info=True)
        await query.message.reply_text("⚠️ Ошибка при отображении меню")

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    
    logger.info(f"Получен callback: {query.data} от пользователя {update.effective_user.id}")
    
    try:
        user = update.effective_user
        if not user:
            return
            
        if not await check_user_access(user.id, context.application):
            await show_access_denied(update)
            return
        
        now = datetime.now(CONFIG.timezone)
        
        # 1. Сначала проверяем back_to_menu - это самый частый случай
        if query.data == "back_to_menu":
            logger.info("Обработка back_to_menu")
            await show_main_menu_from_callback(update, user.id)
            return
        
        # 2. Затем проверяем историю сообщений
        if query.data.startswith("history_"):
            logger.info(f"Обработка истории: {query.data}")
            data_parts = query.data.split('_')
            if len(data_parts) >= 3:
                action = data_parts[1]
                page = int(data_parts[2])
                
                if action == "prev":
                    page = max(0, page - 1)
                else:  # "next"
                    page += 1
                
                context.user_data['history_page'] = page
                from admin import message_history
                await message_history(update, context)
            return
        
        # 3. Обработка back_to_main_menu
        if query.data == "back_to_main_menu":
            await show_main_menu_from_callback(update, user.id)
            return
        
        # Остальная обработка callback'ов
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
    """Получает ID пользователя в БД через SQLAlchemy"""
    user = db.session.query(User).filter(User.telegram_id == telegram_id).first()
    return user.id if user else None