# ##bitrix/ops.py
"""
Единая точка для операций над заказами в Bitrix24.

Раньше одинаковые блоки «отменить / почистить / найти / обновить» были
скопированы в 11 мест (handlers/, vk_bot/, max_bot/). Теперь подключение,
логирование и контекст заказа живут здесь, а вызывающий код делает один вызов
вместо связки try + async with + except.

Контракт: функции не бросают исключения. При ошибке возвращают False/None
и пишут причину в лог, поэтому вызывающему коду свой try/except не нужен.
"""
import logging
from datetime import datetime
from typing import Optional

from database import db
from models import Order
from time_config import TIME_CONFIG
from services.time_service import order_deadline_for

logger = logging.getLogger(__name__)


def _ctx(order_id) -> str:
    """Префикс логов с id заказа, если он известен вызывающему."""
    return f"Заказ {order_id}: " if order_id is not None else ""


async def cancel_in_bitrix(bitrix_id, order_id=None) -> bool:
    """Помечает заказ отменённым в Bitrix24.

    Вызывается при отмене заказа пользователем, если заказ уже отправлен в Bitrix.
    """
    context = _ctx(order_id)
    try:
        from bitrix.sync import BitrixSync
        async with BitrixSync() as sync:
            cancelled = await sync._cancel_bitrix_order(bitrix_id)
    except Exception as e:
        logger.error(f"❌ {context}ошибка при отмене в Bitrix (ID: {bitrix_id}): {e}")
        return False

    if cancelled:
        logger.info(f"✅ {context}отменён в Bitrix (ID: {bitrix_id})")
    else:
        logger.warning(f"⚠️ {context}не удалось отменить в Bitrix (ID: {bitrix_id})")
    return cancelled


async def cleanup_cancelled(order_id) -> bool:
    """
    Немедленно удаляет отменённый заказ из базы.
    Условия удаления:
    - Заказ должен быть отменен (is_cancelled = TRUE)
    - Заказ должен быть создан в боте (is_from_bitrix = FALSE)
    - Заказ не должен иметь bitrix_order_id (не синхронизирован с Bitrix)
    - Дата заказа должна быть сегодняшней или будущей

    Работает только с БД, подключение к Bitrix24 не требуется.
    """
    try:
        now = datetime.now(TIME_CONFIG.TIMEZONE)
        today = now.date()

        with db.get_session() as session:
            # Получаем информацию о заказе
            order = session.query(Order).filter(Order.id == order_id).first()
            if not order:
                logger.warning(f"Заказ {order_id} не найден")
                return False

            # 🔍 ДИАГНОСТИКА: проверяем состояние заказа в новой сессии
            logger.info(f"🔍 DIAG cleanup: order.id={order.id}, is_cancelled={order.is_cancelled}, "
                        f"is_from_bitrix={order.is_from_bitrix}, bitrix_order_id={order.bitrix_order_id}, "
                        f"is_sent_to_bitrix={order.is_sent_to_bitrix}, "
                        f"target_date={order.target_date}, session={id(session)}")

            # 🔥 ДОБАВИТЬ ПРОВЕРКУ ВРЕМЕНИ ДЛЯ УДАЛЕНИЯ
            # Срок берётся по дате заказа с учётом режима поставщика:
            # у нового поставщика это 15:00 предыдущего рабочего дня.
            if order.target_date == today:
                deadline = order_deadline_for(today)
                if deadline and now >= deadline:
                    logger.warning(f"⏰ Срок по заказу на сегодня истёк ({deadline.strftime('%d.%m %H:%M')})")
                    return False

            # 🔥 ИСПРАВЛЕНИЕ: правильное преобразование target_date
            # Если target_date уже date объект - используем как есть
            # Если строка - преобразуем в date
            if isinstance(order.target_date, str):
                try:
                    target_date = datetime.strptime(order.target_date, "%Y-%m-%d").date()
                except ValueError:
                    logger.error(f"Неверный формат даты в заказе {order_id}: {order.target_date}")
                    return False
            else:
                target_date = order.target_date

            # Проверяем условия для удаления
            if not order.is_cancelled:
                logger.warning(f"🔍 DIAG cleanup: Заказ {order_id} не отменен (is_cancelled={order.is_cancelled}), удаление невозможно")
                return False

            if order.is_from_bitrix:
                logger.warning(f"Заказ {order_id} создан в Bitrix, удаление невозможно")
                return False

            if order.bitrix_order_id:
                logger.warning(f"Заказ {order_id} уже синхронизирован с Bitrix (ID: {order.bitrix_order_id}), удаление невозможно")
                return False

            # 🔥 ИСПРАВЛЕНИЕ: Заказы, которые были отправлены в Bitrix (is_sent_to_bitrix=True),
            # не должны удаляться, даже если bitrix_order_id был очищен при отмене.
            # Они нужны для истории и для предотвращения повторной отправки в Bitrix.
            if order.is_sent_to_bitrix:
                logger.warning(f"Заказ {order_id} уже был отправлен в Bitrix (is_sent_to_bitrix=True), удаление невозможно")
                return False

            # Проверяем что дата заказа сегодня или в будущем
            if target_date < today:
                logger.warning(f"Заказ {order_id} на прошедшую дату {target_date}, удаление невозможно")
                return False

            # Удаляем заказ
            session.delete(order)
            session.commit()

        logger.info(f"✅ Немедленно удален отмененный заказ {order_id} на дату {target_date}")
        return True

    except Exception as e:
        logger.error(f"❌ Ошибка немедленного удаления заказа {order_id}: {e}")
        return False


async def find_bitrix_order_id(order_data: dict, crm_employee_id: str = None, order_id=None) -> Optional[str]:
    """Ищет заказ в Bitrix для пользователя на дату из order_data.

    Нужен когда заказ пришёл из Bitrix, но локально не сохранён bitrix_order_id.
    Возвращает Bitrix ID, если заказ существует, иначе None.
    """
    context = _ctx(order_id)
    try:
        from bitrix.sync import BitrixSync
        async with BitrixSync() as sync:
            found_bitrix_id = await sync._find_existing_bitrix_order(order_data, crm_employee_id)
    except Exception as e:
        logger.error(f"❌ {context}ошибка при поиске Bitrix ID для отмены: {e}")
        return None

    if found_bitrix_id:
        logger.info(f"🔍 {context}найден Bitrix ID {found_bitrix_id} для отмены")
    else:
        logger.warning(f"⚠️ {context}не найден заказ в Bitrix для отмены")
    return found_bitrix_id


async def update_quantity(bitrix_id, quantity, location=None, crm_employee_id=None, order_id=None) -> bool:
    """Обновляет количество порций (и локацию, если передана) заказа в Bitrix24.

    Используется, когда пользователь меняет количество после отправки заказа в Bitrix.
    """
    context = _ctx(order_id)
    update_data = {'quantity': quantity}
    if location is not None:
        update_data['location'] = location

    try:
        from bitrix.sync import BitrixSync
        async with BitrixSync() as sync:
            updated = await sync._update_bitrix_order(
                int(bitrix_id),
                update_data,
                crm_employee_id
            )
    except Exception as e:
        logger.error(f"❌ {context}ошибка при обновлении количества в Bitrix (ID: {bitrix_id}): {e}")
        return False

    if updated:
        logger.info(f"✅ {context}количество обновлено в Bitrix (ID: {bitrix_id}, порций: {quantity})")
    else:
        logger.warning(f"⚠️ {context}не удалось обновить количество в Bitrix (ID: {bitrix_id})")
    return updated