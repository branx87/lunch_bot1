#!/usr/bin/env python3
import asyncio
import os
import sys

# Добавляем путь к проекту
sys.path.append('/app')

from bitrix.sync import BitrixSync
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def debug_order_5310():
    """Диагностика проблемы с заказом 5310"""
    try:
        # Создаем экземпляр BitrixSync
        sync = BitrixSync()
        
        logger = logging.getLogger(__name__)
        logger.info("🔧 ЗАПУСК ДИАГНОСТИКИ ЗАКАЗА 5310")
        
        # 1. Проверим заказ в Bitrix
        logger.info("📊 Получаем заказ 5310 из Bitrix...")
        params = {
            'entityTypeId': 1222,
            'id': 5310
        }
        
        bitrix_order = await sync.bx.call('crm.item.get', params)
        logger.info(f"📊 Заказ 5310 из Bitrix: {bitrix_order}")
        
        if not bitrix_order:
            logger.error("❌ Заказ 5310 не найден в Bitrix!")
            return
        
        # 2. Проверим поля сотрудника
        employee_crm_id = bitrix_order.get('ufCrm45_1743599470')
        employee_bitrix_id = bitrix_order.get('ufCrm45_1751956286')
        
        logger.info(f"👤 Поля сотрудника: CRM_ID={employee_crm_id}, Bitrix_ID={employee_bitrix_id}")
        
        # 3. Найдем локальный заказ с bitrix_order_id = 5310
        from database import db
        with db.get_session() as session:
            from models import Order
            
            local_order = session.query(Order).filter(
                Order.bitrix_order_id == '5310'
            ).first()
            
            if local_order:
                logger.info(f"✅ Локальный заказ найден: ID {local_order.id}, User_ID: {local_order.user_id}")
            else:
                logger.info("❌ Локальный заказ НЕ найден по bitrix_order_id")
                
                # 4. Найдем все заказы на сегодня
                from datetime import datetime
                today_orders = session.query(Order).filter(
                    Order.target_date == datetime.now().date()
                ).all()
                
                logger.info(f"📋 Всего заказов на сегодня: {len(today_orders)}")
                for order in today_orders:
                    logger.info(f"   - Заказ {order.id}: bitrix_id={order.bitrix_order_id}, user_id={order.user_id}, target_date={order.target_date}")
        
        # 5. Проверим сотрудника в локальной базе
        if employee_crm_id:
            logger.info(f"🔍 Ищем сотрудника с CRM ID: {employee_crm_id}")
            from models import User
            with db.get_session() as session:
                user = session.query(User).filter(
                    User.crm_employee_id == str(employee_crm_id)
                ).first()
                if user:
                    logger.info(f"✅ Сотрудник найден: {user.full_name} (ID: {user.id})")
                else:
                    logger.error(f"❌ Сотрудник с CRM ID {employee_crm_id} НЕ НАЙДЕН!")
                    
        if employee_bitrix_id:
            logger.info(f"🔍 Ищем сотрудника с Bitrix ID: {employee_bitrix_id}")
            from models import User
            with db.get_session() as session:
                user = session.query(User).filter(
                    User.bitrix_id == str(employee_bitrix_id)
                ).first()
                if user:
                    logger.info(f"✅ Сотрудник найден: {user.full_name} (ID: {user.id})")
                else:
                    logger.error(f"❌ Сотрудник с Bitrix ID {employee_bitrix_id} НЕ НАЙДЕН!")
                    
        logger.info("🔧 ДИАГНОСТИКА ЗАВЕРШЕНА")
        
    except Exception as e:
        logger.error(f"❌ Ошибка диагностики: {e}", exc_info=True)

async def main():
    """Главная функция"""
    await debug_order_5310()

if __name__ == "__main__":
    # Запускаем асинхронную функцию
    asyncio.run(main())