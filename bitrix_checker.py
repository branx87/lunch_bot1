# bitrix_checker.py
import asyncio
import logging
from datetime import datetime, timedelta
from sqlalchemy import text
from database import db
from models import User, Order
from bitrix.sync import BitrixSync

# Настройка логирования - ВАЖНО: используем корневой логгер
logger = logging.getLogger()  # Или logging.getLogger(__name__)

class BitrixChecker:
    def __init__(self):
        self.bitrix_sync = BitrixSync()
    
    async def test_bitrix_connection(self):
        """Тестирует подключение к Bitrix"""
        try:
            logger.info("🔌 Тестируем подключение к Bitrix24...")
            # 🔥 ИСПРАВЛЕНИЕ: используем self.bitrix_sync.bx вместо self.bx
            result = await self.bitrix_sync.bx.call('crm.item.fields', {'entityTypeId': 1222})
            logger.info("✅ Подключение к Bitrix24 работает")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к Bitrix: {e}")
            return False

    async def check_employees_without_bitrix(self):
        """Проверяет сотрудников без Bitrix ID"""
        logger.info("👥 Проверяем сотрудников без Bitrix ID...")
        with db.get_session() as session:
            employees_without_bitrix = session.query(User).filter(
                User.is_employee == True,
                User.bitrix_id == None
            ).all()
            
            if employees_without_bitrix:
                logger.warning(f"⚠️ НАЙДЕНЫ СОТРУДНИКИ БЕЗ BITRIX ID: {len(employees_without_bitrix)}")
                for emp in employees_without_bitrix:
                    logger.warning(f"  👤 {emp.full_name} (отдел: {emp.department})")
                return False
            else:
                logger.info("✅ У всех сотрудников есть Bitrix ID")
                return True

    async def check_employees_without_crm_id(self):
        """Проверяет сотрудников без CRM ID"""
        logger.info("👥 Проверяем сотрудников без CRM ID...")
        with db.get_session() as session:
            employees_without_crm = session.query(User).filter(
                User.is_employee == True,
                User.crm_employee_id == None
            ).all()
            
            if employees_without_crm:
                logger.warning(f"⚠️ НАЙДЕНЫ СОТРУДНИКИ БЕЗ CRM ID: {len(employees_without_crm)}")
                for emp in employees_without_crm[:5]:
                    logger.warning(f"  👤 {emp.full_name} (Bitrix ID: {emp.bitrix_id})")
                if len(employees_without_crm) > 5:
                    logger.warning(f"  ... и еще {len(employees_without_crm) - 5} сотрудников")
                return False
            else:
                logger.info("✅ У всех сотрудников есть CRM ID")
                return True

    async def check_duplicates(self):
        """Проверяет потенциальные дубликаты заказов на СЕГОДНЯ"""
        today = datetime.now().date().isoformat()
        logger.info(f"🔍 Проверяем дубликаты заказов на {today}...")
        
        with db.get_session() as session:
            duplicates = session.execute(text('''
                SELECT user_id, COUNT(*) as count 
                FROM orders 
                WHERE target_date = :date 
                AND is_cancelled = FALSE
                GROUP BY user_id 
                HAVING COUNT(*) > 1
            '''), {'date': today}).fetchall()
            
            if duplicates:
                logger.error(f"❌ НАЙДЕНЫ ДУБЛИКАТЫ: {len(duplicates)}")
                for user_id, count in duplicates:
                    user = session.query(User).filter(User.id == user_id).first()
                    logger.error(f"  👤 {user.full_name}: {count} заказов")
                return False
            else:
                logger.info("✅ Дубликатов не найдено")
                return True

    async def check_today_orders(self):
        """Проверяет заказы на СЕГОДНЯ, готовые к отправке в Bitrix"""
        today = datetime.now().date().isoformat()
        logger.info(f"📋 Проверяем заказы на {today}...")
        
        with db.get_session() as session:
            orders_to_send = session.query(Order).filter(
                Order.is_sent_to_bitrix == False,
                Order.is_cancelled == False,
                Order.target_date == today,
                Order.bitrix_order_id == None,
                Order.is_from_bitrix == False
            ).all()
            
            logger.info(f"📤 ЗАКАЗОВ ДЛЯ ОТПРАВКИ СЕГОДНЯ: {len(orders_to_send)}")
            
            if orders_to_send:
                for order in orders_to_send:
                    user = session.query(User).filter(User.id == order.user_id).first()
                    bitrix_info = f"Bitrix ID: {user.bitrix_id}" if user.bitrix_id else "❌ НЕТ BITRIX ID"
                    crm_info = f"CRM ID: {user.crm_employee_id}" if user.crm_employee_id else "❌ НЕТ CRM ID"
                    logger.info(f"  👤 {user.full_name}")
                    logger.info(f"    🍽 {order.quantity} порций | 📍 {user.location}")
                    logger.info(f"    {bitrix_info} | {crm_info}")
            else:
                logger.info("ℹ️ На сегодня нет заказов для отправки")
            
            return len(orders_to_send)

    async def check_tomorrow_orders(self):
        """Проверяет заказы на завтра, готовые к отправке в Bitrix"""
        tomorrow = (datetime.now() + timedelta(days=1)).date().isoformat()
        logger.info(f"📋 Проверяем заказы на {tomorrow}...")
        
        with db.get_session() as session:
            orders_to_send = session.query(Order).filter(
                Order.is_sent_to_bitrix == False,
                Order.is_cancelled == False,
                Order.target_date == tomorrow,
                Order.bitrix_order_id == None,
                Order.is_from_bitrix == False
            ).all()
            
            logger.info(f"📤 ЗАКАЗОВ ДЛЯ ОТПРАВКИ: {len(orders_to_send)}")
            
            if orders_to_send:
                for order in orders_to_send:
                    user = session.query(User).filter(User.id == order.user_id).first()
                    bitrix_info = f"Bitrix ID: {user.bitrix_id}" if user.bitrix_id else "❌ НЕТ BITRIX ID"
                    crm_info = f"CRM ID: {user.crm_employee_id}" if user.crm_employee_id else "❌ НЕТ CRM ID"
                    logger.info(f"  👤 {user.full_name}")
                    logger.info(f"    🍽 {order.quantity} порций | 📍 {user.location}")
                    logger.info(f"    {bitrix_info} | {crm_info}")
            else:
                logger.info("ℹ️ На завтра нет заказов для отправки")
            
            return len(orders_to_send)

    async def check_users_without_location(self):
        """Проверяет пользователей без указанной локации"""
        logger.info("📍 Проверяем пользователей без локации...")
        with db.get_session() as session:
            users_without_location = session.query(User).filter(
                User.location == None
            ).all()
            
            if users_without_location:
                logger.warning(f"⚠️ ПОЛЬЗОВАТЕЛИ БЕЗ ЛОКАЦИИ: {len(users_without_location)}")
                for user in users_without_location[:5]:
                    logger.warning(f"  👤 {user.full_name}")
                if len(users_without_location) > 5:
                    logger.warning(f"  ... и еще {len(users_without_location) - 5} пользователей")
                return False
            else:
                logger.info("✅ У всех пользователей указана локация")
                return True

    async def check_already_sent_orders(self):
        """Проверяет заказы, которые уже были отправлены в Bitrix на СЕГОДНЯ"""
        today = datetime.now().date().isoformat()
        logger.info(f"✅ Проверяем уже отправленные заказы на {today}...")
        
        with db.get_session() as session:
            sent_orders = session.query(Order).filter(
                Order.target_date == today,
                Order.is_sent_to_bitrix == True
            ).all()
            
            logger.info(f"📨 УЖЕ ОТПРАВЛЕНО В BITRIX: {len(sent_orders)}")
            
            for order in sent_orders:
                user = session.query(User).filter(User.id == order.user_id).first()
                logger.info(f"  👤 {user.full_name}: Bitrix Order ID {order.bitrix_order_id}")
            
            return len(sent_orders)

    async def run_all_checks(self):
        """Запускает все проверки на СЕГОДНЯШНИЙ день"""
        today = datetime.now().date().isoformat()
        logger.info(f"🚀 ЗАПУСК ПРОВЕРОК НА СЕГОДНЯ ({today})")
        logger.info("=" * 60)
        
        # 1. Проверка безопасности отправки (ГЛАВНАЯ ПРОВЕРКА)
        is_safe, orders, safety_issues = await self.check_send_orders_safety()
        
        # 2. Остальные проверки
        connection_ok = await self.test_bitrix_connection()
        
        # 3. Информационные проверки
        await self.check_employees_without_bitrix()
        await self.check_employees_without_crm_id() 
        await self.check_users_without_location()
        
        logger.info("=" * 60)
        logger.info("📊 ИТОГИ ПРОВЕРОК:")
        logger.info("=" * 60)
        
        critical_issues = []
        
        # 🔥 ИСПРАВЛЕНИЕ: проверяем реальные проблемы безопасности
        # "Опасность дублей" должна быть только если есть реальные дубли пользователей
        has_real_duplicates = any("ДУБЛИКАТЫ ПОЛЬЗОВАТЕЛЕЙ" in issue for issue in safety_issues)
        has_critical_errors = any("❌" in issue for issue in safety_issues)
        
        if has_real_duplicates:
            critical_issues.append("❌ Найдены дубликаты заказов у пользователей")
        if has_critical_errors:
            critical_issues.append("❌ Критические ошибки в данных")
        if not connection_ok:
            critical_issues.append("❌ Нет подключения к Bitrix")
        
        if not critical_issues:
            logger.info("🎉 ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ! Система готова.")
            if safety_issues:
                logger.info("💡 Технические замечания:")
                for issue in safety_issues:
                    logger.info(f"  {issue}")
        else:
            logger.error("❌ КРИТИЧЕСКИЕ ПРОБЛЕМЫ:")
            for issue in critical_issues:
                logger.error(f"  {issue}")
        
        logger.info(f"📈 СТАТИСТИКА:")
        logger.info(f"  📤 Заказов для отправки: {len(orders)}")
        
        success = len(critical_issues) == 0
        
        if success:
            if len(orders) > 0:
                logger.info(f"🎯 СИСТЕМА ГОТОВА! Будет отправлено {len(orders)} заказов")
            else:
                logger.info("ℹ️ Нет заказов для отправки")
        else:
            logger.error("🚨 НУЖНО СРОЧНО ИСПРАВИТЬ ПРОБЛЕМЫ!")
        
        return success, safety_issues, len(orders)

    async def check_send_orders_safety(self):
        """Проверяет безопасность отправки заказов - предотвращает дубли"""
        today = datetime.now().date().isoformat()
        logger.info(f"🔒 ПРОВЕРКА БЕЗОПАСНОСТИ ОТПРАВКИ НА {today}")
        logger.info("=" * 60)
        
        with db.get_session() as session:
            # 1. Проверяем заказы для отправки
            orders_to_send = session.query(Order).filter(
                Order.is_sent_to_bitrix == False,
                Order.is_cancelled == False,
                Order.target_date == today,
                Order.bitrix_order_id == None,
                Order.is_from_bitrix == False
            ).all()
            
            logger.info(f"📋 Заказов для отправки: {len(orders_to_send)}")
            
            # 2. Проверяем потенциальные проблемы
            issues = []
            
            # Проверка пользователей без Bitrix ID
            users_without_bitrix = []
            for order in orders_to_send:
                user = session.query(User).filter(User.id == order.user_id).first()
                if not user or not user.bitrix_id:
                    users_without_bitrix.append(order.id)
            
            if users_without_bitrix:
                issues.append(f"❌ Заказы без Bitrix ID: {users_without_bitrix}")
            
            # Проверка пользователей без CRM ID (предупреждение)
            users_without_crm = []
            for order in orders_to_send:
                user = session.query(User).filter(User.id == order.user_id).first()
                if user and not user.crm_employee_id:
                    users_without_crm.append(order.id)
            
            if users_without_crm:
                issues.append(f"⚠️ Заказы без CRM ID (будут использовать Bitrix ID): {users_without_crm}")
            
            # 3. Проверяем уникальность пользователей (на случай дублей в базе)
            user_orders = {}
            for order in orders_to_send:
                if order.user_id not in user_orders:
                    user_orders[order.user_id] = []
                user_orders[order.user_id].append(order.id)
            
            duplicates = {user_id: orders for user_id, orders in user_orders.items() if len(orders) > 1}
            if duplicates:
                issues.append(f"🚨 ДУБЛИКАТЫ ПОЛЬЗОВАТЕЛЕЙ: {duplicates}")
            
            # 4. Проверяем подключение к Bitrix - ИСПРАВЛЕННАЯ СТРОКА
            try:
                # 🔥 ИСПРАВЛЕНИЕ: используем self.bitrix_sync.bx вместо self.bx
                await self.bitrix_sync.bx.call('crm.item.fields', {'entityTypeId': 1222})
                logger.info("✅ Подключение к Bitrix24 работает")
            except Exception as e:
                issues.append(f"❌ Ошибка подключения к Bitrix: {e}")
            
            # 5. Выводим детальную информацию о заказах
            logger.info("📦 ДЕТАЛИ ЗАКАЗОВ:")
            for order in orders_to_send:
                user = session.query(User).filter(User.id == order.user_id).first()
                if user:
                    bitrix_status = "✅" if user.bitrix_id else "❌"
                    crm_status = "✅" if user.crm_employee_id else "⚠️"
                    logger.info(f"  👤 {user.full_name}")
                    logger.info(f"    🍽 {order.quantity} порций | 📍 {user.location}")
                    logger.info(f"    Bitrix ID: {bitrix_status} {user.bitrix_id} | CRM ID: {crm_status} {user.crm_employee_id}")
            
            # 6. Итоги проверки
            logger.info("=" * 60)
            if not issues:
                logger.info("🎉 ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ! Отправка безопасна.")
                return True, orders_to_send, []
            else:
                logger.error("🚨 ОБНАРУЖЕНЫ ПРОБЛЕМЫ:")
                for issue in issues:
                    logger.error(f"  {issue}")
                return False, orders_to_send, issues

async def main():
    """Основная функция для запуска проверок"""
    checker = BitrixChecker()
    await checker.run_all_checks()

if __name__ == "__main__":
    asyncio.run(main())