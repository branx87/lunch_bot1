# bitrix_checker.py
import asyncio
import logging
from datetime import datetime, timedelta
from sqlalchemy import text
from database import db
from models import User, Order
from bitrix.sync import BitrixSync  # Импортируем ваш основной класс

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BitrixChecker:
    def __init__(self):
        self.bitrix_sync = BitrixSync()
    
    async def test_bitrix_connection(self):
        """Тестирует подключение к Bitrix"""
        try:
            print("🔌 Тестируем подключение к Bitrix24...")
            result = await self.bitrix_sync.bx.call('crm.item.fields', {'entityTypeId': 1222})
            print("✅ Подключение к Bitrix24 работает")
            return True
        except Exception as e:
            print(f"❌ Ошибка подключения к Bitrix: {e}")
            return False

    async def check_employees_without_bitrix(self):
        """Проверяет сотрудников без Bitrix ID"""
        print("\n👥 Проверяем сотрудников без Bitrix ID...")
        with db.get_session() as session:
            employees_without_bitrix = session.query(User).filter(
                User.is_employee == True,
                User.bitrix_id == None
            ).all()
            
            if employees_without_bitrix:
                print(f"⚠️ НАЙДЕНЫ СОТРУДНИКИ БЕЗ BITRIX ID: {len(employees_without_bitrix)}")
                for emp in employees_without_bitrix:
                    print(f"  👤 {emp.full_name} (отдел: {emp.department})")
                return False
            else:
                print("✅ У всех сотрудников есть Bitrix ID")
                return True

    async def check_employees_without_crm_id(self):
        """Проверяет сотрудников без CRM ID"""
        print("\n👥 Проверяем сотрудников без CRM ID...")
        with db.get_session() as session:
            employees_without_crm = session.query(User).filter(
                User.is_employee == True,
                User.crm_employee_id == None
            ).all()
            
            if employees_without_crm:
                print(f"⚠️ НАЙДЕНЫ СОТРУДНИКИ БЕЗ CRM ID: {len(employees_without_crm)}")
                for emp in employees_without_crm[:5]:  # Показываем первые 5
                    print(f"  👤 {emp.full_name} (Bitrix ID: {emp.bitrix_id})")
                if len(employees_without_crm) > 5:
                    print(f"  ... и еще {len(employees_without_crm) - 5} сотрудников")
                return False
            else:
                print("✅ У всех сотрудников есть CRM ID")
                return True

    async def check_duplicates(self):
        """Проверяет потенциальные дубликаты заказов на завтра"""
        tomorrow = (datetime.now() + timedelta(days=1)).date().isoformat()
        print(f"\n🔍 Проверяем дубликаты заказов на {tomorrow}...")
        
        with db.get_session() as session:
            # Проверяем дубли по пользователю и дате
            duplicates = session.execute(text('''
                SELECT user_id, COUNT(*) as count 
                FROM orders 
                WHERE target_date = :date 
                AND is_cancelled = FALSE
                GROUP BY user_id 
                HAVING COUNT(*) > 1
            '''), {'date': tomorrow}).fetchall()
            
            if duplicates:
                print(f"❌ НАЙДЕНЫ ДУБЛИКАТЫ: {len(duplicates)}")
                for user_id, count in duplicates:
                    user = session.query(User).filter(User.id == user_id).first()
                    print(f"  👤 {user.full_name}: {count} заказов")
                return False
            else:
                print("✅ Дубликатов не найдено")
                return True

    async def check_tomorrow_orders(self):
        """Проверяет заказы на завтра, готовые к отправке в Bitrix"""
        tomorrow = (datetime.now() + timedelta(days=1)).date().isoformat()
        print(f"\n📋 Проверяем заказы на {tomorrow}...")
        
        with db.get_session() as session:
            orders_to_send = session.query(Order).filter(
                Order.is_sent_to_bitrix == False,
                Order.is_cancelled == False,
                Order.target_date == tomorrow,
                Order.bitrix_order_id == None,
                Order.is_from_bitrix == False
            ).all()
            
            print(f"📤 ЗАКАЗОВ ДЛЯ ОТПРАВКИ: {len(orders_to_send)}")
            
            if orders_to_send:
                for order in orders_to_send:
                    user = session.query(User).filter(User.id == order.user_id).first()
                    bitrix_info = f"Bitrix ID: {user.bitrix_id}" if user.bitrix_id else "❌ НЕТ BITRIX ID"
                    crm_info = f"CRM ID: {user.crm_employee_id}" if user.crm_employee_id else "❌ НЕТ CRM ID"
                    print(f"  👤 {user.full_name}")
                    print(f"    🍽 {order.quantity} порций | 📍 {user.location}")
                    print(f"    {bitrix_info} | {crm_info}")
                    print()
            else:
                print("ℹ️ На завтра нет заказов для отправки")
            
            return len(orders_to_send)

    async def check_users_without_location(self):
        """Проверяет пользователей без указанной локации"""
        print("\n📍 Проверяем пользователей без локации...")
        with db.get_session() as session:
            users_without_location = session.query(User).filter(
                User.location == None
            ).all()
            
            if users_without_location:
                print(f"⚠️ ПОЛЬЗОВАТЕЛИ БЕЗ ЛОКАЦИИ: {len(users_without_location)}")
                for user in users_without_location[:5]:
                    print(f"  👤 {user.full_name}")
                if len(users_without_location) > 5:
                    print(f"  ... и еще {len(users_without_location) - 5} пользователей")
                return False
            else:
                print("✅ У всех пользователей указана локация")
                return True

    async def check_already_sent_orders(self):
        """Проверяет заказы, которые уже были отправлены в Bitrix"""
        tomorrow = (datetime.now() + timedelta(days=1)).date().isoformat()
        print(f"\n✅ Проверяем уже отправленные заказы на {tomorrow}...")
        
        with db.get_session() as session:
            sent_orders = session.query(Order).filter(
                Order.target_date == tomorrow,
                Order.is_sent_to_bitrix == True
            ).all()
            
            print(f"📨 УЖЕ ОТПРАВЛЕНО В BITRIX: {len(sent_orders)}")
            
            for order in sent_orders:
                user = session.query(User).filter(User.id == order.user_id).first()
                print(f"  👤 {user.full_name}: Bitrix Order ID {order.bitrix_order_id}")
            
            return len(sent_orders)

    async def run_all_checks(self):
        """Запускает все проверки"""
        print("🚀 ЗАПУСК ПРОВЕРОК ПЕРЕД ЗАВТРАШНИМ ДНЕМ")
        print("=" * 60)
        
        results = {}
        
        # Запускаем проверки
        results['connection'] = await self.test_bitrix_connection()
        results['employees_bitrix'] = await self.check_employees_without_bitrix()
        results['employees_crm'] = await self.check_employees_without_crm_id()
        results['duplicates'] = await self.check_duplicates()
        results['location'] = await self.check_users_without_location()
        orders_count = await self.check_tomorrow_orders()
        sent_count = await self.check_already_sent_orders()
        
        print("\n" + "=" * 60)
        print("📊 ИТОГИ ПРОВЕРОК:")
        print("=" * 60)
        
        all_passed = all(results.values())
        
        if all_passed:
            print("🎉 ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ УСПЕШНО!")
        else:
            print("⚠️ ЕСТЬ ПРОБЛЕМЫ ДЛЯ ИСПРАВЛЕНИЯ:")
            for check_name, passed in results.items():
                status = "✅" if passed else "❌"
                print(f"  {status} {check_name}")
        
        print(f"\n📈 СТАТИСТИКА:")
        print(f"  📤 Заказов для отправки: {orders_count}")
        print(f"  📨 Уже отправлено: {sent_count}")
        print(f"  🍽 Всего заказов на завтра: {orders_count + sent_count}")
        
        if all_passed and orders_count > 0:
            print(f"\n🎯 ВСЕ СИСТЕМЫ ГОТОВЫ! Завтра будет отправлено {orders_count} заказов")
        elif orders_count == 0:
            print(f"\nℹ️ На завтра нет новых заказов для отправки")
        else:
            print(f"\n💡 Рекомендуется исправить проблемы перед завтрашним днем")
        
        return all_passed

async def main():
    """Основная функция для запуска проверок"""
    checker = BitrixChecker()
    await checker.run_all_checks()

if __name__ == "__main__":
    # Запускаем проверки
    asyncio.run(main())