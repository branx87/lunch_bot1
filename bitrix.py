# ##bitrix.py
# bitrix.py
from datetime import datetime
import os
from fast_bitrix24 import Bitrix
from dotenv import load_dotenv
import logging
from typing import List, Dict, Optional
import asyncio
from db import db  # Импортируем наш экземпляр базы данных

logger = logging.getLogger(__name__)
logging.getLogger('fast_bitrix24').setLevel(logging.WARNING)

class BitrixSync:
    def __init__(self):
        """Инициализация подключения к Bitrix24"""
        try:
            load_dotenv('data/configs/.env')
            self.webhook = os.getenv('BITRIX_WEBHOOK')
            if not self.webhook:
                raise ValueError("BITRIX_WEBHOOK не найден в .env")
            
            logger.info("Подключение к Bitrix24 инициализировано")
            self.bx = Bitrix(self.webhook)
            
        except Exception as e:
            logger.critical(f"Ошибка инициализации BitrixSync: {e}")
            raise

    def _find_bitrix_employee(self, local_name: str, bitrix_employees: Dict[str, dict]) -> Optional[dict]:
        """Ищем соответствие сотрудника с учетом возможного отчества в Bitrix"""
        # 1. Прямое совпадение
        if local_name in bitrix_employees:
            return bitrix_employees[local_name]
        
        # 2. Разбиваем имена на части
        local_parts = local_name.lower().split()
        if not local_parts:
            return None
        
        # 3. Ищем по разным комбинациям:
        # - Фамилия + Имя (без отчества)
        search_key_simple = f"{local_parts[0]} {local_parts[1]}"
        
        # - Фамилия + первая буква имени (на случай Иванов И.И.)
        search_key_initial = f"{local_parts[0]} {local_parts[1][0]}"
        
        for bitrix_name, bitrix_data in bitrix_employees.items():
            bitrix_name_lower = bitrix_name.lower()
            
            # Проверяем разные варианты совпадений
            if (bitrix_name_lower.startswith(search_key_simple) or 
                bitrix_name_lower.startswith(search_key_initial)):
                return bitrix_data
            
            # Дополнительно проверяем совпадение Bitrix ID
            if bitrix_data.get('id') and self.get_bitrix_id(local_name) == bitrix_data['id']:
                return bitrix_data
        
        return None

    async def sync_employees(self) -> Dict[str, int]:
        """Улучшенная синхронизация сотрудников"""
        stats = {
            'total': 0, 
            'updated': 0, 
            'added': 0,
            'errors': 0, 
            'no_match': 0,
            'merged': 0
        }
        
        try:
            # 1. Получаем данные из Bitrix
            crm_employees = await self._get_crm_employees()
            if not crm_employees:
                logger.error("Не удалось получить сотрудников из Bitrix")
                return stats

            # 2. Создаем улучшенную структуру для поиска
            bitrix_employees = {}
            for emp in crm_employees:
                name = emp['VALUE']
                normalized = self._normalize_name(name)
                
                # Добавляем разные варианты имен для поиска
                parts = normalized.split()
                if len(parts) >= 2:
                    # Вариант с фамилией и именем (без отчества)
                    simple_key = f"{parts[0]} {parts[1]}"
                    bitrix_employees[simple_key] = {'id': emp['ID'], 'name': name}
                    
                    # Вариант с инициалом имени (Иванов И.И.)
                    initial_key = f"{parts[0]} {parts[1][0]}"
                    bitrix_employees[initial_key] = {'id': emp['ID'], 'name': name}
                
                # Оригинальное полное имя
                bitrix_employees[normalized] = {'id': emp['ID'], 'name': name}

            # 3. Синхронизируем локальных сотрудников
            local_employees = db.get_employees(active_only=False)
            stats['total'] = len(local_employees)
            
            for employee in local_employees:
                try:
                    local_name = self._normalize_name(employee['full_name'])
                    bitrix_emp = self._find_bitrix_employee(local_name, bitrix_employees)

                    if bitrix_emp:
                        # Проверяем, не изменился ли Bitrix ID
                        if employee.get('bitrix_id') != bitrix_emp['id']:
                            success = db.update_user_bitrix_data(
                                user_id=employee['id'],
                                bitrix_id=bitrix_emp['id'],
                                entity_type='crm_employee'
                            )
                            if success:
                                stats['updated'] += 1
                                logger.debug(f"Сопоставлено: {employee['full_name']} -> {bitrix_emp['id']}")
                            else:
                                stats['errors'] += 1
                    else:
                        stats['no_match'] += 1
                        logger.warning(f"Не найдено соответствие для: {employee['full_name']}")

                except Exception as e:
                    stats['errors'] += 1
                    logger.error(f"Ошибка обработки {employee}: {e}")

            # 4. Добавляем новых сотрудников из Bitrix
            existing_names = {self._normalize_name(e['full_name']) for e in local_employees}
            
            for bitrix_emp in bitrix_employees.values():
                bitrix_name = bitrix_emp['name']
                normalized = self._normalize_name(bitrix_name)
                
                # Проверяем все возможные варианты имени
                parts = normalized.split()
                simple_name = f"{parts[0]} {parts[1]}" if len(parts) >= 2 else normalized
                
                if simple_name not in existing_names and normalized not in existing_names:
                    try:
                        # Добавляем сокращенное имя (без отчества)
                        display_name = ' '.join(parts[:2]) if len(parts) >= 2 else bitrix_name
                        
                        db.execute(
                            """INSERT INTO users 
                            (full_name, is_employee, is_verified, bitrix_id, bitrix_entity_type)
                            VALUES (?, ?, ?, ?, ?)""",
                            (display_name, True, False, bitrix_emp['id'], 'crm_employee')
                        )
                        stats['added'] += 1
                        logger.info(f"Добавлен сотрудник: {display_name} (Bitrix: {bitrix_name})")
                    except Exception as e:
                        stats['errors'] += 1
                        logger.error(f"Ошибка добавления сотрудника {bitrix_name}: {e}")

            logger.info(f"Синхронизация завершена. Статистика: {stats}")
            return stats

        except Exception as e:
            logger.error(f"Ошибка синхронизации: {e}", exc_info=True)
            return stats

    @staticmethod
    def _normalize_name(name: str) -> str:
        """Нормализуем имя для сравнения"""
        if not name:
            return ""
        return (
            name.strip().lower()
            .replace("ё", "е")
            .translate(str.maketrans("", "", ".,-"))
        )

    async def _get_crm_employees(self) -> List[Dict[str, str]]:
        """Получаем список сотрудников из CRM Bitrix"""
        try:
            # Получаем поля сущности
            fields = await self.bx.get_all(
                'crm.item.fields',
                {'entityTypeId': 1222}  # Убедитесь, что это правильный ID вашей сущности
            )
            
            # Находим поле "Сотрудник"
            emp_field = next(
                (field for field in fields.values() 
                 if field.get('title') == 'Сотрудник' and field.get('type') == 'enumeration'),
                None
            )
            
            if not emp_field:
                logger.error("Поле 'Сотрудник' не найдено в CRM")
                return []
                
            return emp_field.get('items', [])
            
        except Exception as e:
            logger.error(f"Ошибка получения сотрудников из CRM: {e}")
            return []

    def get_bitrix_id(self, user_id: int) -> Optional[int]:
        """Получаем Bitrix ID пользователя"""
        try:
            result = db.execute(
                "SELECT bitrix_id FROM users WHERE id = ? LIMIT 1",
                (user_id,)
            )
            return result[0][0] if result else None
        except Exception as e:
            logger.error(f"Ошибка получения Bitrix ID: {e}")
            return None
        
    async def sync_orders(self, start_date: str, end_date: str) -> Dict[str, int]:
        """Синхронизирует заказы из Bitrix в локальную базу"""
        stats = {
            'total': 0,
            'added': 0,
            'updated': 0,
            'errors': 0
        }
        
        try:
            # Получаем заказы из Bitrix
            bitrix_orders = await self._get_bitrix_orders(start_date, end_date)
            if not bitrix_orders:
                logger.warning(f"Не найдено заказов в Bitrix за период {start_date} - {end_date}")
                return stats
                
            # Получаем локальные заказы за тот же период
            local_orders = self._get_local_orders(start_date, end_date)
            
            # Синхронизируем
            for bitrix_order in bitrix_orders:
                try:
                    # Ищем пользователя по Bitrix ID
                    user_id = self._find_user_by_bitrix_id(bitrix_order['employee_id'])
                    if not user_id:
                        logger.warning(f"Пользователь с Bitrix ID {bitrix_order['employee_id']} не найден")
                        stats['errors'] += 1
                        continue
                        
                    # Проверяем, есть ли такой заказ уже в локальной базе
                    existing_order = self._find_local_order(local_orders, user_id, bitrix_order['date'])
                    
                    if existing_order:
                        # Обновляем существующий заказ
                        if self._update_local_order(existing_order['id'], bitrix_order):
                            stats['updated'] += 1
                    else:
                        # Добавляем новый заказ
                        if self._add_local_order(user_id, bitrix_order):
                            stats['added'] += 1
                            
                    stats['total'] += 1
                    
                except Exception as e:
                    logger.error(f"Ошибка обработки заказа {bitrix_order}: {e}")
                    stats['errors'] += 1
                    
            logger.info(f"Синхронизация заказов завершена. Добавлено: {stats['added']}, Обновлено: {stats['updated']}, Ошибок: {stats['errors']}")
            return stats
            
        except Exception as e:
            logger.error(f"Ошибка синхронизации заказов: {e}", exc_info=True)
            return stats

    async def _get_bitrix_orders(self, start_date: str, end_date: str) -> List[Dict]:
        """Получает заказы из Bitrix за указанный период"""
        params = {
            'entityTypeId': 1222,  # ID сущности "Заказы обедов"
            'select': [
                'id', 
                'ufCrm45_1743599470',  # ID сотрудника
                'ufCrm45ObedyCount',   # Количество обедов
                'ufCrm45ObedyFrom',    # Локация
                'createdTime'          # Дата создания
            ],
            'filter': {
                '>=createdTime': f'{start_date}T00:00:00+03:00',
                '<createdTime': f'{end_date}T00:00:00+03:00'
            }
        }
        
        try:
            orders = await self.bx.get_all('crm.item.list', params)
            return [self._parse_bitrix_order(o) for o in orders]
        except Exception as e:
            logger.error(f"Ошибка получения заказов из Bitrix: {e}")
            return []

    def _parse_bitrix_order(self, order: Dict) -> Dict:
        """Парсит данные заказа из Bitrix в наш формат"""
        return {
            'bitrix_id': order.get('id'),
            'employee_id': order.get('ufCrm45_1743599470'),
            'quantity': self._map_quantity(order.get('ufCrm45ObedyCount')),
            'location': self._map_location(order.get('ufCrm45ObedyFrom')),
            'date': order.get('createdTime', '').split('T')[0],
            'is_from_bitrix': True  # Добавляем признак, что заказ из Bitrix
        }

    def _get_local_orders(self, start_date: str, end_date: str) -> List[Dict]:
        """Получает локальные заказы за период"""
        db.cursor.execute("""
            SELECT id, user_id, target_date, quantity, is_cancelled 
            FROM orders 
            WHERE target_date BETWEEN ? AND ?
        """, (start_date, end_date))
        
        columns = [col[0] for col in db.cursor.description]
        return [dict(zip(columns, row)) for row in db.cursor.fetchall()]

    def _find_user_by_bitrix_id(self, bitrix_id: int) -> Optional[int]:
        """Находит локальный ID пользователя по Bitrix ID"""
        db.cursor.execute("""
            SELECT id FROM users WHERE bitrix_id = ? LIMIT 1
        """, (bitrix_id,))
        result = db.cursor.fetchone()
        return result[0] if result else None

    def _find_local_order(self, local_orders: List[Dict], user_id: int, date: str) -> Optional[Dict]:
        """Ищет заказ в локальной базе"""
        for order in local_orders:
            if order['user_id'] == user_id and order['target_date'] == date:
                return order
        return None

    def _update_local_order(self, order_id: int, bitrix_order: Dict) -> bool:
        """Обновляет локальный заказ данными из Bitrix"""
        try:
            db.cursor.execute("""
                UPDATE orders 
                SET quantity = ?, 
                    is_from_bitrix = TRUE,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ? AND is_cancelled = FALSE
            """, (bitrix_order['quantity'], order_id))
            db.conn.commit()
            return db.cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Ошибка обновления заказа {order_id}: {e}")
            return False

    def _add_local_order(self, user_id: int, bitrix_order: Dict) -> bool:
        """Добавляет новый заказ из Bitrix"""
        try:
            db.cursor.execute("""
                INSERT INTO orders (
                    user_id, 
                    target_date, 
                    order_time, 
                    quantity, 
                    is_from_bitrix
                ) VALUES (?, ?, ?, ?, TRUE)
            """, (
                user_id,
                bitrix_order['date'],
                datetime.now().strftime('%H:%M'),
                bitrix_order['quantity']
            ))
            db.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления заказа: {e}")
            return False

async def test_sync():
    """Тестовая функция для проверки"""
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    sync = BitrixSync()
    result = await sync.sync_employees()
    print(f"Результат синхронизации: {result}")

    # Проверяем несколько обновленных записей
    test_users = db.execute('''
        SELECT full_name, bitrix_id 
        FROM users 
        WHERE bitrix_id IS NOT NULL
        LIMIT 5
    ''')
    print("Примеры сопоставлений:")
    for user in test_users:
        print(f"{user[0]} -> {user[1]}")

if __name__ == '__main__':
    asyncio.run(test_sync())