# ##bitrix.py
from datetime import datetime, timedelta
import os
from fast_bitrix24 import Bitrix
from dotenv import load_dotenv
import logging
from typing import List, Dict, Optional
import asyncio
from db import db
import json

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
            
            self._quantity_map = {
                '821': 1, '822': 2, '823': 3, '824': 4, '825': 5,
                '1743599470': 1, '1743599471': 2, '1743599472': 3
            }
            
            self._location_map = {
                '826': 'Офис', '827': 'ПЦ 1', '828': 'ПЦ 2', '1063': 'Склад'
            }
            
            self._status_map = {
                '1061': False,  # "Да" - заказ принят (не отменен)
                '1062': True    # "Нет" - заказ отменен
            }
            
            logger.info("Подключение к Bitrix24 инициализировано")
            self.bx = Bitrix(self.webhook)
            
        except Exception as e:
            logger.critical(f"Ошибка инициализации BitrixSync: {e}")
            raise

    async def sync_last_two_months_orders(self) -> Dict[str, int]:
        """Синхронизирует заказы за последние 2 месяца"""
        end_date = datetime.now()
        start_date = end_date.replace(day=1) - timedelta(days=60)
        
        logger.info(f"Синхронизация заказов с {start_date.date()} по {end_date.date()}")
        
        await self.sync_employees()
        
        return await self.sync_orders(
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        )

    async def sync_employees(self) -> Dict[str, int]:
        """Синхронизация сотрудников"""
        stats = {
            'total': 0, 'updated': 0, 'added': 0,
            'errors': 0, 'no_match': 0, 'merged': 0
        }
        
        try:
            crm_employees = await self._get_crm_employees()
            if not crm_employees:
                logger.error("Не удалось получить сотрудников из Bitrix")
                return stats

            bitrix_employees = self._create_employee_search_structure(crm_employees)
            
            local_employees = db.get_employees(active_only=False)
            stats['total'] = len(local_employees)
            
            for employee in local_employees:
                await self._sync_single_employee(employee, bitrix_employees, stats)

            await self._add_new_employees(bitrix_employees, stats)

            logger.info(f"Синхронизация сотрудников завершена. Статистика: {stats}")
            return stats

        except Exception as e:
            logger.error(f"Ошибка синхронизации сотрудников: {e}", exc_info=True)
            return stats

    async def sync_orders(self, start_date: str, end_date: str) -> Dict[str, int]:
        """Синхронизирует заказы из Bitrix в локальную базу"""
        stats = {'total': 0, 'added': 0, 'updated': 0, 'errors': 0}
        
        try:
            bitrix_orders = await self._get_bitrix_orders(start_date, end_date)
            if not bitrix_orders:
                logger.warning(f"Не найдено заказов за период {start_date} - {end_date}")
                return stats
                
            logger.info(f"Получено {len(bitrix_orders)} заказов из Bitrix")
            
            # Сортируем заказы по ID перед обработкой
            bitrix_orders.sort(key=lambda x: int(x['id']))
            
            for order in bitrix_orders:
                parsed_order = self._parse_bitrix_order(order)
                if not parsed_order:
                    stats['errors'] += 1
                    continue
                    
                await self._process_single_order(parsed_order, stats)
                
            logger.info(f"Синхронизация заказов завершена. Добавлено: {stats['added']}, Обновлено: {stats['updated']}, Ошибок: {stats['errors']}")
            return stats
            
        except Exception as e:
            logger.error(f"Ошибка синхронизации заказов: {e}")
            return stats

    async def _get_bitrix_orders(self, start_date: str, end_date: str) -> List[Dict]:
        params = {
            'entityTypeId': 1222,
            'select': ['id', 'ufCrm45_1743599470', 'ufCrm45ObedyCount', 
                    'ufCrm45ObedyFrom', 'ufCrm45_1744188327370', 'createdTime'],
            'filter': {
                '>=createdTime': f'{start_date}T00:00:00+03:00',
                '<=createdTime': f'{end_date}T23:59:59+03:00'
            }
        }
        
        try:
            logger.info(f"Запрос заказов с {start_date} по {end_date}")
            orders = await self.bx.get_all('crm.item.list', params)
            
            if orders:
                logger.info(f"Получено {len(orders)} заказов")
                # Сортируем заказы по ID сразу после получения
                orders.sort(key=lambda x: int(x['id']))
            else:
                logger.warning("Не получено ни одного заказа")
                
            return orders
        except Exception as e:
            logger.error(f"Ошибка получения заказов: {e}")
            return []

    def _parse_bitrix_order(self, order: Dict) -> Optional[Dict]:
        """Парсит данные заказа из Bitrix"""
        try:
            employee_id = order.get('ufCrm45_1743599470')
            if not employee_id:
                logger.warning(f"Заказ {order.get('id')} без ID сотрудника")
                return None
                
            status_value = order.get('ufCrm45_1744188327370')
            is_cancelled = False
            
            if isinstance(status_value, list) and status_value:
                status_id = str(status_value[0].get('ID', '')) if isinstance(status_value[0], dict) else str(status_value[0])
                is_cancelled = self._status_map.get(status_id, False)
            elif isinstance(status_value, dict):
                status_id = str(status_value.get('ID', ''))
                is_cancelled = self._status_map.get(status_id, False)
            elif status_value is not None:
                status_id = str(status_value)
                is_cancelled = self._status_map.get(status_id, False)
            
            bitrix_quantity = str(order.get('ufCrm45ObedyCount', ''))
            quantity = self._quantity_map.get(bitrix_quantity, 1)
                
            location_code = str(order.get('ufCrm45ObedyFrom', ''))
            location = self._location_map.get(location_code, 'Неизвестно')
                
            created_time = order.get('createdTime', '')
            date = created_time.split('T')[0] if created_time else datetime.now().strftime('%Y-%m-%d')
                
            return {
                'bitrix_id': str(order.get('id')),
                'employee_id': employee_id,
                'quantity': quantity,
                'bitrix_quantity': bitrix_quantity,
                'location': location,
                'date': date,
                'created_time': created_time,
                'is_cancelled': is_cancelled,
                'is_from_bitrix': True
            }
        except Exception as e:
            logger.error(f"Ошибка парсинга заказа {order.get('id', 'unknown')}: {e}")
            return None

    async def _process_single_order(self, order: Dict, stats: Dict):
        """Обрабатывает один заказ"""
        try:
            if not order or not isinstance(order, dict):
                logger.error("Некорректный формат заказа")
                stats['errors'] += 1
                return
                
            if not order.get('bitrix_id'):
                logger.error("Заказ без ID из Bitrix")
                stats['errors'] += 1
                return
            
            user_id = await self._get_local_user_id(order['employee_id'])
            if not user_id:
                logger.warning(f"Пользователь с Bitrix ID {order['employee_id']} не найден")
                stats['errors'] += 1
                return
                
            await self._update_user_location(user_id, order['location'])
                
            existing_order = self._find_local_order(order['bitrix_id'])
            
            if existing_order:
                if self._update_local_order(existing_order['id'], order):
                    stats['updated'] += 1
            else:
                if self._add_local_order(user_id, order):
                    stats['added'] += 1
                    
            stats['total'] += 1
        except Exception as e:
            logger.error(f"Ошибка обработки заказа: {e}")
            stats['errors'] += 1

    async def _get_local_user_id(self, bitrix_id: str) -> Optional[int]:
        """Находит локальный ID пользователя по Bitrix ID"""
        try:
            result = db.execute(
                "SELECT id FROM users WHERE bitrix_id = ? LIMIT 1",
                (bitrix_id,)
            )
            return result[0][0] if result else None
        except Exception as e:
            logger.error(f"Ошибка поиска пользователя: {e}")
            return None

    def _find_local_order(self, bitrix_order_id: str) -> Optional[Dict]:
        """Ищет заказ в локальной базе по ID из Bitrix"""
        try:
            db.cursor.execute("""
                SELECT id, quantity FROM orders 
                WHERE bitrix_order_id = ? 
                LIMIT 1
            """, (bitrix_order_id,))
            
            result = db.cursor.fetchone()
            if result:
                return {'id': result[0], 'quantity': result[1]}
            return None
        except Exception as e:
            logger.error(f"Ошибка поиска заказа: {e}")
            return None

    def _update_local_order(self, order_id: int, order: Dict) -> bool:
        """Обновляет локальный заказ"""
        try:
            db.cursor.execute("""
                UPDATE orders SET 
                    quantity = ?,
                    bitrix_quantity_id = ?,
                    is_cancelled = ?,
                    updated_at = datetime('now')
                WHERE id = ?
            """, (
                order['quantity'],
                order.get('bitrix_quantity', ''),
                order['is_cancelled'],
                order_id
            ))
            db.conn.commit()
            return db.cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Ошибка обновления заказа: {e}")
            return False

    def _add_local_order(self, user_id: int, order: Dict) -> bool:
        """Добавляет новый заказ"""
        try:
            db.cursor.execute("""
                INSERT INTO orders (
                    user_id, target_date, order_time, 
                    quantity, bitrix_quantity_id, bitrix_order_id,
                    is_cancelled, is_from_bitrix, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, TRUE, datetime('now'))
            """, (
                user_id,
                order['date'],
                datetime.now().strftime('%H:%M'),
                order['quantity'],
                order.get('bitrix_quantity', ''),
                order.get('bitrix_id', ''),
                order['is_cancelled']
            ))
            db.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления заказа: {e}")
            return False

    async def _update_user_location(self, user_id: int, location: str) -> bool:
        """Обновляет локацию пользователя"""
        try:
            db.cursor.execute("""
                UPDATE users SET location = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ? AND (location IS NULL OR location != ?)
            """, (location, user_id, location))
            db.conn.commit()
            return db.cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Ошибка обновления локации пользователя {user_id}: {e}")
            return False

    async def _get_crm_employees(self) -> List[Dict[str, str]]:
        """Получаем список сотрудников из CRM Bitrix"""
        try:
            fields = await self.bx.get_all(
                'crm.item.fields',
                {'entityTypeId': 1222}
            )
            
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

    def _create_employee_search_structure(self, crm_employees: List[Dict]) -> Dict[str, Dict]:
        """Создает структуру для поиска сотрудников"""
        bitrix_employees = {}
        for emp in crm_employees:
            name = emp['VALUE']
            normalized = self._normalize_name(name)
            parts = normalized.split()
            
            if len(parts) >= 2:
                simple_key = f"{parts[0]} {parts[1]}"
                bitrix_employees[simple_key] = {'id': emp['ID'], 'name': name}
                
                initial_key = f"{parts[0]} {parts[1][0]}"
                bitrix_employees[initial_key] = {'id': emp['ID'], 'name': name}
            
            bitrix_employees[normalized] = {'id': emp['ID'], 'name': name}
        
        return bitrix_employees

    async def _sync_single_employee(self, employee: Dict, bitrix_employees: Dict, stats: Dict):
        """Синхронизирует одного сотрудника"""
        try:
            local_name = self._normalize_name(employee['full_name'])
            bitrix_emp = self._find_bitrix_employee(local_name, bitrix_employees)

            if bitrix_emp:
                if employee.get('bitrix_id') != bitrix_emp['id']:
                    success = db.update_user_bitrix_data(
                        user_id=employee['id'],
                        bitrix_id=bitrix_emp['id'],
                        entity_type='crm_employee'
                    )
                    if success:
                        stats['updated'] += 1
                    else:
                        stats['errors'] += 1
            else:
                stats['no_match'] += 1
        except Exception as e:
            stats['errors'] += 1
            logger.error(f"Ошибка обработки {employee}: {e}")

    async def _add_new_employees(self, bitrix_employees: Dict, stats: Dict):
        """Добавляет новых сотрудников из Bitrix"""
        existing_names = {
            self._normalize_name(e['full_name']) 
            for e in db.get_employees(active_only=False)
        }
        
        for bitrix_emp in bitrix_employees.values():
            bitrix_name = bitrix_emp['name']
            normalized = bitrix_name
            parts = normalized.split()
            simple_name = ' '.join(parts[:2]) if len(parts) >= 2 else bitrix_name
            
            if not self._user_exists(bitrix_emp['id'], simple_name):
                try:
                    db.execute(
                        """INSERT INTO users 
                        (full_name, is_employee, is_verified, bitrix_id, bitrix_entity_type)
                        VALUES (?, ?, ?, ?, ?)""",
                        (bitrix_name, True, False, bitrix_emp['id'], 'crm_employee')
                    )
                    stats['added'] += 1
                    logger.info(f"Добавлен новый сотрудник: {bitrix_name}")
                except Exception as e:
                    stats['errors'] += 1
                    logger.error(f"Ошибка добавления сотрудника: {e}")

    def _find_bitrix_employee(self, local_name: str, bitrix_employees: Dict[str, dict]) -> Optional[dict]:
        """Ищем соответствие сотрудника с учетом возможного отчества в Bitrix"""
        if local_name in bitrix_employees:
            return bitrix_employees[local_name]
        
        local_parts = local_name.lower().split()
        if not local_parts:
            return None
        
        search_key_simple = f"{local_parts[0]} {local_parts[1]}"
        search_key_initial = f"{local_parts[0]} {local_parts[1][0]}"
        
        for bitrix_name, bitrix_data in bitrix_employees.items():
            bitrix_name_lower = bitrix_name.lower()
            
            if (bitrix_name_lower.startswith(search_key_simple) or 
                bitrix_name_lower.startswith(search_key_initial)):
                return bitrix_data
            
            if bitrix_data.get('id') and self.get_bitrix_id(local_name) == bitrix_data['id']:
                return bitrix_data
        
        return None

    def _user_exists(self, bitrix_id: int, full_name: str) -> bool:
        """Проверяет существование пользователя по Bitrix ID или имени"""
        try:
            if bitrix_id:
                result = db.execute(
                    "SELECT 1 FROM users WHERE bitrix_id = ? LIMIT 1",
                    (bitrix_id,)
                )
                if result:
                    return True
            
            name_parts = full_name.split()
            simple_name = ' '.join(name_parts[:2]) if len(name_parts) >= 2 else full_name
            
            result = db.execute(
                "SELECT 1 FROM users WHERE full_name = ? OR full_name = ? LIMIT 1",
                (full_name, simple_name)
            )
            return bool(result)
        except Exception as e:
            logger.error(f"Ошибка проверки пользователя: {e}")
            return False

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

    @staticmethod
    def _normalize_name(name: str) -> str:
        """Нормализует имя для сравнения"""
        if not name:
            return ""
        return (
            name.strip().lower()
            .replace("ё", "е")
            .translate(str.maketrans("", "", ".,-"))
        )

async def main():
    """Тестовая функция для проверки"""
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    sync = BitrixSync()
    
    result = await sync.sync_last_two_months_orders()
    print(f"Результат синхронизации: {result}")

if __name__ == '__main__':
    asyncio.run(main())