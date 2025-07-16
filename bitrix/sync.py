# ##bitrix/sync.py
from typing import Dict, List, Optional
import asyncio
import logging
from datetime import datetime, time, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import os
from fast_bitrix24 import Bitrix
from dotenv import load_dotenv
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
            self.scheduler = AsyncIOScheduler()
            self.is_running = False
            
        except Exception as e:
            logger.critical(f"Ошибка инициализации BitrixSync: {e}")
            raise

    async def run_sync_tasks(self):
        """Запуск фоновых задач синхронизации"""
        if self.is_running:
            return
            
        try:
            self._setup_schedules()
            self.scheduler.start()
            self.is_running = True
            logger.info("Фоновые задачи синхронизации запущены")
        except Exception as e:
            logger.error(f"Ошибка запуска задач синхронизации: {e}")

    def _setup_schedules(self):
        """Настройка расписания синхронизации"""
        # Синхронизация из Bitrix каждые 5 минут (6:00-10:00)
        self.scheduler.add_job(
            self.sync_last_two_months_orders,
            'cron',
            minute='*/5',
            hour='6-10',
            day_of_week='mon-fri'
        )
        
        # # Тестовая синхронизация в удобное время
        # self.scheduler.add_job(
        #     self._push_to_bitrix,
        #     'cron',
        #     minute=2,
        #     hour=10,
        #     day_of_week='mon-fri'
        # )

        # Основная синхронизация в 9:29
        self.scheduler.add_job(
            self._push_to_bitrix,
            'cron',
            minute=29,
            hour=9,
            day_of_week='mon-fri'
        )
        
        # Закрытие заказов в 9:30
        self.scheduler.add_job(
            self.close_orders_at_930,
            'cron',
            minute=30,
            hour=9,
            day_of_week='mon-fri'
        )

    # Все остальные методы из bitrix.py (sync_employees, sync_orders и т.д.)
    # должны быть перенесены сюда без изменений

    def _clean_string(self, text: str) -> str:
        """Очищает строку от недопустимых символов для SQLite"""
        if not text:
            return text
            
        # Удаляем символы, которые могут вызвать проблемы в SQL
        forbidden_chars = ['#', '--', '/*', '*/']
        for char in forbidden_chars:
            text = text.replace(char, '')
            
        return text.strip()

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
            employee_id = self._clean_string(str(order.get('ufCrm45_1743599470', '')))
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
            
            bitrix_quantity = self._clean_string(str(order.get('ufCrm45ObedyCount', '')))
            quantity = self._quantity_map.get(bitrix_quantity, 1)
                
            location_code = self._clean_string(str(order.get('ufCrm45ObedyFrom', '')))
            location = self._location_map.get(location_code, 'Неизвестно')
                
            created_time = self._clean_string(order.get('createdTime', ''))
            date = created_time.split('T')[0] if created_time else datetime.now().strftime('%Y-%m-%d')
                
            return {
                'bitrix_id': self._clean_string(str(order.get('id'))),
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
        """Обрабатывает один заказ с проверкой дубликатов и улучшенным логированием"""
        try:
            # 1. Валидация входящего заказа
            if not order or not isinstance(order, dict):
                logger.error("Некорректный формат заказа")
                stats['errors'] += 1
                return
                
            bitrix_id = order.get('bitrix_id')
            if not bitrix_id:
                logger.error("Заказ без ID из Bitrix")
                stats['errors'] += 1
                return

            # 2. Поиск пользователя в локальной БД
            user_id = await self._get_local_user_id(order['employee_id'])
            if not user_id:
                logger.warning(f"Пользователь с Bitrix ID {order['employee_id']} не найден")
                stats['errors'] += 1
                return

            # 3. Обновление локации пользователя
            await self._update_user_location(user_id, order['location'])

            # 4. Проверка на существующий заказ
            existing_order = self._find_local_order(bitrix_id)
            
            if existing_order:
                # 4.1. Проверка на полный дубликат (такой же пользователь и дата)
                if (existing_order['user_id'] == user_id and 
                    existing_order['target_date'] == order['date']):
                    logger.debug(f"Полный дубликат заказа Bitrix ID {bitrix_id} (локальный ID {existing_order['id']})")
                    stats['duplicates'] = stats.get('duplicates', 0) + 1
                    return
                    
                # 4.2. Обновление существующего заказа
                if self._update_local_order(existing_order['id'], order):
                    logger.debug(f"Обновлен заказ Bitrix ID {bitrix_id} (локальный ID {existing_order['id']})")
                    stats['updated'] += 1
            else:
                # 4.3. Создание нового заказа
                if self._add_local_order(user_id, order):
                    logger.debug(f"Добавлен новый заказ Bitrix ID {bitrix_id}")
                    stats['added'] += 1

            stats['total'] += 1

        except Exception as e:
            logger.error(f"Ошибка обработки заказа {order.get('bitrix_id', 'unknown')}: {str(e)}", exc_info=True)
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
            # Очищаем все строковые значения перед обновлением
            bitrix_quantity = self._clean_string(str(order.get('bitrix_quantity', '')))
            
            db.cursor.execute("""
                UPDATE orders SET 
                    quantity = ?,
                    bitrix_quantity_id = ?,
                    is_cancelled = ?,
                    updated_at = datetime('now')
                WHERE id = ?
            """, (
                order['quantity'],
                bitrix_quantity,
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
            # Очищаем все строковые значения перед вставкой
            bitrix_id = self._clean_string(str(order.get('bitrix_id', '')))
            bitrix_quantity = self._clean_string(str(order.get('bitrix_quantity', '')))
            target_date = self._clean_string(order.get('date', datetime.now().strftime('%Y-%m-%d')))
            created_time = self._clean_string(order.get('created_time', ''))
            
            db.cursor.execute("""
                INSERT INTO orders (
                    user_id, target_date, order_time, 
                    quantity, bitrix_quantity_id, is_cancelled, 
                    is_from_bitrix, bitrix_order_id, is_active,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, TRUE, datetime('now'))
            """, (
                user_id,
                target_date,
                created_time.split('T')[1][:8] if 'T' in created_time else datetime.now().strftime('%H:%M'),
                order['quantity'],
                bitrix_quantity,
                order['is_cancelled'],
                True,  # is_from_bitrix
                bitrix_id
            ))
            db.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления заказа: {e}")
            return False
    
    async def _update_user_location(self, user_id: int, location: str) -> bool:
        """Обновляет локацию пользователя"""
        try:
            # Очищаем локацию перед обновлением
            clean_location = self._clean_string(location)
            
            db.cursor.execute("""
                UPDATE users SET location = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ? AND (location IS NULL OR location != ?)
            """, (clean_location, user_id, clean_location))
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
    
    async def _push_to_bitrix(self):
        """Отправляет в Bitrix только локальные заказы на сегодня"""
        try:
            today = datetime.now().date().isoformat()
            
            # Получаем данные и преобразуем в словари вручную
            result = db.execute('''
                SELECT 
                    o.id, o.target_date, o.order_time, o.quantity,
                    o.bitrix_quantity_id, u.location, o.is_from_bitrix,
                    o.is_sent_to_bitrix, o.is_cancelled,
                    u.bitrix_id, u.full_name
                FROM orders o
                JOIN users u ON o.user_id = u.id
                WHERE o.target_date = ?
                AND o.is_from_bitrix = FALSE
                AND o.is_sent_to_bitrix = FALSE
                AND o.is_cancelled = FALSE
                AND u.bitrix_id IS NOT NULL
            ''', (today,))
            
            if not result:
                logger.info("Нет заказов для отправки на сегодня")
                return

            # Ручное преобразование результатов в словари
            columns = [
                'id', 'target_date', 'order_time', 'quantity',
                'bitrix_quantity_id', 'location', 'is_from_bitrix',
                'is_sent_to_bitrix', 'is_cancelled',
                'bitrix_id', 'full_name'
            ]
            pending_orders = [dict(zip(columns, row)) for row in result]

            success_count = 0
            for order in pending_orders:
                try:
                    # Проверка данных перед отправкой
                    if not all(key in order for key in ['bitrix_id', 'quantity', 'target_date', 'order_time']):
                        logger.error(f"Неполные данные в заказе ID {order.get('id')}")
                        continue

                    bitrix_id = await self._create_bitrix_order({
                        'bitrix_id': order['bitrix_id'],
                        'quantity': order['quantity'],
                        'target_date': order['target_date'],
                        'order_time': order['order_time'],
                        'location': order.get('location', 'Офис')
                    })
                    
                    if bitrix_id:
                        db.execute('''
                            UPDATE orders 
                            SET is_sent_to_bitrix = TRUE,
                                bitrix_order_id = ?
                            WHERE id = ?
                        ''', (bitrix_id, order['id']))
                        success_count += 1
                        logger.debug(f"Заказ ID {order['id']} успешно отправлен в Bitrix")
                        
                except Exception as e:
                    logger.error(f"Ошибка обработки заказа ID {order.get('id')}: {str(e)}")

            logger.info(f"Итог: отправлено {success_count}/{len(pending_orders)} заказов")
            
        except Exception as e:
            logger.error(f"Критическая ошибка в _push_to_bitrix: {str(e)}", exc_info=True)
            raise

    async def _create_bitrix_order(self, order_data: dict) -> Optional[str]:
        """Создает заказ в Bitrix24 с улучшенной обработкой ошибок"""
        try:
            # Проверка обязательных полей
            required_fields = {
                'bitrix_id': (str, int),  # Может быть как строкой, так и числом
                'quantity': int,
                'target_date': str,
                'order_time': str
            }
            
            for field, field_types in required_fields.items():
                if field not in order_data:
                    raise ValueError(f"Отсутствует обязательное поле: {field}")
                
                if not isinstance(order_data[field], field_types if isinstance(field_types, tuple) else (field_types,)):
                    raise ValueError(f"Поле {field} должно быть типа {field_types.__name__ if not isinstance(field_types, tuple) else ' или '.join(t.__name__ for t in field_types)}")

            # Преобразуем bitrix_id в строку, если это число
            bitrix_id = str(order_data['bitrix_id']) if isinstance(order_data['bitrix_id'], int) else order_data['bitrix_id']

            # Маппинг значений
            quantity_map = {1: '821', 2: '822', 3: '823', 4: '824', 5: '825'}
            location_map = {
                'Офис': '826',
                'ПЦ 1': '827',
                'ПЦ 2': '828',
                'Склад': '1063'
            }

            params = {
                'entityTypeId': 1222,
                'fields': {
                    'ufCrm45_1743599470': bitrix_id,  # Используем преобразованный bitrix_id
                    'ufCrm45ObedyCount': quantity_map.get(order_data['quantity'], '821'),
                    'ufCrm45ObedyFrom': location_map.get(order_data.get('location', 'Офис'), '826'),
                    'createdTime': f"{order_data['target_date']}T{order_data['order_time']}+03:00"
                }
            }

             # Отправка запроса
            logger.debug(f"Отправка заказа в Bitrix: {params}")
            result = await self.bx.call('crm.item.add', params)
            
            # Упрощенная проверка ответа
            if not result or 'id' not in result:
                logger.error(f"Неверный ответ от Bitrix: {result}")
                return None
                
            # Возвращаем ID созданного заказа    
            return str(result['id'])  # Bitrix всегда возвращает ID в корне объекта
            
        except Exception as e:
            logger.error(f"Ошибка создания заказа: {str(e)}")
            return None
        
    async def close_orders_at_930(self):
        """Закрывает прием заказов в 9:30"""
        now = datetime.now()
        if now.time() >= time(9, 30) and now.time() < time(9, 31):
            # Обновляем настройки бота
            db.execute("UPDATE bot_settings SET setting_value = 'False' WHERE setting_name = 'orders_enabled'")
            
            # Отправляем все оставшиеся заказы
            await self._push_to_bitrix()