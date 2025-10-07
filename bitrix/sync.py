# ##bitrix/sync.py
from typing import Dict, List, Optional
import asyncio
import logging
from datetime import datetime, time, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram.ext import ContextTypes
import os
from fast_bitrix24 import Bitrix
from dotenv import load_dotenv
from db import CONFIG, db
import json

logger = logging.getLogger(__name__)
logging.getLogger('fast_bitrix24').setLevel(logging.WARNING)

class BitrixSync:
    def __init__(self):
        """Инициализация подключения к Bitrix24"""
        try:
            load_dotenv('data/configs/.env')
            self.webhook = os.getenv('BITRIX_WEBHOOK')
            self.rest_webhook = os.getenv('BITRIX_REST_WEBHOOK')
            if not self.webhook or not self.rest_webhook:
                raise ValueError("BITRIX_WEBHOOK или BITRIX_REST_WEBHOOK не найден в .env")
            
            # ID пользователей для определения источника заказа
            self.BOT_USER_IDS = ['1']  # Бот
            self.BITRIX_USER_IDS = ['24']   # Обычные пользователи Bitrix
            
            self._quantity_map = {
                '821': 1, '822': 2, '823': 3, '824': 4, '825': 5
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
            # Для REST API будем использовать обычные requests
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
        """Настройка расписания синхронизации с улучшенной обработкой ошибок"""
        # Синхронизация из Bitrix каждые 5 минут (6:00-10:00)
        self.scheduler.add_job(
            self.sync_recent_orders,
            'cron',
            minute='*/5',
            hour='6-10',
            day_of_week='mon-fri',
            kwargs={'hours': 24}  # Синхронизируем только последние 24 часа
        )
        
        # 🔥 ВОССТАНОВЛЕНИЕ: Отправка накопленных заказов в 9:25:00
        self.scheduler.add_job(
            self._push_to_bitrix_with_retry,
            'cron',
            minute=25,          # Только в 25 минут
            hour=9,             # Только в 9 часов утра
            day_of_week='mon-fri', # Только по будням
            second=0            # В начале минуты (0 секунд)
        )
        

        # Финальная попытка в 9:29:59 (за 1 сек до закрытия)
        self.scheduler.add_job(
            self._push_to_bitrix_with_retry,
            'cron',
            minute=29,
            hour=9,
            day_of_week='mon-fri',
            second=59  # Критически важная секунда!
        )

        # Жесткое закрытие в 9:30:00
        self.scheduler.add_job(
            self.close_orders_at_930,
            'cron',
            minute=30,
            hour=9,
            day_of_week='mon-fri',
            second=0  # Точное время
        )

        # Ежедневная очистка ВСЕХ отмененных заказов в 23:00
        self.scheduler.add_job(
            self.cleanup_all_cancelled_orders,
            'cron',
            hour=23,
            minute=0
        )

    async def _push_to_bitrix_with_retry(self, context: ContextTypes.DEFAULT_TYPE = None):
        """Отправка заказов в Bitrix с повторными попытками и уведомлениями"""
        try:
            success = await self._push_to_bitrix()
            if not success:
                error_msg = "⚠️ Не удалось отправить некоторые заказы в Bitrix"
                logger.warning(error_msg)
                await self._notify_admin(error_msg, context)
        except Exception as e:
            error_msg = f"❌ Критическая ошибка при отправке в Bitrix: {str(e)}"
            logger.error(error_msg, exc_info=True)
            await self._notify_admin(error_msg, context)

    async def _notify_admin(self, message: str, context: ContextTypes.DEFAULT_TYPE = None):
        """Улучшенная версия уведомления администраторов с использованием существующей логики"""
        try:
            if not hasattr(CONFIG, 'admin_ids') or not CONFIG.admin_ids:
                logger.warning("ADMIN_IDS не установлены в конфиге")
                return
            
            # Если передан context (для отправки через бота)
            if context and hasattr(context, 'bot'):
                for admin_id in CONFIG.admin_ids:
                    try:
                        await context.bot.send_message(
                            chat_id=admin_id,
                            text=message
                        )
                        logger.info(f"Уведомление отправлено администратору {admin_id}")
                    except Exception as e:
                        logger.error(f"Не удалось отправить сообщение админу {admin_id}: {e}")
            else:
                # Логируем, если нет возможности отправить через бота
                logger.info(f"Уведомление для администраторов (нет context.bot): {message}")
                
        except Exception as e:
            logger.error(f"Ошибка в _notify_admin: {e}")

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
        """Синхронизирует заказы за последние 2 дня"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=2)  # Только 2 дня вместо 2 месяцев
        
        logger.info(f"Синхронизация заказов с {start_date.date()} по {end_date.date()}")
        
        return await self.sync_orders(
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        )

    async def sync_employees(self) -> Dict[str, int]:
        """Синхронизация всех сотрудников из Bitrix REST API"""
        stats = {
            'total': 0, 'updated': 0, 'added': 0,
            'errors': 0, 'no_match': 0, 'merged': 0
        }
        
        try:
            # 1. Получаем всех сотрудников из REST API
            rest_employees = await self._get_rest_employees()
            if not rest_employees:
                logger.error("Не удалось получить сотрудников из Bitrix REST API")
                return stats

            # Логируем общее количество
            logger.info(f"Получено {len(rest_employees)} сотрудников из Bitrix")
            stats['total'] = len(rest_employees)

            # 2. Получаем старых сотрудников из CRM для сопоставления
            crm_employees = await self._get_crm_employees()
            crm_employee_map = {self._normalize_name(emp['VALUE']): emp['ID'] for emp in crm_employees}
            
            # 3. Создаем mapping между REST сотрудниками и CRM ID по имени
            rest_to_crm_mapping = {}
            for rest_emp in rest_employees:
                rest_name_normalized = self._normalize_name(rest_emp['ФИО'])
                crm_id = crm_employee_map.get(rest_name_normalized)
                if crm_id:
                    rest_to_crm_mapping[rest_emp['ID']] = crm_id

            # 4. Получаем всех существующих сотрудников из базы
            existing_employees = db.get_employees(active_only=False)
            existing_bitrix_ids = {str(e.get('bitrix_id')) for e in existing_employees if e.get('bitrix_id')}
            existing_names = {self._normalize_name(e['full_name']) for e in existing_employees}
            
            # 5. Обновляем существующих и добавляем новых сотрудников
            for rest_emp in rest_employees:
                try:
                    bitrix_id = rest_emp['ID']
                    rest_name = rest_emp['ФИО']
                    rest_name_normalized = self._normalize_name(rest_name)
                    
                    # Ищем существующего сотрудника по bitrix_id
                    existing_by_id = next((e for e in existing_employees if str(e.get('bitrix_id')) == bitrix_id), None)
                    
                    # Ищем существующего сотрудника по имени
                    existing_by_name = next((e for e in existing_employees if self._normalize_name(e['full_name']) == rest_name_normalized), None)
                    
                    if existing_by_id:
                        # Обновляем существующего сотрудника по bitrix_id
                        await self._update_existing_employee(existing_by_id, rest_emp, rest_to_crm_mapping, stats)
                    elif existing_by_name:
                        # Обновляем существующего сотрудника по имени
                        await self._update_existing_employee(existing_by_name, rest_emp, rest_to_crm_mapping, stats)
                    else:
                        # Добавляем совершенно нового сотрудника
                        await self._add_new_employee(rest_emp, rest_to_crm_mapping, stats)
                        
                except Exception as e:
                    stats['errors'] += 1
                    logger.error(f"Ошибка обработки сотрудника {rest_emp.get('ФИО', 'unknown')}: {e}")
            
            logger.info(f"Синхронизация сотрудников завершена. Статистика: {stats}")
            return stats

        except Exception as e:
            logger.error(f"Ошибка синхронизации сотрудников: {e}", exc_info=True)
            return stats

    async def sync_orders(self, start_date: str, end_date: str, incremental: bool = True) -> Dict[str, int]:
        """Синхронизирует заказы из Bitrix в локальную базу"""
        stats = {
            'processed': 0, 'added': 0, 'updated': 0,
            'exists': 0, 'skipped': 0, 'errors': 0
        }
        
        try:
            bitrix_orders = await self._get_bitrix_orders(start_date, end_date)
            if not bitrix_orders:
                logger.warning(f"Не найдено заказов за период {start_date} - {end_date}")
                return stats
                
            # 🔥 ЗАКОММЕНТИРУЙТЕ ЭТУ СТРОКУ - анализ проблемных заказов
            # await self.log_problematic_orders(start_date, end_date)
            
            # Сортируем заказы по ID перед обработкой
            bitrix_orders.sort(key=lambda x: int(x['id']))
            
            for order in bitrix_orders:
                parsed_order = self._parse_bitrix_order(order)
                if not parsed_order:
                    stats['errors'] += 1
                    continue
                    
                # 🔥 ИНКРЕМЕНТАЛЬНАЯ ПРОВЕРКА
                if incremental and not self._need_order_update(parsed_order):
                    stats['skipped'] += 1
                    continue
                    
                await self._process_single_order(parsed_order, stats)
                
            logger.info(
                f"Синхронизация завершена. Обработано: {stats['processed']}, "
                f"Добавлено: {stats['added']}, Обновлено: {stats['updated']}, "
                f"Пропущено: {stats['skipped']}, Ошибок: {stats['errors']}"
            )
            return stats
            
        except Exception as e:
            logger.error(f"Ошибка синхронизации заказов: {e}")
            return stats

    async def _get_bitrix_orders(self, start_date: str, end_date: str) -> List[Dict]:
        params = {
            'entityTypeId': 1222,
            'select': [
                'id', 
                'ufCrm45_1751956286',  # 🔥 Новое поле (bitrix_id)
                'ufCrm45_1743599470',  # 🔥 Старое поле (crm_employee_id)
                'ufCrm45ObedyCount', 
                'ufCrm45ObedyFrom', 
                'ufCrm45_1744188327370', 
                'createdTime', 
                'createdBy', 
                'updatedBy', 
                'assignedById'
            ],
            'filter': {
                '>=createdTime': f'{start_date}T00:00:00+03:00',
                '<=createdTime': f'{end_date}T23:59:59+03:00'
            }
        }
        
        try:
            logger.info(f"Запрос заказов с {start_date} по {end_date}")
            orders = await self.bx.get_all('crm.item.list', params)
            
            if orders:
                logger.info(f"Получено {len(orders)} заказов из Bitrix")
            else:
                logger.warning("Не получено ни одного заказа за указанный период")
                
            return orders
        except Exception as e:
            logger.error(f"Ошибка получения заказов: {e}")
            return []

    def _parse_bitrix_order(self, order: Dict) -> Optional[Dict]:
        """Парсит данные заказа из Bitrix с приоритетом для CRM employee_id"""
        try:
            bitrix_order_id = str(order.get('id', ''))
            
            # 🔥 ПРИОРИТЕТ: сначала проверяем старое поле (CRM employee_id)
            employee_crm_id = order.get('ufCrm45_1743599470')    # Старое поле - ПРИОРИТЕТ
            employee_bitrix_id = order.get('ufCrm45_1751956286')  # Новое поле - резерв
            
            # Определяем какое ID использовать (приоритет для CRM ID)
            search_field = None
            search_value = None
            
            if employee_crm_id is not None:
                search_field = 'crm_employee_id'
                search_value = str(employee_crm_id)
                logger.debug(f"Используем CRM ID: {search_value} для заказа {bitrix_order_id}")
            elif employee_bitrix_id is not None:
                search_field = 'bitrix_id'
                search_value = str(employee_bitrix_id)
                logger.debug(f"Используем Bitrix ID: {search_value} для заказа {bitrix_order_id} (CRM ID отсутствует)")
            else:
                logger.warning(f"Заказ {bitrix_order_id} без ID сотрудника (оба поля пустые)")
                return None
                
            # Определяем источник заказа
            is_from_bitrix = self._determine_order_source(order)
            
            # Остальная логика остается той же
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
                'bitrix_order_id': bitrix_order_id,
                'search_field': search_field,        # Поле для поиска
                'search_value': search_value,        # Значение для поиска
                'quantity': quantity,
                'bitrix_quantity': bitrix_quantity,
                'location': location,
                'date': date,
                'created_time': created_time,
                'is_cancelled': is_cancelled,
                'is_from_bitrix': is_from_bitrix
            }
        except Exception as e:
            logger.error(f"Ошибка парсинга заказа {order.get('id', 'unknown')}: {e}")
            return None
    
    async def _process_single_order(self, order: Dict, stats: Dict):
        """Обрабатывает один заказ с приоритетом для CRM employee_id"""
        try:
            search_field = order.get('search_field')
            search_value = order.get('search_value')
            bitrix_order_id = order.get('bitrix_order_id')
            
            if not search_field or not search_value:
                logger.warning(f"Заказ {bitrix_order_id} без данных сотрудника")
                stats['skipped'] += 1
                return

            # 🔥 Правильный поиск в зависимости от типа ID
            user_id = None
            if search_field == 'crm_employee_id':
                user_id = await self._get_local_user_id_by_crm_id(search_value)
                if not user_id:
                    logger.debug(f"Пользователь с CRM ID {search_value} не найден, пробуем Bitrix ID...")
                    # Если не нашли по CRM ID, пробуем найти сотрудника и получить его Bitrix ID
                    employee = await self._find_employee_by_crm_id(search_value)
                    if employee and employee.get('bitrix_id'):
                        user_id = await self._get_local_user_id(employee['bitrix_id'])
                        
            elif search_field == 'bitrix_id':
                user_id = await self._get_local_user_id(search_value)
                
            if not user_id:
                logger.warning(f"Сотрудник {search_field}={search_value} не найден для заказа {bitrix_order_id}")
                stats['skipped'] += 1
                return

            # 🔥 КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: сначала проверяем существование заказа по bitrix_order_id
            existing_order = None
            if bitrix_order_id:
                existing_order = self._find_local_order(bitrix_order_id)
            
            # Если заказ не найден по bitrix_order_id, проверяем по user_id и дате
            if not existing_order:
                existing_order = self._find_local_order_by_user_and_date(user_id, order['date'])
            
            order_id = None
            success = False
            
            if existing_order:
                order_id = existing_order['id']
                logger.debug(f"Обновление существующего заказа {bitrix_order_id or order_id}")
                success = self._update_local_order(order_id, order)
                if success:
                    stats['updated'] += 1
                    logger.info(f"✅ Обновлен заказ {bitrix_order_id or order_id} (источник: {'Bitrix' if order.get('is_from_bitrix') else 'Бот'})")
                else:
                    stats['errors'] += 1
                    logger.error(f"❌ Ошибка обновления заказа {bitrix_order_id or order_id}")
            else:
                success = self._add_local_order(user_id, order)
                
                if success:
                    stats['added'] += 1
                    logger.info(f"✅ Добавлен заказ {bitrix_order_id} (источник: {'Bitrix' if order.get('is_from_bitrix') else 'Бот'})")
                else:
                    stats['errors'] += 1
                    logger.error(f"❌ Ошибка добавления заказа {bitrix_order_id}")

            # Обновляем локацию пользователя
            if order.get('location') and order['location'] != 'Неизвестно':
                await self._update_user_location(user_id, order['location'])

            # 🔥 Обновляем метку времени синхронизации
            if success and order_id:
                db.execute(
                    "UPDATE orders SET last_synced_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (order_id,)
                )

            stats['processed'] += 1

        except Exception as e:
            logger.error(f"❌ Критическая ошибка обработки заказа {order.get('bitrix_order_id', 'unknown')}: {str(e)}")
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
                SELECT id, user_id FROM orders 
                WHERE bitrix_order_id = ? 
                LIMIT 1
            """, (bitrix_order_id,))
            result = db.cursor.fetchone()
            if result:
                return {'id': result[0], 'user_id': result[1]}
            return None
        except Exception as e:
            logger.error(f"Ошибка поиска заказа: {e}")
            return None
        
    def _get_full_order(self, order_id: int) -> Optional[Dict]:
        """Возвращает полные данные заказа по ID, включая user_id и target_date"""
        try:
            db.cursor.execute("""
                SELECT id, user_id, target_date 
                FROM orders 
                WHERE id = ?
                LIMIT 1
            """, (order_id,))
            
            result = db.cursor.fetchone()
            if result:
                return {
                    'id': result[0],
                    'user_id': result[1], 
                    'target_date': result[2]
                }
            return None
        except Exception as e:
            logger.error(f"Ошибка получения полных данных заказа {order_id}: {e}")
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
                    is_from_bitrix = ?,
                    updated_at = datetime('now')
                WHERE id = ?
            """, (
                order['quantity'],
                bitrix_quantity,
                order['is_cancelled'],
                order.get('is_from_bitrix', True),
                order_id
            ))
            db.conn.commit()
            
            success = db.cursor.rowcount > 0
            if success:
                logger.debug(f"Заказ {order_id} успешно обновлен")
            else:
                logger.warning(f"Заказ {order_id} не был обновлен (rowcount: {db.cursor.rowcount})")
                
            return success
        except Exception as e:
            logger.error(f"Ошибка обновления заказа {order_id}: {e}")
            return False

    def _add_local_order(self, user_id: int, order: Dict) -> bool:
        """Добавляет новый заказ с проверкой на дубликаты"""
        try:
            bitrix_order_id = str(order.get('bitrix_order_id', ''))
            target_date = str(order.get('date', datetime.now().strftime('%Y-%m-%d')))
            
            # 🔥 ДОПОЛНИТЕЛЬНАЯ ПРОВЕРКА: убедимся что заказа для этого пользователя на эту дату нет
            existing_order = db.execute(
                "SELECT 1 FROM orders WHERE user_id = ? AND target_date = ? LIMIT 1",
                (user_id, target_date)
            )
            if existing_order:
                logger.warning(f"⚠️ Заказ для пользователя {user_id} на дату {target_date} уже существует! Пропускаем дубликат.")
                return False  # Не добавляем дубликат

            if not bitrix_order_id:
                logger.error("Не указан bitrix_order_id для заказа")
                return False

            # Подготовка остальных полей из order
            quantity = int(order.get('quantity', 1))
            bitrix_quantity = str(order.get('bitrix_quantity', '821'))
            is_cancelled = bool(order.get('is_cancelled', False))
            target_date = str(order.get('date', datetime.now().strftime('%Y-%m-%d')))
            
            created_time = str(order.get('created_time', ''))
            order_time = (
                created_time.split('T')[1][:8] 
                if 'T' in created_time 
                else datetime.now().strftime('%H:%M')
            )

            # Вставка в базу данных
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
                order_time,
                quantity,
                bitrix_quantity,
                is_cancelled,
                order.get('is_from_bitrix', True),
                bitrix_order_id
            ))
            
            db.conn.commit()
            
            if db.cursor.rowcount == 1:
                logger.info(f"✅ Успешно добавлен заказ Bitrix ID: {bitrix_order_id}")
                return True
                
            return False
            
        except Exception as e:
            logger.error(f"❌ Ошибка добавления заказа: {e}", exc_info=True)
            db.conn.rollback()
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

    async def _sync_single_employee(self, employee: Dict, rest_employees: List[Dict], rest_to_crm_mapping: Dict, stats: Dict):
        """Синхронизирует одного сотрудника"""
        try:
            local_name = self._normalize_name(employee['full_name'])
            
            # Ищем сотрудника в REST данных по имени
            rest_emp = None
            for emp in rest_employees:
                if self._normalize_name(emp['ФИО']) == local_name:
                    rest_emp = emp
                    break
            
            if rest_emp:
                update_needed = False
                update_data = {}
                
                # Получаем текущие значения из базы данных
                current_bitrix_id = employee.get('bitrix_id')
                current_position = employee.get('position', '')
                current_department = employee.get('department', '')
                current_is_deleted = employee.get('is_deleted', False)
                
                # Сравниваем с данными из Bitrix
                new_bitrix_id = rest_emp['ID']
                if current_bitrix_id != new_bitrix_id:
                    update_data['bitrix_id'] = new_bitrix_id
                    update_needed = True
                
                new_position = rest_emp.get('Должность', '')
                if current_position != new_position:
                    update_data['position'] = new_position
                    update_needed = True
                
                new_department = rest_emp.get('Подразделение', '')
                if current_department != new_department:
                    update_data['department'] = new_department
                    update_needed = True
                
                # Обновляем статус активности
                is_active = rest_emp.get('Активен', True)
                new_is_deleted = not is_active
                if current_is_deleted != new_is_deleted:
                    update_data['is_deleted'] = new_is_deleted
                    update_needed = True
                
                # Обновляем CRM ID если есть соответствие
                crm_info = rest_to_crm_mapping.get(new_bitrix_id)
                if crm_info and employee.get('crm_employee_id') != crm_info['crm_id']:
                    update_data['crm_employee_id'] = crm_info['crm_id']
                    update_needed = True
                
                if update_needed:
                    success = db.update_user_data(
                        user_id=employee['id'],
                        **update_data
                    )
                    if success:
                        stats['updated'] += 1
                        logger.info(f"Обновлены данные сотрудника {employee['full_name']}")
                    else:
                        stats['errors'] += 1
            else:
                stats['no_match'] += 1
                logger.warning(f"Сотрудник {employee['full_name']} не найден в Bitrix")
                
        except Exception as e:
            stats['errors'] += 1
            logger.error(f"Ошибка обработки {employee}: {e}")
            
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
        """Нормализует имя для сравнения (учитывает ФИО)"""
        if not name:
            return ""
        return (
            name.strip().lower()
            .replace("ё", "е")
            .translate(str.maketrans("", "", ".,-"))
        )
    
    async def _push_to_bitrix(self) -> bool:
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
                AND o.bitrix_order_id IS NULL  -- ⚠️ Важно: только заказы без bitrix_id
                AND u.bitrix_id IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 FROM orders o2 
                    WHERE o2.user_id = o.user_id 
                    AND o2.target_date = o.target_date
                    AND o2.is_from_bitrix = TRUE
                    AND o2.is_cancelled = FALSE
                )
            ''', (today,))
            
            if not result:
                logger.info("Нет заказов для отправки на сегодня")
                return True  # Успех, так как нечего отправлять

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
                        
                except Exception as e:
                    logger.error(f"Ошибка обработки заказа ID {order.get('id')}: {str(e)}")

            logger.info(f"Итог: отправлено {success_count}/{len(pending_orders)} заказов")
            return success_count == len(pending_orders)  # True если все успешно
            
        except Exception as e:
            logger.error(f"Критическая ошибка в _push_to_bitrix: {str(e)}", exc_info=True)
            return False

    async def _create_bitrix_order(self, order_data: dict) -> Optional[str]:
        """Создает заказ в Bitrix24 с приоритетом для CRM employee_id"""
        try:
            # Проверка обязательных полей
            required_fields = {
                'bitrix_id': (str, int),
                'quantity': int,
                'target_date': str,
                'order_time': str
            }
            
            for field, field_types in required_fields.items():
                if field not in order_data:
                    raise ValueError(f"Отсутствует обязательное поле: {field}")

            # Получаем пользователя чтобы узнать его CRM ID
            user_id = order_data.get('bitrix_id')
            user_data = db.execute(
                "SELECT crm_employee_id FROM users WHERE bitrix_id = ? LIMIT 1",
                (user_id,)
            )
            
            crm_employee_id = None
            if user_data and user_data[0][0]:
                crm_employee_id = user_data[0][0]
                logger.debug(f"Найден CRM ID {crm_employee_id} для пользователя {user_id}")

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
                    'ufCrm45ObedyCount': quantity_map.get(order_data['quantity'], '821'),
                    'ufCrm45ObedyFrom': location_map.get(order_data.get('location', 'Офис'), '826'),
                    'createdTime': f"{order_data['target_date']}T{order_data['order_time']}+03:00"
                }
            }

            # 🔥 ПРИОРИТЕТ: используем CRM employee_id если есть
            if crm_employee_id:
                params['fields']['ufCrm45_1743599470'] = crm_employee_id
                logger.info(f"📤 Отправка заказа с CRM ID: {crm_employee_id}")
            else:
                params['fields']['ufCrm45_1751956286'] = user_id
                logger.info(f"📤 Отправка заказа с Bitrix ID: {user_id} (CRM ID отсутствует)")

            result = await self.bx.call('crm.item.add', params)
            
            if not result or 'id' not in result:
                logger.error(f"Неверный ответ от Bitrix: {result}")
                return None
                
            return str(result['id'])
            
        except Exception as e:
            logger.error(f"Ошибка создания заказа: {str(e)}")
            return None
        
    async def _get_user_name_by_bitrix_id(self, bitrix_id: str) -> Optional[str]:
        """Получает имя пользователя по его Bitrix ID"""
        try:
            result = db.execute(
                "SELECT full_name FROM users WHERE bitrix_id = ? LIMIT 1",
                (bitrix_id,)
            )
            return result[0][0] if result else "Unknown"
        except Exception as e:
            logger.error(f"Ошибка получения имени пользователя: {e}")
            return "Unknown"
        
    def _find_employee_by_name(self, crm_employees: List[Dict], user_name: str) -> Optional[Dict]:
        """Ищет сотрудника в списке CRM по имени"""
        if not user_name or user_name == "Unknown":
            return None
            
        normalized_search = self._normalize_name(user_name)
        
        for employee in crm_employees:
            normalized_employee = self._normalize_name(employee['VALUE'])
            
            # Простое сравнение
            if normalized_search == normalized_employee:
                return employee
            
            # Поиск по частичному совпадению (фамилия + имя)
            search_parts = normalized_search.split()
            employee_parts = normalized_employee.split()
            
            if len(search_parts) >= 2 and len(employee_parts) >= 2:
                if search_parts[0] == employee_parts[0] and search_parts[1] == employee_parts[1]:
                    return employee
        
        return None
        
    # Добавить новый метод для получения сотрудников через REST API
    async def _get_rest_employees(self) -> List[Dict]:
        """Получает сотрудников через REST API с полной информацией включая отчество"""
        import requests
        import json
        
        try:
            # 1. Запрашиваем подразделения
            logger.info("Запрашиваю подразделения через REST API...")
            dep_response = requests.get(self.rest_webhook + 'department.get')
            dep_data = dep_response.json()

            dept_dict = {}
            dept_parent_dict = {}

            if 'result' in dep_data:
                for dept in dep_data['result']:
                    dept_id_key = str(dept['ID'])
                    dept_dict[dept_id_key] = dept['NAME']
                    dept_parent_dict[dept_id_key] = str(dept.get('PARENT', ''))
                logger.info(f"Получено {len(dept_dict)} подразделений")
            else:
                logger.error("Ошибка при запросе отделов:", dep_data)
                return []

            # Функция для построения полного пути отдела
            def get_full_department_name(dept_id):
                if not dept_id or dept_id not in dept_dict:
                    return 'Не указано'
                
                name_parts = [dept_dict[dept_id]]
                parent_id = dept_parent_dict.get(dept_id)
                
                while parent_id and parent_id in dept_dict:
                    name_parts.append(dept_dict[parent_id])
                    parent_id = dept_parent_dict.get(parent_id)
                
                return ' -> '.join(reversed(name_parts))

            # 2. Запрашиваем сотрудников с пагинацией
            logger.info("Запрашиваю сотрудников через REST API...")
            all_users = []
            start = 0
            batch_size = 50
            
            while True:
                params = {
                    'FILTER[USER_TYPE]': 'employee',
                    'start': start
                }
                user_response = requests.get(self.rest_webhook + 'user.get', params=params)
                user_data = user_response.json()

                if 'result' not in user_data or not user_data['result']:
                    break
                    
                all_users.extend(user_data['result'])
                start += batch_size
                
                # Если получено меньше batch_size, значит это последняя страница
                if len(user_data['result']) < batch_size:
                    break

            logger.info(f"Получено {len(all_users)} сотрудников")

            result_list = []
            for user in all_users:
                dept_id_list = user.get('UF_DEPARTMENT', [])
                dept_id = str(dept_id_list[0]) if dept_id_list else None

                # Формируем полное ФИО с отчеством
                last_name = user.get('LAST_NAME', '')
                first_name = user.get('NAME', '')
                second_name = user.get('SECOND_NAME', '')  # Добавляем отчество
                
                # Собираем полное имя с отчеством
                full_name_parts = [last_name, first_name]
                if second_name:
                    full_name_parts.append(second_name)
                full_name = ' '.join(filter(None, full_name_parts))

                # В _get_rest_employees() после сбора городов добавьте:
                city_fields = ['PERSONAL_CITY', 'WORK_CITY', 'UF_CITY', 'UF_LOCATION']
                city = None
                for field in city_fields:
                    if user.get(field):
                        city = user.get(field)
                        break

                # # 🔥 Отладка: покажем что нашли
                # if city:
                #     logger.info(f"🏙️ Найден город для {full_name}: {city}")
                # else:
                #     logger.info(f"⚠️ Город не найден для {full_name}")

                employee_info = {
                    'ID': str(user['ID']),
                    'ФИО': full_name,
                    'Фамилия': last_name,
                    'Имя': first_name,
                    'Отчество': second_name,
                    'Должность': user.get('WORK_POSITION', 'Не указана'),
                    'Подразделение': dept_dict.get(dept_id, 'Не указано'),
                    'Подразделение_полное': get_full_department_name(dept_id),
                    'Активен': user.get('ACTIVE', False),
                    'Город': city  # 🔥 Город добавлен
                }
                result_list.append(employee_info)

            # Логирование после того как all_users определен
            if all_users:
                logger.debug(f"Пример данных сотрудника: {all_users[0]}")
            else:
                logger.debug("Нет данных сотрудников")

            return result_list
                
        except Exception as e:
            logger.error(f"Ошибка получения сотрудников через REST API: {e}")
            return []
        
    def _user_exists_by_bitrix_id(self, bitrix_id: str) -> bool:
        """Проверяет существование пользователя по Bitrix ID"""
        try:
            result = db.execute(
                "SELECT 1 FROM users WHERE bitrix_id = ? LIMIT 1",
                (bitrix_id,)
            )
            return bool(result)
        except Exception as e:
            logger.error(f"Ошибка проверки пользователя по Bitrix ID: {e}")
            return False
        
    async def _get_local_user_id_by_crm_id(self, crm_employee_id: str) -> Optional[int]:
        """Находит локальный ID пользователя по CRM employee_id"""
        try:
            result = db.execute(
                "SELECT id FROM users WHERE crm_employee_id = ? LIMIT 1",
                (crm_employee_id,)
            )
            return result[0][0] if result else None
        except Exception as e:
            logger.error(f"Ошибка поиска пользователя по CRM ID: {e}")
            return None
        
    def remove_duplicate_employees(self):
        """Удаляет дублирующихся сотрудников"""
        try:
            # Находим дубли по bitrix_id
            duplicates = db.execute('''
                SELECT bitrix_id, COUNT(*) as count 
                FROM users 
                WHERE bitrix_id IS NOT NULL 
                GROUP BY bitrix_id 
                HAVING COUNT(*) > 1
            ''')
            
            for bitrix_id, count in duplicates:
                # Оставляем первую запись, удаляем остальные
                db.execute('''
                    DELETE FROM users 
                    WHERE id NOT IN (
                        SELECT MIN(id) 
                        FROM users 
                        WHERE bitrix_id = ? 
                        GROUP BY bitrix_id
                    ) AND bitrix_id = ?
                ''', (bitrix_id, bitrix_id))
                logger.info(f"Удалено {count-1} дублей для bitrix_id {bitrix_id}")
                
            # Находим дубли по имени
            name_duplicates = db.execute('''
                SELECT full_name, COUNT(*) as count 
                FROM users 
                GROUP BY full_name 
                HAVING COUNT(*) > 1
            ''')
            
            for full_name, count in name_duplicates:
                # Оставляем первую запись, удаляем остальные
                db.execute('''
                    DELETE FROM users 
                    WHERE id NOT IN (
                        SELECT MIN(id) 
                        FROM users 
                        WHERE full_name = ? 
                        GROUP BY full_name
                    ) AND full_name = ?
                ''', (full_name, full_name))
                logger.info(f"Удалено {count-1} дублей по имени: {full_name}")
                
        except Exception as e:
            logger.error(f"Ошибка удаления дублей: {e}")

    async def _update_existing_employee(self, existing_employee: Dict, rest_emp: Dict, rest_to_crm_mapping: Dict, stats: Dict):
        """Обновляет данные существующего сотрудника только при реальных изменениях"""
        try:
            # 🔥 ДОБАВЬТЕ ОТЛАДОЧНОЕ ЛОГИРОВАНИЕ
            logger.debug(f"🔍 Проверка сотрудника: {rest_emp['ФИО']}")
            logger.debug(f"   Текущий город в БД: '{existing_employee.get('city')}' (тип: {type(existing_employee.get('city'))})")
            logger.debug(f"   Новый город из Bitrix: '{rest_emp.get('Город')}'")
            
            update_data = {}
            bitrix_id = rest_emp['ID']
            
            # Проверяем изменения позиции
            new_position = rest_emp.get('Должность', '')
            current_position = existing_employee.get('position', '')
            if current_position != new_position and new_position:
                update_data['position'] = new_position
            
            # Проверяем изменения отдела
            new_department = rest_emp.get('Подразделение', '')
            current_department = existing_employee.get('department', '')
            if current_department != new_department and new_department:
                update_data['department'] = new_department
            
            # Проверяем изменения статуса активности
            is_active = rest_emp.get('Активен', True)
            new_is_deleted = not is_active
            current_is_deleted = existing_employee.get('is_deleted', False)
            if current_is_deleted != new_is_deleted:
                update_data['is_deleted'] = new_is_deleted
            
            # Проверяем изменения CRM ID
            crm_id = rest_to_crm_mapping.get(bitrix_id)
            current_crm_id = existing_employee.get('crm_employee_id')
            if crm_id and current_crm_id != crm_id:
                update_data['crm_employee_id'] = crm_id

            # 🔥 ИСПРАВЛЕНИЕ: правильная логика для городов
            new_city = rest_emp.get('Город', '')
            current_city = existing_employee.get('city', '')
            
            # Дополнительная отладка
            logger.debug(f"   current_city после get: '{current_city}' (длина: {len(current_city) if current_city else 0})")
            
            # Различаем три случая:
            if not current_city and new_city:
                # Случай 1: Первоначальная установка
                update_data['city'] = new_city
                logger.info(f"🏙️ Устанавливаем город для {rest_emp['ФИО']}: '{new_city}'")
            elif current_city and new_city and current_city != new_city:
                # Случай 2: Реальное изменение
                update_data['city'] = new_city
                logger.info(f"🏙️ Изменяем город для {rest_emp['ФИО']}: '{current_city}' → '{new_city}'")
            else:
                # Случай 3: Пропускаем - город не изменился
                logger.debug(f"✅ Город для {rest_emp['ФИО']} актуален: '{current_city}'")

            # 🔥 ИСПОЛЬЗУЕМ МЕТОД ДЛЯ ПРОВЕРКИ РЕАЛЬНЫХ ИЗМЕНЕНИЙ (для остальных полей)
            other_fields_update_data = {k: v for k, v in update_data.items() if k != 'city'}
            
            has_other_changes = other_fields_update_data and self._has_real_changes(existing_employee, other_fields_update_data)
            has_city_changes = 'city' in update_data  # Город уже прошел проверку выше
            
            if has_city_changes or has_other_changes:
                success = db.update_user_data(
                    user_id=existing_employee['id'],
                    **update_data
                )
                if success:
                    stats['updated'] += 1
                    changes_list = []
                    if has_city_changes:
                        changes_list.append('город')
                    if has_other_changes:
                        changes_list.extend(list(other_fields_update_data.keys()))
                    logger.info(f"🔄 Обновлен сотрудник: {rest_emp['ФИО']} - изменения: {changes_list}")
                else:
                    stats['errors'] += 1
            else:
                # Логируем отладочную информацию только при debug уровне
                logger.debug(f"✅ Данные сотрудника {rest_emp['ФИО']} актуальны")
                        
        except Exception as e:
            stats['errors'] += 1
            logger.error(f"Ошибка обновления сотрудника {rest_emp['ФИО']}: {e}")

    async def cleanup_inactive_employees(self):
        """Помечает как удаленных сотрудников, которых нет в активных Bitrix"""
        try:
            # Получаем всех активных сотрудников из Bitrix
            rest_employees = await self._get_rest_employees()
            if not rest_employees:
                return
                
            active_bitrix_ids = {emp['ID'] for emp in rest_employees if emp.get('Активен', True)}
            
            # Помечаем как удаленных тех, кого нет в активных
            db.execute('''
                UPDATE users 
                SET is_deleted = TRUE, updated_at = CURRENT_TIMESTAMP
                WHERE is_employee = TRUE 
                AND bitrix_id IS NOT NULL 
                AND bitrix_id NOT IN ({})
            '''.format(','.join(['?' for _ in active_bitrix_ids])), list(active_bitrix_ids))
            
            logger.info(f"Обновлен статус неактивных сотрудников")
            
        except Exception as e:
            logger.error(f"Ошибка очистки неактивных сотрудников: {e}")
            
    async def _add_new_employee(self, rest_emp: Dict, rest_to_crm_mapping: Dict, stats: Dict):
        """Добавляет нового сотрудника из Bitrix"""
        try:
            bitrix_id = rest_emp['ID']
            crm_id = rest_to_crm_mapping.get(bitrix_id)
            
            # 🔥 УБРАТЬ маппинг подразделений на локации!
            # Локация заказа определяется в заказе, а не здесь
            department = rest_emp.get('Подразделение', '')
            city = rest_emp.get('Город', '')

            db.execute(
                """INSERT INTO users 
                (full_name, is_employee, is_verified, bitrix_id, crm_employee_id,
                position, department, city, is_deleted, bitrix_entity_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    rest_emp['ФИО'], 
                    True, 
                    False, 
                    bitrix_id,
                    crm_id,
                    rest_emp.get('Должность', ''),
                    department,
                    city,
                    not rest_emp.get('Активен', True),
                    'rest_employee'
                )
            )
            stats['added'] += 1
            logger.info(f"✅ Добавлен новый сотрудник: {rest_emp['ФИО']} из города {city or 'не указан'}")
            
        except Exception as e:
            stats['errors'] += 1
            logger.error(f"Ошибка добавления сотрудника {rest_emp['ФИО']}: {e}")
            
    def _determine_order_source(self, order_data: Dict) -> bool:
        """
        Определяет источник заказа на основе данных из Bitrix.
        Возвращает True если заказ создан в Bitrix, False если создан ботом.
        """
        try:
            created_by = str(order_data.get('createdBy', ''))
            updated_by = str(order_data.get('updatedBy', ''))
            assigned_by = str(order_data.get('assignedById', ''))
            
            # Логика определения источника:
            # Если любой из пользователей - обычный пользователь Bitrix, считаем заказ из Bitrix
            if (created_by in self.BITRIX_USER_IDS or 
                updated_by in self.BITRIX_USER_IDS or 
                assigned_by in self.BITRIX_USER_IDS):
                return True
                
            # Если все пользователи - бот/системные, считаем заказ из бота
            if (created_by in self.BOT_USER_IDS and 
                (not updated_by or updated_by in self.BOT_USER_IDS) and 
                (not assigned_by or assigned_by in self.BOT_USER_IDS)):
                return False
                
            # По умолчанию считаем заказ из Bitrix (более безопасный вариант)
            return True
            
        except Exception as e:
            logger.error(f"Ошибка определения источника заказа: {e}")
            return True  # По умолчанию считаем из Bitrix
        
    async def update_existing_orders_sources(self):
        """
        Обновляет источник (is_from_bitrix) для уже существующих заказов
        на основе данных из Bitrix.
        """
        try:
            logger.info("Начинаем обновление источников существующих заказов...")
            
            # Получаем все заказы из Bitrix за последние 2 месяца
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
            
            bitrix_orders = await self._get_bitrix_orders(start_date, end_date)
            if not bitrix_orders:
                logger.warning("Не получено заказов для обновления")
                return
                
            updated_count = 0
            for order in bitrix_orders:
                parsed_order = self._parse_bitrix_order(order)
                if not parsed_order:
                    continue
                    
                bitrix_id = parsed_order['bitrix_id']
                is_from_bitrix = parsed_order['is_from_bitrix']
                
                # Обновляем заказ в базе
                db.cursor.execute("""
                    UPDATE orders 
                    SET is_from_bitrix = ?
                    WHERE bitrix_order_id = ?
                """, (is_from_bitrix, bitrix_id))
                
                if db.cursor.rowcount > 0:
                    updated_count += 1
                    
            db.conn.commit()
            logger.info(f"Обновлено источников для {updated_count} заказов")
            
        except Exception as e:
            logger.error(f"Ошибка обновления источников заказов: {e}")
            db.conn.rollback()

    async def _find_employee_by_crm_id(self, crm_id: str) -> Optional[Dict]:
        """Находит сотрудника по CRM ID в базе данных"""
        try:
            result = db.execute(
                "SELECT id, full_name, bitrix_id FROM users WHERE crm_employee_id = ? LIMIT 1",
                (crm_id,)
            )
            if result:
                return {
                    'id': result[0][0],
                    'full_name': result[0][1],
                    'bitrix_id': result[0][2]
                }
            return None
        except Exception as e:
            logger.error(f"Ошибка поиска сотрудника по CRM ID {crm_id}: {e}")
            return None

    def _need_order_update(self, order: Dict) -> bool:
        """Проверяет нужно ли обновлять заказ (инкрементальная синхронизация)"""
        bitrix_id = order.get('bitrix_order_id')
        if not bitrix_id:
            return True  # Новый заказ без ID
            
        # Проверяем существование заказа
        existing = db.execute(
            """SELECT id, updated_at, last_synced_at, is_cancelled, quantity 
            FROM orders WHERE bitrix_order_id = ?""",
            (bitrix_id,)
        )
        
        if not existing:
            return True  # Новый заказ - нужно добавить
        
        order_id, db_updated, last_synced, db_cancelled, db_quantity = existing[0]
        
        # Проверяем, изменились ли важные данные
        current_cancelled = order.get('is_cancelled', False)
        current_quantity = order.get('quantity', 1)
        
        # Если данные не изменились и заказ уже синхронизирован - пропускаем
        if (db_cancelled == current_cancelled and 
            db_quantity == current_quantity and
            last_synced and db_updated and 
            last_synced >= db_updated):
            return False
            
        return True  # Данные изменились или нужна синхронизация
    
    async def sync_recent_orders(self, hours: int = 24):
        """Синхронизирует только заказы за последние N часов"""
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(hours=hours)).strftime('%Y-%m-%d')
        
        logger.info(f"🔄 Инкрементальная синхронизация за {hours} часов...")
        return await self.sync_orders(start_date, end_date, incremental=True)
    
    def _find_local_order_by_user_and_date(self, user_id: int, target_date: str) -> Optional[Dict]:
        """Ищет заказ в локальной базе по user_id и дате"""
        try:
            db.cursor.execute("""
                SELECT id, bitrix_order_id FROM orders 
                WHERE user_id = ? AND target_date = ? 
                LIMIT 1
            """, (user_id, target_date))
            result = db.cursor.fetchone()
            if result:
                return {'id': result[0], 'bitrix_order_id': result[1]}
            return None
        except Exception as e:
            logger.error(f"Ошибка поиска заказа по user_id и дате: {e}")
            return None
        
    async def cancel_order_immediate_cleanup(self, order_id: int) -> bool:
        """
        Немедленное удаление отмененного заказа из базы.
        Условия удаления:
        - Заказ должен быть отменен (is_cancelled = TRUE)
        - Заказ должен быть создан в боте (is_from_bitrix = FALSE)
        - Заказ не должен иметь bitrix_order_id (не синхронизирован с Bitrix)
        - Дата заказа должна быть сегодняшней или будущей
        """
        try:
            now = datetime.now(CONFIG.timezone)
            today = now.date()
            
            # Получаем информацию о заказе
            db.cursor.execute("""
                SELECT id, target_date, is_cancelled, is_from_bitrix, bitrix_order_id 
                FROM orders WHERE id = ?
            """, (order_id,))
            
            order_info = db.cursor.fetchone()
            if not order_info:
                logger.warning(f"Заказ {order_id} не найден")
                return False
                
            order_id, target_date_str, is_cancelled, is_from_bitrix, bitrix_order_id = order_info
            target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
            
            # Проверяем условия для удаления
            if not is_cancelled:
                logger.warning(f"Заказ {order_id} не отменен, удаление невозможно")
                return False
                
            if is_from_bitrix:
                logger.warning(f"Заказ {order_id} создан в Bitrix, удаление невозможно")
                return False
                
            if bitrix_order_id:
                logger.warning(f"Заказ {order_id} уже синхронизирован с Bitrix (ID: {bitrix_order_id}), удаление невозможно")
                return False
                
            # Удаляем заказ
            db.cursor.execute("DELETE FROM orders WHERE id = ?", (order_id,))
            db.conn.commit()
            
            logger.info(f"✅ Немедленно удален отмененный заказ {order_id} на дату {target_date}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка немедленного удаления заказа {order_id}: {e}")
            db.conn.rollback()
            return False
        
    async def cleanup_all_cancelled_orders(self):
        """Ежедневная очистка всех отмененных заказов"""
        try:
            with db.conn:
                db.cursor.execute("""
                    DELETE FROM orders 
                    WHERE is_cancelled = TRUE 
                    AND is_from_bitrix = FALSE
                    AND bitrix_order_id IS NULL
                """)
                
                deleted_count = db.cursor.rowcount
                
            logger.info(f"✅ Ежедневная очистка: удалено {deleted_count} отмененных заказов")
            return deleted_count
            
        except Exception as e:
            logger.error(f"❌ Ошибка ежедневной очистки отмененных заказов: {e}")
            return 0
        
    async def close_orders_at_930(self):
        """Финальное закрытие с гарантированной проверкой и детальным логированием"""
        current_time = datetime.now(CONFIG.timezone).strftime('%H:%M:%S')
        
        # Последняя попытка синхронизации
        logger.info(f"🔄 [{current_time}] Запуск финальной синхронизации перед закрытием")
        sync_result = await self._push_to_bitrix_with_retry()
        
        if not sync_result:
            logger.critical(f"⚠️ [{current_time}] Последняя синхронизация провалилась!")
        else:
            logger.info(f"✅ [{current_time}] Все актуальные заказы синхронизированы")
        
        # Основная логика закрытия
        await self._disable_ordering()
        closure_time = datetime.now(CONFIG.timezone).strftime('%H:%M:%S.%f')[:-3]
        logger.info(f"⏹ [{closure_time}] Прием заказов официально закрыт")

    async def log_problematic_orders(self, start_date: str, end_date: str):
        """Логирует заказы с проблемными сотрудниками"""
        try:
            bitrix_orders = await self._get_bitrix_orders(start_date, end_date)
            if not bitrix_orders:
                return
                
            problematic_orders = []
            for order in bitrix_orders:
                employee_crm_id = order.get('ufCrm45_1743599470')
                employee_bitrix_id = order.get('ufCrm45_1751956286')
                
                if not employee_crm_id and not employee_bitrix_id:
                    problematic_orders.append({
                        'id': order.get('id'),
                        'reason': 'Оба ID отсутствуют'
                    })
                elif employee_crm_id and not self._user_exists_by_crm_id(employee_crm_id):
                    problematic_orders.append({
                        'id': order.get('id'),
                        'reason': f'CRM ID {employee_crm_id} не найден'
                    })
                elif employee_bitrix_id and not self._user_exists_by_bitrix_id(employee_bitrix_id):
                    problematic_orders.append({
                        'id': order.get('id'),
                        'reason': f'Bitrix ID {employee_bitrix_id} не найден'
                    })
            
            if problematic_orders:
                logger.warning(f"Найдено {len(problematic_orders)} проблемных заказов:")
                for order in problematic_orders[:10]:  # Логируем первые 10
                    logger.warning(f"Заказ {order['id']}: {order['reason']}")
                    
        except Exception as e:
            logger.error(f"Ошибка анализа проблемных заказов: {e}")

    def _need_city_update(self, user_id: int, new_city: str) -> bool:
        """Проверяет, нужно ли обновлять город для пользователя"""
        try:
            # Проверяем текущий город в базе
            result = db.execute(
                "SELECT city FROM users WHERE id = ? LIMIT 1",
                (user_id,)
            )
            
            if not result:
                return False
                
            current_city = result[0][0]
            
            # Если город уже установлен и не пустой - не обновляем
            if current_city and current_city != 'None' and current_city != '':
                return False
                
            # Если новый город пустой - не обновляем
            if not new_city or new_city == '':
                return False
                
            return True
                
        except Exception as e:
            logger.error(f"Ошибка проверки города для пользователя {user_id}: {e}")
            return False
        
    def _has_real_changes(self, existing_employee: Dict, update_data: Dict) -> bool:
        """
        Проверяет есть ли реальные изменения в данных перед обновлением.
        Игнорирует случаи когда оба значения пустые.
        """
        try:
            for field, new_value in update_data.items():
                current_value = existing_employee.get(field)
                
                # Если оба значения пустые/None - пропускаем
                if not current_value and not new_value:
                    continue
                    
                # Если текущее значение пустое, а новое есть - это изменение
                if not current_value and new_value:
                    return True
                    
                # Если текущее значение есть, а новое пустое - это изменение
                if current_value and not new_value:
                    return True

                # Для строковых полей - сравниваем с очисткой
                if isinstance(current_value, str) and isinstance(new_value, str):
                    current_clean = current_value.strip().lower()
                    new_clean = new_value.strip().lower()
                    
                    if current_clean != new_clean:
                        return True
                        
                # Для булевых/числовых полей - прямое сравнение
                elif current_value != new_value:
                    return True
                    
            return False
            
        except Exception as e:
            logger.error(f"Ошибка проверки изменений: {e}")
            return True

def _user_exists_by_crm_id(self, crm_id: str) -> bool:
    """Проверяет существование пользователя по CRM ID"""
    try:
        result = db.execute(
            "SELECT 1 FROM users WHERE crm_employee_id = ? LIMIT 1",
            (crm_id,)
        )
        return bool(result)
    except Exception as e:
        logger.error(f"Ошибка проверки пользователя по CRM ID: {e}")
        return False