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
from database import db
from config import CONFIG
from models import User, Order, BitrixMapping
from sqlalchemy import text
import json
import requests
import ssl
import urllib3
import aiohttp
import warnings
from time_config import TIME_CONFIG

# Отключаем SSL предупреждения для requests
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore', message='Unverified HTTPS request')

# Для aiohttp
import ssl
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

logger = logging.getLogger(__name__)
logging.getLogger('bitrix.sync').setLevel(logging.INFO)  # или DEBUG для детальных логов
logging.getLogger('fast_bitrix24').setLevel(logging.WARNING)

class BitrixSync:
    def __init__(self, bot_application=None):
        """Инициализация подключения к Bitrix24 с нормальным SSL"""
        try:
            load_dotenv('data/configs/.env')
            self.webhook = os.getenv('BITRIX_WEBHOOK')
            self.rest_webhook = os.getenv('BITRIX_REST_WEBHOOK')
            if not self.webhook or not self.rest_webhook:
                raise ValueError("BITRIX_WEBHOOK или BITRIX_REST_WEBHOOK не найден в .env")
            
            # 🔥 ПРОСТОЙ КЛИЕНТ БЕЗ КАСТОМНЫХ НАСТРОЕК
            self.bx = Bitrix(self.webhook)
            
            # 🔥 ДОБАВЬТЕ ЭТУ СТРОКУ
            self.bot_application = bot_application
            
            # Остальной код инициализации...
            self.BOT_USER_IDS = ['1']
            self.BITRIX_USER_IDS = ['24']
            
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
            
            logger.info("✅ Подключение к Bitrix24 инициализировано (SSL включен)")
            self.scheduler = AsyncIOScheduler(timezone=TIME_CONFIG.TIMEZONE)
            self.is_running = False
            
            # 🔥 ДОБАВЛЯЕМ: флаг для отслеживания активных сессий
            self._active_sessions = []
            
        except Exception as e:
            logger.critical(f"Ошибка инициализации BitrixSync: {e}")
            raise

    async def close(self):
        """Корректное закрытие всех ресурсов"""
        logger.info("🔄 Закрытие BitrixSync...")
        try:
            # Останавливаем планировщик ТОЛЬКО если он запущен
            if hasattr(self, 'scheduler') and self.scheduler and self.scheduler.running:
                self.scheduler.shutdown()
                logger.info("✅ Планировщик остановлен")
            
            # Закрываем активные сессии
            for session in self._active_sessions:
                try:
                    if not session.closed:
                        await session.close()
                except Exception as e:
                    logger.warning(f"Ошибка закрытия сессии: {e}")
            self._active_sessions.clear()
            logger.info("✅ BitrixSync корректно закрыт")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при закрытии BitrixSync: {e}")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

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
        """Настройка расписания синхронизации с использованием TIME_CONFIG"""
        # Получаем рабочие дни в формате cron
        work_days_cron = self._get_cron_days(TIME_CONFIG.WORK_DAYS)
        
        # Синхронизация из Bitrix каждые 5 минут (в рабочее время)
        self.scheduler.add_job(
            self.sync_recent_orders,
            'cron',
            minute='*/5',
            hour=f'6-10',  # с 6:00 до 10:00
            day_of_week=work_days_cron,
            kwargs={'hours': 24}
        )
        
        # Отправка накопленных заказов
        self.scheduler.add_job(
            self._push_to_bitrix_with_retry,  # ✅ БЕЗ lambda
            'cron',
            minute=TIME_CONFIG.MODIFICATION_DEADLINE.minute - 9,  # 9:21
            hour=TIME_CONFIG.MODIFICATION_DEADLINE.hour,
            day_of_week=work_days_cron,
            second=0
        )
        
        # Финальная попытка перед закрытием
        self.scheduler.add_job(
            self._push_to_bitrix_with_retry,
            'cron',
            minute=TIME_CONFIG.ORDER_DEADLINE.minute - 1,  # 9:29
            hour=TIME_CONFIG.ORDER_DEADLINE.hour,
            day_of_week=work_days_cron,
            second=50
        )

        # Жесткое закрытие в ORDER_DEADLINE
        self.scheduler.add_job(
            self.close_orders_at_930,
            'cron',
            minute=TIME_CONFIG.ORDER_DEADLINE.minute,  # 9:30
            hour=TIME_CONFIG.ORDER_DEADLINE.hour,
            day_of_week=work_days_cron,
            second=0
        )

        # Ежедневная очистка ВСЕХ отмененных заказов
        self.scheduler.add_job(
            self.cleanup_all_cancelled_orders,
            'cron',
            hour=23,
            minute=0
        )

    def _get_cron_days(self, days_list):
        """Конвертирует список дней в формат cron"""
        # days_list: [0,1,2,3,4] -> 'mon,tue,wed,thu,fri'
        day_names = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']
        return ','.join([day_names[day] for day in days_list])

    async def _push_to_bitrix_with_retry(self, context: ContextTypes.DEFAULT_TYPE = None):
        """Отправка заказов в Bitrix с повторными попытками и уведомлениями"""
        try:
            success = await self._push_to_bitrix()
            
            if not success:
                error_msg = "⚠️ Не удалось отправить некоторые заказы в Bitrix"
                logger.warning(error_msg)
                
                # 🔥 ИСПОЛЬЗУЕМ bot_application для получения context
                if CONFIG.master_admin_id and self.bot_application:
                    # Создаем минимальный context для отправки сообщения
                    try:
                        from telegram import InlineKeyboardButton, InlineKeyboardMarkup
                        
                        failed_count = len(getattr(self, '_last_failed_order_ids', []))
                        detailed_msg = f"{error_msg}\n\n"
                        if failed_count > 0:
                            detailed_msg += f"❌ Не отправлено заказов: {failed_count}\n"
                        detailed_msg += "⏰ Следующая попытка через несколько минут\n\n"
                        detailed_msg += "💡 Запустите отправку вручную: /manual_sync"
                        
                        keyboard = InlineKeyboardMarkup([[
                            InlineKeyboardButton("🔄 Отправить вручную", callback_data="manual_push_orders")
                        ]])
                        
                        await self.bot_application.bot.send_message(
                            chat_id=CONFIG.master_admin_id,
                            text=detailed_msg,
                            reply_markup=keyboard
                        )
                        logger.info(f"✅ Уведомление отправлено админу {CONFIG.master_admin_id}")
                    except Exception as e:
                        logger.error(f"Ошибка отправки уведомления: {e}")
            else:
                logger.info("✅ Все заказы успешно отправлены в Bitrix")
                
        except Exception as e:
            error_msg = f"❌ Критическая ошибка при отправке в Bitrix: {str(e)}"
            logger.error(error_msg, exc_info=True)

    async def _notify_master_admin_with_button(
        self, 
        message: str, 
        context: ContextTypes.DEFAULT_TYPE,
        failed_count: int = 0
    ):
        """Уведомление главного админа с кнопкой ручной отправки"""
        try:
            from telegram import InlineKeyboardButton, InlineKeyboardMarkup
            
            # 🔥 ПРОВЕРКА АДАПТИРОВАНА ПОД ВАШ CONFIG
            if not CONFIG.master_admin_id:
                logger.warning("master_admin_id не установлен в конфиге")
                return
            
            # Формируем детальное сообщение
            detailed_msg = f"{message}\n\n"
            if failed_count > 0:
                detailed_msg += f"❌ Не отправлено заказов: {failed_count}\n"
            detailed_msg += "⏰ Следующая автоматическая попытка через несколько минут\n\n"
            detailed_msg += "💡 Вы можете запустить отправку вручную прямо сейчас:"
            
            # Создаем кнопку для ручной отправки
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "🔄 Отправить заказы вручную", 
                    callback_data="manual_push_orders"
                )
            ]])
            
            await context.bot.send_message(
                chat_id=CONFIG.master_admin_id,  # 🔥 ИСПОЛЬЗУЕМ ВАШ CONFIG
                text=detailed_msg,
                reply_markup=keyboard
            )
            
            logger.info(f"✅ Уведомление с кнопкой отправлено главному админу {CONFIG.master_admin_id}")
            
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления главному админу: {e}")

    async def get_pending_orders_info(self) -> dict:
        """Получить информацию о неотправленных заказах"""
        try:
            today = datetime.now(TIME_CONFIG.TIMEZONE).date().isoformat()
            
            with db.get_session() as session:
                pending_orders = session.query(Order).filter(
                    Order.is_sent_to_bitrix == False,
                    Order.is_cancelled == False,
                    Order.target_date == today,
                    Order.bitrix_order_id == None,
                    Order.is_from_bitrix == False
                ).all()
                
                return {
                    'count': len(pending_orders),
                    'order_ids': [order.id for order in pending_orders],
                    'date': today
                }
        except Exception as e:
            logger.error(f"Ошибка получения информации о неотправленных заказах: {e}")
            return {'count': 0, 'order_ids': [], 'date': None}

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

    def _clean_string(self, text: str) -> str:
        """Очищает строку от недопустимых символов"""
        if not text:
            return text
            
        # Удаляем символы, которые могут вызвать проблемы
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
        """Синхронизация всех сотрудников из Bitrix REST API с улучшенным сопоставлением"""
        stats = {
            'total': 0, 'updated': 0, 'added': 0,
            'errors': 0, 'no_match': 0, 'merged': 0, 'exists': 0
        }
        
        try:
            # 1. Получаем всех сотрудников из REST API
            rest_employees = await self._get_rest_employees()
            if not rest_employees:
                logger.error("Не удалось получить сотрудников из Bitrix REST API")
                return stats

            logger.info(f"Получено {len(rest_employees)} сотрудников из Bitrix")
            stats['total'] = len(rest_employees)

            # 2. Получаем сотрудников из CRM для сопоставления
            crm_employees = await self._get_crm_employees()
            
            # 🔥 СОЗДАЕМ УЛУЧШЕННУЮ МАПУ ДЛЯ ПОИСКА
            crm_employee_map = {}
            for emp in crm_employees:
                crm_name = emp['VALUE']
                crm_id = emp['ID']
                
                # Нормализуем имя для поиска
                normalized_full = self._normalize_name(crm_name)
                crm_employee_map[normalized_full] = crm_id
                
                # Создаем ключи для поиска по фамилии и имени (без отчества)
                name_parts = crm_name.split()
                if len(name_parts) >= 2:
                    # Ключ: фамилия + имя
                    fi_key = f"{name_parts[0]} {name_parts[1]}"
                    crm_employee_map[self._normalize_name(fi_key)] = crm_id
                    
                    # Ключ: фамилия + первая буква имени
                    fi_initial_key = f"{name_parts[0]} {name_parts[1][0]}"
                    crm_employee_map[self._normalize_name(fi_initial_key)] = crm_id

            # 3. Создаем mapping между REST сотрудниками и CRM ID
            rest_to_crm_mapping = {}
            for rest_emp in rest_employees:
                rest_name = rest_emp['ФИО']
                rest_name_normalized = self._normalize_name(rest_name)
                
                # 🔥 УЛУЧШЕННЫЙ ПОИСК СООТВЕТСТВИЯ
                crm_id = None
                
                # Сначала ищем по полному ФИО
                crm_id = crm_employee_map.get(rest_name_normalized)
                
                # Если не нашли, ищем по фамилии и имени
                if not crm_id:
                    rest_name_parts = rest_name.split()
                    if len(rest_name_parts) >= 2:
                        fi_key = f"{rest_name_parts[0]} {rest_name_parts[1]}"
                        crm_id = crm_employee_map.get(self._normalize_name(fi_key))
                
                if crm_id:
                    rest_to_crm_mapping[rest_emp['ID']] = crm_id
                    logger.debug(f"Найдено соответствие: {rest_name} -> CRM ID: {crm_id}")

            logger.info(f"Создано {len(rest_to_crm_mapping)} соответствий REST -> CRM")

            # 3.5. Получаем данные из сущности 1120 (дата трудоустройства, рабочее время)
            entity_1120_map = await self._get_entity_1120_employees()

            # 4. Получаем всех существующих сотрудников из базы
            with db.get_session() as session:
                existing_employees = session.query(User).filter(
                    User.is_employee == True
                ).all()
                
                # 🔥 СОЗДАЕМ СЛОВАРЬ ДЛЯ БЫСТРОГО ПОИСКА
                existing_by_bitrix_id = {}
                existing_by_name = {}
                
                for emp in existing_employees:
                    emp_dict = {
                        'id': emp.id,
                        'full_name': emp.full_name,
                        'position': emp.position,
                        'department': emp.department,
                        'city': emp.city,
                        'is_deleted': emp.is_deleted,
                        'crm_employee_id': emp.crm_employee_id,
                        'bitrix_id': emp.bitrix_id,
                        'employment_date': emp.employment_date,
                        'work_time_start': emp.work_time_start,
                        'work_time_end': emp.work_time_end,
                    }

                    if emp.bitrix_id:
                        existing_by_bitrix_id[str(emp.bitrix_id)] = emp_dict

                    # Добавляем в поиск по имени
                    normalized_name = self._normalize_name(emp.full_name)
                    existing_by_name[normalized_name] = emp_dict
            
            # 5. Обновляем существующих и добавляем новых сотрудников
            for rest_emp in rest_employees:
                try:
                    bitrix_id = rest_emp['ID']
                    rest_name = rest_emp['ФИО']
                    
                    # 🔥 ПЕРВЫЙ ПРИОРИТЕТ: ищем по bitrix_id
                    existing_employee = None
                    if bitrix_id in existing_by_bitrix_id:
                        existing_employee = existing_by_bitrix_id[bitrix_id]
                        logger.debug(f"Найден сотрудник по Bitrix ID: {rest_name}")
                    else:
                        # 🔥 ВТОРОЙ ПРИОРИТЕТ: ищем по имени
                        normalized_name = self._normalize_name(rest_name)
                        if normalized_name in existing_by_name:
                            existing_employee = existing_by_name[normalized_name]
                            logger.debug(f"Найден сотрудник по имени: {rest_name}")
                    
                    if existing_employee:
                        # ОБНОВЛЯЕМ существующего сотрудника
                        await self._update_existing_employee(existing_employee, rest_emp, rest_to_crm_mapping, stats, entity_1120_map)
                    else:
                        # ДОБАВЛЯЕМ нового сотрудника
                        await self._add_new_employee(rest_emp, rest_to_crm_mapping, stats, entity_1120_map)
                        
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
                
            # Сортируем заказы по ID перед обработкой
            bitrix_orders.sort(key=lambda x: int(x['id']))
            
            # 🔥 ИСПРАВЛЕНИЕ: используем существующий метод вместо отсутствующего
            for order in bitrix_orders:
                parsed_order = self._parse_bitrix_order(order)
                if not parsed_order:
                    stats['errors'] += 1
                    continue
                    
                # 🔥 ИНКРЕМЕНТАЛЬНАЯ ПРОВЕРКА
                if incremental and not self._need_order_update(parsed_order):
                    stats['skipped'] += 1
                    continue
                    
                # 🔥 ИСПРАВЛЕНИЕ: используем _process_single_order вместо _process_single_order_with_session
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
            
            # 🔥 ДОБАВЬТЕ ТАЙМАУТ И ПОВТОРНЫЕ ПОПЫТКИ
            import asyncio
            for attempt in range(3):
                try:
                    orders = await asyncio.wait_for(
                        self.bx.get_all('crm.item.list', params),
                        timeout=30.0
                    )
                    
                    if orders:
                        logger.info(f"Получено {len(orders)} заказов из Bitrix")
                    else:
                        logger.warning("Не получено ни одного заказа за указанный период")
                        
                    return orders
                    
                except asyncio.TimeoutError:
                    logger.warning(f"Таймаут при получении заказов (попытка {attempt + 1}/3)")
                    if attempt < 2:
                        await asyncio.sleep(5)
                    else:
                        raise
                        
        except Exception as e:
            logger.error(f"Ошибка получения заказов после 3 попыток: {e}")
            return []

    def _parse_bitrix_order(self, order: Dict) -> Optional[Dict]:
        """Парсит данные заказа из Bitrix с приоритетом для CRM crm_employee_id"""
        try:
            bitrix_order_id = str(order.get('id', ''))
            
            # 🔥 ПРИОРИТЕТ: сначала проверяем старое поле (CRM crm_employee_id)
            employee_crm_id = order.get('ufCrm45_1743599470')    # Старое поле - ПРИОРИТЕТ
            employee_bitrix_id = order.get('ufCrm45_1751956286')  # Новое поле - резерв
            
            # Определяем какое ID использовать (приоритет для CRM ID)
            crm_employee_id = None
            bitrix_user_id = None
            
            if employee_crm_id is not None:
                crm_employee_id = str(employee_crm_id)
                logger.debug(f"Используем CRM ID: {crm_employee_id} для заказа {bitrix_order_id}")
            elif employee_bitrix_id is not None:
                bitrix_user_id = str(employee_bitrix_id)
                logger.debug(f"Используем Bitrix ID: {bitrix_user_id} для заказа {bitrix_order_id} (CRM ID отсутствует)")
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
                'bitrix_id': bitrix_order_id,  # ← ИЗМЕНИТЬ НА bitrix_id
                'crm_employee_id': crm_employee_id,  # ← ДОБАВИТЬ
                'bitrix_user_id': bitrix_user_id,  # ← ДОБАВИТЬ
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
        """Обрабатывает один заказ с улучшенной логикой поиска сотрудника"""
        try:
            # 🔥 ИСПРАВЛЕНИЕ: используем новые поля из _parse_bitrix_order
            crm_employee_id = order.get('crm_employee_id')
            bitrix_user_id = order.get('bitrix_user_id')
            bitrix_id = order.get('bitrix_id')  # ← ВАЖНО: это ID заказа из Bitrix
            
            if not crm_employee_id and not bitrix_user_id:
                logger.warning(f"Заказ {bitrix_id} без данных сотрудника")
                stats['skipped'] += 1
                return

            user_id = None
            
            # 🔥 УЛУЧШЕННАЯ ЛОГИКА ПОИСКА СОТРУДНИКА
            if crm_employee_id:
                # 1. Прямой поиск по CRM ID
                user_id = await self._get_local_user_id_by_crm_id(crm_employee_id)
                
                if not user_id:
                    # 2. Если не нашли по CRM ID, используем улучшенный поиск по имени
                    logger.debug(f"Пользователь с CRM ID {crm_employee_id} не найден, ищем по имени...")
                    user_id = await self._find_user_by_crm_id_via_name(crm_employee_id)
                        
            elif bitrix_user_id:
                # Прямой поиск по Bitrix ID
                user_id = await self._get_local_user_id(bitrix_user_id)
                
            if not user_id:
                logger.warning(f"Сотрудник не найден для заказа {bitrix_id}")
                await self.sync_employees()  # Пробуем синхронизировать сотрудников
                stats['skipped'] += 1
                return

            # 🔥 ИЗМЕНЕНИЕ: Ищем заказ ТОЛЬКО по bitrix_order_id
            existing_order = None
            if bitrix_id:
                existing_order = self._find_local_order(bitrix_id)
            
            order_id = None
            success = False
            
            if existing_order:
                order_id = existing_order['id']
                success = self._update_local_order(order_id, order)
                if success:
                    stats['updated'] += 1
                    logger.info(f"✅ Обновлен заказ {bitrix_id}")
                else:
                    stats['errors'] += 1
            else:
                success = self._add_local_order(user_id, order)
                if success:
                    stats['added'] += 1
                    logger.info(f"✅ Добавлен заказ {bitrix_id}")
                else:
                    stats['errors'] += 1

            # Обновляем локацию пользователя
            if order.get('location') and order['location'] != 'Неизвестно':
                await self._update_user_location(user_id, order['location'])

            if success and order_id:
                with db.get_session() as session:
                    session.execute(
                        text("UPDATE orders SET last_synced_at = CURRENT_TIMESTAMP WHERE id = :order_id"),
                        {'order_id': order_id}
                    )
                    session.commit()

            stats['processed'] += 1

        except Exception as e:
            logger.error(f"❌ Критическая ошибка обработки заказа {order.get('bitrix_id', 'unknown')}: {str(e)}")
            stats['errors'] += 1

    async def _find_user_by_crm_id_via_name(self, crm_id: str) -> Optional[int]:
        """Ищет пользователя по CRM ID через поиск по имени в CRM с учетом ФИО и обновляет crm_employee_id"""
        try:
            # Получаем список сотрудников из CRM
            crm_employees = await self._get_crm_employees()
            if not crm_employees:
                return None
                
            # Находим сотрудника в CRM по ID
            crm_employee = None
            for emp in crm_employees:
                if str(emp.get('ID')) == crm_id:
                    crm_employee = emp
                    break
                    
            if not crm_employee:
                logger.warning(f"Сотрудник с CRM ID {crm_id} не найден в списке CRM")
                return None
                
            # Получаем имя из CRM
            crm_employee_name = crm_employee.get('VALUE')
            if not crm_employee_name:
                logger.warning(f"У сотрудника CRM ID {crm_id} нет имени")
                return None
                
            logger.info(f"🔍 Ищем локального сотрудника по имени из CRM: '{crm_employee_name}'")
            
            # Ищем в локальной базе по имени с улучшенной логикой
            with db.get_session() as session:
                users = session.query(User).filter(User.is_employee == True).all()
                
                found_user = None
                
                # ШАГ 1: Пытаемся найти по полному совпадению (ФИО)
                for user in users:
                    local_name_normalized = self._normalize_name(user.full_name)
                    crm_name_normalized = self._normalize_name(crm_employee_name)
                    
                    # Полное совпадение ФИО
                    if local_name_normalized == crm_name_normalized:
                        logger.info(f"✅ Найден сотрудник по полному ФИО: '{user.full_name}' -> '{crm_employee_name}'")
                        found_user = user
                        break
                
                # ШАГ 2: Если не нашли по полному ФИО, ищем по фамилии и имени
                if not found_user:
                    crm_name_parts = crm_employee_name.split()
                    if len(crm_name_parts) >= 2:
                        # Берем только фамилию и имя из CRM
                        crm_last_first = f"{crm_name_parts[0]} {crm_name_parts[1]}"
                        crm_last_first_normalized = self._normalize_name(crm_last_first)
                        
                        for user in users:
                            local_name_normalized = self._normalize_name(user.full_name)
                            local_name_parts = local_name_normalized.split()
                            
                            if len(local_name_parts) >= 2:
                                # Берем только фамилию и имя из локальной базы
                                local_last_first = f"{local_name_parts[0]} {local_name_parts[1]}"
                                
                                if local_last_first == crm_last_first_normalized:
                                    logger.info(f"✅ Найден сотрудник по ФИ: '{user.full_name}' -> '{crm_employee_name}'")
                                    found_user = user
                                    break
                
                # ШАГ 3: Дополнительная попытка - ищем по фамилии и первой букве имени
                if not found_user and len(crm_name_parts) >= 2:
                    crm_last_initial = f"{crm_name_parts[0]} {crm_name_parts[1][0]}"
                    crm_last_initial_normalized = self._normalize_name(crm_last_initial)
                    
                    for user in users:
                        local_name_normalized = self._normalize_name(user.full_name)
                        local_name_parts = local_name_normalized.split()
                        
                        if len(local_name_parts) >= 2:
                            local_last_initial = f"{local_name_parts[0]} {local_name_parts[1][0]}"
                            
                            if local_last_initial == crm_last_initial_normalized:
                                logger.info(f"✅ Найден сотрудник по фамилии и инициалу: '{user.full_name}' -> '{crm_employee_name}'")
                                found_user = user
                                break
                
                if found_user:
                    # 🔥 ВАЖНО: Обновляем crm_employee_id у найденного пользователя
                    if found_user.crm_employee_id != crm_id:
                        logger.info(f"💾 Обновляем CRM ID для сотрудника {found_user.full_name}: {found_user.crm_employee_id} -> {crm_id}")
                        found_user.crm_employee_id = crm_id
                        session.commit()
                    
                    return found_user.id
                else:
                    logger.warning(f"❌ Не найден локальный сотрудник для имени из CRM: '{crm_employee_name}'")
                    return None
                    
        except Exception as e:
            logger.error(f"Ошибка поиска пользователя по CRM ID через имя: {e}")
            return None

    async def _get_local_user_id(self, bitrix_id: str) -> Optional[int]:
        """Находит локальный ID пользователя по Bitrix ID"""
        try:
            with db.get_session() as session:
                user = session.query(User).filter(
                    User.bitrix_id == bitrix_id
                ).first()
                return user.id if user else None
        except Exception as e:
            logger.error(f"Ошибка поиска пользователя: {e}")
            return None

    def _find_local_order(self, bitrix_id: str) -> Optional[Dict]:
        """Ищет заказ в локальной базе по ID из Bitrix"""
        try:
            with db.get_session() as session:
                order = session.query(Order).filter(
                    Order.bitrix_order_id == bitrix_id  # ← ищем ТОЛЬКО по bitrix_order_id
                ).first()
                if order:
                    return {
                        'id': order.id, 
                        'user_id': order.user_id,
                        'bitrix_order_id': order.bitrix_order_id,
                        'quantity': order.quantity,
                        'is_cancelled': order.is_cancelled
                    }
                return None
        except Exception as e:
            logger.error(f"Ошибка поиска заказа: {e}")
            return None
        
    def _get_full_order(self, order_id: int) -> Optional[Dict]:
        """Возвращает полные данные заказа по ID, включая user_id и target_date"""
        try:
            with db.get_session() as session:
                order = session.query(Order).filter(Order.id == order_id).first()
                if order:
                    return {
                        'id': order.id,
                        'user_id': order.user_id, 
                        'target_date': order.target_date
                    }
                return None
        except Exception as e:
            logger.error(f"Ошибка получения полных данных заказа {order_id}: {e}")
            return None

    def _update_local_order(self, order_id: int, order: Dict) -> bool:
        """Обновляет локальный заказ - С ОТЛАДКОЙ"""
        try:
            with db.get_session() as session:
                db_order = session.query(Order).filter(Order.id == order_id).first()
                if db_order:
                    # 🔥 ОТЛАДКА: Логируем текущие значения
                    logger.debug(f"🔍 ОБНОВЛЕНИЕ заказа {order_id}:")
                    logger.debug(f"   Текущие: cancelled={db_order.is_cancelled}, quantity={db_order.quantity}")
                    logger.debug(f"   Новые: cancelled={order['is_cancelled']}, quantity={order['quantity']}")
                    
                    # Проверяем реальные изменения
                    real_changes = False
                    if db_order.is_cancelled != order['is_cancelled']:
                        real_changes = True
                        db_order.is_cancelled = order['is_cancelled']
                        
                    if db_order.quantity != order['quantity']:
                        real_changes = True  
                        db_order.quantity = order['quantity']
                    
                    if real_changes:
                        db_order.updated_at = datetime.now()
                        db_order.last_synced_at = datetime.now()
                        session.commit()
                        logger.info(f"✅ Реальные изменения в заказе {order_id}")
                        return True
                    else:
                        # 🔥 ИСПРАВЛЕНИЕ: Не обновляем если нет реальных изменений
                        logger.debug(f"🔄 Заказ {order_id} не имеет реальных изменений - пропускаем")
                        # Но все равно обновляем last_synced_at чтобы избежать цикла
                        db_order.last_synced_at = datetime.now()
                        session.commit()
                        return True  # Возвращаем True чтобы статистика была корректной
                        
                return False
                
        except Exception as e:
            logger.error(f"❌ Ошибка обновления заказа {order_id}: {e}")
            return False

    def _add_local_order(self, user_id: int, order: Dict) -> bool:
        """Добавляет новый заказ - РАЗРЕШАЕМ НЕСКОЛЬКО ЗАКАЗОВ В ДЕНЬ"""
        try:
            bitrix_id = str(order.get('bitrix_id', ''))
            target_date = str(order.get('date', datetime.now().strftime('%Y-%m-%d')))
            
            with db.get_session() as session:
                # 🔥 ЗАКОММЕНТИРУЙ ЭТУ ПРОВЕРКУ - ОНА МЕШАЕТ!
                # existing_order = session.query(Order).filter(
                #     Order.user_id == user_id,
                #     Order.target_date == target_date
                # ).first()
                # 
                # if existing_order:
                #     logger.warning(f"⚠️ Заказ для пользователя {user_id} на дату {target_date} уже существует! Пропускаем дубликат.")
                #     return False

                # 🔥 ОСТАВЬ ТОЛЬКО проверку по bitrix_order_id
                if bitrix_id:
                    existing_order = session.query(Order).filter(
                        Order.bitrix_order_id == bitrix_id
                    ).first()
                    
                    if existing_order:
                        logger.warning(f"⚠️ Заказ с Bitrix ID {bitrix_id} уже существует! Пропускаем дубликат.")
                        return False

                if not bitrix_id:
                    logger.error("Не указан bitrix_id для заказа")
                    return False

                # Создаем новый заказ
                new_order = Order(
                    user_id=user_id,
                    target_date=target_date,
                    order_time=datetime.now().strftime('%H:%M:%S'),
                    quantity=order['quantity'],
                    bitrix_quantity_id=str(order.get('bitrix_quantity', '821')),
                    is_cancelled=order.get('is_cancelled', False),
                    is_from_bitrix=order.get('is_from_bitrix', True),
                    bitrix_order_id=bitrix_id,
                    is_active=True,
                    last_synced_at=datetime.now()
                )
                
                session.add(new_order)
                session.commit()
                
                logger.info(f"✅ Успешно добавлен заказ Bitrix ID: {bitrix_id}")
                return True
                
        except Exception as e:
            logger.error(f"❌ Ошибка добавления заказа: {e}", exc_info=True)
            return False
    
    async def _update_user_location(self, user_id: int, location: str) -> bool:
        """Обновляет локацию пользователя"""
        try:
            with db.get_session() as session:
                # Очищаем локацию перед обновлением
                clean_location = self._clean_string(location)
                
                user = session.query(User).filter(User.id == user_id).first()
                if user and user.location != clean_location:
                    user.location = clean_location
                    user.updated_at = datetime.now()
                    session.commit()
                    return True
                return False
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

    # Маппинг ufCrm20WorkTime -> (начало, конец) рабочего дня
    _work_time_map = {
        '1650': ('07:00', '16:00'),
        '1651': ('08:00', '17:00'),
        '1652': ('09:00', '18:00'),
        '1657': ('08:30', '17:30'),
    }

    async def _get_entity_1120_employees(self) -> Dict[str, Dict]:
        """
        Получает данные сотрудников из сущности 1120 (HR-карточки).
        Матчинг по нормализованному ФИО (title), т.к. assignedById меняется при увольнении.
        Возвращает словарь: normalized_name -> {employment_date, work_time_start, work_time_end}
        """
        result_map = {}
        try:
            params = {
                'entityTypeId': 1120,
                'select': [
                    'id',
                    'title',
                    'ufCrm20DataTrydoystroistva',
                    'ufCrm20WorkTime'
                ]
            }

            items = await asyncio.wait_for(
                self.bx.get_all('crm.item.list', params),
                timeout=60.0
            )

            if not items:
                logger.warning("Не получено данных из сущности 1120")
                return result_map

            logger.info(f"Получено {len(items)} записей из сущности 1120")

            for item in items:
                title = item.get('title', '')
                if not title:
                    continue

                # Парсим дату трудоустройства
                employment_date_raw = item.get('ufCrm20DataTrydoystroistva')
                employment_date = None
                if employment_date_raw:
                    try:
                        date_str = employment_date_raw.split('T')[0]
                        employment_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                    except (ValueError, TypeError) as e:
                        logger.debug(f"Неверный формат даты трудоустройства '{employment_date_raw}' для '{title}': {e}")

                # Парсим рабочее время
                work_time_id = str(item.get('ufCrm20WorkTime', '')) if item.get('ufCrm20WorkTime') else None
                work_time_start = None
                work_time_end = None
                if work_time_id and work_time_id in self._work_time_map:
                    work_time_start, work_time_end = self._work_time_map[work_time_id]
                elif work_time_id:
                    logger.debug(f"Неизвестный ufCrm20WorkTime: {work_time_id} для '{title}'")

                data = {
                    'employment_date': employment_date,
                    'work_time_start': work_time_start,
                    'work_time_end': work_time_end,
                }

                # Ключ — нормализованное полное ФИО
                normalized_full = self._normalize_name(title)
                result_map[normalized_full] = data

                # Дополнительный ключ — фамилия + имя (без отчества)
                name_parts = title.split()
                if len(name_parts) >= 2:
                    fi_key = self._normalize_name(f"{name_parts[0]} {name_parts[1]}")
                    if fi_key not in result_map:
                        result_map[fi_key] = data

            logger.info(f"Построена карта данных из сущности 1120 для {len(result_map)} записей")
            return result_map

        except asyncio.TimeoutError:
            logger.error("Таймаут при получении данных из сущности 1120")
            return result_map
        except Exception as e:
            logger.error(f"Ошибка получения данных из сущности 1120: {e}", exc_info=True)
            return result_map

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
                    success = self._update_user_data_in_db(employee['id'], update_data)
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
            
    def _update_user_data_in_db(self, user_id: int, update_data: Dict) -> bool:
        """Обновляет данные пользователя в базе"""
        try:
            with db.get_session() as session:
                user = session.query(User).filter(User.id == user_id).first()
                if user:
                    for key, value in update_data.items():
                        setattr(user, key, value)
                    user.updated_at = datetime.now()
                    session.commit()
                    return True
                return False
        except Exception as e:
            logger.error(f"Ошибка обновления пользователя {user_id}: {e}")
            return False

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
            with db.get_session() as session:
                if bitrix_id:
                    user = session.query(User).filter(User.bitrix_id == bitrix_id).first()
                    if user:
                        return True
                
                name_parts = full_name.split()
                simple_name = ' '.join(name_parts[:2]) if len(name_parts) >= 2 else full_name
                
                user = session.query(User).filter(
                    (User.full_name == full_name) | (User.full_name == simple_name)
                ).first()
                return user is not None
                
        except Exception as e:
            logger.error(f"Ошибка проверки пользователя: {e}")
            return False

    def get_bitrix_id(self, user_id: int) -> Optional[int]:
        """Получаем Bitrix ID пользователя"""
        try:
            with db.get_session() as session:
                user = session.query(User).filter(User.id == user_id).first()
                return user.bitrix_id if user else None
        except Exception as e:
            logger.error(f"Ошибка получения Bitrix ID: {e}")
            return None

    @staticmethod
    def _normalize_name(name: str) -> str:
        """Нормализует имя для сравнения (учитывает ФИО)"""
        if not name:
            return ""
        normalized = (
            name.strip().lower()
            .replace("ё", "е")
            .translate(str.maketrans("", "", ".,-"))
        )
        logger.debug(f"Нормализация имени: '{name}' -> '{normalized}'")
        return normalized
    
    async def _push_to_bitrix(self) -> bool:
        """Отправка заказов в Bitrix с правильным управлением сессиями"""
        try:
            today = datetime.now(TIME_CONFIG.TIMEZONE).date().isoformat()
            
            # 🔥 ШАГ 1: Получаем ID заказов (не объекты!)
            with db.get_session() as session:
                orders_ids = session.query(Order.id).filter(
                    Order.is_sent_to_bitrix == False,
                    Order.is_cancelled == False,
                    Order.target_date == today,
                    Order.bitrix_order_id == None,
                    Order.is_from_bitrix == False
                ).all()
                
                # Извлекаем только ID
                order_ids_list = [order_id[0] for order_id in orders_ids]
            
            if not order_ids_list:
                logger.info("📦 Нет заказов для отправки в Bitrix24")
                return True
                
            logger.info(f"📤 Найдено {len(order_ids_list)} заказов для отправки")
            
            success_count = 0
            error_count = 0
            failed_order_ids = []  # 🔥 Сохраняем ID неотправленных заказов
            
            # 🔥 ШАГ 2: Обрабатываем каждый заказ в отдельной сессии
            for order_id in order_ids_list:
                try:
                    # Открываем новую сессию для каждого заказа
                    with db.get_session() as order_session:
                        order = order_session.query(Order).filter(
                            Order.id == order_id
                        ).first()
                        
                        if not order:
                            logger.warning(f"Заказ {order_id} не найден")
                            continue
                        
                        # Получаем пользователя в той же сессии
                        user = order_session.query(User).filter(
                            User.id == order.user_id
                        ).first()
                        
                        if not user or not user.bitrix_id:
                            logger.warning(f"❌ Пользователь для заказа {order_id} не найден или нет Bitrix ID")
                            error_count += 1
                            failed_order_ids.append(order_id)
                            continue
                        
                        # Формируем данные для Bitrix
                        order_data = {
                            'bitrix_id': user.bitrix_id,
                            'quantity': order.quantity,
                            'target_date': str(order.target_date),
                            'order_time': order.order_time or '09:00:00',
                            'location': user.location or 'Офис'
                        }
                        
                        # Отправляем в Bitrix
                        bitrix_id = await self._create_bitrix_order(
                            order_data, 
                            user.crm_employee_id
                        )
                        
                        if bitrix_id:
                            # Обновляем заказ в той же сессии
                            order.is_sent_to_bitrix = True
                            order.bitrix_order_id = str(bitrix_id)
                            order.updated_at = datetime.now()
                            order_session.commit()
                            success_count += 1
                            logger.info(f"✅ УСПЕШНО: Заказ {order_id} -> Bitrix ID: {bitrix_id}")
                        else:
                            logger.error(f"❌ Не удалось создать заказ {order_id} в Bitrix")
                            error_count += 1
                            failed_order_ids.append(order_id)
                            
                except Exception as e:
                    logger.error(f"❌ Ошибка обработки заказа {order_id}: {e}", exc_info=True)
                    error_count += 1
                    failed_order_ids.append(order_id)
            
            logger.info(f"📤 Итог отправки: Успешно: {success_count}, Ошибок: {error_count}")
            
            # 🔥 ШАГ 3: Сохраняем информацию о неотправленных заказах
            if failed_order_ids:
                # Сохраняем в атрибут экземпляра для последующего использования
                self._last_failed_order_ids = failed_order_ids
            
            # # Синхронизация сотрудников
            # logger.info("🔄 Пробуем синхронизировать сотрудников...")
            # await self.sync_employees()
            
            return error_count == 0
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка в _push_to_bitrix: {str(e)}", exc_info=True)
            return False

    async def _create_bitrix_order(self, order_data: dict, user_crm_id: str = None) -> Optional[str]:
        """Создает заказ в Bitrix24 - ИСПРАВЛЕННАЯ ВЕРСИЯ"""
        try:
            # 🔥 УСИЛЕННАЯ ПРОВЕРКА ОБЯЗАТЕЛЬНЫХ ПОЛЕЙ
            required_fields = {
                'bitrix_id': ['bitrix_id'],
                'quantity': ['quantity'], 
                'target_date': ['target_date'],
                'order_time': ['order_time']
            }
            
            missing_fields = []
            for field_name, field_aliases in required_fields.items():
                found = False
                for alias in field_aliases:
                    if alias in order_data and order_data[alias]:
                        found = True
                        break
                if not found:
                    missing_fields.append(field_name)
                    
            if missing_fields:
                logger.error(f"❌ Отсутствуют обязательные поля: {missing_fields}")
                return None

            # 🔥 ИСПРАВЛЕНИЕ: используем переданный CRM ID
            user_id = order_data['bitrix_id']
            crm_employee_id = user_crm_id
            
            # Маппинг значений
            quantity_map = {1: '821', 2: '822', 3: '823', 4: '824', 5: '825'}
            location_map = {
                'Офис': '826',
                'ПЦ 1': '827', 
                'ПЦ 2': '828',
                'Склад': '1063'
            }

            # 🔥 ПРАВИЛЬНОЕ ФОРМАТИРОВАНИЕ ВРЕМЕНИ
            target_date = order_data['target_date']
            order_time = order_data['order_time']
            
            # Если время не содержит секунд, добавляем
            if ':' in order_time and order_time.count(':') == 1:
                order_time = order_time + ':00'
                
            created_time = f"{target_date}T{order_time}+03:00"

            params = {
                'entityTypeId': 1222,
                'fields': {
                    'ufCrm45ObedyCount': quantity_map.get(order_data['quantity'], '821'),
                    'ufCrm45ObedyFrom': location_map.get(order_data.get('location', 'Офис'), '826'),
                    'createdTime': created_time
                }
            }

            # 🔥 ПРИОРИТЕТ: используем CRM employee_id если есть
            if crm_employee_id:
                params['fields']['ufCrm45_1743599470'] = crm_employee_id
            else:
                params['fields']['ufCrm45_1751956286'] = user_id

            result = await self.bx.call('crm.item.add', params)
            
            if not result or 'id' not in result:
                logger.error(f"❌ Неверный ответ от Bitrix: {result}")
                return None
                
            logger.info(f"✅ Успешно создан заказ в Bitrix: {result['id']}")
            
            # 🔥 ДОБАВЬТЕ: небольшую задержку между запросами
            await asyncio.sleep(1.0)  # Увеличил задержку до 1 секунды

            return str(result['id'])
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания заказа в Bitrix: {str(e)}", exc_info=True)
            return None
        
    async def _get_user_name_by_bitrix_id(self, bitrix_id: str) -> Optional[str]:
        """Получает имя пользователя по его Bitrix ID"""
        try:
            with db.get_session() as session:
                user = session.query(User).filter(User.bitrix_id == bitrix_id).first()
                return user.full_name if user else "Unknown"
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
    # Закоментировал на время просроченого сертификата
    async def _get_rest_employees(self) -> List[Dict]:
        """Получает сотрудников через REST API с датой трудоустройства"""
        import requests
            
        try:
            # 1. Запрашиваем подразделения (существующий код)
            logger.info("Запрашиваю подразделения через REST API...")
            
            def get_all_departments():
                """Рекурсивно получает все подразделения"""
                all_deps = {}
                
                def fetch_deps(start=0):
                    params = {'start': start}
                    response = requests.get(self.rest_webhook + 'department.get', params=params)
                    data = response.json()
                    
                    if 'result' in data and data['result']:
                        for dept in data['result']:
                            dept_id = str(dept['ID'])
                            all_deps[dept_id] = dept['NAME']
                        
                        if len(data['result']) >= 50:
                            fetch_deps(start + 50)
                
                fetch_deps()
                return all_deps
            
            dept_dict = get_all_departments()
            logger.info(f"Получено {len(dept_dict)} подразделений")

            # 2. Запрашиваем сотрудников с полем UF_EMPLOYMENT_DATE
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
                
                if len(user_data['result']) < batch_size:
                    break

            logger.info(f"Получено {len(all_users)} сотрудников")

            result_list = []
            for user in all_users:
                dept_id_list = user.get('UF_DEPARTMENT', [])
                dept_id = str(dept_id_list[0]) if dept_id_list else None

                # Преобразование ID в название
                department_name = 'Не указано'
                if dept_id and dept_id in dept_dict:
                    department_name = dept_dict[dept_id]

                # Формируем ФИО
                last_name = user.get('LAST_NAME', '')
                first_name = user.get('NAME', '')
                second_name = user.get('SECOND_NAME', '')
                
                full_name_parts = [last_name, first_name]
                if second_name:
                    full_name_parts.append(second_name)
                full_name = ' '.join(filter(None, full_name_parts))

                # Город
                city_fields = ['PERSONAL_CITY', 'WORK_CITY', 'UF_CITY', 'UF_LOCATION']
                city = None
                for field in city_fields:
                    if user.get(field):
                        city = user.get(field)
                        break

                # 🔥 ИСПРАВЛЕНИЕ: Обработка даты трудоустройства
                employment_date = user.get('UF_EMPLOYMENT_DATE')
                if employment_date:
                    # Преобразуем дату из формата Bitrix
                    try:
                        # Bitrix обычно возвращает в формате "YYYY-MM-DD"
                        employment_date = datetime.strptime(employment_date, '%Y-%m-%d').date()
                    except (ValueError, TypeError) as e:
                        employment_date = None
                        logger.debug(f"Неверный формат даты трудоустройства '{user.get('UF_EMPLOYMENT_DATE')}': {e}")
                else:
                    # 🔥 ВАЖНО: Если даты нет или пустая строка - устанавливаем None
                    employment_date = None

                employee_info = {
                    'ID': str(user['ID']),
                    'ФИО': full_name,
                    'Фамилия': last_name,
                    'Имя': first_name,
                    'Отчество': second_name,
                    'Должность': user.get('WORK_POSITION', 'Не указана'),
                    'Подразделение': department_name,
                    'Активен': user.get('ACTIVE', False),
                    'Город': city,
                    'UF_EMPLOYMENT_DATE': employment_date  # 🔥 Теперь всегда корректное значение
                }
                
                result_list.append(employee_info)

            return result_list
                
        except Exception as e:
            logger.error(f"Ошибка получения сотрудников через REST API: {e}")
            return []
        
    def _user_exists_by_bitrix_id(self, bitrix_id: str) -> bool:
        """Проверяет существование пользователя по Bitrix ID"""
        try:
            with db.get_session() as session:
                user = session.query(User).filter(User.bitrix_id == bitrix_id).first()
                return user is not None
        except Exception as e:
            logger.error(f"Ошибка проверки пользователя по Bitrix ID: {e}")
            return False
        
    async def _get_local_user_id_by_crm_id(self, crm_employee_id: str) -> Optional[int]:
        """Находит локальный ID пользователя по CRM crm_employee_id"""
        try:
            with db.get_session() as session:
                user = session.query(User).filter(User.crm_employee_id == crm_employee_id).first()
                return user.id if user else None
        except Exception as e:
            logger.error(f"Ошибка поиска пользователя по CRM ID: {e}")
            return None
        
    def remove_duplicate_employees(self):
        """Удаляет дублирующихся сотрудников"""
        try:
            with db.get_session() as session:
                # Находим дубли по bitrix_id
                duplicates = session.execute(text('''
                    SELECT bitrix_id, COUNT(*) as count 
                    FROM users 
                    WHERE bitrix_id IS NOT NULL 
                    GROUP BY bitrix_id 
                    HAVING COUNT(*) > 1
                ''')).fetchall()
                
                for bitrix_id, count in duplicates:
                    # Оставляем первую запись, удаляем остальные
                    session.execute(text('''
                        DELETE FROM users 
                        WHERE id NOT IN (
                            SELECT MIN(id) 
                            FROM users 
                            WHERE bitrix_id = :bitrix_id 
                            GROUP BY bitrix_id
                        ) AND bitrix_id = :bitrix_id
                    '''), {'bitrix_id': bitrix_id})
                    logger.info(f"Удалено {count-1} дублей для bitrix_id {bitrix_id}")
                
                session.commit()
                
        except Exception as e:
            logger.error(f"Ошибка удаления дублей: {e}")
            
    async def _update_existing_employee(self, existing_employee: Dict, rest_emp: Dict, rest_to_crm_mapping: Dict, stats: Dict, entity_1120_map: Dict = None):
        """Обновляет данные существующего сотрудника с датой трудоустройства и рабочим временем из сущности 1120"""
        try:
            update_data = {}
            bitrix_id = rest_emp['ID']
            
            # Проверяем изменения в отделе
            new_department = rest_emp.get('Подразделение', '')
            current_department = existing_employee.get('department', '')
            
            # 🔥 ИСПРАВЛЕНИЕ: Правильное сравнение отделов
            if new_department and new_department != 'Не указано' and current_department != new_department:
                update_data['department'] = new_department
                if 'Саушкин' in rest_emp['ФИО']:
                    logger.info(f"🎯 Саушкин: обновляем отдел '{current_department}' -> '{new_department}'")
            
            # 🔥 ДОБАВЛЕНО: Проверяем изменения в ФИО
            new_full_name = rest_emp.get('ФИО', '')
            current_full_name = existing_employee.get('full_name', '')
            if current_full_name != new_full_name and new_full_name:
                update_data['full_name'] = new_full_name
                logger.info(f"👤 Обновляем ФИО для сотрудника: '{current_full_name}' -> '{new_full_name}'")
            
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
            
            # 🔥 ИСПРАВЛЕНИЕ: Правильная логика для CRM ID
            new_crm_id = rest_to_crm_mapping.get(bitrix_id)
            current_crm_id = existing_employee.get('crm_employee_id')
            
            # Преобразуем в строки для корректного сравнения
            current_crm_str = str(current_crm_id) if current_crm_id is not None else None
            new_crm_str = str(new_crm_id) if new_crm_id is not None else None
            
            # Обновляем CRM ID только если:
            # 1. Новый CRM ID существует И
            # 2. Текущий CRM ID пустой/None ИЛИ они действительно разные
            if new_crm_str:
                if not current_crm_str:
                    # Случай 1: У сотрудника нет CRM ID, устанавливаем новый
                    update_data['crm_employee_id'] = new_crm_str
                    logger.info(f"💾 Устанавливаем CRM ID для {rest_emp['ФИО']}: '{new_crm_str}'")
                elif current_crm_str != new_crm_str:
                    # Случай 2: CRM ID действительно изменился
                    update_data['crm_employee_id'] = new_crm_str
                    logger.info(f"💾 Изменяем CRM ID для {rest_emp['ФИО']}: '{current_crm_str}' → '{new_crm_str}'")
                else:
                    # Случай 3: CRM ID не изменился - пропускаем
                    logger.debug(f"✅ CRM ID для {rest_emp['ФИО']} актуален: '{current_crm_str}'")
            elif current_crm_str:
                # Случай 4: CRM ID пропал в Bitrix, но был у нас - оставляем как есть
                logger.debug(f"⚠️ CRM ID для {rest_emp['ФИО']} пропал в Bitrix, сохраняем текущий: '{current_crm_str}'")

            # 🔥 ИСПРАВЛЕНИЕ: правильная логика для городов
            new_city = rest_emp.get('Город', '')
            current_city = existing_employee.get('city', '')
            
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

            # Обновление даты трудоустройства и рабочего времени из сущности 1120 (матчинг по ФИО)
            if entity_1120_map:
                emp_name_normalized = self._normalize_name(rest_emp['ФИО'])
                emp_1120 = entity_1120_map.get(emp_name_normalized)
                # Если не нашли по полному ФИО, пробуем по фамилии + имени
                if not emp_1120:
                    name_parts = rest_emp['ФИО'].split()
                    if len(name_parts) >= 2:
                        fi_key = self._normalize_name(f"{name_parts[0]} {name_parts[1]}")
                        emp_1120 = entity_1120_map.get(fi_key)
                if emp_1120:
                    # Дата трудоустройства
                    new_employment_date = emp_1120.get('employment_date')
                    current_employment_date = existing_employee.get('employment_date')
                    if new_employment_date and new_employment_date != current_employment_date:
                        update_data['employment_date'] = new_employment_date
                        logger.info(f"📅 Обновлена дата трудоустройства для {rest_emp['ФИО']}: {current_employment_date} -> {new_employment_date}")

                    # Рабочее время - начало
                    new_wt_start = emp_1120.get('work_time_start')
                    current_wt_start = existing_employee.get('work_time_start')
                    if new_wt_start and new_wt_start != current_wt_start:
                        update_data['work_time_start'] = new_wt_start
                        logger.info(f"🕐 Обновлено начало рабочего дня для {rest_emp['ФИО']}: {current_wt_start} -> {new_wt_start}")

                    # Рабочее время - конец
                    new_wt_end = emp_1120.get('work_time_end')
                    current_wt_end = existing_employee.get('work_time_end')
                    if new_wt_end and new_wt_end != current_wt_end:
                        update_data['work_time_end'] = new_wt_end
                        logger.info(f"🕐 Обновлен конец рабочего дня для {rest_emp['ФИО']}: {current_wt_end} -> {new_wt_end}")

            # 🔥 ИСПОЛЬЗУЕМ МЕТОД ДЛЯ ПРОВЕРКИ РЕАЛЬНЫХ ИЗМЕНЕНИЙ
            if update_data and self._has_real_changes(existing_employee, update_data):
                success = self._update_user_data_in_db(existing_employee['id'], update_data)
                if success:
                    stats['updated'] += 1
                    changes_list = list(update_data.keys())
                    logger.info(f"Обновлен сотрудник: {rest_emp['ФИО']} - изменения: {changes_list}")
                else:
                    stats['errors'] += 1
                        
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
            
            with db.get_session() as session:
                # Помечаем как удаленных тех, кого нет в активных
                session.execute(
                    text('''
                        UPDATE users 
                        SET is_deleted = TRUE, updated_at = CURRENT_TIMESTAMP
                        WHERE is_employee = TRUE 
                        AND bitrix_id IS NOT NULL 
                        AND bitrix_id NOT IN :active_ids
                    '''),
                    {'active_ids': tuple(active_bitrix_ids)}
                )
                session.commit()
            
            logger.info(f"Обновлен статус неактивных сотрудников")
            
        except Exception as e:
            logger.error(f"Ошибка очистки неактивных сотрудников: {e}")
            
    async def _add_new_employee(self, rest_emp: Dict, rest_to_crm_mapping: Dict, stats: Dict, entity_1120_map: Dict = None):
        """Добавляет нового сотрудника из Bitrix с датой трудоустройства и рабочим временем из сущности 1120"""
        try:
            bitrix_id = rest_emp['ID']

            if self._user_exists_by_bitrix_id(bitrix_id):
                logger.debug(f"Сотрудник с Bitrix ID {bitrix_id} уже существует, пропускаем")
                stats['exists'] += 1
                return

            crm_id = rest_to_crm_mapping.get(bitrix_id)
            department = rest_emp.get('Подразделение', '')
            city = rest_emp.get('Город', '')

            # Получаем данные из сущности 1120 (матчинг по ФИО)
            employment_date = None
            work_time_start = None
            work_time_end = None
            if entity_1120_map:
                emp_name_normalized = self._normalize_name(rest_emp['ФИО'])
                emp_1120 = entity_1120_map.get(emp_name_normalized)
                if not emp_1120:
                    name_parts = rest_emp['ФИО'].split()
                    if len(name_parts) >= 2:
                        fi_key = self._normalize_name(f"{name_parts[0]} {name_parts[1]}")
                        emp_1120 = entity_1120_map.get(fi_key)
                if emp_1120:
                    employment_date = emp_1120.get('employment_date')
                    work_time_start = emp_1120.get('work_time_start')
                    work_time_end = emp_1120.get('work_time_end')

            with db.get_session() as session:
                try:
                    new_user = User(
                        full_name=rest_emp['ФИО'],
                        is_employee=True,
                        is_verified=False,
                        bitrix_id=bitrix_id,
                        crm_employee_id=crm_id,
                        position=rest_emp.get('Должность', ''),
                        department=department,
                        city=city,
                        is_deleted=not rest_emp.get('Активен', True),
                        bitrix_entity_type='rest_employee',
                        employment_date=employment_date,
                        work_time_start=work_time_start,
                        work_time_end=work_time_end,
                    )
                    session.add(new_user)
                    session.commit()

                    stats['added'] += 1
                    logger.info(f"✅ Добавлен новый сотрудник: {rest_emp['ФИО']}, отдел: {department}, дата трудоустройства: {employment_date}, график: {work_time_start}-{work_time_end}")
                    
                except Exception as e:
                    logger.error(f"❌ Ошибка при добавлении сотрудника {rest_emp['ФИО']}: {e}")
                    stats['errors'] += 1
                    session.rollback()
            
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
                    
                bitrix_id = parsed_order['bitrix_order_id']
                is_from_bitrix = parsed_order['is_from_bitrix']
                
                # Обновляем заказ в базе
                with db.get_session() as session:
                    db_order = session.query(Order).filter(Order.bitrix_order_id == bitrix_id).first()
                    if db_order:
                        db_order.is_from_bitrix = is_from_bitrix
                        updated_count += 1
                    
                session.commit()
                    
            logger.info(f"Обновлено источников для {updated_count} заказов")
            
        except Exception as e:
            logger.error(f"Ошибка обновления источников заказов: {e}")

    async def _find_employee_by_crm_id(self, crm_id: str) -> Optional[Dict]:
        """Находит сотрудника по CRM ID в базе данных"""
        try:
            with db.get_session() as session:
                user = session.query(User).filter(User.crm_employee_id == crm_id).first()
                if user:
                    return {
                        'id': user.id,
                        'full_name': user.full_name,
                        'bitrix_id': user.bitrix_id
                    }
                return None
        except Exception as e:
            logger.error(f"Ошибка поиска сотрудника по CRM ID {crm_id}: {e}")
            return None

    def _need_order_update(self, order: Dict) -> bool:
        """Проверяет нужно ли обновлять заказ - ИСПРАВЛЕННАЯ ВЕРСИЯ"""
        bitrix_id = order.get('bitrix_id')
        if not bitrix_id:
            return True
            
        with db.get_session() as session:
            existing = session.query(Order).filter(Order.bitrix_order_id == bitrix_id).first()
            
            if not existing:
                return True
                
            # 🔥 ИСПРАВЛЕНИЕ: Правильное сравнение данных
            current_cancelled = order.get('is_cancelled', False)
            current_quantity = order.get('quantity', 1)
            
            # Сравниваем КРИТИЧЕСКИЕ поля
            if (existing.is_cancelled != current_cancelled or 
                existing.quantity != current_quantity):
                logger.info(f"📝 Заказ {bitrix_id} изменился: cancelled {existing.is_cancelled}->{current_cancelled}, quantity {existing.quantity}->{current_quantity}")
                return True
                
            # 🔥 ДОБАВЛЕНО: Проверка временных меток для отладки
            if not existing.last_synced_at:
                logger.debug(f"🆕 Заказ {bitrix_id} никогда не синхронизировался")
                return True
                
            # 🔥 ИСПРАВЛЕНИЕ: Не обновляем если данные не изменились
            logger.debug(f"✅ Заказ {bitrix_id} не изменился - пропускаем")
            return False
    
    async def sync_recent_orders(self, hours: int = 24):
        """Синхронизирует только заказы за последние N часов"""
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(hours=hours)).strftime('%Y-%m-%d')
        
        logger.info(f"🔄 Инкрементальная синхронизация за {hours} часов...")
        return await self.sync_orders(start_date, end_date, incremental=True)
    
    def _find_local_order_by_user_and_date(self, user_id: int, target_date: str) -> Optional[Dict]:
        """Ищет заказ в локальной базе по user_id и дате"""
        try:
            with db.get_session() as session:
                order = session.query(Order).filter(
                    Order.user_id == user_id,
                    Order.target_date == target_date
                ).first()
                if order:
                    return {'id': order.id, 'bitrix_order_id': order.bitrix_order_id}
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
            now = datetime.now(TIME_CONFIG.TIMEZONE)  # ← ИСПРАВИТЬ
            today = now.date()
            
            with db.get_session() as session:
                # Получаем информацию о заказе
                order = session.query(Order).filter(Order.id == order_id).first()
                if not order:
                    logger.warning(f"Заказ {order_id} не найден")
                    return False
                    
                # 🔥 ДОБАВИТЬ ПРОВЕРКУ ВРЕМЕНИ ДЛЯ УДАЛЕНИЯ
                if order.target_date == today and now.time() >= TIME_CONFIG.MODIFICATION_DEADLINE:
                    logger.warning(f"⏰ Время для удаления заказов на сегодня истекло ({TIME_CONFIG.MODIFICATION_DEADLINE.strftime('%H:%M')})")
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
                    logger.warning(f"Заказ {order_id} не отменен, удаление невозможно")
                    return False
                    
                if order.is_from_bitrix:
                    logger.warning(f"Заказ {order_id} создан в Bitrix, удаление невозможно")
                    return False
                    
                if order.bitrix_order_id:
                    logger.warning(f"Заказ {order_id} уже синхронизирован с Bitrix (ID: {order.bitrix_order_id}), удаление невозможно")
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
        
    async def cleanup_all_cancelled_orders(self):
        """Ежедневная очистка всех отмененных заказов"""
        try:
            with db.get_session() as session:
                deleted_count = session.query(Order).filter(
                    Order.is_cancelled == True,
                    Order.is_from_bitrix == False,
                    Order.bitrix_order_id == None
                ).delete()
                session.commit()
                
            logger.info(f"✅ Ежедневная очистка: удалено {deleted_count} отмененных заказов")
            return deleted_count
            
        except Exception as e:
            logger.error(f"❌ Ошибка ежедневной очистки отмененных заказов: {e}")
            return 0
        
    async def close_orders_at_930(self):
        """Финальное закрытие - только логирование, отправка уже произошла в 9:29:50"""
        closure_time = datetime.now(TIME_CONFIG.TIMEZONE).strftime('%H:%M:%S.%f')[:-3]
        logger.info(f"⏹ [{closure_time}] Прием заказов на сегодня закрыт (система остается включенной)")

    async def _disable_ordering(self):
        """Пустой метод - больше не отключаем заказы полностью"""
        logger.info("ℹ️ Заказы НЕ отключаются полностью (только на сегодня по времени)")
        # Ничего не делаем - заказы остаются включенными в БД

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
            with db.get_session() as session:
                user = session.query(User).filter(User.id == user_id).first()
                if not user:
                    return False
                    
                current_city = user.city
                
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
        """
        try:
            for field, new_value in update_data.items():
                current_value = existing_employee.get(field)
                
                # 🔥 ДОБАВЬ ОТЛАДОЧНЫЙ ВЫВОД
                logger.debug(f"🔍 Проверка поля {field}: текущее='{current_value}', новое='{new_value}'")
                
                # Если оба значения None/пустые - пропускаем
                if not current_value and not new_value:
                    continue
                    
                # Если текущее значение пустое, а новое есть - это изменение
                if not current_value and new_value:
                    logger.debug(f"✅ Изменение в {field}: None -> '{new_value}'")
                    return True
                    
                # Если текущее значение есть, а новое пустое - это изменение
                if current_value and not new_value:
                    logger.debug(f"✅ Изменение в {field}: '{current_value}' -> None")
                    return True

                # Сравниваем строки
                if str(current_value) != str(new_value):
                    logger.debug(f"✅ Изменение в {field}: '{current_value}' -> '{new_value}'")
                    return True
                    
            logger.debug("❌ Нет реальных изменений")
            return False
            
        except Exception as e:
            logger.error(f"Ошибка проверки изменений: {e}")
            return True

    def _user_exists_by_crm_id(self, crm_id: str) -> bool:
        """Проверяет существование пользователя по CRM ID"""
        try:
            with db.get_session() as session:
                user = session.query(User).filter(User.crm_employee_id == crm_id).first()
                return user is not None
        except Exception as e:
            logger.error(f"Ошибка проверки пользователя по CRM ID: {e}")
            return False