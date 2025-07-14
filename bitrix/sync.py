# ##bitrix/sync.py
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
        
        # Отправка в Bitrix в 9:31
        self.scheduler.add_job(
            self._push_to_bitrix,
            'cron',
            minute='31',
            hour='9',
            day_of_week='mon-fri'
        )

    # Все остальные методы из bitrix.py (sync_employees, sync_orders и т.д.)
    # должны быть перенесены сюда без изменений
    
    async def _push_to_bitrix(self):
        """Отправка локальных заказов в Bitrix"""
        try:
            logger.info("Отправка заказов в Bitrix...")
            today = datetime.now().date()
            
            # Получаем заказы, которые еще не были отправлены в Bitrix
            pending_orders = db.execute('''
                SELECT * FROM orders 
                WHERE target_date = ? 
                AND is_sent_to_bitrix = FALSE
                AND is_cancelled = FALSE
            ''', (today.isoformat(),))
            
            for order in pending_orders:
                # Ваш код для создания заказа в Bitrix
                bitrix_id = await self._create_bitrix_order(order)
                if bitrix_id:
                    db.execute('''
                        UPDATE orders 
                        SET is_sent_to_bitrix = TRUE,
                            bitrix_order_id = ? 
                        WHERE id = ?
                    ''', (bitrix_id, order['id']))
            
            logger.info(f"Отправлено {len(pending_orders)} заказов в Bitrix")
            
        except Exception as e:
            logger.error(f"Ошибка отправки в Bitrix: {e}")

    async def _create_bitrix_order(self, order_data: dict) -> Optional[str]:
        """Создание заказа в Bitrix"""
        try:
            # Ваша реализация создания заказа в Bitrix
            # Возвращает ID созданного заказа или None при ошибке
            return "12345"  # Пример, замените на реальный код
        except Exception as e:
            logger.error(f"Ошибка создания заказа в Bitrix: {e}")
            return None