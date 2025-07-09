# ##bitrix.py
# bitrix.py
import os
from fast_bitrix24 import Bitrix
from dotenv import load_dotenv
import logging
from typing import List, Dict, Optional
import asyncio
from db import db  # Импортируем ваш модуль работы с БД

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

    async def sync_employees(self) -> Dict[str, int]:
        """Основной метод синхронизации сотрудников"""
        stats = {'total': 0, 'updated': 0, 'errors': 0}
        
        try:
            # 1. Получаем сотрудников из поля CRM Bitrix
            crm_employees = await self._get_crm_employees()
            if not crm_employees:
                logger.error("Не удалось получить сотрудников из CRM")
                return stats
                
            # 2. Получаем локальных пользователей
            local_users = db.get_employees(active_only=False)
            stats['total'] = len(local_users)
            
            # 3. Сопоставляем записи
            for user in local_users:
                try:
                    full_name = user['full_name'].strip().lower()
                    
                    # Ищем соответствие в Bitrix
                    bitrix_emp = next(
                        (emp for emp in crm_employees 
                         if emp['VALUE'].strip().lower() == full_name),
                        None
                    )
                    
                    if bitrix_emp:
                        db.add_bitrix_mapping(
                            local_id=user['id'],
                            local_type='user',
                            bitrix_id=bitrix_emp['ID'],
                            bitrix_entity_type='crm_employee'
                        )
                        stats['updated'] += 1
                        
                except Exception as e:
                    stats['errors'] += 1
                    logger.error(f"Ошибка сопоставления {user}: {e}")
            
            logger.info(f"Синхронизация завершена. Обновлено: {stats['updated']}/{stats['total']}")
            return stats
            
        except Exception as e:
            logger.error(f"Критическая ошибка синхронизации: {e}")
            return stats

    async def _get_crm_employees(self) -> List[Dict[str, str]]:
        """Получаем список сотрудников из поля CRM"""
        try:
            # Получаем поля сущности (entityTypeId=1222 из вашего первого кода)
            fields = await self.bx.get_all(
                'crm.item.fields',
                {'entityTypeId': 1222}
            )
            
            # Находим поле "Сотрудник"
            emp_field = next(
                (f for f in fields.values() 
                 if f.get('title') == 'Сотрудник' and f.get('type') == 'enumeration'),
                None
            )
            
            return emp_field.get('items', []) if emp_field else []
            
        except Exception as e:
            logger.error(f"Ошибка получения сотрудников из CRM: {e}")
            return []
        
    # Добавляем в класс BitrixSync
    def get_bitrix_id(self, local_id: int) -> Optional[int]:
        """Получаем Bitrix ID по локальному ID пользователя"""
        try:
            result = db.execute(
                "SELECT bitrix_id FROM bitrix_mapping WHERE local_id = ? AND local_type = 'user'",
                (local_id,)
            )
            return result[0][0] if result else None
        except Exception as e:
            logger.error(f"Ошибка получения Bitrix ID: {e}")
            return None

async def test_sync():
    """Тестовая функция"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    sync = BitrixSync()
    result = await sync.sync_employees()
    print(f"Результат: {result}")

if __name__ == '__main__':
    asyncio.run(test_sync())