# ##bitrix.py
# bitrix.py
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

    async def sync_employees(self) -> Dict[str, int]:
        """Синхронизация сотрудников с автоматическим добавлением новых"""
        stats = {
            'total': 0, 
            'updated': 0, 
            'added': 0,
            'errors': 0, 
            'no_match': 0
        }
        
        try:
            # 1. Получаем данные из Bitrix
            crm_employees = await self._get_crm_employees()
            if not crm_employees:
                logger.error("Не удалось получить сотрудников из Bitrix")
                return stats

            # 2. Получаем локальных сотрудников и создаем множество имен
            local_employees = db.get_employees(active_only=False)
            local_names = {self._normalize_name(e['full_name']) for e in local_employees}
            stats['total'] = len(local_employees)
            
            # 3. Создаем структуры для поиска
            bitrix_employees = {
                self._normalize_name(emp['VALUE']): {
                    'id': emp['ID'],
                    'name': emp['VALUE']
                }
                for emp in crm_employees
            }

            # 4. Проходим по локальным сотрудникам для обновления Bitrix ID
            for employee in local_employees:
                try:
                    local_name = self._normalize_name(employee['full_name'])
                    bitrix_emp = self._find_bitrix_employee(local_name, bitrix_employees)

                    if bitrix_emp:
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

            # 5. Добавляем новых сотрудников из Bitrix, которых нет в локальной базе
            for bitrix_emp in bitrix_employees.values():
                bitrix_name = bitrix_emp['name']
                normalized_name = self._normalize_name(bitrix_name)
                
                # Проверяем, есть ли такой сотрудник в локальной базе
                if normalized_name not in local_names:
                    try:
                        # Добавляем нового сотрудника (is_employee=True, is_verified=False)
                        db.execute(
                            """INSERT INTO users 
                            (full_name, is_employee, is_verified, bitrix_id, bitrix_entity_type)
                            VALUES (?, ?, ?, ?, ?)""",
                            (bitrix_name, True, False, bitrix_emp['id'], 'crm_employee')
                        )
                        stats['added'] += 1
                        logger.info(f"Добавлен новый сотрудник: {bitrix_name} (Bitrix ID: {bitrix_emp['id']})")
                    except Exception as e:
                        stats['errors'] += 1
                        logger.error(f"Ошибка добавления сотрудника {bitrix_name}: {e}")

            logger.info(
                f"Синхронизация завершена. Всего: {stats['total']}, "
                f"Обновлено: {stats['updated']}, "
                f"Добавлено: {stats['added']}, "
                f"Без соответствия: {stats['no_match']}, "
                f"Ошибок: {stats['errors']}"
            )
            return stats

        except Exception as e:
            logger.error(f"Ошибка синхронизации: {e}", exc_info=True)
            return stats

    def _find_bitrix_employee(self, local_name: str, bitrix_employees: Dict[str, int]) -> Optional[int]:
        """Ищем соответствие сотрудника с учетом отчества"""
        # Прямое совпадение (если вдруг есть)
        if local_name in bitrix_employees:
            return bitrix_employees[local_name]
        
        # Ищем по фамилии и имени (игнорируя отчество)
        local_parts = local_name.split()
        if len(local_parts) >= 2:
            search_key = f"{local_parts[0]} {local_parts[1]}"
            for bitrix_name, bitrix_id in bitrix_employees.items():
                if bitrix_name.startswith(search_key):
                    return bitrix_id
        return None

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