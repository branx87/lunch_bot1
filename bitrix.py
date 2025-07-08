# ##bitrix.py
import os
from fast_bitrix24 import Bitrix
from dotenv import load_dotenv
from db import db
import logging

logger = logging.getLogger(__name__)

class BitrixSync:
    def __init__(self):
        load_dotenv('data/configs/.env')
        self.webhook = os.getenv('BITRIX_WEBHOOK')
        self.bx = Bitrix(self.webhook)

    async def sync_employees(self):
        """Синхронизирует сотрудников из Bitrix в локальную БД"""
        try:
            # Получаем сотрудников из Bitrix
            bitrix_users = self.bx.call('user.get', {'filter': {'ACTIVE': True}})
            
            # Сопоставляем с локальными пользователями по ФИО
            local_users = db.get_employees(active_only=False)
            local_map = {user['full_name'].lower(): user for user in local_users}

            for bx_user in bitrix_users:
                full_name = f"{bx_user['NAME']} {bx_user['LAST_NAME']}".strip()
                local_user = local_map.get(full_name.lower())
                
                if local_user:
                    db.add_bitrix_mapping(
                        local_id=local_user['id'],
                        local_type='user',
                        bitrix_id=bx_user['ID'],
                        bitrix_entity_type='employee'
                    )
                    logger.info(f"Сопоставлен: {full_name} (Bitrix ID: {bx_user['ID']})")
                    
        except Exception as e:
            logger.error(f"Ошибка синхронизации: {e}")