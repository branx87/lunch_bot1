# ##test_bitrix.py
import asyncio
from fast_bitrix24 import Bitrix
from dotenv import load_dotenv
import os

load_dotenv('data/configs/.env')
webhook = os.getenv('BITRIX_WEBHOOK')

async def test():
    bx = Bitrix(webhook)
    try:
        # Тест простого запроса
        print(await bx.call('app.info', {}))
        # Тест получения пользователей
        users = await bx.get_all('user.get', {'filter': {'ACTIVE': True}, 'select': ['ID']})
        print(f"Получено пользователей: {len(users)}")
    except Exception as e:
        print(f"Ошибка: {e}")

asyncio.run(test())