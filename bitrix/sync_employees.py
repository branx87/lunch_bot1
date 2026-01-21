# ##sync_employees.py
#!/usr/bin/env python3
import asyncio
import logging

# Относительный импорт
from .sync import BitrixSync

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    """Основная функция синхронизации сотрудников"""
    sync = BitrixSync()
    stats = await sync.sync_employees_with_departments()
    logger.info(f"Синхронизация завершена: {stats}")

if __name__ == '__main__':
    asyncio.run(main())