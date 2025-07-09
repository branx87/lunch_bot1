# ##bitrix_export_monthly.py
import os
import asyncio
from fast_bitrix24 import Bitrix
from dotenv import load_dotenv
import pandas as pd
import logging
from datetime import datetime, timedelta
from bitrix import BitrixSync
from db import db  # Импортируем экземпляр базы данных

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Загрузка настроек
load_dotenv('data/configs/.env')
WEBHOOK = os.getenv('BITRIX_WEBHOOK')

def map_quantity(quantity_id):
    """Преобразует ID количества обедов в число"""
    quantity_map = {
        '821': '1',
        '822': '2',
        '823': '3',
        '824': '4',
        '825': '5'
    }
    return quantity_map.get(str(quantity_id), '0')

def map_location(location_id):
    """Преобразует ID локации в название"""
    location_map = {
        '826': 'Офис',
        '827': 'ПЦ 1',
        '828': 'ПЦ 2',
        '1063': 'Склад'
    }
    return location_map.get(str(location_id), 'Неизвестно')

async def export_monthly_orders(year=None, month=None):
    """Экспорт и синхронизация заказов из Bitrix24"""
    try:
        # Инициализация
        sync = BitrixSync()
        
        # Определяем даты
        now = datetime.now()
        year = year if year is not None else now.year
        month = month if month is not None else now.month
        
        start_date = datetime(year=year, month=month, day=1).strftime('%Y-%m-%d')
        if month == 12:
            end_date = datetime(year=year+1, month=1, day=1).strftime('%Y-%m-%d')
        else:
            end_date = datetime(year=year, month=month+1, day=1).strftime('%Y-%m-%d')
        
        # Синхронизируем заказы
        sync_stats = await sync.sync_orders(start_date, end_date)
        logger.info(f"Синхронизация завершена: {sync_stats}")
        
        # Получаем данные из локальной БД (уже синхронизированные)
        db.cursor.execute("""
            SELECT 
                o.id,
                u.full_name,
                o.quantity,
                o.target_date,
                CASE WHEN o.is_from_bitrix THEN 'Bitrix' ELSE 'Бот' END as source
            FROM orders o
            JOIN users u ON o.user_id = u.id
            WHERE o.target_date BETWEEN ? AND ?
              AND o.is_cancelled = FALSE
            ORDER BY o.target_date, u.full_name
        """, (start_date, end_date))
        
        orders = db.cursor.fetchall()
        
        # Создаем DataFrame и сохраняем в Excel
        if orders:
            df = pd.DataFrame(orders, columns=['ID', 'Сотрудник', 'Количество', 'Дата', 'Источник'])
            filename = f"orders_export_{year}-{month:02d}.xlsx"
            df.to_excel(filename, index=False)
            logger.info(f"Данные сохранены в {filename}")
            return df
        else:
            logger.warning("Нет данных для экспорта")
            return None
            
    except Exception as e:
        logger.error(f"Ошибка при экспорте: {e}", exc_info=True)
        raise

# async def export_monthly_orders(year=None, month=None):
#     """
#     Экспорт заказов обедов из Bitrix24 за указанный месяц
#     :param year: Год (если None, будет использован текущий)
#     :param month: Месяц (1-12, если None, будет использован текущий)
#     """
#     try:
#         # Инициализация подключения
#         bx = Bitrix(WEBHOOK)
#         logger.info("Подключение к Bitrix24 установлено")

#         # Определяем год и месяц
#         now = datetime.now()
#         year = year if year is not None else now.year
#         month = month if month is not None else now.month

#         # Определяем даты начала и конца месяца
#         start_date = datetime(year=year, month=month, day=1)
#         if month == 12:
#             end_date = datetime(year=year+1, month=1, day=1)
#         else:
#             end_date = datetime(year=year, month=month+1, day=1)
        
#         # Форматируем даты для Bitrix24
#         start_date_str = start_date.strftime('%Y-%m-%d')
#         end_date_str = end_date.strftime('%Y-%m-%d')

#         # Параметры запроса с фильтром по месяцу
#         params = {
#             'entityTypeId': 1222,
#             'select': [
#                 'id', 
#                 'ufCrm45_1743599470',  # ID сотрудника
#                 'ufCrm45ObedyCount',   # Количество обедов (ID)
#                 'ufCrm45ObedyFrom',    # Локация (ID)
#                 'createdTime'          # Дата создания
#             ],
#             'filter': {
#                 '>=createdTime': f'{start_date_str}T00:00:00+03:00',
#                 '<createdTime': f'{end_date_str}T00:00:00+03:00'
#             }
#         }

#         logger.info(f"Запрашиваю данные о заказах за {year}-{month:02d}...")
        
#         # Получаем данные
#         orders = await bx.get_all('crm.item.list', params)
        
#         if not orders:
#             logger.warning(f"Не найдено заказов за {year}-{month:02d}")
#             return

#         logger.info(f"Всего получено {len(orders)} заказов за месяц")

#         # Получаем список сотрудников из БД
#         cursor = db.conn.cursor()
#         cursor.execute("SELECT id, full_name, bitrix_id FROM users WHERE is_employee = TRUE AND is_deleted = FALSE")
#         employees = cursor.fetchall()
#         employee_map = {str(e[2]): e[1] for e in employees if e[2]}  # Создаем словарь {bitrix_id: full_name}

#         # Обработка данных
#         processed_data = []
#         for order in orders:
#             # Получаем имя сотрудника
#             employee_bitrix_id = str(order.get('ufCrm45_1743599470', ''))
#             employee_name = employee_map.get(employee_bitrix_id, f"Неизвестный сотрудник (ID: {employee_bitrix_id})")

#             # Преобразуем количество и локацию
#             quantity = map_quantity(order.get('ufCrm45ObedyCount'))
#             location = map_location(order.get('ufCrm45ObedyFrom'))

#             # Обработка даты
#             created_time = order.get('createdTime', '')
#             created_date = created_time.split('T')[0] if created_time else ''

#             processed_data.append({
#                 'ID': order.get('id'),
#                 'Сотрудник': employee_name,
#                 'Количество_обедов': quantity,
#                 'Локация': location,
#                 'Дата_создания': created_date
#             })

#         # Создаем DataFrame
#         df = pd.DataFrame(processed_data)
        
#         # Сохраняем в Excel
#         filename = f"bitrix_orders_export_{year}-{month:02d}.xlsx"
#         df.to_excel(filename, index=False)
#         logger.info(f"Данные сохранены в файл: {filename}")

#         # Выводим пример данных
#         print("\nПервые 5 заказов:")
#         print(df.head().to_string(index=False))

#         return df

#     except Exception as e:
#         logger.error(f"Ошибка при экспорте: {str(e)}", exc_info=True)
#         raise

if __name__ == '__main__':
    # Примеры использования:
    # 1. Текущий месяц
    # asyncio.run(export_monthly_orders())
    
    # 2. Конкретный месяц и год
    asyncio.run(export_monthly_orders(year=2025, month=7))
    
    # 3. Предыдущий месяц
    # today = datetime.now()
    # first_day_of_current_month = today.replace(day=1)
    # last_month = first_day_of_current_month - timedelta(days=1)
    # asyncio.run(export_monthly_orders(year=last_month.year, month=last_month.month))