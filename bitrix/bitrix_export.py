# ##bitrix_export_monthly.py
import os
import asyncio
from fast_bitrix24 import Bitrix
from dotenv import load_dotenv
import pandas as pd
import logging
from datetime import datetime, timedelta
from bitrix.sync import BitrixSync
from db import db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv('data/configs/.env')
WEBHOOK = os.getenv('BITRIX_WEBHOOK')

def map_quantity(quantity_id):
    """Преобразует ID количества обедов в число"""
    quantity_map = {
        '821': 1,
        '822': 2,
        '823': 3,
        '824': 4,
        '825': 5
    }
    return quantity_map.get(str(quantity_id), 1)

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
    """Экспорт заказов обедов из Bitrix24 за указанный месяц"""
    try:
        bx = Bitrix(WEBHOOK)
        logger.info("Подключение к Bitrix24 установлено")

        now = datetime.now()
        year = year if year is not None else now.year
        month = month if month is not None else now.month

        start_date = datetime(year=year, month=month, day=1)
        if month == 12:
            end_date = datetime(year=year+1, month=1, day=1) - timedelta(days=1)
        else:
            end_date = datetime(year=year, month=month+1, day=1) - timedelta(days=1)
        
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')

        logger.info(f"Запрашиваю данные о заказах с {start_date_str} по {end_date_str}...")
        
        params = {
            'entityTypeId': 1222,
            'select': [
                'id', 
                'ufCrm45_1751956286',
                'ufCrm45ObedyCount',
                'ufCrm45ObedyFrom',
                'ufCrm45_1744188327370',
                'createdTime'
            ],
            'filter': {
                '>=createdTime': f'{start_date_str}T00:00:00+03:00',
                '<=createdTime': f'{end_date_str}T23:59:59+03:00'
            }
        }

        orders = await bx.get_all('crm.item.list', params)
        
        if not orders:
            logger.warning(f"Не найдено заказов за {year}-{month:02d}")
            return None

        logger.info(f"Всего получено {len(orders)} заказов за месяц")
        orders.sort(key=lambda x: int(x['id']))
        
        bitrix_sync = BitrixSync()
        await bitrix_sync.sync_employees()

        processed_data = []
        stats = {'total': 0, 'added': 0, 'updated': 0, 'errors': 0}

        for order in orders:
            parsed_order = bitrix_sync._parse_bitrix_order(order)
            if not parsed_order:
                stats['errors'] += 1
                continue
                
            await bitrix_sync._process_single_order(parsed_order, stats)
            
            employee_bitrix_id = str(order.get('ufCrm45_1751956286', ''))
            employee_name = await get_employee_name(employee_bitrix_id)
            
            quantity = map_quantity(order.get('ufCrm45ObedyCount'))
            location = map_location(order.get('ufCrm45ObedyFrom'))
            created_time = order.get('createdTime', '')
            created_date = created_time.split('T')[0] if created_time else ''

            # Обновить processed_data в export_monthly_orders
            processed_data.append({
                'ID_заказа_Bitrix': order.get('id'),
                'ID_сотрудника': employee_bitrix_id,
                'Сотрудник': employee_name,
                'Количество_обедов': quantity,
                'Локация': location,
                'Дата_заказа': created_date,
                'Время_заказа': created_time.split('T')[1][:8] if 'T' in created_time else ''
            })

        df = pd.DataFrame(processed_data)
        df = df.sort_values(by=['ID_заказа_Bitrix'])

        logger.info(f"Статистика обработки заказов: Добавлено: {stats['added']}, Обновлено: {stats['updated']}, Ошибок: {stats['errors']}")

        filename = f"bitrix_orders_export_{year}-{month:02d}.xlsx"
        df.to_excel(filename, index=False)
        logger.info(f"Данные сохранены в файл: {filename}")

        print("\nПервые 5 заказов:")
        print(df.head().to_string(index=False))

        return df

    except Exception as e:
        logger.error(f"Ошибка при экспорте: {str(e)}", exc_info=True)
        raise

# Временное решение для старых заказов
# В bitrix_export.py обновите функцию get_employee_name
async def get_employee_name(search_value: str, search_by: str = 'crm_employee_id') -> str:
    """Получаем имя сотрудника с приоритетом для CRM ID"""
    try:
        # Сначала ищем по CRM ID
        if search_by == 'crm_employee_id':
            result = db.execute(
                "SELECT full_name, position, department FROM users WHERE crm_employee_id = ? LIMIT 1",
                (search_value,)
            )
            if result:
                full_name = result[0][0]
                position = result[0][1] if len(result[0]) > 1 else None
                department = result[0][2] if len(result[0]) > 2 else None
                
                info_parts = []
                if position:
                    info_parts.append(position)
                if department:
                    info_parts.append(department)
                
                info_str = f" ({', '.join(info_parts)})" if info_parts else ""
                return f"{full_name}{info_str}"
        
        # Если не нашли по CRM ID, ищем по Bitrix ID
        result = db.execute(
            "SELECT full_name, position, department FROM users WHERE bitrix_id = ? LIMIT 1",
            (search_value,)
        )
        
        if result:
            full_name = result[0][0]
            position = result[0][1] if len(result[0]) > 1 else None
            department = result[0][2] if len(result[0]) > 2 else None
            
            info_parts = []
            if position:
                info_parts.append(position)
            if department:
                info_parts.append(department)
            
            info_str = f" ({', '.join(info_parts)})" if info_parts else ""
            return f"{full_name}{info_str} (по Bitrix ID)"
        else:
            return f"Неизвестный сотрудник (ID: {search_value})"
    except Exception as e:
        logger.error(f"Ошибка получения имени сотрудника: {e}")
        return f"Неизвестный сотрудник (ID: {search_value})"

if __name__ == '__main__':
    asyncio.run(export_monthly_orders(year=2025, month=7))