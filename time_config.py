# ##time_config.py
from datetime import time, timedelta
import pytz

class TimeConfig:
    """Централизованная конфигурация временных настроек бота"""
    
    # Часовой пояс
    TIMEZONE = pytz.timezone('Europe/Moscow')
    
    # Время приема заказов
    ORDER_DEADLINE = time(9, 30)  # До скольки принимаются заказы
    
    # Время для изменений/отмен
    MODIFICATION_DEADLINE = time(9, 30)  # До скольки можно изменять/отменять
    
    # Время немедленной синхронизации (для заказов на сегодня после этого времени)
    IMMEDIATE_SYNC_TIME = time(9, 25)
    
    # Выходные дни (0=понедельник, 6=воскресенье)
    WEEKEND_DAYS = [5, 6]  # Суббота, воскресенье
    
    # Время для cron-задач
    MORNING_REMINDER_TIME = time(9, 0)    # Напоминания в 9:00
    MORNING_REPORTS_TIME = time(9, 31)    # Отчеты в 9:31
    ACCOUNTING_REPORT_TIME = time(11, 0)  # Бухгалтерский отчет в 11:00
    SYNC_EMPLOYEES_TIME = time(18, 0)     # Синхронизация в 18:00
    
    # Дни для cron-задач (0=понедельник, 6=воскресенье)
    WORK_DAYS = [0, 1, 2, 3, 4]  # Пн-Пт

    MAX_PORTIONS = 5  # Максимальное количество порций в заказе

# Создаем глобальный экземпляр для импорта
TIME_CONFIG = TimeConfig()