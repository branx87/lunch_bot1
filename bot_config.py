# # ##bot_config.py
# import os
# import logging
# from dotenv import load_dotenv
# from db import Database
# import pytz

# logger = logging.getLogger(__name__)

# load_dotenv()

# class BotConfig:
#     def __init__(self):
#         self._load_env_vars()
#         self._load_db_data()
#         self.timezone = pytz.timezone('Europe/Moscow')
#         self.locations = ["Офис", "ПЦ 1", "ПЦ 2", "Склад"]

#     def _load_env_vars(self):
#         """Загрузка переменных окружения"""
#         self.token = os.getenv('BOT_TOKEN')
#         self.admin_ids = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
#         self.provider_ids = [int(x) for x in os.getenv("PROVIDER_IDS", "").split(",") if x.strip()]
#         self.accounting_ids = [int(x) for x in os.getenv("ACCOUNTING_IDS", "").split(",") if x.strip()]

#     def _load_db_data(self):
#         """Загрузка динамических данных из БД"""
#         try:
#             # Загрузка сотрудников (используем get_employees вместо get_all_staff)
#             employees = db.get_employees(active_only=True)
#             self.staff_names = {emp['full_name'].lower() for emp in employees}
            
#             # Добавляем варианты с обратным порядком имен
#             for name in list(self.staff_names):
#                 parts = name.split()
#                 if len(parts) >= 2:
#                     reversed_name = f"{parts[1]} {parts[0]}"
#                     if len(parts) > 2:
#                         reversed_name += " " + " ".join(parts[2:])
#                     self.staff_names.add(reversed_name)

#             # Загрузка праздников
#             holidays = db.get_holidays()
#             self.holidays = {h['date']: h['name'] for h in holidays}

#             # Загрузка меню
#             menu_items = db.get_full_menu()
#             self.menu = {}
#             for item in menu_items:
#                 self.menu[item['day']] = {
#                     "first": item['first_course'],
#                     "main": item['main_course'],
#                     "salad": item['salad']
#                 }

#             logger.info("Конфигурация успешно загружена из БД")
#         except Exception as e:
#             logger.error(f"Ошибка при загрузке конфигурации: {e}")
#             raise

# # Глобальный объект конфигурации
# CONFIG = BotConfig()