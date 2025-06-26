# ##config.py
import os
import logging
from dotenv import load_dotenv
from db import db
import pytz

logger = logging.getLogger(__name__)

load_dotenv()

class BotConfig:
    def __init__(self):
        self._load_env_vars()
        self._load_db_data()
        self._timezone = pytz.timezone('Europe/Moscow')
        self._locations = ["Офис", "ПЦ 1", "ПЦ 2", "Склад"]

    def _load_env_vars(self):
        """Загрузка переменных окружения"""
        self._token = os.getenv('BOT_TOKEN')
        self._admin_ids = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
        self._provider_ids = [int(x) for x in os.getenv("PROVIDER_IDS", "").split(",") if x.strip()]
        self._accounting_ids = [int(x) for x in os.getenv("ACCOUNTING_IDS", "").split(",") if x.strip()]

    def _load_db_data(self):
        """Загрузка динамических данных из БД"""
        try:
            # Загрузка сотрудников
            employees = db.get_employees(active_only=True)
            self._staff_names = {emp['full_name'].lower() for emp in employees}
            
            # Добавляем варианты с обратным порядком имен
            for name in list(self._staff_names):
                parts = name.split()
                if len(parts) >= 2:
                    reversed_name = f"{parts[1]} {parts[0]}"
                    if len(parts) > 2:
                        reversed_name += " " + " ".join(parts[2:])
                    self._staff_names.add(reversed_name)

            # Загрузка праздников
            holidays = db.get_holidays()
            self._holidays = {h['date']: h['name'] for h in holidays}

            # Загрузка меню
            menu_items = db.get_full_menu()
            self._menu = {}
            for item in menu_items:
                self._menu[item['day']] = {
                    "first": item['first_course'],
                    "main": item['main_course'],
                    "salad": item['salad']
                }

            logger.info("Конфигурация успешно загружена из БД")
        except Exception as e:
            logger.error(f"Ошибка при загрузке конфигурации: {e}")
            raise

    def reload(self):
        """Перезагружает конфигурацию из .env и БД"""
        logger.info("🔄 Перезагрузка конфигурации...")
        self._load_env_vars()
        self._load_db_data()

    @property
    def token(self):
        return self._token

    @property
    def admin_ids(self):
        return self._admin_ids

    @property
    def provider_ids(self):
        return self._provider_ids

    @property
    def accounting_ids(self):
        return self._accounting_ids

    @property
    def staff_names(self):
        return self._staff_names

    @property
    def holidays(self):
        return self._holidays

    @property
    def menu(self):
        return self._menu

    @property
    def timezone(self):
        return self._timezone

    @property
    def locations(self):
        return self._locations


# Глобальный объект конфигурации
CONFIG = BotConfig()

# Для обратной совместимости
MENU = CONFIG.menu
HOLIDAYS = CONFIG.holidays
ADMIN_IDS = CONFIG.admin_ids
TIMEZONE = CONFIG.timezone
LOCATIONS = CONFIG.locations