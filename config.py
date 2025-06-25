# ##config.py
import os
import logging
from dotenv import load_dotenv
import pytz
from pathlib import Path
from typing import List, Set, Dict, Any

# Настройка базовых путей
BASE_DIR = Path(__file__).parent  # Теперь указывает на корень проекта
DATA_DIR = BASE_DIR / 'data'
LOGS_DIR = DATA_DIR / 'logs'
REPORTS_DIR = DATA_DIR / 'reports'
CONFIGS_DIR = DATA_DIR / 'configs'
DB_PATH = DATA_DIR / 'lunch_bot.db'

# Создание необходимых директорий
for directory in [DATA_DIR, LOGS_DIR, REPORTS_DIR, CONFIGS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv(CONFIGS_DIR / '.env')

class BotConfig:
    def __init__(self):
        """Инициализация конфигурации бота"""
        self._timezone = pytz.timezone('Europe/Moscow')
        self._locations = ["Офис", "ПЦ 1", "ПЦ 2", "Склад"]
        self._token = os.getenv('BOT_TOKEN')
        self._admin_ids = self._parse_ids(os.getenv("ADMIN_IDS", ""))
        self._provider_ids = self._parse_ids(os.getenv("PROVIDER_IDS", ""))
        self._accounting_ids = self._parse_ids(os.getenv("ACCOUNTING_IDS", ""))
        self._staff_names = set()
        self._holidays = {}
        self._menu = {}

    def _parse_ids(self, ids_str: str) -> List[int]:
        return [int(x) for x in ids_str.split(",") if x.strip()]

    def init_db_data(self, db_connection):
        """Инициализирует данные из БД"""
        try:
            # Загрузка сотрудников
            employees = db_connection.get_employees(active_only=True)
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
            self._holidays = {h['date']: h['name'] for h in db_connection.get_holidays()}

            # Загрузка меню
            self._menu = {
                item['day']: {
                    "first": item['first_course'],
                    "main": item['main_course'],
                    "salad": item['salad']
                } for item in db_connection.get_full_menu()
            }

            logger.info("Конфигурация успешно загружена из БД")
        except Exception as e:
            logger.error(f"Ошибка при загрузке конфигурации: {e}")
            raise

    @property
    def token(self) -> str:
        return self._token

    @property
    def admin_ids(self) -> List[int]:
        return self._admin_ids

    @property
    def provider_ids(self) -> List[int]:
        return self._provider_ids

    @property
    def accounting_ids(self) -> List[int]:
        return self._accounting_ids

    @property
    def staff_names(self) -> Set[str]:
        return self._staff_names

    @property
    def holidays(self) -> Dict[str, str]:
        return self._holidays

    @property
    def menu(self) -> Dict[str, Dict[str, str]]:
        return self._menu

    @property
    def timezone(self):
        return self._timezone

    @property
    def locations(self) -> List[str]:
        return self._locations

    @property
    def db_path(self) -> Path:
        return DB_PATH

    @property
    def reports_dir(self) -> Path:
        return REPORTS_DIR

    @property
    def logs_dir(self) -> Path:
        return LOGS_DIR

# Глобальный объект конфигурации
CONFIG = BotConfig()

# Для обратной совместимости
HOLIDAYS = CONFIG.holidays
TIMEZONE = CONFIG.timezone
LOCATIONS = CONFIG.locations
MENU = CONFIG.menu
ADMIN_IDS = CONFIG.admin_ids