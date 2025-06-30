# ##config.py
import os
import logging
from pathlib import Path
from dotenv import load_dotenv
import pytz
import sqlite3

# Настройка путей
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / 'data'
CONFIGS_DIR = BASE_DIR / 'data' / 'configs'
DB_PATH = DATA_DIR / 'lunch_bot.db'

# Создаем папки если их нет
DATA_DIR.mkdir(parents=True, exist_ok=True)
CONFIGS_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv(CONFIGS_DIR / '.env')

class BotConfig:
    def __init__(self, db_connection):  # Теперь требует db_connection
        self._token = None
        self._admin_ids = []
        self._provider_ids = []
        self._accounting_ids = []
        self._staff_names = set()
        self._holidays = {}
        self._menu = {}
        self._timezone = pytz.timezone('Europe/Moscow')
        self._locations = ["Офис", "ПЦ 1", "ПЦ 2", "Склад"]
        self._db = db_connection  # Сохраняем соединение с БД
        self._orders_enabled = self._load_orders_status()
        
        self._load_env_vars()
        self._load_db_data()
        
        # Затем инициализируем БД и загружаем данные
        from db import db  # Импортируем здесь, чтобы избежать циклических импортов
        self._load_db_data()

    def _load_env_vars(self):
        """Загружает переменные окружения"""
        try:
            self._token = os.getenv('BOT_TOKEN')
            if not self._token:
                raise ValueError("Токен бота не указан в .env файле!")
                
            self._admin_ids = self._parse_ids(os.getenv("ADMIN_IDS", ""))
            self._provider_ids = self._parse_ids(os.getenv("PROVIDER_IDS", ""))
            self._accounting_ids = self._parse_ids(os.getenv("ACCOUNTING_IDS", ""))
            
        except Exception as e:
            logger.error(f"Ошибка загрузки переменных окружения: {e}")
            raise

    def _parse_ids(self, ids_str: str) -> list[int]:
        """Преобразует строку с ID в список чисел"""
        return [int(x) for x in ids_str.split(",") if x.strip()]

    def _load_db_data(self):
        """Загружает данные из базы данных"""
        try:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            
            # Загрузка сотрудников
            cursor.execute("SELECT full_name FROM users WHERE is_employee = 1")
            self._staff_names = {row[0].lower() for row in cursor.fetchall()}
            
            # Загрузка праздников
            cursor.execute("SELECT date, name FROM holidays")
            self._holidays = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Загрузка меню
            cursor.execute("SELECT day, first_course, main_course, salad FROM menu")
            self._menu = {
                row[0]: {"first": row[1], "main": row[2], "salad": row[3]}
                for row in cursor.fetchall()
            }
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Ошибка загрузки данных из БД: {e}")
            raise

    def _load_orders_status(self):
        """Загружает статус заказов из БД"""
        try:
            self._db.cursor.execute("SELECT setting_value FROM bot_settings WHERE setting_name = 'orders_enabled'")
            result = self._db.cursor.fetchone()
            return result and result[0] == 'True'
        except Exception as e:
            logger.error(f"Ошибка загрузки статуса заказов: {e}")
            return True
            
    def toggle_orders(self, enabled: bool):
        """Переключает статус заказов и сохраняет в БД"""
        try:
            self._orders_enabled = enabled
            self._db.cursor.execute(
                """INSERT OR REPLACE INTO bot_settings 
                (setting_name, setting_value) 
                VALUES (?, ?)""",
                ('orders_enabled', str(enabled))
            )
            self._db.conn.commit()
            logger.info(f"Статус заказов обновлен: {'разрешены' if enabled else 'запрещены'}")
        except Exception as e:
            logger.error(f"Ошибка сохранения статуса заказов: {e}")
            raise

    @property
    def token(self) -> str:
        return self._token

    @property
    def admin_ids(self) -> list[int]:
        return self._admin_ids

    @property
    def provider_ids(self) -> list[int]:
        return self._provider_ids

    @property
    def accounting_ids(self) -> list[int]:
        return self._accounting_ids

    @property
    def staff_names(self) -> set:
        return self._staff_names

    @property
    def holidays(self) -> dict:
        return self._holidays

    @property
    def menu(self) -> dict:
        return self._menu

    @property
    def timezone(self):
        return self._timezone

    @property
    def locations(self) -> list:
        return self._locations
    
    @property
    def orders_enabled(self):
        return self._orders_enabled