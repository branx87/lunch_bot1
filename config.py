# ##config.py
import os
import logging
from pathlib import Path
from dotenv import load_dotenv
import pytz
import sqlite3
from database import db
from sqlalchemy.orm import Session
from datetime import datetime, time, timedelta
from time_config import TIME_CONFIG

# Настройка путей - ОСТАВЛЯЕМ относительные
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / 'data'
CONFIGS_DIR = BASE_DIR / 'data' / 'configs'
DB_PATH = DATA_DIR / 'db' / 'lunch_bot.db'
LOGS_DIR = DATA_DIR / 'logs'
REPORTS_DIR = DATA_DIR / 'reports'

# Создаем папки если их нет
DATA_DIR.mkdir(parents=True, exist_ok=True)
CONFIGS_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv(CONFIGS_DIR / '.env')

class BotConfig:
    def __init__(self, database):
        self._token = None
        self._admin_ids = []
        self._provider_ids = []
        self._accounting_ids = []
        self._master_admin_id = None  # 🔥 ДОБАВИТЬ
        self._staff_names = set()
        self._holidays = {}
        self._menu = {}
        self._timezone = TIME_CONFIG.TIMEZONE  # Используем из TIME_CONFIG
        self._locations = ["Офис", "ПЦ 1", "ПЦ 2", "Склад"]
        self._db = database
        self._orders_enabled = self._load_orders_status()
        
        self._load_env_vars()
        self._load_db_data()

    def _load_env_vars(self):
        """Загружает переменные окружения с отладкой"""
        try:
            # # Логируем все переменные для отладки
            # all_env_vars = dict(os.environ)
            # logger.info("🔍 Все переменные окружения:")
            # for key, value in all_env_vars.items():
            #     if any(x in key.lower() for x in ['admin', 'token', 'id']):
            #         logger.info(f"   {key}: {value}")
            
            self._token = os.getenv('BOT_TOKEN')
            if not self._token:
                raise ValueError("Токен бота не указан в .env файле!")

            self._proxy_url = os.getenv('PROXY_URL', '').strip() or None
            if self._proxy_url:
                logger.info(f"🔗 Прокси настроен: {self._proxy_url}")
                
            admin_ids_str = os.getenv("ADMIN_IDS", "")
            logger.info(f"📋 ADMIN_IDS из os.getenv: '{admin_ids_str}'")
            
            self._admin_ids = self._parse_ids(admin_ids_str)
            self._provider_ids = self._parse_ids(os.getenv("PROVIDER_IDS", ""))
            self._accounting_ids = self._parse_ids(os.getenv("ACCOUNTING_IDS", ""))

            # Max messenger IDs (закомментирован — требует юрлицо)
            self._max_admin_ids = self._parse_ids(os.getenv("MAX_ADMIN_IDS", ""))
            self._max_provider_ids = self._parse_ids(os.getenv("MAX_PROVIDER_IDS", ""))
            self._max_accounting_ids = self._parse_ids(os.getenv("MAX_ACCOUNTING_IDS", ""))

            # VK messenger IDs
            self._vk_admin_ids = self._parse_ids(os.getenv("VK_ADMIN_IDS", ""))
            self._vk_provider_ids = self._parse_ids(os.getenv("VK_PROVIDER_IDS", ""))
            self._vk_accounting_ids = self._parse_ids(os.getenv("VK_ACCOUNTING_IDS", ""))
            
            # 🔥 НОВОЕ: Загрузка master_admin_id
            master_admin_str = os.getenv("MASTER_ADMIN_ID", "")
            if master_admin_str.strip():
                self._master_admin_id = int(master_admin_str.strip())
                logger.info(f"👑 Главный админ: {self._master_admin_id}")
            elif self._admin_ids:
                # Если не указан - берем первого админа из списка
                self._master_admin_id = self._admin_ids[0]
                logger.info(f"👑 Главный админ (по умолчанию): {self._master_admin_id}")
            
            logger.info(
                f"✅ Загружены ID: админы={self._admin_ids}, "
                f"поставщики={self._provider_ids}, "
                f"бухгалтеры={self._accounting_ids}, "
                f"главный админ={self._master_admin_id}"
            )
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки переменных окружения: {e}")
            raise

    def _parse_ids(self, ids_str: str) -> list[int]:
        """Преобразует строку с ID в список чисел"""
        return [int(x) for x in ids_str.split(",") if x.strip()]

    def _load_db_data(self):
        """Загружает данные из базы данных"""
        try:
            with self._db.get_session() as session:
                # Загрузка сотрудников
                from models import User
                users = session.query(User).filter(
                    User.is_employee == True, 
                    User.is_deleted == False
                ).all()
                self._staff_names = {user.full_name.lower() for user in users}
                
                # Загрузка праздников
                from models import Holiday
                holidays = session.query(Holiday).all()
                self._holidays = {holiday.date: holiday.name for holiday in holidays}
                
                # Загрузка меню
                from models import Menu
                menu_items = session.query(Menu).all()
                self._menu = {
                    item.day: {
                        "first": item.first_course,
                        "main": item.main_course, 
                        "salad": item.salad
                    } for item in menu_items
                }
                
        except Exception as e:
            logger.error(f"Ошибка загрузки данных из БД: {e}")
            # Значения по умолчанию
            self._staff_names = set()
            self._holidays = {}
            self._menu = {}

    def _load_orders_status(self):
        """Загружает статус заказов из БД"""
        try:
            with self._db.get_session() as session:
                from models import BotSetting
                setting = session.query(BotSetting).filter(
                    BotSetting.setting_name == 'orders_enabled'
                ).first()
                return setting and setting.setting_value == 'True'
        except Exception as e:
            logger.error(f"Ошибка загрузки статуса заказов: {e}")
            return True
            
    def toggle_orders(self, enabled: bool):
        """Переключает статус заказов"""
        try:
            with self._db.get_session() as session:
                from models import BotSetting
                setting = session.query(BotSetting).filter(
                    BotSetting.setting_name == 'orders_enabled'
                ).first()
                
                if setting:
                    setting.setting_value = str(enabled)
                else:
                    setting = BotSetting(
                        setting_name='orders_enabled',
                        setting_value=str(enabled)
                    )
                    session.add(setting)
                
                self._orders_enabled = enabled
                logger.info(f"Статус заказов обновлен: {'разрешены' if enabled else 'запрещены'}")
                
        except Exception as e:
            logger.error(f"Ошибка сохранения статуса заказов: {e}")
            raise

    def reload(self):
        """Перезагружает конфигурацию из файла .env и базы данных"""
        try:
            # Перезагружаем переменные окружения
            load_dotenv(CONFIGS_DIR / '.env', override=True)
            self._load_env_vars()
            
            # Перезагружаем данные из базы данных
            self._load_db_data()
            
            logger.info("Конфигурация успешно перезагружена")
        except Exception as e:
            logger.error(f"Ошибка при перезагрузке конфигурации: {e}")
            raise

    def are_orders_accepted_now(self) -> bool:
        """
        Старая логика: проверяет только глобальный статус
        Временные ограничения проверяются отдельно в can_modify_order()
        """
        return self._orders_enabled

    def get_orders_status_message(self) -> str:
        """Возвращает сообщение о статусе приема заказов"""
        if not self._orders_enabled:
            return "❌ Прием заказов отключен администратором"
        
        try:
            now = datetime.now(self.timezone)
            if not self.are_orders_accepted_now():
                if now.weekday() in TIME_CONFIG.WEEKEND_DAYS:
                    return "❌ Прием заказов не осуществляется в выходные дни"
                else:
                    return f"❌ Прием заказов на сегодня завершен (до {TIME_CONFIG.ORDER_DEADLINE.strftime('%H:%M')})"
            
            return f"✅ Прием заказов открыт (до {TIME_CONFIG.ORDER_DEADLINE.strftime('%H:%M')})"
        except Exception as e:
            logger.error(f"Ошибка получения статуса заказов: {e}")
            return "❌ Ошибка проверки статуса заказов"

    @property
    def token(self) -> str:
        return self._token

    @property
    def proxy_url(self) -> str | None:
        return self._proxy_url

    @property
    def admin_ids(self) -> list[int]:
        return self._admin_ids

    @property
    def master_admin_id(self) -> int:
        """ID главного администратора"""
        return self._master_admin_id

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

    @property
    def max_admin_ids(self) -> list[int]:
        return self._max_admin_ids

    @property
    def max_provider_ids(self) -> list[int]:
        return self._max_provider_ids

    @property
    def max_accounting_ids(self) -> list[int]:
        return self._max_accounting_ids

    @property
    def vk_admin_ids(self) -> list[int]:
        return self._vk_admin_ids

    @property
    def vk_provider_ids(self) -> list[int]:
        return self._vk_provider_ids

    @property
    def vk_accounting_ids(self) -> list[int]:
        return self._vk_accounting_ids

# Создаем глобальный экземпляр CONFIG
try:
    from database import db
    CONFIG = BotConfig(db)
except Exception as e:
    logging.error(f"Ошибка создания CONFIG: {e}")
    CONFIG = None