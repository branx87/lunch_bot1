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
from time import monotonic
from time_config import TIME_CONFIG

# Настройка путей - ОСТАВЛЯЕМ относительные
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / 'data'
CONFIGS_DIR = BASE_DIR / 'data' / 'configs'
DB_PATH = DATA_DIR / 'db' / 'lunch_bot.db'
LOGS_DIR = DATA_DIR / 'logs'
REPORTS_DIR = DATA_DIR / 'reports'

# Как часто процесс перечитывает статус заказов из БД.
# Компромисс: админ видит переключение во всех мессенджерах почти сразу,
# но при каждом нажатии кнопки в БД не лезем.
ORDERS_STATUS_TTL = 5.0

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
        self._inspector_allowed_bitrix_ids = []  # 🔥 Bitrix ID пользователей, кто может заказывать для инспектора
        self._inspector_crm_id = None  # 🔥 CRM ID инспектора в Bitrix
        self._staff_names = set()
        self._holidays = {}
        self._menu = {}
        self._timezone = TIME_CONFIG.TIMEZONE  # Используем из TIME_CONFIG
        self._locations = ["Офис", "ПЦ 1", "ПЦ 2", "Склад"]
        self._db = database
        self._orders_enabled = self._load_orders_status()
        self._orders_status_at = monotonic()
        logger.info(
            f"Статус заказов при старте: "
            f"{'разрешены' if self._orders_enabled else 'запрещены'}"
        )
        # Заказы через форму Bitrix24 отключены (пользуются 3-4 человека).
        self._accept_bitrix_form_orders = self._load_flag('accept_bitrix_form_orders', False)
        self._form_orders_at = monotonic()
        
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

            # Bitrix24 messenger IDs
            self._b24_admin_ids = self._parse_ids(os.getenv("B24_ADMIN_IDS", ""))
            self._b24_provider_ids = self._parse_ids(os.getenv("B24_PROVIDER_IDS", ""))
            self._b24_accounting_ids = self._parse_ids(os.getenv("B24_ACCOUNTING_IDS", ""))
            
            # 🔥 НОВОЕ: Загрузка master_admin_id
            master_admin_str = os.getenv("MASTER_ADMIN_ID", "")
            if master_admin_str.strip():
                self._master_admin_id = int(master_admin_str.strip())
                logger.info(f"👑 Главный админ: {self._master_admin_id}")
            elif self._admin_ids:
                # Если не указан - берем первого админа из списка
                self._master_admin_id = self._admin_ids[0]
                logger.info(f"👑 Главный админ (по умолчанию): {self._master_admin_id}")
            
            # 🔥 НОВОЕ: Загрузка списка Bitrix ID кто может заказывать для инспектора
            self._inspector_allowed_bitrix_ids = self._parse_ids(os.getenv("INSPECTOR_ALLOWED_BITRIX_IDS", ""))
            
            # 🔥 НОВОЕ: CRM ID инспектора в Bitrix (для поля ufCrm45_1743599470)
            inspector_crm_str = os.getenv("INSPECTOR_CRM_ID", "")
            if inspector_crm_str.strip():
                self._inspector_crm_id = inspector_crm_str.strip()
                logger.info(f"🕵️ CRM ID инспектора: {self._inspector_crm_id}")
            
            logger.info(
                f"✅ Загружены ID: админы={self._admin_ids}, "
                f"поставщики={self._provider_ids}, "
                f"бухгалтеры={self._accounting_ids}, "
                f"главный админ={self._master_admin_id}, "
                f"инспектор (Bitrix ID) могут заказывать={self._inspector_allowed_bitrix_ids}"
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
                if not setting:
                    logger.debug("Настройка orders_enabled не найдена в БД — считаем заказы разрешёнными")
                    return True

                raw = (setting.setting_value or '').strip()
                enabled = raw.lower() == 'true'
                # Статус перечитывается каждые ORDERS_STATUS_TTL секунд,
                # поэтому в лог уходит только отладка: INFO-запись на
                # каждое действие пользователя забивала лог одинаковыми строками.
                logger.debug(f"Статус заказов в БД: setting_value={raw!r} -> {'разрешены' if enabled else 'запрещены'}")
                return enabled
        except Exception as e:
            logger.error(f"Ошибка загрузки статуса заказов: {e}")
            return True

    def _load_flag(self, name: str, default: bool) -> bool:
        """Читает булев флаг из таблицы BotSetting.

        Отсутствующая строка — это не ошибка, а значение по умолчанию.
        """
        try:
            with self._db.get_session() as session:
                from models import BotSetting
                setting = session.query(BotSetting).filter(
                    BotSetting.setting_name == name
                ).first()
                if not setting:
                    # Отсутствующая строка — обычное значение по умолчанию,
                    # а не предупреждение: флаг читается каждые 5 секунд.
                    logger.debug(
                        f"Настройка {name} не найдена в БД — "
                        f"берём значение по умолчанию: {default}"
                    )
                    return default

                raw = (setting.setting_value or '').strip()
                value = raw.lower() == 'true'
                logger.debug(f"Флаг {name} в БД: setting_value={raw!r} -> {value}")
                return value
        except Exception as e:
            logger.error(f"Ошибка загрузки флага {name}: {e}")
            return default

    def _refresh_form_orders_flag(self):
        """Перечитывает флаг приёма заказов из формы Bitrix24 (кэш 5 секунд).

        Флаг живёт в БД, поэтому его смена видна всем процессам
        (Telegram / VK / MAX / Bitrix24) без перезапуска.
        """
        if monotonic() - self._form_orders_at < ORDERS_STATUS_TTL:
            return
        enabled = self._load_flag('accept_bitrix_form_orders', False)
        if enabled != self._accept_bitrix_form_orders:
            logger.info(
                f"Приём заказов через форму Bitrix24: "
                f"{'разрешён' if enabled else 'запрещён'}"
            )
        self._accept_bitrix_form_orders = enabled
        self._form_orders_at = monotonic()

    def set_accept_bitrix_form_orders(self, enabled: bool):
        """Включает/выключает приём заказов через форму Bitrix24."""
        try:
            with self._db.get_session() as session:
                from models import BotSetting
                setting = session.query(BotSetting).filter(
                    BotSetting.setting_name == 'accept_bitrix_form_orders'
                ).first()
                if setting:
                    setting.setting_value = str(enabled)
                else:
                    session.add(BotSetting(
                        setting_name='accept_bitrix_form_orders',
                        setting_value=str(enabled)
                    ))
                session.commit()

            self._accept_bitrix_form_orders = enabled
            self._form_orders_at = monotonic()
            logger.info(
                f"Приём заказов через форму Bitrix24: "
                f"{'разрешён' if enabled else 'запрещён'}"
            )
        except Exception as e:
            logger.error(f"Ошибка сохранения флага accept_bitrix_form_orders: {e}")
            raise

    def _refresh_orders_status(self):
        """Перечитывает статус заказов из БД, если кэш устарел.

        Заказы включаются админом в одном мессенджере, а проверять статус
        должны все три процесса (Telegram / VK / Bitrix24) — у каждого свой
        процесс и своя копия CONFIG в памяти, поэтому без перечитывания
        остальные продолжали бы принимать заказы.
        """
        if monotonic() - self._orders_status_at < ORDERS_STATUS_TTL:
            return
        enabled = self._load_orders_status()
        if enabled != self._orders_enabled:
            logger.info(
                f"Статус заказов изменился: "
                f"{'разрешены' if enabled else 'запрещены'}"
            )
        self._orders_enabled = enabled
        self._orders_status_at = monotonic()
            
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
                self._orders_status_at = monotonic()
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
        Глобальный статус приема заказов.
        Актуальное значение подтягивается из БД (см. _refresh_orders_status),
        поэтому переключение админом видно всем мессенджерам.
        Временные ограничения проверяются отдельно в can_modify_order().
        """
        self._refresh_orders_status()
        return self._orders_enabled

    def get_orders_status_message(self) -> str:
        """Статус приема заказов: на какой день и до какого времени можно заказать."""
        if not self.are_orders_accepted_now():
            return "❌ Приём заказов временно приостановлен"

        try:
            from services.time_service import next_working_day, is_new_supplier_date
            from services.menu_service import DAYS_RU

            now = datetime.now(self.timezone)
            if now.weekday() in TIME_CONFIG.WEEKEND_DAYS:
                return "❌ Прием заказов не осуществляется в выходные дни"

            today = now.date()

            if not is_new_supplier_date(today):
                # Старый поставщик: заказ на сегодня до 09:30
                if now.time() >= TIME_CONFIG.ORDER_DEADLINE:
                    return f"❌ Прием заказов на сегодня завершен (до {TIME_CONFIG.ORDER_DEADLINE.strftime('%H:%M')})"
                return f"✅ Прием заказов открыт (до {TIME_CONFIG.ORDER_DEADLINE.strftime('%H:%M')})"

            # Новый поставщик: заказ на следующий рабочий день до 15:00
            nxt = next_working_day(today, self.holidays)
            return (
                f"✅ Приём заказов открыт до {TIME_CONFIG.ADVANCE_ORDER_DEADLINE.strftime('%H:%M')} — "
                f"на {nxt.strftime('%d.%m')} ({DAYS_RU[nxt.weekday()].lower()})"
            )
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
        self._refresh_orders_status()
        return self._orders_enabled

    @property
    def accept_bitrix_form_orders(self) -> bool:
        """Разрешены ли заказы через форму Bitrix24.

        По умолчанию — нет: заказы оформляются через бот,
        а в Bitrix24 они попадают выгрузкой в день обеда.
        """
        self._refresh_form_orders_flag()
        return self._accept_bitrix_form_orders

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

    @property
    def b24_admin_ids(self) -> list[int]:
        return self._b24_admin_ids

    @property
    def b24_provider_ids(self) -> list[int]:
        return self._b24_provider_ids

    @property
    def b24_accounting_ids(self) -> list[int]:
        return self._b24_accounting_ids

    @property
    def inspector_allowed_bitrix_ids(self) -> list[int]:
        """Bitrix ID пользователей, которые могут заказывать для инспектора"""
        return self._inspector_allowed_bitrix_ids

    @property
    def inspector_crm_id(self) -> str | None:
        """CRM ID инспектора в Bitrix (для поля ufCrm45_1743599470)"""
        return self._inspector_crm_id

# Создаем глобальный экземпляр CONFIG
try:
    from database import db
    CONFIG = BotConfig(db)
except Exception as e:
    logging.error(f"Ошибка создания CONFIG: {e}")
    CONFIG = None