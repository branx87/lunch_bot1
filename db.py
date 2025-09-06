# ##db.py
from datetime import datetime
import shutil
import sqlite3
import logging
import pandas as pd
from typing import Optional, List, Dict, Any
import os
from pathlib import Path
import atexit

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        # Используем относительные пути (как было изначально)
        db_dir = Path('data/db')
        db_dir.mkdir(parents=True, exist_ok=True)  # Создаем папку если ее нет
        db_path = db_dir / 'lunch_bot.db'
        
        # Создаем папку для бэкапов
        backup_dir = Path('data/backups')
        backup_dir.mkdir(exist_ok=True)
        
        # Делаем бэкап при каждом запуске (если БД существует)
        if db_path.exists():
            backup_name = f"lunch_bot_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
            shutil.copy2(db_path, backup_dir / backup_name)
            logger.info(f"Создан бэкап базы данных: {backup_name}")
        
        self.conn = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
        self.cursor = self.conn.cursor()
        self._init_db()
        if not self._is_data_loaded():
            self._load_initial_data()
        
        # Регистрируем очистку при завершении работы
        atexit.register(self.cleanup)

    def cleanup(self):
        """Корректно закрываем соединение с базой и очищаем WAL файлы"""
        try:
            logger.info("Очистка базы данных...")
            
            # Сначала выполняем checkpoint если соединение еще открыто
            if hasattr(self, 'conn') and self.conn:
                try:
                    self.cursor.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                except Exception as e:
                    logger.warning(f"Не удалось выполнить checkpoint: {e}")
            
            # Затем закрываем соединение
            if hasattr(self, 'conn') and self.conn:
                self.conn.close()
                logger.info("База данных корректно закрыта")
                
        except Exception as e:
            logger.error(f"Ошибка при очистке базы: {e}")

    def _is_data_loaded(self):
        """Проверяет, были ли уже загружены основные данные"""
        # Проверяем наличие хотя бы одного сотрудника и одного дня меню
        has_employees = self.cursor.execute("SELECT 1 FROM users WHERE is_employee = TRUE LIMIT 1").fetchone()
        has_menu = self.cursor.execute("SELECT 1 FROM menu LIMIT 1").fetchone()
        return has_employees is not None and has_menu is not None

    def _load_initial_data(self):
        """Загружает начальные данные из Excel файла только при первом запуске"""
        try:
            config_path = Path('data') / 'configs' / 'config.xlsx'
            if not config_path.exists():
                logger.warning(f"Файл {config_path} не найден, пропускаем загрузку данных")
                return

            logger.info("Начальная загрузка данных из Excel...")
            df = pd.read_excel(config_path, header=0)
            
            # Загрузка сотрудников (без дублирования)
            existing_employees = {e['full_name'].lower() for e in self.get_employees(active_only=False)}
            employees = df.iloc[1:, 6].dropna().unique().tolist()  # Берем только уникальные значения
            
            new_employees = 0
            for emp in employees:
                emp_name = str(emp).strip()
                if emp_name.lower() not in existing_employees:
                    self.add_user(full_name=emp_name, is_employee=True)
                    new_employees += 1
            logger.info(f"Добавлено {new_employees} новых сотрудников")
            
            # Загрузка меню (полная перезапись)
            self.cursor.execute("DELETE FROM menu")  # Очищаем старое меню
            
            menu_data = {}
            current_day = None
            
            df = pd.read_excel(config_path, header=None)
            menu_data = {}
            current_day = None

            idx = 2  # Начинаем с I2 (третья строка в Pandas — индекс 2)

            while idx < len(df):
                day_cell = df.iloc[idx, 8]

                if pd.isna(day_cell):
                    idx += 1
                    continue

                day_str = str(day_cell).strip().lower()

                # Проверяем, является ли ячейка днём недели
                if any(day in day_str for day in ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"]):
                    current_day = day_str.capitalize()
                    idx += 1

                    # Получаем блюда из следующих трёх строк
                    first = df.iloc[idx, 8] if idx < len(df) and not pd.isna(df.iloc[idx, 8]) else ""
                    idx += 1
                    main = df.iloc[idx, 8] if idx < len(df) and not pd.isna(df.iloc[idx, 8]) else ""
                    idx += 1
                    salad = df.iloc[idx, 8] if idx < len(df) and not pd.isna(df.iloc[idx, 8]) else ""
                    idx += 1

                    if current_day and first and main and salad:
                        menu_data[current_day] = {
                            "first": str(first).strip(),
                            "main": str(main).strip(),
                            "salad": str(salad).strip()
                        }
                    else:
                        logger.warning(f"Пропущено меню для {current_day} — отсутствуют блюда")
                        current_day = None
                else:
                    # Если это не день недели, просто переходим дальше
                    idx += 1

            # Сохраняем меню в базу данных
            added_days = 0
            for day, dishes in menu_data.items():
                if dishes["first"] and dishes["main"] and dishes["salad"]:
                    self.add_menu(day, dishes["first"], dishes["main"], dishes["salad"])
                    added_days += 1

            logger.info(f"✅ Добавлено меню для {added_days} дней")
            
            # Загрузка праздников (без дублирования)
            holidays_df = df.iloc[1:, 10:12].dropna()
            existing_holidays = {(h['date'], h['name']) for h in self.get_holidays()}
            
            new_holidays = 0
            for idx in range(len(holidays_df)):
                row = holidays_df.iloc[idx]
                date = row.iloc[0].strftime('%d.%m.%Y') if hasattr(row.iloc[0], 'strftime') else str(row.iloc[0])
                name = str(row.iloc[1]).strip()
                
                if (date, name) not in existing_holidays:
                    self.add_holiday(date, name)
                    new_holidays += 1
            
            logger.info(f"Добавлено {new_holidays} новых праздников")
            
        except Exception as e:
            logger.error(f"Ошибка загрузки данных из Excel: {e}", exc_info=True)
            raise

    def _init_db(self):
        """Создание таблиц и индексов"""
        try:
            # Настройки SQLite
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self.conn.execute("PRAGMA busy_timeout=5000")

            # Таблица пользователей (объединенная с сотрудниками)
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bitrix_id INTEGER,
                    crm_employee_id INTEGER,
                    telegram_id INTEGER UNIQUE,
                    full_name TEXT NOT NULL,
                    position TEXT,
                    department TEXT,
                    phone TEXT,
                    location TEXT,
                    is_verified BOOLEAN DEFAULT FALSE,
                    is_employee BOOLEAN DEFAULT FALSE,
                    username TEXT,
                    is_deleted BOOLEAN DEFAULT FALSE,
                    notifications_enabled BOOLEAN DEFAULT TRUE,
                    bitrix_entity_type TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Таблица соответствий Bitrix
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS bitrix_mapping (
                    local_id INTEGER NOT NULL,
                    local_type TEXT NOT NULL,
                    bitrix_id INTEGER NOT NULL,
                    bitrix_entity_type TEXT NOT NULL,
                    last_sync TEXT DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (local_id, local_type)
                )
            ''')

            # Таблица заказов
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bitrix_order_id TEXT,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    user_id INTEGER NOT NULL,
                    target_date TEXT NOT NULL,
                    order_time TEXT NOT NULL,
                    quantity INTEGER NOT NULL CHECK(quantity BETWEEN 1 AND 5),
                    bitrix_quantity_id TEXT,
                    is_cancelled BOOLEAN DEFAULT FALSE,
                    is_from_bitrix BOOLEAN DEFAULT FALSE,
                    is_sent_to_bitrix BOOLEAN DEFAULT FALSE,
                    is_preliminary BOOLEAN DEFAULT FALSE,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    last_synced_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
                )
            ''')

            # Таблица праздников
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS holidays (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    name TEXT NOT NULL,
                    is_recurring BOOLEAN DEFAULT FALSE,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(date, name)
                )
            ''')

            # Таблица меню
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS menu (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    day TEXT NOT NULL UNIQUE,
                    first_course TEXT NOT NULL,
                    main_course TEXT NOT NULL,
                    salad TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Таблица сообщений администраторов
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS admin_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    admin_id INTEGER,
                    user_id INTEGER NOT NULL,
                    message_text TEXT NOT NULL,
                    is_broadcast BOOLEAN DEFAULT FALSE,
                    is_unregistered BOOLEAN DEFAULT FALSE,
                    sent_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(admin_id) REFERENCES users(id),
                    FOREIGN KEY(user_id) REFERENCES users(id)
                )
            ''')

            # Таблица обратной связи
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS feedback_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    provider_id INTEGER NOT NULL,
                    message_text TEXT NOT NULL,
                    sent_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    is_processed BOOLEAN DEFAULT FALSE,
                    FOREIGN KEY(user_id) REFERENCES users(id),
                    FOREIGN KEY(provider_id) REFERENCES users(id)
                )
            ''')
            
            # Таблица настроек бота
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS bot_settings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    setting_name TEXT UNIQUE,
                    setting_value TEXT,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Добавим начальное значение, если таблица пустая
            self.cursor.execute('''
                INSERT OR IGNORE INTO bot_settings (setting_name, setting_value)
                VALUES ('orders_enabled', 'True')
            ''')

            # === 1. Проверяем и добавляем недостающие столбцы ===
            # Проверяем city в users
            cursor = self.cursor.execute("PRAGMA table_info(users)")
            user_cols = [row[1] for row in cursor.fetchall()]
            if 'city' not in user_cols:
                self.cursor.execute("ALTER TABLE users ADD COLUMN city TEXT")
                logger.info("✅ Добавлен столбец 'city' в users")

            # Проверяем last_synced_at в orders
            cursor = self.cursor.execute("PRAGMA table_info(orders)")
            order_cols = [row[1] for row in cursor.fetchall()]
            if 'last_synced_at' not in order_cols:
                self.cursor.execute("ALTER TABLE orders ADD COLUMN last_synced_at TEXT DEFAULT CURRENT_TIMESTAMP")
                logger.info("✅ Добавлен столбец 'last_synced_at' в orders")

            # === 2. Очищаем дубликаты перед созданием индекса ===
            # Сначала удаляем дубликаты bitrix_order_id
            try:
                self.cursor.execute('''
                    DELETE FROM orders 
                    WHERE id NOT IN (
                        SELECT MIN(id) 
                        FROM orders 
                        WHERE bitrix_order_id IS NOT NULL 
                        GROUP BY bitrix_order_id
                    ) AND bitrix_order_id IS NOT NULL
                ''')
                deleted_count = self.cursor.rowcount
                if deleted_count > 0:
                    logger.info(f"✅ Удалено {deleted_count} дубликатов заказов")
            except Exception as e:
                logger.warning(f"Не удалось очистить дубликаты: {e}")

            # === 3. Создаем уникальный индекс если еще нет ===
            try:
                self.cursor.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_bitrix_order_id 
                    ON orders(bitrix_order_id) 
                    WHERE bitrix_order_id IS NOT NULL
                """)
                logger.info("✅ Уникальный индекс создан")
            except sqlite3.OperationalError as e:
                logger.warning(f"Не удалось создать уникальный индекс: {e}")
                # Продолжаем работу без уникального индекса

            # Создаем остальные индексы
            # Для users
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_verified ON users(is_verified)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_employee ON users(is_employee)")
            
            # Для orders
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_target_date ON orders(target_date)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_is_cancelled ON orders(is_cancelled)")
            
            # Для holidays
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_holidays_date ON holidays(date)")
            
            # Для menu
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_menu_day ON menu(day)")
            
            # Для admin_messages
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_admin_messages_admin ON admin_messages(admin_id)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_admin_messages_user ON admin_messages(user_id)")
            
            # Для feedback_messages
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_feedback_user ON feedback_messages(user_id)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_feedback_provider ON feedback_messages(provider_id)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_feedback_processed ON feedback_messages(is_processed)")

            self.conn.commit()
            logger.info("✅ Все таблицы и индексы созданы или проверены успешно")

        except Exception as e:
            logger.critical(f"Ошибка инициализации БД: {e}")
            self.conn.rollback()
            raise

    def execute(self, query, params=()):
        """Безопасное выполнение SQL-запроса с принудительной синхронизацией"""
        try:
            self.cursor.execute("BEGIN")
            if not isinstance(params, (tuple, list, dict)):
                raise ValueError("Параметры должны быть кортежем, списком или словарём")

            self.cursor.execute(query, params)
            result = self.cursor.fetchall()
            self.conn.commit()
            
            # Принудительная синхронизация после commit
            self.conn.execute("PRAGMA synchronous = NORMAL")
            self.conn.execute("PRAGMA journal_mode = WAL")
            
            return result
        except sqlite3.Error as e:
            self.conn.rollback()
            logger.error(f"Ошибка выполнения SQL: {e} | Query: {query} | Params: {params}")
            raise

    # ===== МЕТОДЫ ДЛЯ РАБОТЫ С ПОЛЬЗОВАТЕЛЯМИ =====
    def add_user(self, full_name: str, telegram_id: Optional[int] = None,
                phone: Optional[str] = None, location: Optional[str] = None,
                is_employee: bool = False, username: Optional[str] = None) -> int:
        """Добавляет или обновляет пользователя"""
        try:
            self.cursor.execute(
                """
                INSERT INTO users (telegram_id, full_name, phone, location, is_employee, username)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(telegram_id) DO UPDATE SET
                    full_name = excluded.full_name,
                    phone = excluded.phone,
                    location = excluded.location,
                    is_employee = excluded.is_employee,
                    username = excluded.username,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (telegram_id, full_name.strip(), phone, location, is_employee, username)
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Ошибка при добавлении пользователя: {e}")
            return -1
        
    def update_user_data(self, user_id: int, **kwargs) -> bool:
        """Обновляет данные пользователя"""
        try:
            if not kwargs:
                return False
                
            # Список допустимых полей для обновления
            allowed_fields = ['bitrix_id', 'position', 'department', 'is_deleted', 
                            'full_name', 'phone', 'location', 'is_verified', 
                            'is_employee', 'username', 'notifications_enabled']
            
            # Фильтруем только допустимые поля
            update_data = {k: v for k, v in kwargs.items() if k in allowed_fields}
            
            if not update_data:
                return False
                
            set_clause = ', '.join([f"{key} = ?" for key in update_data.keys()])
            values = list(update_data.values())
            values.append(user_id)
            
            self.cursor.execute(
                f"UPDATE users SET {set_clause}, updated_at = datetime('now') WHERE id = ?",
                values
            )
            self.conn.commit()
            return self.cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Ошибка обновления данных пользователя: {e}")
            return False

    def verify_user(self, telegram_id: int, full_name: str, phone: str, username: str) -> bool:
        """Подтверждает пользователя и заполняет его данные"""
        try:
            self.cursor.execute(
                """
                UPDATE users 
                SET is_verified = TRUE,
                    full_name = ?,
                    phone = ?,
                    username = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE telegram_id = ?
                """,
                (full_name.strip(), phone.strip(), username, telegram_id)
            )
            self.conn.commit()
            return self.cursor.rowcount > 0
        except sqlite3.Error as e:
            logger.error(f"Ошибка при подтверждении пользователя: {e}")
            return False

    def get_user(self, telegram_id: int) -> Optional[dict]:
        """Возвращает данные пользователя со всеми полями"""
        row = self.cursor.execute(
            "SELECT * FROM users WHERE telegram_id = ? AND is_deleted = FALSE",
            (telegram_id,)
        ).fetchone()
        return self._row_to_dict(row) if row else None

    def get_employees(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """Получаем список всех сотрудников со всеми полями"""
        try:
            query = '''
                SELECT id, full_name, bitrix_id, crm_employee_id, position, 
                    department, is_deleted, is_verified, location, 
                    is_employee, created_at, updated_at
                FROM users 
                WHERE is_employee = TRUE
                {}
                ORDER BY full_name
            '''.format("AND is_deleted = FALSE" if active_only else "")
            
            self.cursor.execute(query)
            columns = [col[0] for col in self.cursor.description]
            return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
        except Exception as e:
            logger.error(f"Ошибка получения сотрудников: {e}")
            return []
        
    def update_user_bitrix_data(self, user_id: int, bitrix_id: int, entity_type: str) -> bool:
        """Обновляем Bitrix ID пользователя"""
        try:
            self.cursor.execute('''
                UPDATE users 
                SET bitrix_id = ?, bitrix_entity_type = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (bitrix_id, entity_type, user_id))
            self.conn.commit()
            return self.cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Ошибка обновления Bitrix данных: {e}")
            return False
        
    def __del__(self):
        """Закрываем соединение при удалении объекта"""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
    
    # Методы для работы с Bitrix:
    def get_bitrix_id(self, local_id: int, entity_type: str) -> Optional[int]:
        """Возвращает bitrix_id для локального объекта"""
        row = self.cursor.execute(
            "SELECT bitrix_id FROM bitrix_mapping WHERE local_id = ? AND local_type = ?",
            (local_id, entity_type)
        ).fetchone()
        return row[0] if row else None

    def add_bitrix_mapping(self, local_id: int, local_type: str, bitrix_id: int, bitrix_entity_type: str):
        """Добавляет или обновляет соответствие"""
        self.cursor.execute(
            """
            INSERT INTO bitrix_mapping (local_id, local_type, bitrix_id, bitrix_entity_type)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(local_id, local_type) DO UPDATE SET
                bitrix_id = excluded.bitrix_id,
                last_sync = CURRENT_TIMESTAMP
            """,
            (local_id, local_type, bitrix_id, bitrix_entity_type)
        )
        self.conn.commit()

    # ===== МЕТОДЫ ДЛЯ РАБОТЫ С ПРАЗДНИКАМИ =====
    def add_holiday(self, date: str, name: str, is_recurring: bool = False) -> int:
        """Добавляет праздник в базу"""
        try:
            self.cursor.execute(
                "INSERT INTO holidays (date, name, is_recurring) VALUES (?, ?, ?)",
                (date, name, is_recurring)
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.IntegrityError:
            logger.warning(f"Праздник {name} на {date} уже существует")
            return -1

    def get_holidays(self) -> List[dict]:
        """Возвращает список всех праздников"""
        return self._rows_to_dicts(
            self.cursor.execute("SELECT * FROM holidays ORDER BY date").fetchall()
        )

    # ===== МЕТОДЫ ДЛЯ РАБОТЫ С МЕНЮ =====
    def add_menu(self, day: str, first_course: str, main_course: str, salad: str) -> int:
        """Добавляет или обновляет запись меню"""
        try:
            self.cursor.execute(
                """
                INSERT INTO menu (day, first_course, main_course, salad)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(day) DO UPDATE SET
                    first_course = excluded.first_course,
                    main_course = excluded.main_course,
                    salad = excluded.salad,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (day.strip(), first_course.strip(), main_course.strip(), salad.strip())
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Ошибка при обновлении меню: {e}")
            return -1

    def get_full_menu(self) -> List[dict]:
        """Возвращает всё меню на неделю"""
        return self._rows_to_dicts(
            self.cursor.execute(
                "SELECT day, first_course, main_course, salad FROM menu ORDER BY day"
            ).fetchall()
        )
    
    def get_menu_by_day(self, day: str) -> Optional[dict]:
        """Возвращает меню на указанный день недели"""
        row = self.cursor.execute(
            "SELECT * FROM menu WHERE day = ?",
            (day,)
        ).fetchone()
        return self._row_to_dict(row) if row else None

    # ===== МЕТОДЫ ДЛЯ РАБОТЫ С СООБЩЕНИЯМИ =====
    def add_admin_message(self, admin_id: int, user_id: Optional[int], message_text: str, is_broadcast: bool = False) -> int:
        """Добавляет сообщение от администратора"""
        try:
            self.cursor.execute(
                "INSERT INTO admin_messages (admin_id, user_id, message_text, is_broadcast) VALUES (?, ?, ?, ?)",
                (admin_id, user_id, message_text, is_broadcast)
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Ошибка при добавлении сообщения админа: {e}")
            return -1

    def add_feedback(self, user_id: int, provider_id: int, message_text: str) -> int:
        """Добавляет сообщение обратной связи"""
        try:
            self.cursor.execute(
                "INSERT INTO feedback_messages (user_id, provider_id, message_text) VALUES (?, ?, ?, ?)",
                (user_id, provider_id, message_text)
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Ошибка при добавлении обратной связи: {e}")
            return -1

    def get_unprocessed_feedback(self) -> List[dict]:
        """Возвращает необработанные сообщения обратной связи"""
        return self._rows_to_dicts(
            self.cursor.execute(
                "SELECT * FROM feedback_messages WHERE is_processed = FALSE ORDER BY sent_at"
            ).fetchall()
        )

    def mark_feedback_processed(self, feedback_id: int) -> bool:
        """Помечает сообщение обратной связи как обработанное"""
        self.cursor.execute(
            "UPDATE feedback_messages SET is_processed = TRUE WHERE id = ?",
            (feedback_id,)
        )
        self.conn.commit()
        return self.cursor.rowcount > 0

    # ===== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ =====
    def _row_to_dict(self, row: tuple) -> Optional[dict]:
        """Конвертирует строку результата в словарь"""
        if not row:
            return None
        return dict(zip([col[0] for col in self.cursor.description], row))

    def _rows_to_dicts(self, rows: List[tuple]) -> List[dict]:
        """Конвертирует список строк в список словарей"""
        if not rows:
            return []
        columns = [col[0] for col in self.cursor.description]
        return [dict(zip(columns, row)) for row in rows]
    
    def _is_db_initialized(self):
        """Проверяет, есть ли уже данные в БД"""
        return self.cursor.execute("SELECT 1 FROM holidays LIMIT 1").fetchone() is not None
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Автоматическая очистка при выходе из контекста"""
        self.cleanup()

# Создаем глобальный экземпляр базы данных
db = Database()

# Безопасная инициализация CONFIG с обработкой ошибок
try:
    from config import BotConfig
    CONFIG = BotConfig(db)
except ImportError as e:
    logging.error(f"Ошибка импорта BotConfig: {e}")
    CONFIG = None
except Exception as e:
    logging.error(f"Ошибка создания CONFIG: {e}")
    CONFIG = None