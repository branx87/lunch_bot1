# ##db.py
import os
import sqlite3
import logging
import pandas as pd
from typing import Optional, Dict, List, Any, Set, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_path: str, configs_dir: str = None):
        """Инициализация соединения с базой данных"""
        self.db_path = db_path
        self.configs_dir = Path(configs_dir) if configs_dir else None
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        self._init_db_structure()
        
        if self.configs_dir:
            self._load_initial_data_if_needed()

    def _init_db_structure(self) -> None:
        """Создание таблиц и индексов"""
        try:
            # Оптимизация SQLite
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self.conn.execute("PRAGMA busy_timeout=5000")
            self.conn.execute("PRAGMA foreign_keys=ON")

            # Определение таблиц
            tables = [
                '''CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER UNIQUE,
                    full_name TEXT NOT NULL,
                    phone TEXT,
                    location TEXT,
                    is_verified BOOLEAN DEFAULT FALSE,
                    is_employee BOOLEAN DEFAULT FALSE,
                    username TEXT,
                    is_deleted BOOLEAN DEFAULT FALSE,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )''',
                
                '''CREATE TABLE IF NOT EXISTS orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    target_date TEXT NOT NULL,
                    order_time TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    quantity INTEGER NOT NULL CHECK(quantity BETWEEN 1 AND 3),
                    is_preliminary BOOLEAN DEFAULT FALSE,
                    is_cancelled BOOLEAN DEFAULT FALSE,
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
                )''',
                
                '''CREATE TABLE IF NOT EXISTS holidays (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    name TEXT NOT NULL,
                    is_recurring BOOLEAN DEFAULT FALSE,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(date, name)
                )''',
                
                '''CREATE TABLE IF NOT EXISTS menu (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    day TEXT NOT NULL UNIQUE,
                    first_course TEXT NOT NULL,
                    main_course TEXT NOT NULL,
                    salad TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )''',
                
                '''CREATE TABLE IF NOT EXISTS admin_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    admin_id INTEGER,
                    user_id INTEGER NOT NULL,
                    message_text TEXT NOT NULL,
                    is_broadcast BOOLEAN DEFAULT FALSE,
                    is_unregistered BOOLEAN DEFAULT FALSE,
                    sent_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(admin_id) REFERENCES users(id),
                    FOREIGN KEY(user_id) REFERENCES users(id)
                )''',
                
                '''CREATE TABLE IF NOT EXISTS feedback_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    provider_id INTEGER NOT NULL,
                    message_text TEXT NOT NULL,
                    sent_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    is_processed BOOLEAN DEFAULT FALSE,
                    FOREIGN KEY(user_id) REFERENCES users(id),
                    FOREIGN KEY(provider_id) REFERENCES users(id)
                )'''
            ]

            # Определение индексов
            indexes = [
                # Для users
                "CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id)",
                "CREATE INDEX IF NOT EXISTS idx_users_verified ON users(is_verified)",
                "CREATE INDEX IF NOT EXISTS idx_users_employee ON users(is_employee)",
                
                # Для orders
                "CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id)",
                "CREATE INDEX IF NOT EXISTS idx_orders_target_date ON orders(target_date)",
                
                # Для holidays
                "CREATE INDEX IF NOT EXISTS idx_holidays_date ON holidays(date)",
                
                # Для menu
                "CREATE INDEX IF NOT EXISTS idx_menu_day ON menu(day)",
                
                # Для admin_messages
                "CREATE INDEX IF NOT EXISTS idx_admin_messages_admin ON admin_messages(admin_id)",
                "CREATE INDEX IF NOT EXISTS idx_admin_messages_user ON admin_messages(user_id)",
                
                # Для feedback_messages
                "CREATE INDEX IF NOT EXISTS idx_feedback_user ON feedback_messages(user_id)",
                "CREATE INDEX IF NOT EXISTS idx_feedback_provider ON feedback_messages(provider_id)",
                "CREATE INDEX IF NOT EXISTS idx_feedback_processed ON feedback_messages(is_processed)"
            ]

            with self.conn:
                # Создаем таблицы
                for sql in tables:
                    self.cursor.execute(sql)
                
                # Создаем индексы
                for sql in indexes:
                    self.cursor.execute(sql)
            
            logger.info("Структура БД успешно инициализирована")
            
        except Exception as e:
            logger.critical(f"Ошибка инициализации БД: {e}")
            raise

    def _load_initial_data_if_needed(self) -> bool:
        """Загрузка начальных данных при необходимости"""
        try:
            if not self._is_data_loaded():
                return self._load_initial_data()
            return False
        except Exception as e:
            logger.error(f"Ошибка загрузки данных: {e}")
            return False

    def _is_data_loaded(self) -> bool:
        """Проверка наличия данных"""
        try:
            has_employees = self.cursor.execute(
                "SELECT 1 FROM users WHERE is_employee = TRUE LIMIT 1"
            ).fetchone()
            
            has_menu = self.cursor.execute(
                "SELECT 1 FROM menu LIMIT 1"
            ).fetchone()
            
            return has_employees is not None and has_menu is not None
        except sqlite3.Error as e:
            logger.error(f"Ошибка проверки данных: {e}")
            return False

    def _load_initial_data(self) -> bool:
        """Загрузка данных из Excel"""
        try:
            config_path = self.configs_dir / 'config.xlsx'
            if not config_path.exists():
                logger.warning(f"Файл {config_path} не найден")
                return False

            logger.info(f"Загрузка данных из {config_path}")
            
            # Чтение Excel файла один раз
            df = pd.read_excel(config_path, header=None)
            
            # Загрузка данных
            employees_added = self._load_employees(df)
            menu_days_added = self._load_menu(df)
            holidays_added = self._load_holidays(df)
            
            logger.info(f"Загружено: {employees_added} сотрудников, "
                       f"{menu_days_added} дней меню, {holidays_added} праздников")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка загрузки Excel: {e}", exc_info=True)
            return False

    def _load_employees(self, df: pd.DataFrame) -> int:
        """Загрузка сотрудников"""
        existing_employees = {
            e['full_name'].lower() 
            for e in self.get_employees(active_only=False)
        }
        
        employees = df.iloc[1:, 6].dropna().unique().tolist()
        new_employees = 0
        
        for emp in employees:
            emp_name = str(emp).strip()
            if emp_name.lower() not in existing_employees:
                self.add_user(full_name=emp_name, is_employee=True)
                new_employees += 1
                
        return new_employees

    def _load_menu(self, df: pd.DataFrame) -> int:
        """Загрузка меню"""
        menu_data = {}
        current_day = None
        idx = 2  # Начинаем с I2 (третья строка в Pandas — индекс 2)

        # Очищаем старое меню
        self.cursor.execute("DELETE FROM menu")
        
        while idx < len(df):
            day_cell = df.iloc[idx, 8]

            if pd.isna(day_cell):
                idx += 1
                continue

            day_str = str(day_cell).strip().lower()
            week_days = ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"]

            if any(day in day_str for day in week_days):
                current_day = day_str.capitalize()
                idx += 1

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
                idx += 1

        # Сохраняем меню в базу данных
        added_days = 0
        for day, dishes in menu_data.items():
            if dishes["first"] and dishes["main"] and dishes["salad"]:
                self.add_menu(day, dishes["first"], dishes["main"], dishes["salad"])
                added_days += 1

        return added_days

    def _load_holidays(self, df: pd.DataFrame) -> int:
        """Загрузка праздников"""
        holidays_df = df.iloc[1:, 10:12].dropna()
        
        existing_holidays = {
            (h['date'], h['name']) 
            for h in self.get_holidays()
        }
        
        new_holidays = 0
        for idx in range(len(holidays_df)):
            row = holidays_df.iloc[idx]
            date = row.iloc[0].strftime('%d.%m.%Y') if hasattr(row.iloc[0], 'strftime') else str(row.iloc[0])
            name = str(row.iloc[1]).strip()
            
            if (date, name) not in existing_holidays:
                self.add_holiday(date, name)
                new_holidays += 1
        
        return new_holidays

    # ===== МЕТОДЫ ДЛЯ РАБОТЫ С ПОЛЬЗОВАТЕЛЯМИ =====
    def add_user(self, full_name: str, **kwargs) -> int:
        """Добавление пользователя"""
        try:
            fields = []
            values = []
            for k, v in kwargs.items():
                if v is not None:
                    fields.append(k)
                    values.append(v.strip() if isinstance(v, str) else v)
            
            sql = f"""
                INSERT INTO users (full_name, {', '.join(fields)})
                VALUES (?, {', '.join(['?']*len(fields))})
                ON CONFLICT(telegram_id) DO UPDATE SET
                    {', '.join(f"{f}=excluded.{f}" for f in fields)},
                    updated_at = CURRENT_TIMESTAMP
            """
            self.cursor.execute(sql, (full_name.strip(), *values))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Ошибка добавления пользователя: {e}")
            return -1

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

    def get_user(self, telegram_id: int) -> Optional[Dict[str, Any]]:
        """Возвращает данные пользователя"""
        try:
            row = self.cursor.execute(
                "SELECT * FROM users WHERE telegram_id = ? AND is_deleted = FALSE",
                (telegram_id,)
            ).fetchone()
            return dict(row) if row else None
        except sqlite3.Error as e:
            logger.error(f"Ошибка при получении пользователя: {e}")
            return None

    def get_employees(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """Возвращает список сотрудников"""
        try:
            query = "SELECT * FROM users WHERE is_employee = TRUE"
            if active_only:
                query += " AND is_deleted = FALSE"
                
            self.cursor.execute(query)
            return [dict(row) for row in self.cursor.fetchall()]
        except sqlite3.Error as e:
            logger.error(f"Ошибка при получении сотрудников: {e}")
            return []

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
        except sqlite3.Error as e:
            logger.error(f"Ошибка при добавлении праздника: {e}")
            return -1

    def get_holidays(self) -> List[Dict[str, Any]]:
        """Возвращает список всех праздников"""
        try:
            self.cursor.execute("SELECT * FROM holidays ORDER BY date")
            return [dict(row) for row in self.cursor.fetchall()]
        except sqlite3.Error as e:
            logger.error(f"Ошибка при получении праздников: {e}")
            return []

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

    def get_full_menu(self) -> List[Dict[str, Any]]:
        """Возвращает всё меню на неделю"""
        try:
            self.cursor.execute(
                "SELECT day, first_course, main_course, salad FROM menu ORDER BY day"
            )
            return [dict(row) for row in self.cursor.fetchall()]
        except sqlite3.Error as e:
            logger.error(f"Ошибка при получении меню: {e}")
            return []

    def get_menu_by_day(self, day: str) -> Optional[Dict[str, Any]]:
        """Возвращает меню на указанный день недели"""
        try:
            row = self.cursor.execute(
                "SELECT * FROM menu WHERE day = ?", 
                (day,)
            ).fetchone()
            return dict(row) if row else None
        except sqlite3.Error as e:
            logger.error(f"Ошибка при получении меню: {e}")
            return None

    def __enter__(self):
        """Поддержка контекстного менеджера"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Закрытие соединения при выходе из контекста"""
        self.close()

    def close(self) -> None:
        """Закрытие соединения"""
        try:
            if self.conn:
                self.conn.close()
        except Exception as e:
            logger.error(f"Ошибка закрытия БД: {e}")