"""
Модуль для автоматического резервного копирования PostgreSQL базы данных
с загрузкой на Яндекс.Диск и ротацией бекапов по дням недели.
"""
import os
import subprocess
import logging
from contextlib import contextmanager
from pathlib import Path
from datetime import datetime, timedelta, date
import tempfile
import asyncio
from typing import Optional
import yadisk
from database import db
from models import Order, User, Holiday, Menu, AdminMessage, FeedbackMessage, BotSetting
from sqlalchemy import and_

logger = logging.getLogger(__name__)

# Переменные, которыми задаётся прокси для всего процесса.
PROXY_ENV_VARS = ('HTTPS_PROXY', 'HTTP_PROXY', 'https_proxy', 'http_proxy',
                   'ALL_PROXY', 'all_proxy')


@contextmanager
def without_proxy():
    """Временно убирает прокси из окружения.

    Яндекс.Диск — российский сервис, прокси ему не нужен. Но requests внутри
    yadisk читает переменные окружения, поэтому при мёртвом прокси бэкап
    просто не загружался (29.10 — «Ошибка подключения к Яндекс.Диску»).
    """
    saved = {key: os.environ.pop(key, None) for key in PROXY_ENV_VARS}
    try:
        yield
    finally:
        for key, value in saved.items():
            if value is not None:
                os.environ[key] = value


class BackupManager:
    """Управление резервным копированием базы данных"""

    # Дни недели для ротации
    WEEKDAYS = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']

    def __init__(self):
        """Инициализация менеджера бекапов"""
        self.yandex_token = os.getenv('YANDEX_DISK_TOKEN')
        self.yandex_folder = os.getenv('YANDEX_DISK_FOLDER', '/lunch_bot_backups')
        self.db_url = os.getenv('DATABASE_URL')
        self.backup_months = int(os.getenv('BACKUP_MONTHS', '3'))
        self.local_backup_dir = Path('data/backups')
        self.local_backup_dir.mkdir(parents=True, exist_ok=True)

        # Парсим DATABASE_URL для pg_dump
        self._parse_db_credentials()

        # Инициализация Яндекс.Диск клиента (без прокси — сервис российский)
        self.yadisk_client = None
        if self.yandex_token:
            try:
                with without_proxy():
                    self.yadisk_client = yadisk.YaDisk(token=self.yandex_token)
                    if self.yadisk_client.check_token():
                        logger.info("✅ Подключение к Яндекс.Диску успешно")
                    else:
                        logger.warning("⚠️ Невалидный токен Яндекс.Диска")
                        self.yadisk_client = None
            except Exception as e:
                logger.error(f"❌ Ошибка подключения к Яндекс.Диску: {e}")
                self.yadisk_client = None
        else:
            logger.warning("⚠️ Токен Яндекс.Диска не указан в .env")

    def _parse_db_credentials(self):
        """Парсит DATABASE_URL для получения параметров подключения"""
        # Формат: postgresql://user:password@host:port/database
        if not self.db_url:
            raise ValueError("DATABASE_URL не указан в переменных окружения")

        try:
            # Убираем префикс postgresql://
            url = self.db_url.replace('postgresql://', '')

            # Разделяем на credentials и host/db
            credentials, host_db = url.split('@')
            user, password = credentials.split(':')

            # Разделяем host:port/database
            host_port, database = host_db.split('/')
            host, port = host_port.split(':') if ':' in host_port else (host_port, '5434')

            self.db_host = host
            self.db_port = port
            self.db_name = database
            self.db_user = user
            self.db_password = password

            logger.info(f"📊 База данных: {self.db_name}@{self.db_host}:{self.db_port}")
        except Exception as e:
            logger.error(f"❌ Ошибка парсинга DATABASE_URL: {e}")
            raise

    def _get_weekday_name(self, date: datetime = None) -> str:
        """Возвращает название дня недели на английском"""
        if date is None:
            date = datetime.now()
        return self.WEEKDAYS[date.weekday()]

    def _create_temp_dump(self) -> Optional[Path]:
        """Создает временный дамп базы данных с фильтрацией по датам"""
        try:
            # Создаем временный файл для дампа
            temp_file = tempfile.NamedTemporaryFile(
                mode='w+b',
                suffix='.sql',
                delete=False,
                dir=self.local_backup_dir
            )
            temp_path = Path(temp_file.name)
            temp_file.close()

            logger.info("📦 Создание дампа базы данных...")

            # Дата для фильтрации (последние N месяцев)
            cutoff_date = (datetime.now() - timedelta(days=self.backup_months * 30)).date()
            logger.info(f"📅 Бекап данных с {cutoff_date.strftime('%Y-%m-%d')}")

            # Формируем переменные окружения для pg_dump
            env = os.environ.copy()
            env['PGPASSWORD'] = self.db_password

            # 1. Создаем дамп структуры (схемы)
            schema_dump_cmd = [
                'pg_dump',
                '-h', self.db_host,
                '-p', self.db_port,
                '-U', self.db_user,
                '-d', self.db_name,
                '--schema-only',  # Только структура
                '-f', str(temp_path)
            ]

            logger.info(f"🔧 Экспорт структуры БД...")
            result = subprocess.run(
                schema_dump_cmd,
                env=env,
                capture_output=True,
                text=True,
                timeout=300
            )

            if result.returncode != 0:
                logger.error(f"❌ Ошибка pg_dump (схема): {result.stderr}")
                temp_path.unlink()
                return None

            # 2. Экспортируем данные с фильтрацией
            with temp_path.open('a', encoding='utf-8') as f:
                f.write("\n-- ========================================\n")
                f.write("-- Данные таблиц (последние 3 месяца)\n")
                f.write("-- ========================================\n\n")

            # Экспортируем все данные из справочных таблиц (без фильтрации по дате)
            reference_tables = ['users', 'menu', 'holidays', 'bot_settings', 'bitrix_mapping']

            for table in reference_tables:
                data_dump_cmd = [
                    'pg_dump',
                    '-h', self.db_host,
                    '-p', self.db_port,
                    '-U', self.db_user,
                    '-d', self.db_name,
                    '--data-only',  # Только данные
                    '-t', table,
                    '--column-inserts'  # Для читаемости
                ]

                logger.info(f"📋 Экспорт данных таблицы {table}...")
                result = subprocess.run(
                    data_dump_cmd,
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=300
                )

                if result.returncode == 0:
                    with temp_path.open('a', encoding='utf-8') as f:
                        f.write(f"\n-- Таблица: {table}\n")
                        f.write(result.stdout)
                else:
                    logger.warning(f"⚠️ Ошибка экспорта {table}: {result.stderr}")

            # 3. Экспортируем orders с фильтрацией через SQLAlchemy
            logger.info(f"📋 Экспорт заказов за последние {self.backup_months} месяцев...")

            from sqlalchemy import text

            with temp_path.open('a', encoding='utf-8') as f:
                f.write(f"\n-- Таблица: orders (за последние {self.backup_months} месяцев)\n")
                f.write(f"-- Данные с {cutoff_date.strftime('%Y-%m-%d')}\n\n")
                
                # Выгружаем через базу данных
                query = text("""
                    SELECT * FROM orders 
                    WHERE target_date >= :cutoff_date 
                    ORDER BY created_at
                """)
                
                db_result = db.session.execute(query, {"cutoff_date": cutoff_date})
                orders = db_result.fetchall()
                columns = list(db_result.keys())
                
                logger.info(f"📦 Найдено {len(orders)} заказов для экспорта")
                
                for order in orders:
                    # Правильное форматирование значений
                    formatted_values = []
                    for v in order:
                        if v is None:
                            formatted_values.append('NULL')
                        elif isinstance(v, bool):
                            formatted_values.append(str(v).upper())  # TRUE/FALSE
                        elif isinstance(v, (int, float)):
                            formatted_values.append(str(v))
                        elif isinstance(v, (datetime, date)):
                            formatted_values.append(f"'{v}'")
                        else:
                            # Экранируем одинарные кавычки (вынесли за f-string)
                            escaped_value = str(v).replace("'", "''")
                            formatted_values.append(f"'{escaped_value}'")
                    
                    insert_sql = f"INSERT INTO public.orders ({', '.join(columns)}) VALUES ({', '.join(formatted_values)});\n"
                    f.write(insert_sql)

            # 4. Экспортируем сообщения за последние 3 месяца
            message_tables = ['admin_messages', 'feedback_messages']
            cutoff_timestamp = datetime.now() - timedelta(days=self.backup_months * 30)

            for table in message_tables:
                data_dump_cmd = [
                    'pg_dump',
                    '-h', self.db_host,
                    '-p', self.db_port,
                    '-U', self.db_user,
                    '-d', self.db_name,
                    '--data-only',
                    '-t', table,
                    '--column-inserts'
                ]

                logger.info(f"📋 Экспорт данных таблицы {table}...")
                msg_result = subprocess.run(  # Переименовал в msg_result
                    data_dump_cmd,
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=300
                )

                if msg_result.returncode == 0:
                    with temp_path.open('a', encoding='utf-8') as f:
                        f.write(f"\n-- Таблица: {table} (последние {self.backup_months} месяцев)\n")
                        f.write(msg_result.stdout)

            # Проверяем размер созданного дампа
            dump_size = temp_path.stat().st_size
            logger.info(f"✅ Дамп создан: {temp_path} ({dump_size / 1024 / 1024:.2f} МБ)")

            return temp_path

        except subprocess.TimeoutExpired:
            logger.error("❌ Превышено время ожидания pg_dump")
            if temp_path.exists():
                temp_path.unlink()
            return None
        except Exception as e:
            logger.error(f"❌ Ошибка создания дампа: {e}", exc_info=True)
            if temp_path.exists():
                temp_path.unlink()
            return None

    def _compress_dump(self, dump_path: Path) -> Optional[Path]:
        """Сжимает дамп с помощью gzip"""
        try:
            import gzip
            import shutil

            compressed_path = dump_path.with_suffix('.sql.gz')

            logger.info(f"📦 Сжатие дампа...")
            with open(dump_path, 'rb') as f_in:
                with gzip.open(compressed_path, 'wb', compresslevel=9) as f_out:
                    shutil.copyfileobj(f_in, f_out)

            # Удаляем несжатый дамп
            dump_path.unlink()

            compressed_size = compressed_path.stat().st_size
            logger.info(f"✅ Дамп сжат: {compressed_path} ({compressed_size / 1024 / 1024:.2f} МБ)")

            return compressed_path

        except Exception as e:
            logger.error(f"❌ Ошибка сжатия дампа: {e}")
            return None

    def _upload_to_yandex_disk(self, file_path: Path, remote_name: str) -> bool:
        """Загружает файл на Яндекс.Диск"""
        if not self.yadisk_client:
            logger.warning("⚠️ Яндекс.Диск не настроен, пропускаем загрузку")
            return False

        try:
            # Загрузка идёт напрямую: Яндекс.Диск российский, прокси не нужен
            with without_proxy():
                # Создаем папку если её нет
                if not self.yadisk_client.exists(self.yandex_folder):
                    self.yadisk_client.mkdir(self.yandex_folder)
                    logger.info(f"📁 Создана папка на Яндекс.Диске: {self.yandex_folder}")

                remote_path = f"{self.yandex_folder}/{remote_name}"

                # Удаляем старый файл если существует
                if self.yadisk_client.exists(remote_path):
                    self.yadisk_client.remove(remote_path)
                    logger.info(f"🗑️ Удален старый бекап: {remote_name}")

                # Загружаем новый файл
                logger.info(f"☁️ Загрузка на Яндекс.Диск: {remote_name}...")
                self.yadisk_client.upload(str(file_path), remote_path)

            logger.info(f"✅ Файл загружен на Яндекс.Диск: {remote_path}")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка загрузки на Яндекс.Диск: {e}")
            return False

    def _cleanup_old_local_backups(self, keep_days: int = 7):
        """Удаляет локальные бекапы старше N дней"""
        try:
            cutoff_date = datetime.now() - timedelta(days=keep_days)

            for backup_file in self.local_backup_dir.glob('*.sql.gz'):
                # Проверяем возраст файла
                file_mtime = datetime.fromtimestamp(backup_file.stat().st_mtime)
                if file_mtime < cutoff_date:
                    backup_file.unlink()
                    logger.info(f"🗑️ Удален старый локальный бекап: {backup_file.name}")

        except Exception as e:
            logger.error(f"❌ Ошибка очистки старых бекапов: {e}")

    async def create_backup(self, upload_to_cloud: bool = True) -> Optional[Path]:
        """
        Создает резервную копию базы данных

        Args:
            upload_to_cloud: Загружать ли бекап на Яндекс.Диск

        Returns:
            Path к созданному бекапу или None при ошибке
        """
        try:
            logger.info("=" * 60)
            logger.info("🔄 Начало создания резервной копии базы данных")
            logger.info("=" * 60)

            # Получаем название дня недели для ротации
            weekday = self._get_weekday_name()
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

            # 1. Создаем дамп
            dump_path = await asyncio.to_thread(self._create_temp_dump)
            if not dump_path:
                logger.error("❌ Не удалось создать дамп базы данных")
                return None

            # 2. Сжимаем дамп
            compressed_path = await asyncio.to_thread(self._compress_dump, dump_path)
            if not compressed_path:
                logger.error("❌ Не удалось сжать дамп")
                return None

            # 3. Переименовываем с учетом дня недели для ротации
            final_name = f"lunch_bot_backup_{weekday}_{timestamp}.sql.gz"
            final_path = self.local_backup_dir / final_name
            compressed_path.rename(final_path)

            logger.info(f"✅ Локальный бекап создан: {final_path}")

            # 4. Загружаем на Яндекс.Диск
            if upload_to_cloud:
                # Имя на Яндекс.Диске содержит только день недели для ротации
                cloud_name = f"lunch_bot_backup_{weekday}.sql.gz"
                upload_success = await asyncio.to_thread(
                    self._upload_to_yandex_disk,
                    final_path,
                    cloud_name
                )

                if upload_success:
                    logger.info(f"✅ Бекап загружен на Яндекс.Диск: {cloud_name}")
                else:
                    logger.warning("⚠️ Не удалось загрузить на Яндекс.Диск")

            # 5. Очищаем старые локальные бекапы
            await asyncio.to_thread(self._cleanup_old_local_backups)

            logger.info("=" * 60)
            logger.info("✅ Резервное копирование успешно завершено")
            logger.info("=" * 60)

            return final_path

        except Exception as e:
            logger.error(f"❌ Критическая ошибка при создании бекапа: {e}", exc_info=True)
            return None

    def list_available_backups(self) -> dict:
        """Возвращает список доступных бекапов для восстановления"""
        backups = {
            'local': [],
            'cloud': []
        }

        # Локальные бекапы
        for backup_file in sorted(
            self.local_backup_dir.glob('*.sql.gz'),
            key=lambda x: x.stat().st_mtime,
            reverse=True
        ):
            backups['local'].append({
                'name': backup_file.name,
                'path': str(backup_file),
                'size_mb': backup_file.stat().st_size / 1024 / 1024,
                'created': datetime.fromtimestamp(backup_file.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')
            })

        # Бекапы на Яндекс.Диске
        if self.yadisk_client:
            try:
                if self.yadisk_client.exists(self.yandex_folder):
                    for item in self.yadisk_client.listdir(self.yandex_folder):
                        if item.name.endswith('.sql.gz'):
                            backups['cloud'].append({
                                'name': item.name,
                                'path': f"{self.yandex_folder}/{item.name}",
                                'size_mb': item.size / 1024 / 1024,
                                'created': item.created.strftime('%Y-%m-%d %H:%M:%S') if item.created else 'N/A'
                            })
            except Exception as e:
                logger.error(f"❌ Ошибка получения списка бекапов с Яндекс.Диска: {e}")

        return backups

    async def download_from_cloud(self, remote_name: str) -> Optional[Path]:
        """Скачивает бекап с Яндекс.Диска"""
        if not self.yadisk_client:
            logger.error("❌ Яндекс.Диск не настроен")
            return None

        try:
            remote_path = f"{self.yandex_folder}/{remote_name}"
            local_path = self.local_backup_dir / remote_name

            logger.info(f"☁️ Скачивание с Яндекс.Диска: {remote_name}...")
            with without_proxy():
                await asyncio.to_thread(
                    self.yadisk_client.download,
                    remote_path,
                    str(local_path)
                )

            logger.info(f"✅ Файл скачан: {local_path}")
            return local_path

        except Exception as e:
            logger.error(f"❌ Ошибка скачивания с Яндекс.Диска: {e}")
            return None

    async def restore_backup(self, backup_path: Path, confirm: bool = False) -> dict:
        """
        Восстанавливает базу данных из бекапа

        ВНИМАНИЕ: Это опасная операция! Все текущие данные будут перезаписаны!

        Args:
            backup_path: Путь к файлу бекапа (.sql.gz)
            confirm: Подтверждение операции

        Returns:
            dict с результатом операции
        """
        result = {
            'success': False,
            'message': '',
            'details': {}
        }

        if not confirm:
            result['message'] = '⚠️ Требуется подтверждение! Операция перезапишет ВСЕ текущие данные!'
            return result

        if not backup_path.exists():
            result['message'] = f'❌ Файл бекапа не найден: {backup_path}'
            return result

        try:
            logger.info("=" * 60)
            logger.info("🔄 НАЧАЛО ВОССТАНОВЛЕНИЯ БАЗЫ ДАННЫХ")
            logger.info(f"📦 Файл: {backup_path}")
            logger.info("=" * 60)

            import gzip

            # 1. Распаковываем бекап
            logger.info("📦 Распаковка бекапа...")
            uncompressed_path = backup_path.with_suffix('')  # убираем .gz

            with gzip.open(backup_path, 'rb') as f_in:
                with open(uncompressed_path, 'wb') as f_out:
                    f_out.write(f_in.read())

            logger.info(f"✅ Распаковано: {uncompressed_path}")

            # 2. Формируем переменные окружения для psql
            env = os.environ.copy()
            env['PGPASSWORD'] = self.db_password

            # 3. Закрываем все соединения SQLAlchemy перед восстановлением
            logger.info("🔌 Закрытие соединений SQLAlchemy...")
            try:
                db.session.close()
                db.engine.dispose()
                logger.info("✅ Соединения SQLAlchemy закрыты")
            except Exception as e:
                logger.warning(f"⚠️ Ошибка закрытия соединений SQLAlchemy: {e}")

            # 4. Принудительно завершаем все другие соединения к БД
            logger.info("🔌 Завершение других соединений к БД...")

            terminate_cmd = [
                'psql',
                '-h', self.db_host,
                '-p', self.db_port,
                '-U', self.db_user,
                '-d', 'postgres',  # Подключаемся к системной БД
                '-c', f'''
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = '{self.db_name}'
                AND pid <> pg_backend_pid();
                '''
            ]

            terminate_result = await asyncio.to_thread(
                subprocess.run,
                terminate_cmd,
                env=env,
                capture_output=True,
                text=True,
                timeout=30
            )

            if terminate_result.returncode == 0:
                logger.info("✅ Другие соединения завершены")
            else:
                logger.warning(f"⚠️ Предупреждение при завершении соединений: {terminate_result.stderr}")

            # 5. Очищаем базу и восстанавливаем
            # Сначала удаляем все таблицы
            logger.info("🗑️ Очистка базы данных...")

            drop_cmd = [
                'psql',
                '-h', self.db_host,
                '-p', self.db_port,
                '-U', self.db_user,
                '-d', self.db_name,
                '-c', '''
                DO $$
                DECLARE r RECORD;
                BEGIN
                    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public')
                    LOOP
                        EXECUTE 'DROP TABLE IF EXISTS public.' || quote_ident(r.tablename) || ' CASCADE';
                    END LOOP;
                END $$;
                '''
            ]

            drop_result = await asyncio.to_thread(
                subprocess.run,
                drop_cmd,
                env=env,
                capture_output=True,
                text=True,
                timeout=60
            )

            if drop_result.returncode != 0:
                logger.warning(f"⚠️ Предупреждение при очистке: {drop_result.stderr}")

            # 6. Восстанавливаем из дампа
            logger.info("📥 Восстановление данных из бекапа...")

            restore_cmd = [
                'psql',
                '-h', self.db_host,
                '-p', self.db_port,
                '-U', self.db_user,
                '-d', self.db_name,
                '-f', str(uncompressed_path)
            ]

            restore_result = await asyncio.to_thread(
                subprocess.run,
                restore_cmd,
                env=env,
                capture_output=True,
                text=True,
                timeout=600
            )

            # 7. Удаляем временный файл
            uncompressed_path.unlink()

            if restore_result.returncode != 0:
                # Проверяем на критические ошибки
                if 'ERROR' in restore_result.stderr:
                    result['message'] = f'❌ Ошибка восстановления: {restore_result.stderr}'
                    result['details']['stderr'] = restore_result.stderr
                    logger.error(f"❌ Ошибка восстановления: {restore_result.stderr}")
                    return result

            # 8. Переподключаемся к БД и исправляем последовательности
            logger.info("🔌 Переподключение к базе данных...")
            try:
                db.reconnect()
                logger.info("✅ Переподключение успешно")
            except Exception as e:
                logger.warning(f"⚠️ Ошибка переподключения: {e}")

            logger.info("🔧 Исправление последовательностей...")
            db.fix_sequences()

            result['success'] = True
            result['message'] = '✅ База данных успешно восстановлена!'
            result['details'] = {
                'backup_file': backup_path.name,
                'restored_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            logger.info("=" * 60)
            logger.info("✅ ВОССТАНОВЛЕНИЕ УСПЕШНО ЗАВЕРШЕНО")
            logger.info("=" * 60)

            return result

        except subprocess.TimeoutExpired:
            result['message'] = '❌ Превышено время ожидания восстановления'
            logger.error("❌ Превышено время ожидания восстановления")
            return result
        except Exception as e:
            result['message'] = f'❌ Критическая ошибка: {str(e)}'
            logger.error(f"❌ Критическая ошибка восстановления: {e}", exc_info=True)
            return result

    def get_backup_status(self) -> dict:
        """Возвращает информацию о бекапах"""
        status = {
            'yandex_disk_configured': self.yadisk_client is not None,
            'local_backups': [],
            'cloud_backups': []
        }

        # Локальные бекапы
        for backup_file in sorted(
            self.local_backup_dir.glob('*.sql.gz'),
            key=lambda x: x.stat().st_mtime,
            reverse=True
        ):
            status['local_backups'].append({
                'name': backup_file.name,
                'size_mb': backup_file.stat().st_size / 1024 / 1024,
                'created': datetime.fromtimestamp(backup_file.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')
            })

        # Бекапы на Яндекс.Диске
        if self.yadisk_client:
            try:
                if self.yadisk_client.exists(self.yandex_folder):
                    for item in self.yadisk_client.listdir(self.yandex_folder):
                        if item.name.endswith('.sql.gz'):
                            status['cloud_backups'].append({
                                'name': item.name,
                                'size_mb': item.size / 1024 / 1024,
                                'created': item.created.strftime('%Y-%m-%d %H:%M:%S') if item.created else 'N/A'
                            })
            except Exception as e:
                logger.error(f"❌ Ошибка получения списка бекапов с Яндекс.Диска: {e}")

        return status


# Глобальный экземпляр
backup_manager = BackupManager()
