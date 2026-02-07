# Настройка автоматического резервного копирования

Система автоматического резервного копирования PostgreSQL базы данных с загрузкой на Яндекс.Диск и ротацией по дням недели.

## Возможности

- ✅ Автоматическое создание бекапов каждую ночь в 03:00
- ✅ Ротация бекапов по дням недели (7 файлов на Яндекс.Диске)
- ✅ Бекап данных только за последние 3 месяца (экономия места)
- ✅ Автоматическая загрузка на Яндекс.Диск
- ✅ Сжатие бекапов с помощью gzip
- ✅ Ручное создание бекапов через команду `/backup`
- ✅ Автоматическая очистка старых локальных бекапов (старше 7 дней)

## Схема работы ротации бекапов

На Яндекс.Диске хранятся бекапы за последние 7 дней:

```
/lunch_bot_backups/
  ├── lunch_bot_backup_monday.sql.gz    (перезаписывается каждый понедельник)
  ├── lunch_bot_backup_tuesday.sql.gz   (перезаписывается каждый вторник)
  ├── lunch_bot_backup_wednesday.sql.gz (перезаписывается каждую среду)
  ├── lunch_bot_backup_thursday.sql.gz  (перезаписывается каждый четверг)
  ├── lunch_bot_backup_friday.sql.gz    (перезаписывается каждую пятницу)
  ├── lunch_bot_backup_saturday.sql.gz  (перезаписывается каждую субботу)
  └── lunch_bot_backup_sunday.sql.gz    (перезаписывается каждое воскресенье)
```

Локально хранятся все бекапы с полными временными метками, старые удаляются автоматически.

## Установка и настройка

### 1. Установка зависимостей

Система бекапов уже добавлена в `requirements.txt`:

```bash
pip install -r requirements.txt
```

Основные зависимости:
- `yadisk>=3.0.0` - для работы с Яндекс.Диском
- `psycopg2-binary` - для работы с PostgreSQL (уже установлен)

### 2. Получение токена Яндекс.Диска

1. Перейдите на страницу: https://yandex.ru/dev/disk/poligon/
2. Войдите в свой аккаунт Яндекс
3. Нажмите кнопку "Получить OAuth-токен"
4. Разрешите доступ приложению
5. Скопируйте полученный токен

**Важно:** Токен предоставляет полный доступ к вашему Яндекс.Диску. Храните его в безопасности!

### 3. Настройка переменных окружения

Добавьте в файл `data/configs/.env` следующие параметры:

```env
# Настройки резервного копирования
# Токен Яндекс.Диска (получить на https://yandex.ru/dev/disk/poligon/)
YANDEX_DISK_TOKEN=y0_AgAAAA...ваш_токен...
# Папка на Яндекс.Диске для хранения бекапов
YANDEX_DISK_FOLDER=/lunch_bot_backups
# Количество месяцев для бекапа (данные за последние N месяцев)
BACKUP_MONTHS=3
```

**Параметры:**

- `YANDEX_DISK_TOKEN` - OAuth токен для доступа к Яндекс.Диску (обязательный для загрузки в облако)
- `YANDEX_DISK_FOLDER` - путь к папке на Яндекс.Диске (по умолчанию `/lunch_bot_backups`)
- `BACKUP_MONTHS` - количество месяцев данных для бекапа (по умолчанию `3`)

### 4. Проверка настроек PostgreSQL

Убедитесь, что в `.env` правильно настроен `DATABASE_URL`:

```env
DATABASE_URL=postgresql://bot_user:password@postgres:5434/lunch_bot
```

### 5. Проверка доступности pg_dump

В Docker-контейнере должны быть доступны утилиты PostgreSQL:

```bash
# Проверка наличия pg_dump
docker exec lunch_bot which pg_dump
```

Если утилиты отсутствуют, добавьте в `Dockerfile`:

```dockerfile
RUN apt-get update && apt-get install -y postgresql-client
```

### 6. Перезапуск бота

После настройки перезапустите бота:

```bash
docker-compose down
docker-compose up -d --build
```

## Использование

### Автоматические бекапы

Бекапы создаются автоматически каждую ночь в **03:00** по московскому времени.

Логи можно посмотреть командой:

```bash
docker logs lunch_bot | grep -i backup
```

### Ручное создание бекапа

Администраторы могут создавать бекапы вручную через Telegram:

#### Команда `/backup`

Создает полный бекап и загружает его на Яндекс.Диск:

```
/backup
```

Создает только локальный бекап (без загрузки в облако):

```
/backup --local
```

#### Команда `/backup_status`

Показывает статус всех бекапов:

```
/backup_status
```

Выводит информацию о:
- Статусе подключения к Яндекс.Диску
- Локальных бекапах (последние 5)
- Бекапах на Яндекс.Диске

## Что включается в бекап

### Полностью (все данные):
- `users` - все пользователи и сотрудники
- `menu` - меню на неделю
- `holidays` - праздники
- `bot_settings` - настройки бота
- `bitrix_mapping` - связи с Bitrix24

### За последние 3 месяца:
- `orders` - заказы обедов
- `admin_messages` - сообщения администраторов
- `feedback_messages` - обратная связь

Это позволяет значительно уменьшить размер бекапа, сохраняя актуальные данные.

## Восстановление из бекапа

### Восстановление локального бекапа

1. Скопируйте бекап в контейнер:

```bash
docker cp data/backups/lunch_bot_backup_monday_2026-01-15_03-00-00.sql.gz lunch_bot_postgres:/tmp/
```

2. Распакуйте и восстановите:

```bash
docker exec -i lunch_bot_postgres bash << 'EOF'
gunzip /tmp/lunch_bot_backup_monday_2026-01-15_03-00-00.sql.gz
psql -U bot_user -d lunch_bot < /tmp/lunch_bot_backup_monday_2026-01-15_03-00-00.sql
rm /tmp/lunch_bot_backup_monday_2026-01-15_03-00-00.sql
EOF
```

### Восстановление из Яндекс.Диска

1. Скачайте бекап с Яндекс.Диска (через веб-интерфейс или YandexDisk API)
2. Следуйте инструкциям выше для восстановления локального бекапа

### Быстрое восстановление через Python

```python
import yadisk

# Подключение к Яндекс.Диску
client = yadisk.YaDisk(token='ваш_токен')

# Скачивание бекапа
client.download(
    '/lunch_bot_backups/lunch_bot_backup_friday.sql.gz',
    'backup.sql.gz'
)
```

## Мониторинг и логи

### Просмотр логов бекапов

```bash
# Все логи связанные с бекапами
docker logs lunch_bot 2>&1 | grep -i backup

# Только успешные бекапы
docker logs lunch_bot 2>&1 | grep "✅.*бекап"

# Только ошибки бекапов
docker logs lunch_bot 2>&1 | grep "❌.*бекап"
```

### Проверка размера бекапов

```bash
# Локальные бекапы
du -sh data/backups/

# Отдельные файлы
ls -lh data/backups/
```

### Проверка статуса через Telegram

Отправьте боту команду `/backup_status` для получения полной информации о бекапах.

## Устранение проблем

### Проблема: "pg_dump: command not found"

**Решение:** Установите PostgreSQL клиент в контейнер бота.

Добавьте в `Dockerfile`:

```dockerfile
RUN apt-get update && apt-get install -y postgresql-client && rm -rf /var/lib/apt/lists/*
```

Пересоберите контейнер:

```bash
docker-compose down
docker-compose build
docker-compose up -d
```

### Проблема: "Яндекс.Диск не настроен"

**Решение:** Проверьте токен в `.env`:

```bash
# Проверка наличия токена
grep YANDEX_DISK_TOKEN data/configs/.env

# Проверка валидности токена (через Python)
python3 << 'EOF'
import yadisk
import os
from dotenv import load_dotenv

load_dotenv('data/configs/.env')
token = os.getenv('YANDEX_DISK_TOKEN')

if token:
    client = yadisk.YaDisk(token=token)
    if client.check_token():
        print("✅ Токен валиден")
    else:
        print("❌ Токен невалиден")
else:
    print("❌ Токен не найден")
EOF
```

### Проблема: "Превышено время ожидания pg_dump"

**Решение:** Увеличьте таймаут в `backup_manager.py` или уменьшите `BACKUP_MONTHS` в `.env`.

### Проблема: Недостаточно места на диске

**Решение:**

1. Уменьшите `BACKUP_MONTHS` (например, до 2 или 1 месяца)
2. Очистите старые локальные бекапы вручную:

```bash
# Удалить бекапы старше 3 дней
find data/backups/ -name "*.sql.gz" -mtime +3 -delete
```

### Проблема: Ошибки доступа к PostgreSQL

**Решение:** Проверьте, что контейнер `lunch_bot` имеет доступ к `postgres`:

```bash
# Проверка подключения
docker exec lunch_bot psql -h postgres -U bot_user -d lunch_bot -c "SELECT 1"
```

## Безопасность

1. **Токен Яндекс.Диска** - храните в секрете, не коммитьте в Git
2. **Файл .env** - убедитесь что он в `.gitignore`
3. **Бекапы содержат персональные данные** - ограничьте доступ к папке `data/backups/`
4. **Права доступа**:
   ```bash
   chmod 600 data/configs/.env
   chmod 700 data/backups/
   ```

## Дополнительные возможности

### Изменение времени бекапа

Отредактируйте в [cron_jobs.py:109](cron_jobs.py#L109):

```python
# Было: каждый день в 03:00
'0 3 * * *'

# Можно изменить, например, на 02:30
'30 2 * * *'
```

### Изменение периода хранения данных

Отредактируйте в `.env`:

```env
# Бекап за последний месяц
BACKUP_MONTHS=1

# Бекап за последние полгода
BACKUP_MONTHS=6
```

### Отключение загрузки на Яндекс.Диск

Оставьте `YANDEX_DISK_TOKEN` пустым в `.env` - бекапы будут создаваться только локально.

## Тестирование

### Тест создания бекапа

```bash
# Войдите в контейнер
docker exec -it lunch_bot bash

# Запустите Python
python3

# Выполните тест
>>> from backup_manager import backup_manager
>>> import asyncio
>>> asyncio.run(backup_manager.create_backup(upload_to_cloud=False))
```

### Тест подключения к Яндекс.Диску

```bash
docker exec -it lunch_bot python3 << 'EOF'
from backup_manager import backup_manager

if backup_manager.yadisk_client:
    print("✅ Яндекс.Диск подключен")
    status = backup_manager.get_backup_status()
    print(f"Бекапов на облаке: {len(status['cloud_backups'])}")
else:
    print("❌ Яндекс.Диск не подключен")
EOF
```

## Поддержка

Если возникли проблемы:

1. Проверьте логи: `docker logs lunch_bot`
2. Используйте команду `/backup_status` для диагностики
3. Убедитесь что все переменные в `.env` настроены правильно
4. Проверьте доступ к PostgreSQL и Яндекс.Диску

## Обновления системы

При обновлении кода не забудьте:

```bash
# Остановить контейнеры
docker-compose down

# Обновить зависимости
pip install -r requirements.txt

# Пересобрать и запустить
docker-compose up -d --build
```

## Архитектура

```
┌─────────────────┐
│   CronManager   │
│  (cron_jobs.py) │
└────────┬────────┘
         │ каждую ночь в 03:00
         ▼
┌─────────────────┐
│ BackupManager   │
│(backup_manager) │
└────────┬────────┘
         │
         ├──► 1. Создание pg_dump (только данные за 3 месяца)
         │
         ├──► 2. Сжатие gzip
         │
         ├──► 3. Сохранение локально с timestamp
         │
         ├──► 4. Загрузка на Яндекс.Диск (с именем дня недели)
         │
         └──► 5. Очистка старых локальных бекапов
```

Система полностью автоматизирована и не требует ручного вмешательства.
