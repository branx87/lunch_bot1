FROM python:3.12-slim-bookworm

WORKDIR /app

# Устанавливаем только необходимые системные зависимости
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libpq-dev \
        gcc \
        && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Копируем зависимости сначала для кэширования
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Удаляем ненужные компиляторы после установки зависимостей
RUN apt-get purge -y gcc && apt-get autoremove -y

# Копируем весь код в контейнер
COPY . .

# Создаем необходимые директории
RUN mkdir -p /app/data/configs /app/data/logs /app/data/reports /app/data/db /app/data/backups

# Создаем непривилегированного пользователя
RUN useradd -m -u 1000 -s /bin/bash appuser && \
    chown -R appuser:appuser /app
USER appuser

# Запускаем бота
CMD ["python", "main.py"]