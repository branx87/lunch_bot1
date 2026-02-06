FROM python:3.12-slim

# Устанавливаем timezone
ENV TZ=Europe/Moscow

WORKDIR /app

# Используем UID/GID пользователя conteiner с хоста (599:599)
ARG USER_ID=599
ARG GROUP_ID=599

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    tzdata \
    postgresql-client \
    gcc \
    libpq-dev \
    && ln -sf /usr/share/zoneinfo/Europe/Moscow /etc/localtime \
    && echo "Europe/Moscow" > /etc/timezone \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Создаем пользователя и группу (используем UID/GID как на хосте - 599:599)
RUN groupadd -g ${GROUP_ID} conteiner && \
    useradd -u ${USER_ID} -g ${GROUP_ID} -m -s /bin/bash conteiner

# Установка Python зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Копирование проекта
COPY . /app/

# Создаем директории для данных внутри контейнера
RUN mkdir -p /app/data/{configs,logs,reports,db,backups}

# Меняем владельца файлов
RUN chown -R conteiner:conteiner /app

# Переменные окружения
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV TZ=Europe/Moscow

# Переключаемся на пользователя conteiner
USER conteiner

# Запуск бота
CMD ["python", "main.py"]