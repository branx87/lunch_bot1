FROM python:3.9

# Установка root-прав
USER root

# Установка утилит и библиотеки tk для matplotlib
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        netcat-openbsd \
        iputils-ping \
        dnsutils \
        net-tools \
        ca-certificates \
        tk-dev \
        tcl-dev \
        libtk8.6 \
        libtcl8.6 \
    && rm -rf /var/lib/apt/lists/*

# Настройка времени
RUN ln -fs /usr/share/zoneinfo/Europe/Moscow /etc/localtime

# Создаем пользователя
RUN useradd -m botuser && mkdir /app && chown -R botuser:botuser /app
WORKDIR /app

# Установка зависимостей
COPY --chown=botuser:botuser requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Копирование кода
COPY --chown=botuser:botuser . .

# Переключаемся на botuser
USER botuser

CMD ["python", "main.py"]