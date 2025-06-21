# Используем официальный образ Python с поддержкой графики
FROM python:3.9-slim

# Устанавливаем системные зависимости
RUN apt-get update && \
    apt-get install -y \
    libtk8.6 \
    python3-tk \
    libpng-dev \
    && rm -rf /var/lib/apt/lists/*

# Рабочая директория
WORKDIR /app

# Копируем зависимости
COPY requirements.txt .

# Устанавливаем Python-зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем проект
COPY . .

# Том для данных
VOLUME /app/data

# Команда запуска
CMD ["python", "main.py"]
