FROM python:3.9-slim

WORKDIR /app

# УСТАНАВЛИВАЕМ tkinter вместо блокировки
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3-tk \
        tk-dev \
        && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/data/configs /app/data/logs /app/data/reports /app/data/db

CMD ["python", "main.py"]