FROM python:3.12-alpine

WORKDIR /app

# Только runtime зависимости
RUN apk add --no-cache postgresql-libs

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/data/configs /app/data/logs /app/data/reports /app/data/db /app/data/backups

# Создаем непривилегированного пользователя
RUN adduser -D -u 1000 appuser && \
    chown -R appuser:appuser /app
USER appuser

CMD ["python", "main.py"]