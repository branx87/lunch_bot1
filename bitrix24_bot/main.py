"""
Bitrix24 bot entry point.
Shares the same PostgreSQL database and business logic with Telegram and VK bots.

Runs a FastAPI HTTP server that receives bot messages forwarded by the PHP relay
on the Bitrix24 server. Returns response JSON — PHP sends it to the user
via \Bitrix\Im\Bot::addMessage() (no REST API needed).

Deploy:
  docker-compose up -d bitrix24_bot
  → listens on B24_BOT_PORT (default 7777)

PHP relay on Bitrix server must point to:
  http://<this_host>:<port>/webhook/bot
"""
import asyncio
import logging
import os
import secrets
import sys
from pathlib import Path

# Add project root to path so shared modules (config, database, etc.) are importable
sys.path.insert(0, str(Path(__file__).parent.parent))

from logging.handlers import RotatingFileHandler

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


def _setup_logging():
    logs_dir = Path("data/logs")
    logs_dir.mkdir(parents=True, exist_ok=True)

    handler = RotatingFileHandler(
        logs_dir / "bitrix24_bot.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[handler, logging.StreamHandler()],
    )


_setup_logging()
logger = logging.getLogger(__name__)

app = FastAPI(title="Bitrix24 Lunch Bot", docs_url=None, redoc_url=None)


@app.on_event("startup")
async def on_startup():
    from database import db
    from models import Base

    if db is None:
        logger.error("База данных не инициализирована")
        sys.exit(1)

    Base.metadata.create_all(bind=db.engine)
    logger.info("=== Bitrix24 bot started ===")


@app.get("/health")
async def health():
    return {"status": "ok"}


_webhook_token = os.getenv("B24_WEBHOOK_TOKEN", "")


def _check_token(request: Request) -> bool:
    """Verify X-Webhook-Token header matches B24_WEBHOOK_TOKEN."""
    if not _webhook_token:
        return True  # token not configured — skip check (dev mode)
    received = request.headers.get("X-Webhook-Token", "")
    return secrets.compare_digest(_webhook_token.encode(), received.encode())


@app.post("/webhook/bot")
async def handle_bot_webhook(request: Request):
    """
    Receives bot messages from Bitrix24 (forwarded by PHP relay).
    Returns JSON with response text/keyboard/file for PHP to send via Bot::addMessage().

    Input (form-encoded):
      data[PARAMS][DIALOG_ID]=...
      data[PARAMS][MESSAGE]=...
      data[PARAMS][FROM_USER_ID]=...
      data[PARAMS][COMMAND]=...          (keyboard buttons)
      data[PARAMS][COMMAND_PARAMS]=...

    Output JSON:
      {"messages": [{"text": "...", "keyboard": [...]}]}
    """
    if not _check_token(request):
        logger.warning(f"[B24Bot] Отклонён запрос с неверным токеном, IP={request.client.host}")
        return JSONResponse(status_code=403, content={"error": "Forbidden"})

    try:
        form = await request.form()

        params = {}
        for key, value in form.items():
            if key.startswith("data[PARAMS][") and key.endswith("]"):
                params[key[13:-1]] = value

        dialog_id = params.get("DIALOG_ID", "")
        if not dialog_id:
            return JSONResponse(content={"messages": []})

        message = params.get("MESSAGE", "")
        command = params.get("COMMAND", "")
        command_params = params.get("COMMAND_PARAMS", "")
        from_user_id = int(params.get("FROM_USER_ID", "0") or "0")

        logger.info(
            f"[B24Bot] user={from_user_id} cmd='{command}' "
            f"text='{message[:50]}' dialog={dialog_id}"
        )

        from bitrix24_bot.handlers import handle_message
        messages = await handle_message(
            dialog_id, from_user_id, message,
            command=command, command_params=command_params,
        )

        return JSONResponse(content={"messages": messages})

    except Exception as e:
        logger.error(f"[B24Bot] Ошибка обработки запроса: {e}", exc_info=True)
        return JSONResponse(content={
            "messages": [{"text": "❌ Ошибка при формировании отчёта."}]
        })


def main():
    port = int(os.getenv("B24_BOT_PORT", "7777"))
    logger.info(f"Запуск Bitrix24 бота на порту {port}")
    uvicorn.run(
        "bitrix24_bot.main:app",
        host="0.0.0.0",
        port=port,
        log_level="info",
    )


if __name__ == "__main__":
    main()
