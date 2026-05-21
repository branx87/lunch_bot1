"""
Bitrix24 bot client — sends messages via bot_message_sender.php on the B24 server.
PHP endpoint calls \Bitrix\Im\Bot::addMessage() internally, no OAuth required.

Requires env: B24_PHP_SENDER_URL, B24_BOT_ID, B24_WEBHOOK_TOKEN
"""
import asyncio
import logging
import os

import httpx

logger = logging.getLogger(__name__)


class BitrixBotClient:
    """Sends proactive bot messages via PHP relay on the Bitrix24 server."""

    def __init__(self, sender_url: str, bot_id: int, webhook_token: str) -> None:
        self._sender_url = sender_url
        self._bot_id = bot_id
        self._webhook_token = webhook_token

    @classmethod
    def from_env(cls) -> "BitrixBotClient":
        return cls(
            sender_url=os.getenv("B24_PHP_SENDER_URL", ""),
            bot_id=int(os.getenv("B24_BOT_ID", "0")),
            webhook_token=os.getenv("B24_WEBHOOK_TOKEN", ""),
        )

    @property
    def is_configured(self) -> bool:
        return bool(self._sender_url and self._bot_id and self._webhook_token)

    async def send_message(
        self,
        dialog_id: str,
        text: str,
        keyboard: list[list[dict]] | None = None,
        *,
        retries: int = 3,
    ) -> bool:
        """
        Send a message from the bot via PHP relay.

        dialog_id: Bitrix24 user ID (str) for private chats, or "chatN" for group chats.
        keyboard: list of rows, each row is a list of button dicts.
        """
        if not self.is_configured:
            logger.warning("[B24Client] Не настроен (нет B24_PHP_SENDER_URL, B24_BOT_ID или B24_WEBHOOK_TOKEN)")
            return False

        payload: dict = {
            "bot_id": self._bot_id,
            "dialog_id": dialog_id,
            "message": text,
        }
        if keyboard:
            payload["keyboard"] = keyboard

        headers = {"X-Webhook-Token": self._webhook_token}

        for attempt in range(1, retries + 1):
            try:
                async with httpx.AsyncClient(timeout=15.0) as client:
                    resp = await client.post(self._sender_url, json=payload, headers=headers)
                    if resp.status_code == 200:
                        data = resp.json()
                        if data.get("ok"):
                            logger.debug(f"[B24Client] Сообщение отправлено dialog={dialog_id}")
                            return True
                        logger.warning(f"[B24Client] PHP ошибка dialog={dialog_id}: {data.get('error', data)}")
                        return False
                    logger.warning(f"[B24Client] HTTP {resp.status_code} dialog={dialog_id}: {resp.text[:200]}")
                    return False
            except (httpx.ConnectError, httpx.TimeoutException, OSError) as e:
                if attempt < retries:
                    delay = attempt * 2
                    logger.warning(
                        f"[B24Client] Сетевая ошибка (попытка {attempt}/{retries}) "
                        f"dialog={dialog_id}: {e}, повтор через {delay}с"
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"[B24Client] Ошибка отправки dialog={dialog_id}: {e}")
            except Exception as e:
                logger.error(f"[B24Client] Ошибка отправки dialog={dialog_id}: {e}")
                return False
        return False
