"""
Direct Bitrix24 REST API client for proactive bot messages.
Uses imbot.message.add — sends messages as the bot (not as a REST user).

Requires env: B24_REST_URL, B24_BOT_ID
"""
import asyncio
import logging
import os

import httpx

logger = logging.getLogger(__name__)


class BitrixBotClient:
    """Sends proactive messages from the bot via imbot.message.add REST API."""

    def __init__(self, rest_url: str, bot_id: int) -> None:
        self._rest_url = rest_url.rstrip("/")
        self._bot_id = bot_id

    @classmethod
    def from_env(cls) -> "BitrixBotClient":
        return cls(
            rest_url=os.getenv("B24_REST_URL", ""),
            bot_id=int(os.getenv("B24_BOT_ID", "0")),
        )

    @property
    def is_configured(self) -> bool:
        return bool(self._rest_url and self._bot_id)

    async def send_message(
        self,
        dialog_id: str,
        text: str,
        keyboard: list[list[dict]] | None = None,
        *,
        retries: int = 3,
    ) -> bool:
        """
        Send a message to a Bitrix24 dialog as the bot.

        dialog_id: Bitrix24 user ID (str) for private chats, or "chatN" for group chats.
        keyboard: list of rows, each row is a list of button dicts.
        """
        if not self.is_configured:
            logger.warning("[B24Client] Не настроен (нет B24_REST_URL или B24_BOT_ID)")
            return False

        url = f"{self._rest_url}/imbot.message.add.json"
        payload: dict = {
            "BOT_ID": self._bot_id,
            "DIALOG_ID": dialog_id,
            "MESSAGE": text,
        }
        if keyboard:
            payload["KEYBOARD"] = keyboard

        for attempt in range(1, retries + 1):
            try:
                async with httpx.AsyncClient(timeout=15.0) as client:
                    resp = await client.post(url, json=payload)
                    if resp.status_code == 200:
                        data = resp.json()
                        if data.get("result"):
                            logger.debug(f"[B24Client] Сообщение отправлено dialog={dialog_id}")
                            return True
                        logger.warning(
                            f"[B24Client] API error dialog={dialog_id}: "
                            f"{data.get('error_description', data)}"
                        )
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
