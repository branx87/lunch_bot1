"""
Bitrix24 imbot REST API client.
Sends text messages and uploads Excel files via Bitrix24 Disk.
"""
import json
import logging
import os
from typing import Optional

import httpx

logger = logging.getLogger(__name__)


class BitrixBotAPI:
    """Sends messages and files to Bitrix24 chat via imbot REST API."""

    def __init__(self, rest_url: str, bot_id: int, b24_user_id: int):
        """
        rest_url  — full REST webhook URL, e.g. https://b24.example.com/rest/788/<token>/
        bot_id    — ID returned by imbot.register
        b24_user_id — Bitrix24 user ID of the REST webhook owner (for disk operations)
        """
        self._rest_url = rest_url.rstrip("/")
        self._bot_id = bot_id
        self._b24_user_id = b24_user_id

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    async def send_message(
        self,
        dialog_id: str,
        text: str,
        keyboard: Optional[list[list[dict]]] = None,
    ) -> bool:
        """Send text message with optional keyboard to a Bitrix24 chat."""
        payload: dict = {
            "BOT_ID": self._bot_id,
            "DIALOG_ID": dialog_id,
            "MESSAGE": text,
        }
        if keyboard is not None:
            payload["KEYBOARD"] = keyboard

        return await self._post("imbot.message.add.json", payload)

    async def send_typing(self, dialog_id: str) -> None:
        """Send typing indicator (fire-and-forget)."""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.post(
                    f"{self._rest_url}/imbot.chat.sendTyping.json",
                    json={"BOT_ID": self._bot_id, "DIALOG_ID": dialog_id},
                )
        except Exception:
            pass

    async def send_file(
        self,
        dialog_id: str,
        file_path: str,
        file_name: str,
        caption: str,
    ) -> bool:
        """
        Upload an Excel file to Bitrix24 Disk and send it in chat.
        Falls back to sending caption as plain text if upload fails.
        """
        file_id = await self._upload_to_disk(file_path, file_name)

        if file_id is None:
            logger.warning(f"[B24Bot] Загрузка файла не удалась, отправляем текст")
            return await self.send_message(dialog_id, caption)

        payload = {
            "BOT_ID": self._bot_id,
            "DIALOG_ID": dialog_id,
            "MESSAGE": caption,
            "ATTACH": [
                {
                    "ID": "report_file",
                    "BLOCKS": [
                        {
                            "LINK": [
                                {
                                    "NAME": file_name,
                                    "LINK": await self._get_file_url(file_id),
                                }
                            ]
                        }
                    ],
                }
            ],
        }

        # Also include FILES if Bitrix24 supports it for this bot
        payload["FILES"] = [file_id]

        return await self._post("imbot.message.add.json", payload)

    # ------------------------------------------------------------------
    # Disk helpers
    # ------------------------------------------------------------------

    async def _upload_to_disk(
        self, file_path: str, file_name: str
    ) -> Optional[int]:
        """
        Upload file to Bitrix24 Disk root folder of the webhook user.
        Returns file_id or None on failure.
        """
        try:
            # Step 1: get user's root disk folder ID
            root_folder_id = await self._get_root_folder_id()
            if root_folder_id is None:
                return None

            # Step 2: upload file
            with open(file_path, "rb") as f:
                file_bytes = f.read()

            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(
                    f"{self._rest_url}/disk.folder.uploadFile.json",
                    data={
                        "id": root_folder_id,
                        "data": json.dumps({"NAME": file_name}),
                    },
                    files={
                        "fileContent": (
                            file_name,
                            file_bytes,
                            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        )
                    },
                )

            data = resp.json()
            result = data.get("result")
            if not result:
                logger.error(f"[B24Bot] disk.folder.uploadFile error: {data.get('error_description', data)}")
                return None

            file_id = result.get("ID")
            logger.info(f"[B24Bot] Файл загружен на диск: id={file_id}, name={file_name}")
            return file_id

        except Exception as e:
            logger.error(f"[B24Bot] Ошибка загрузки файла на диск: {e}", exc_info=True)
            return None

    async def _get_root_folder_id(self) -> Optional[int]:
        """Get Bitrix24 Disk root folder ID for the webhook user."""
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(
                    f"{self._rest_url}/disk.storage.get.json",
                    json={"id": f"user:{self._b24_user_id}"},
                )
            data = resp.json()
            result = data.get("result")
            if not result:
                logger.error(f"[B24Bot] disk.storage.get error: {data.get('error_description', data)}")
                return None
            return result["ROOT_OBJECT"]["ID"]
        except Exception as e:
            logger.error(f"[B24Bot] Ошибка получения корневой папки: {e}")
            return None

    async def _get_file_url(self, file_id: int) -> str:
        """Get download URL for an uploaded file."""
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(
                    f"{self._rest_url}/disk.file.get.json",
                    json={"id": file_id},
                )
            data = resp.json()
            result = data.get("result", {})
            return result.get("DETAIL_URL", "")
        except Exception as e:
            logger.error(f"[B24Bot] Ошибка получения URL файла: {e}")
            return ""

    # ------------------------------------------------------------------
    # HTTP helper
    # ------------------------------------------------------------------

    async def _post(self, method: str, payload: dict) -> bool:
        url = f"{self._rest_url}/{method}"
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.post(url, json=payload)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("result") is not None:
                        return True
                    logger.warning(f"[B24Bot] {method} API error: {data.get('error_description', data)}")
                    return False
                logger.warning(f"[B24Bot] {method} HTTP {resp.status_code}: {resp.text[:200]}")
                return False
        except Exception as e:
            logger.error(f"[B24Bot] Ошибка {method}: {e}")
            return False
