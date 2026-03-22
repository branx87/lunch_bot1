"""
Lightweight VK API client for sending messages from the Telegram process (cron jobs).
Does NOT start a bot — only used for outbound messages.
"""
import os
import logging
import random

logger = logging.getLogger(__name__)

_vk_api = None


def get_vk_api():
    """Get or create a VK API instance for sending messages."""
    global _vk_api
    if _vk_api is not None:
        return _vk_api

    token = os.getenv('VK_BOT_TOKEN')
    if not token:
        logger.info("VK_BOT_TOKEN not set — VK notifications disabled")
        return None

    try:
        from vkbottle import API
        _vk_api = API(token=token)
        logger.info("VK API client initialized for notifications")
        return _vk_api
    except ImportError:
        logger.warning("vkbottle not installed — VK notifications disabled")
        return None
    except Exception as e:
        logger.error(f"Failed to create VK API client: {e}")
        return None


async def send_vk_message(user_vk_id, text):
    """Send a text message to a VK user. Returns True on success."""
    api = get_vk_api()
    if not api:
        return False

    try:
        await api.messages.send(
            peer_id=user_vk_id,
            message=text,
            random_id=random.randint(1, 2**31),
        )
        return True
    except Exception as e:
        logger.warning(f"Failed to send VK message to {user_vk_id}: {e}")
        return False


async def send_vk_document(user_vk_id, file_path, caption=""):
    """Send a file to a VK user. Returns True on success."""
    api = get_vk_api()
    if not api:
        return False

    try:
        from vkbottle.tools import DocMessagesUploader
        uploader = DocMessagesUploader(api)
        doc = await uploader.upload(file_source=file_path, peer_id=user_vk_id)
        await api.messages.send(
            peer_id=user_vk_id,
            message=caption,
            attachment=doc,
            random_id=random.randint(1, 2**31),
        )
        return True
    except Exception as e:
        logger.warning(f"Failed to send VK document to {user_vk_id}: {e}")
        return False
