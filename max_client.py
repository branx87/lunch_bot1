"""
Lightweight Max API client for sending messages from the Telegram process (cron jobs).
Does NOT start a dispatcher — only used for outbound messages.
"""
import os
import logging

logger = logging.getLogger(__name__)

_max_bot = None


def get_max_bot():
    """Get or create a Max Bot instance for sending messages."""
    global _max_bot
    if _max_bot is not None:
        return _max_bot

    token = os.getenv('MAX_BOT_TOKEN')
    if not token:
        logger.info("MAX_BOT_TOKEN not set — Max notifications disabled")
        return None

    try:
        from maxapi import Bot
        _max_bot = Bot(token=token)
        logger.info("Max bot client initialized for notifications")
        return _max_bot
    except ImportError:
        logger.warning("maxapi not installed — Max notifications disabled")
        return None
    except Exception as e:
        logger.error(f"Failed to create Max bot client: {e}")
        return None


async def send_max_message(user_max_id, text):
    """Send a text message to a Max user. Returns True on success."""
    bot = get_max_bot()
    if not bot:
        return False

    try:
        await bot.send_message(chat_id=user_max_id, text=text)
        return True
    except Exception as e:
        logger.warning(f"Failed to send Max message to {user_max_id}: {e}")
        return False


async def send_max_document(user_max_id, file_path, caption=""):
    """Send a file to a Max user. Returns True on success."""
    bot = get_max_bot()
    if not bot:
        return False

    try:
        from maxapi.types import InputMedia
        attachments = [InputMedia(path=file_path)]
        await bot.send_message(chat_id=user_max_id, text=caption, attachments=attachments)
        return True
    except Exception as e:
        logger.warning(f"Failed to send Max document to {user_max_id}: {e}")
        return False
