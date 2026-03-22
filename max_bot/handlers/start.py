"""Handler for bot_started event and /start command in Max."""
import logging
from maxapi import Router, F
from maxapi.types import BotStarted, MessageCreated
from maxapi.methods import Command

from database import db
from services.user_service import get_user_by_messenger, MESSENGER_MAX
from max_bot.keyboards import main_menu_keyboard, phone_request_keyboard
from max_bot.states import Registration

logger = logging.getLogger(__name__)
router = Router()


@router.bot_started()
async def on_bot_started(event: BotStarted):
    """User opened the bot for the first time or pressed Start."""
    user_id = event.user.user_id
    logger.info(f"MAX bot_started from user {user_id}")

    with db.get_session() as session:
        user = get_user_by_messenger(user_id, MESSENGER_MAX, session)
        if user and user.is_verified:
            await event.message.answer(
                f"Добро пожаловать, {user.full_name}! 👋",
                attachments=[main_menu_keyboard(user_id)]
            )
        else:
            await event.message.answer(
                "Добро пожаловать! Для начала работы необходимо пройти регистрацию.\n"
                "Пожалуйста, отправьте свой номер телефона:",
                attachments=[phone_request_keyboard()]
            )


@router.message_created(Command("start"))
async def cmd_start(event: MessageCreated):
    """Handle /start command."""
    user_id = event.user.user_id
    logger.info(f"MAX /start from user {user_id}")

    with db.get_session() as session:
        user = get_user_by_messenger(user_id, MESSENGER_MAX, session)
        if user and user.is_verified:
            await event.message.answer(
                "Главное меню:",
                attachments=[main_menu_keyboard(user_id)]
            )
        else:
            await event.message.answer(
                "Для доступа к боту необходимо пройти регистрацию.\n"
                "Пожалуйста, отправьте свой номер телефона:",
                attachments=[phone_request_keyboard()]
            )
