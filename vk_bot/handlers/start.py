"""Start handler for VK bot."""
import logging
from vkbottle.bot import BotLabeler, Message

from database import db
from services.user_service import get_user_by_messenger, MESSENGER_VK
from vk_bot.keyboards import main_menu_keyboard
from vk_bot.states import Registration

logger = logging.getLogger(__name__)
start_labeler = BotLabeler()


@start_labeler.private_message(text=["/start", "Начать", "начать"])
async def cmd_start(message: Message):
    """Handle /start or 'Начать'."""
    user_id = message.from_id
    logger.info(f"VK /start from user {user_id}")

    with db.get_session() as session:
        user = get_user_by_messenger(user_id, MESSENGER_VK, session)
        if user and user.is_verified:
            await message.answer(
                f"Добро пожаловать, {user.full_name}! 👋",
                keyboard=main_menu_keyboard(user_id)
            )
        else:
            # Start registration — ask for phone
            await message.ctx_api.messages.send(
                peer_id=message.peer_id,
                message=(
                    "Добро пожаловать! Для начала работы нужна регистрация.\n\n"
                    "Введите ваш номер телефона (в любом формате: +7, 8, 7...):"
                ),
                random_id=0,
            )
            # state_peer is None when no state exists yet — use state_dispenser directly
            import vk_bot
            await vk_bot.state_dispenser.set(message.peer_id, Registration.PHONE)
