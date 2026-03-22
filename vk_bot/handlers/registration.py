"""Registration flow for VK: phone (manual) -> name -> location."""
import logging
from datetime import datetime

from vkbottle.bot import BotLabeler, Message, MessageEvent, rules
from vkbottle import GroupEventType

from database import db
from config import CONFIG
from services.user_service import (
    normalize_phone, is_valid_phone, find_employee_by_name, find_user_by_phone,
    register_user_messenger, set_user_location, get_user_by_messenger, MESSENGER_VK
)
from vk_bot.states import Registration
from vk_bot.keyboards import location_keyboard, main_menu_keyboard

logger = logging.getLogger(__name__)
reg_labeler = BotLabeler()


@reg_labeler.private_message(state=Registration.PHONE)
async def on_phone_input(message: Message):
    """User typed their phone number manually."""
    phone_input = message.text.strip()

    if not is_valid_phone(phone_input):
        await message.answer(
            "❌ Неверный формат номера. Введите номер телефона "
            "(минимум 10 цифр, формат: +7, 8, 7...):"
        )
        return

    normalized = normalize_phone(phone_input)
    user_id = message.from_id

    # Check if already registered
    with db.get_session() as session:
        existing = get_user_by_messenger(user_id, MESSENGER_VK, session)
        if existing and existing.is_verified:
            await message.answer(
                f"Вы уже зарегистрированы, {existing.full_name}!",
                keyboard=main_menu_keyboard(user_id)
            )
            import vk_bot
            await vk_bot.state_dispenser.delete(message.peer_id)
            return

        # Try to find user by phone (last 10 digits comparison)
        matched_by_phone = find_user_by_phone(phone_input, session)
        if matched_by_phone:
            # Phone found in DB — save it and ask for name confirmation
            pass  # Will ask for name in next step anyway

    # Store phone and move to name step
    import vk_bot
    await vk_bot.state_dispenser.set(message.peer_id, Registration.FULL_NAME, phone=normalized)
    await message.answer("Введите ваше фамилию имя и отчество:")


@reg_labeler.private_message(state=Registration.FULL_NAME)
async def on_full_name(message: Message):
    """Handle full name input during registration."""
    user_input = message.text.strip()

    if not user_input or len(user_input.split()) < 2:
        await message.answer(
            "❌ Пожалуйста, введите фамилию имя и отчество полностью.\n"
            "Пример: Иванов Иван Иванович"
        )
        return

    user_id = message.from_id
    import vk_bot
    peer_state = await vk_bot.state_dispenser.get(message.peer_id)
    phone = peer_state.payload.get("phone", "") if peer_state else ""

    with db.get_session() as session:
        matched_user = find_employee_by_name(user_input, session, messenger_type=MESSENGER_VK)

        if not matched_user:
            # Name not found among unregistered — try phone match
            matched_user = find_user_by_phone(phone, session)
            if matched_user:
                # Found by phone but name didn't match exactly
                # Check if at least partially matches (same words)
                input_set = {p.lower() for p in user_input.split()}
                db_set = {p.lower() for p in matched_user.full_name.split()} if matched_user.full_name else set()
                if not input_set.intersection(db_set):
                    matched_user = None  # Names don't match at all

        if not matched_user:
            await message.answer(
                "❌ Такого сотрудника нет в системе или он уже зарегистрирован.\n"
                "Проверьте правильность ФИО и попробуйте снова, или напишите /start"
            )
            return

        # Bind VK ID to the matched employee
        register_user_messenger(
            matched_user, user_id, MESSENGER_VK,
            phone=phone
        )
        matched_user.updated_at = datetime.now()
        session.commit()
        # Save before session closes (avoid DetachedInstanceError)
        matched_full_name = matched_user.full_name

    # Ask for location
    await vk_bot.state_dispenser.set(message.peer_id, Registration.LOCATION, phone=phone, full_name=matched_full_name)
    await message.answer("Выберите ваш объект:", keyboard=location_keyboard())


@reg_labeler.raw_event(
    GroupEventType.MESSAGE_EVENT,
    MessageEvent,
    rules.PayloadMapRule([("cmd", "location")]),
)
async def on_location_callback(event: MessageEvent):
    """Handle location selection from inline callback."""
    payload = event.object.payload

    location = payload.get("loc", "")
    if location not in CONFIG.locations:
        await event.show_snackbar("❌ Неизвестная локация")
        return

    user_id = event.object.peer_id

    with db.get_session() as session:
        user = get_user_by_messenger(user_id, MESSENGER_VK, session)
        if user:
            set_user_location(user, location)
            user.updated_at = datetime.now()
            session.commit()

    # Clear FSM state
    import vk_bot
    try:
        await vk_bot.state_dispenser.delete(user_id)
    except KeyError:
        pass  # State already cleared

    await event.edit_message(
        f"✅ Регистрация завершена! Локация: {location}"
    )
    # Send main menu as a new message
    await event.ctx_api.messages.send(
        peer_id=user_id,
        message="Главное меню:",
        keyboard=main_menu_keyboard(user_id),
        random_id=0,
    )
