"""Registration flow for Max messenger: phone -> name -> location."""
import logging
from datetime import datetime

from maxapi import Router, F
from maxapi.types import MessageCreated, MessageCallback
from maxapi.fsm import MemoryContext

from database import db
from config import CONFIG
from services.user_service import (
    normalize_phone, is_valid_phone, find_employee_by_name,
    register_user_messenger, set_user_location, get_user_by_messenger, MESSENGER_MAX
)
from max_bot.states import Registration
from max_bot.keyboards import (
    phone_request_keyboard, location_keyboard, main_menu_keyboard, retry_keyboard
)

logger = logging.getLogger(__name__)
router = Router()


@router.message_created()
async def on_contact_shared(event: MessageCreated, context: MemoryContext):
    """Handle contact sharing (phone number)."""
    # Check if the message contains a contact
    if not event.message.body or not event.message.body.contact:
        return  # Not a contact message, skip

    contact = event.message.body.contact
    phone = getattr(contact, 'phone', None) or getattr(contact, 'phone_number', None)

    if not phone:
        await event.message.answer(
            "❌ Не удалось получить номер телефона. Попробуйте ещё раз:",
            attachments=[phone_request_keyboard()]
        )
        return

    normalized = normalize_phone(str(phone))
    if not is_valid_phone(normalized):
        await event.message.answer(
            "❌ Неверный формат номера телефона. Попробуйте ещё раз:",
            attachments=[phone_request_keyboard()]
        )
        return

    user_id = event.user.user_id

    # Check if already registered
    with db.get_session() as session:
        existing = get_user_by_messenger(user_id, MESSENGER_MAX, session)
        if existing and existing.is_verified:
            await event.message.answer(
                f"Вы уже зарегистрированы, {existing.full_name}!",
                attachments=[main_menu_keyboard(user_id)]
            )
            return

    # Save phone in FSM context and ask for name
    await context.set_state(Registration.full_name)
    await context.update_data(phone=normalized)
    await event.message.answer("Введите ваше фамилию имя и отчество:")


@router.message_created(Registration.full_name)
async def on_full_name(event: MessageCreated, context: MemoryContext):
    """Handle full name input during registration."""
    user_input = event.message.body.text.strip() if event.message.body and event.message.body.text else ""

    if not user_input or len(user_input.split()) < 2:
        await event.message.answer(
            "❌ Пожалуйста, введите фамилию имя и отчество полностью.\n"
            "Пример: Иванов Иван Иванович",
            attachments=[retry_keyboard()]
        )
        return

    user_id = event.user.user_id
    data = await context.get_data()
    phone = data.get('phone')

    with db.get_session() as session:
        matched_user = find_employee_by_name(user_input, session, messenger_type=MESSENGER_MAX)

        if not matched_user:
            await event.message.answer(
                "❌ Такого сотрудника нет в системе или он уже зарегистрирован.",
                attachments=[retry_keyboard()]
            )
            return

        # Bind Max ID to the matched employee
        register_user_messenger(
            matched_user, user_id, MESSENGER_MAX,
            username=getattr(event.user, 'username', None),
            phone=phone
        )
        matched_user.updated_at = datetime.now()
        session.commit()

        await context.update_data(full_name=matched_user.full_name)

    # Ask for location
    await context.set_state(Registration.location)
    await event.message.answer(
        "Выберите ваш объект:",
        attachments=[location_keyboard()]
    )


@router.message_callback(F.callback.payload.startswith("location_"))
async def on_location_selected(event: MessageCallback, context: MemoryContext):
    """Handle location selection callback."""
    location = event.callback.payload.replace("location_", "")

    if location not in CONFIG.locations:
        await event.message.answer(
            "❌ Пожалуйста, выберите объект из списка.",
            attachments=[location_keyboard()]
        )
        return

    user_id = event.user.user_id

    with db.get_session() as session:
        user = get_user_by_messenger(user_id, MESSENGER_MAX, session)
        if user:
            set_user_location(user, location)
            user.updated_at = datetime.now()
            session.commit()

    await context.clear()
    await event.message.answer(
        f"✅ Регистрация завершена! Локация: {location}",
        attachments=[main_menu_keyboard(user_id)]
    )


@router.message_callback(F.callback.payload == "retry_registration")
async def on_retry(event: MessageCallback, context: MemoryContext):
    """Retry registration from the beginning."""
    await context.clear()
    await event.message.answer(
        "Пожалуйста, отправьте свой номер телефона:",
        attachments=[phone_request_keyboard()]
    )
