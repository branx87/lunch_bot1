"""
User-related business logic: registration, lookup, role detection, phone validation.
Messenger-agnostic — used by both Telegram and Max bots.
"""
import logging
from models import User

logger = logging.getLogger(__name__)

MESSENGER_TELEGRAM = "telegram"
MESSENGER_MAX = "max"
MESSENGER_VK = "vk"
MESSENGER_BITRIX24 = "bitrix24"


def _get_messenger_column(messenger_type):
    """Returns the SQLAlchemy column for the given messenger type."""
    if messenger_type == MESSENGER_MAX:
        return User.max_id
    if messenger_type == MESSENGER_VK:
        return User.vk_id
    if messenger_type == MESSENGER_BITRIX24:
        return User.bitrix_id
    return User.telegram_id


def _extract_digits(phone):
    """Extract only digits from a phone string."""
    return ''.join(c for c in (phone or '') if c.isdigit())


def find_user_by_phone(phone_input, session):
    """
    Find a user by phone number, comparing last 10 digits.
    Handles any format: +7, 8, 7, with/without spaces/dashes.
    Returns User or None.
    """
    input_digits = _extract_digits(phone_input)
    if len(input_digits) < 10:
        return None
    input_last10 = input_digits[-10:]

    # Search among unregistered employees
    users = session.query(User).filter(
        User.is_employee == True,
        User.phone.isnot(None),
    ).all()

    for user in users:
        db_digits = _extract_digits(user.phone)
        if len(db_digits) >= 10 and db_digits[-10:] == input_last10:
            return user

    return None


def is_valid_phone(phone):
    """Validate that a string is a valid phone number (>=10 digits)."""
    if not phone:
        return False
    digits = [c for c in phone if c.isdigit()]
    return len(digits) >= 10


def normalize_phone(phone):
    """Normalize phone to +7XXXXXXXXXX format."""
    if not phone:
        return ""
    cleaned = ''.join(c for c in phone if c.isdigit() or c == '+')
    if cleaned.startswith('8'):
        cleaned = '+7' + cleaned[1:]
    elif cleaned.startswith('7') and not cleaned.startswith('+7'):
        cleaned = '+' + cleaned
    return cleaned


def find_employee_by_name(name_input, session, messenger_type=None):
    """
    Find an employee by name (order-independent word matching)
    that doesn't have the specified messenger bound yet.
    If messenger_type is None, requires all messenger IDs to be empty (legacy).
    Returns User or None.
    """
    name_parts = [p for p in name_input.strip().split() if p]
    if len(name_parts) < 2:
        return None

    input_set = {p.lower() for p in name_parts}

    query = session.query(User).filter(User.is_employee == True)

    if messenger_type:
        # Only require that this specific messenger is not bound
        col = _get_messenger_column(messenger_type)
        query = query.filter(col.is_(None))
    else:
        # Legacy: all messengers must be unbound
        query = query.filter(
            User.telegram_id.is_(None),
            User.max_id.is_(None),
            User.vk_id.is_(None),
        )

    candidates = query.all()

    for user in candidates:
        if not user.full_name:
            continue
        db_set = {p.lower() for p in user.full_name.split()}
        if db_set == input_set:
            return user

    return None


def get_user_by_messenger(messenger_id, messenger_type, session):
    """Get User by messenger-specific ID."""
    col = _get_messenger_column(messenger_type)
    return session.query(User).filter(col == messenger_id).first()


def get_verified_user(messenger_id, messenger_type, session):
    """Get verified, non-deleted user by messenger ID. Returns User or None."""
    col = _get_messenger_column(messenger_type)
    return session.query(User).filter(
        col == messenger_id,
        User.is_verified == True,
        User.is_deleted == False
    ).first()


def register_user_messenger(user, messenger_id, messenger_type, username=None, phone=None):
    """
    Bind a messenger ID to an existing User record.
    Does NOT commit — caller must commit the session.
    """
    if messenger_type == MESSENGER_MAX:
        user.max_id = messenger_id
    elif messenger_type == MESSENGER_VK:
        user.vk_id = messenger_id
    else:
        user.telegram_id = messenger_id

    if username:
        user.username = username
    if phone:
        user.phone = phone


def set_user_location(user, location):
    """Set location and verify user. Does NOT commit."""
    user.location = location
    user.is_verified = True


def get_user_role(messenger_id, messenger_type, config):
    """
    Determine user role by messenger ID and config lists.
    Returns 'admin', 'provider', 'accountant', 'employee', or None.
    """
    try:
        if messenger_type == MESSENGER_TELEGRAM:
            if messenger_id in config.admin_ids:
                return 'admin'
            if messenger_id in config.provider_ids:
                return 'provider'
            if messenger_id in config.accounting_ids:
                return 'accountant'
        elif messenger_type == MESSENGER_MAX:
            if messenger_id in getattr(config, 'max_admin_ids', []):
                return 'admin'
            if messenger_id in getattr(config, 'max_provider_ids', []):
                return 'provider'
            if messenger_id in getattr(config, 'max_accounting_ids', []):
                return 'accountant'
        elif messenger_type == MESSENGER_VK:
            if messenger_id in getattr(config, 'vk_admin_ids', []):
                return 'admin'
            if messenger_id in getattr(config, 'vk_provider_ids', []):
                return 'provider'
            if messenger_id in getattr(config, 'vk_accounting_ids', []):
                return 'accountant'
        elif messenger_type == MESSENGER_BITRIX24:
            if messenger_id in getattr(config, 'b24_admin_ids', []):
                return 'admin'
            if messenger_id in getattr(config, 'b24_provider_ids', []):
                return 'provider'
            if messenger_id in getattr(config, 'b24_accounting_ids', []):
                return 'accountant'

        # Check DB for employee status
        from database import db
        col = _get_messenger_column(messenger_type)
        user = db.session.query(User).filter(
            col == messenger_id,
            User.is_employee == True,
            User.is_deleted == False
        ).first()

        if user:
            return 'employee'

        return None
    except Exception as e:
        logger.error(f"Error determining role for {messenger_type}:{messenger_id}: {e}")
        return None
