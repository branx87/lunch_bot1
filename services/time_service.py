"""
Time-related business logic: order deadlines, workday calculations.
Messenger-agnostic — used by both Telegram and Max bots.
"""
from datetime import datetime, date, timedelta
from time_config import TIME_CONFIG


def get_month_date_range(period="current", now=None):
    """
    Get start and end dates for current or previous month.
    
    Args:
        period: "current" for current month, "previous" for previous month
        now: datetime object, defaults to current time
        
    Returns:
        tuple of (start_date, end_date) as date objects
    """
    if now is None:
        now = datetime.now(TIME_CONFIG.TIMEZONE)
    
    if period == "current":
        start_date = now.replace(day=1).date()
        end_date = now.date()
    else:
        first_day_current = now.replace(day=1)
        last_day_prev = first_day_current - timedelta(days=1)
        start_date = last_day_prev.replace(day=1).date()
        end_date = last_day_prev.date()
    
    return start_date, end_date


def get_next_workday(from_date=None):
    """Returns the next working day after from_date."""
    if from_date is None:
        from_date = datetime.now(TIME_CONFIG.TIMEZONE)

    days_to_add = 1
    if from_date.weekday() == 4:  # Friday -> Monday
        days_to_add = 3
    elif from_date.weekday() == 5:  # Saturday -> Monday
        days_to_add = 2

    return from_date + timedelta(days=days_to_add)


def is_weekend(target_date):
    """Check if target_date falls on a weekend."""
    if isinstance(target_date, datetime):
        target_date = target_date.date()
    return target_date.weekday() in TIME_CONFIG.WEEKEND_DAYS


def is_holiday(target_date, holidays):
    """Check if target_date is a holiday."""
    if isinstance(target_date, datetime):
        target_date = target_date.date()
    return target_date.strftime("%Y-%m-%d") in holidays


def _as_date(value):
    """Приводит значение к date (datetime, date или строка YYYY-MM-DD)."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            return None
    return None


def _resolve_holidays(holidays=None):
    """Праздники: явно переданные или из CONFIG."""
    if holidays is not None:
        return holidays
    try:
        from config import CONFIG
        return CONFIG.holidays or {}
    except Exception:
        return {}


def _tz_aware(day, clock):
    """datetime с часовым поясом TIME_CONFIG (pytz требует localize)."""
    naive = datetime.combine(day, clock)
    try:
        return TIME_CONFIG.TIMEZONE.localize(naive)
    except AttributeError:
        return naive.replace(tzinfo=TIME_CONFIG.TIMEZONE)


def is_new_supplier_date(target_date) -> bool:
    """Обслуживает ли target_date новый поставщик (начиная с 26.10.2026)."""
    d = _as_date(target_date)
    if d is None:
        return False
    return d >= TIME_CONFIG.NEW_SUPPLIER_START_DATE


def is_working_day(day, holidays=None) -> bool:
    """Рабочий день: не выходной и не праздник."""
    d = _as_date(day)
    if d is None:
        return False
    if d.weekday() in TIME_CONFIG.WEEKEND_DAYS:
        return False
    return d.strftime("%Y-%m-%d") not in _resolve_holidays(holidays)


def previous_working_day(day, holidays=None) -> date:
    """Последний рабочий день строго раньше day."""
    d = _as_date(day) - timedelta(days=1)
    while not is_working_day(d, holidays):
        d -= timedelta(days=1)
    return d


def next_working_day(from_date=None, holidays=None) -> date:
    """Ближайший рабочий день строго позже from_date."""
    if from_date is None:
        base = datetime.now(TIME_CONFIG.TIMEZONE).date()
    else:
        base = _as_date(from_date)
    d = base + timedelta(days=1)
    while not is_working_day(d, holidays):
        d += timedelta(days=1)
    return d


def next_working_day_offset(from_date=None, holidays=None) -> int:
    """Количество дней до ближайшего рабочего дня (используется как day_offset)."""
    if from_date is None:
        base = datetime.now(TIME_CONFIG.TIMEZONE).date()
    else:
        base = _as_date(from_date)
    return (next_working_day(base, holidays) - base).days


def _parse_placement(placement):
    """(дата, момент времени) из createdTime, datetime или date."""
    if placement is None:
        return None, None

    if isinstance(placement, str):
        raw = placement.strip()
        try:
            dt = datetime.fromisoformat(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=TIME_CONFIG.TIMEZONE)
            return dt.date(), dt
        except ValueError:
            d = _as_date(raw)
            return d, None

    if isinstance(placement, datetime):
        return placement.date(), placement

    d = _as_date(placement)
    return d, None


def meal_date_for_placement(placement, holidays=None):
    """
    Дата обеда для заказа, оформленного в момент placement.

    Это ближайший рабочий день, окно заказов по которому на момент
    оформления ещё не закрыто.

    Новый поставщик: заказ на «сегодня» не принимается — оформленный
    в пятницу заказ (до 15:00) это обед в понедельник, оформленный после 15:00 —
    уже во вторник.
    Старый поставщик: заказ оформляется на сегодня, если не позже 09:30.

    Нужна для заказов, приходящих из Bitrix: в форме нет поля даты, поэтому
    дата заказа в Bitrix — это дата оформления, а не дата обеда.
    """
    d, moment = _parse_placement(placement)
    if d is None:
        return None

    if moment is None:
        # Время неизвестно — считаем, что оформли в начале дня
        moment = _tz_aware(d, TIME_CONFIG.ORDER_DEADLINE).replace(
            hour=0, minute=0, second=0, microsecond=0)

    candidate = d
    for _ in range(14):
        if is_working_day(candidate, holidays):
            deadline = order_deadline_for(candidate, holidays)
            if deadline is not None and deadline > moment:
                return candidate
        candidate = next_working_day(candidate, holidays)

    return d


def order_deadline_for(target_date, holidays=None) -> datetime:
    """
    Момент, после которого заказ на target_date уже не принимается.

    Старый поставщик (до 26.10.2026): target_date в 09:30.
    Новый поставщик «Благовар»: 15:00 предыдущего рабочего дня —
    на понедельник заказ делается в пятницу до 15:00, на вторник в понедельник до 15:00.
    """
    d = _as_date(target_date)
    if d is None:
        return None

    if is_new_supplier_date(d):
        prev = previous_working_day(d, holidays)
        return _tz_aware(prev, TIME_CONFIG.ADVANCE_ORDER_DEADLINE)

    return _tz_aware(d, TIME_CONFIG.ORDER_DEADLINE)


def can_modify_order(target_date, orders_enabled=True, holidays=None):
    """
    Check if an order for target_date can be created/modified/cancelled.

    Единая точка правды для всех мессенджеров: срок берётся из
    order_deadline_for(), который учитывает смену поставщика.
    """
    if not orders_enabled:
        return False

    now = datetime.now(TIME_CONFIG.TIMEZONE)
    d = _as_date(target_date)
    if d is None:
        return False

    # Заказы на выходные и праздники не принимаются
    if d.weekday() in TIME_CONFIG.WEEKEND_DAYS:
        return False
    if d.strftime("%Y-%m-%d") in _resolve_holidays(holidays):
        return False

    # Прошедшие даты недоступны
    if d < now.date():
        return False

    deadline = order_deadline_for(d, holidays)
    if deadline is None:
        return False

    return now < deadline


def report_target_date(reference_date=None, holidays=None) -> date:
    """День по умолчанию для дневного отчёта по запросу.

    Под новым поставщиком заказ на сегодня не принимается, поэтому отчёт
    «за сегодня» обычно пустой. По умолчанию берём ближайший рабочий день
    с обедами: сегодня, если сегодня рабочий день, иначе следующий рабочий.
    """
    now = datetime.now(TIME_CONFIG.TIMEZONE)
    base = _as_date(reference_date) if reference_date is not None else now.date()
    holidays = _resolve_holidays(holidays)

    if is_working_day(base, holidays):
        return base
    return next_working_day(base, holidays)


def report_date_options(days=5, holidays=None, today=None) -> list:
    """Варианты дня для дневного отчёта: [(дата, подпись), ...].

    Подпись: «Сегодня 25.10» для сегодняшнего дня, иначе «Понедельник 26.10».
    """
    from services.menu_service import DAYS_RU

    now = datetime.now(TIME_CONFIG.TIMEZONE)
    base = _as_date(today) if today is not None else now.date()
    holidays = _resolve_holidays(holidays)

    options = []
    candidate = report_target_date(base, holidays)
    for _ in range(days):
        if candidate > base:
            label = f"{DAYS_RU[candidate.weekday()]} {candidate.strftime('%d.%m')}"
        else:
            label = f"Сегодня {candidate.strftime('%d.%m')}"
        options.append((candidate, label))
        candidate = next_working_day(candidate, holidays)
    return options


def admin_can_order_date(target_date, now=None, holidays=None) -> bool:
    """Может ли админ оформить заказ на эту дату.

    Админский заказ обходит окно приёма сотрудников (15:00 предыдущего
    рабочего дня) — иначе не получится заказать за того, кто забыл.
    Но выгрузка в Bitrix24 идёт в день обеда в 09:30, поэтому на сегодня
    заказ возможен только до этого времени. Будущие рабочие дни доступны
    без ограничений.
    """
    if now is None:
        now = datetime.now(TIME_CONFIG.TIMEZONE)
    d = _as_date(target_date)
    if d is None:
        return False
    holidays = _resolve_holidays(holidays)

    if not is_working_day(d, holidays):
        return False
    if d < now.date():
        return False
    if d == now.date():
        return now.time() < TIME_CONFIG.BITRIX_PUSH_TIME
    return True


def admin_order_dates(now=None, days=10, holidays=None) -> list:
    """Даты, доступные админу для заказа: сегодня (до 09:30) и будущие рабочие дни."""
    if now is None:
        now = datetime.now(TIME_CONFIG.TIMEZONE)
    base = now.date()
    holidays = _resolve_holidays(holidays)

    result = []
    candidate = base
    for _ in range(days * 3 + 10):
        if len(result) >= days:
            break
        if admin_can_order_date(candidate, now=now, holidays=holidays):
            result.append(candidate)
        candidate = next_working_day(candidate, holidays)
    return result


def admin_today_note(now=None) -> str:
    """Пояснение, почему на сегодня админский заказ недоступен (пустая строка — доступен)."""
    if now is None:
        now = datetime.now(TIME_CONFIG.TIMEZONE)
    if now.time() < TIME_CONFIG.BITRIX_PUSH_TIME:
        return ""
    return (
        f"⏳ На сегодня ({now.strftime('%d.%m')}) заказать уже нельзя: выгрузка "
        f"в Bitrix24 поставщику проходит в "
        f"{TIME_CONFIG.BITRIX_PUSH_TIME.strftime('%H:%M')} в день обеда. "
        f"Оформите заказ на следующий рабочий день — он попадёт в выгрузку в день обеда."
    )


def next_orderable_date(today=None, holidays=None, now=None) -> date:
    """Ближайший рабочий день, заказ на который ещё можно оформить.

    Важно: окно заказа на «просто следующий рабочий день» может быть уже
    закрыто — в воскресенье заказ на понедельник закрылся в пятницу в 15:00,
    а в понедельник после 15:00 закрывается заказ на вторник. Поэтому ищем
    первый рабочий день, чьё окно ещё открыто.
    """
    if now is None:
        now = datetime.now(TIME_CONFIG.TIMEZONE)
    base = _as_date(today) if today is not None else now.date()
    holidays = _resolve_holidays(holidays)

    candidate = base
    for _ in range(14):
        if is_working_day(candidate, holidays):
            deadline = order_deadline_for(candidate, holidays)
            if deadline is not None and deadline > now:
                return candidate
        candidate = next_working_day(candidate, holidays)

    return base


def order_deadline_message(target_date, holidays=None, action='order', now=None) -> str:
    """Человекочитаемое объяснение, почему заказ на дату недоступен.

    action: 'order' | 'modify' | 'cancel'
    now — текущий момент (по умолчанию системное время), параметр для тестов.
    """
    if now is None:
        now = datetime.now(TIME_CONFIG.TIMEZONE)
    d = _as_date(target_date)
    deadline = order_deadline_for(d, holidays)

    if d is None or deadline is None:
        return "⏳ Приём заказов недоступен"

    if action == 'modify':
        title = "Изменение заказа"
    elif action == 'cancel':
        title = "Отмена заказа"
    else:
        title = "Приём заказов"

    if d == now.date():
        nxt = next_orderable_date(now.date(), holidays, now=now)
        nxt_deadline = order_deadline_for(nxt, holidays)
        deadline_text = (
            nxt_deadline.strftime('%H:%M') if nxt_deadline
            else TIME_CONFIG.ADVANCE_ORDER_DEADLINE.strftime('%H:%M')
        )
        # Если дедлайн не сегодня — указываем и дату, иначе «до 15:00» неоднозначно
        if nxt_deadline and nxt_deadline.date() != now.date():
            deadline_part = nxt_deadline.strftime('%d.%m в %H:%M')
        else:
            deadline_part = deadline_text

        return (
            f"⏳ Заказы на сегодня не принимаются. "
            f"Оформить заказ можно на {nxt.strftime('%d.%m')} "
            f"до {deadline_part}."
        )

    return (
        f"⏳ {title} на {d.strftime('%d.%m')} завершён "
        f"{deadline.strftime('%d.%m')} в {deadline.strftime('%H:%M')}."
    )
