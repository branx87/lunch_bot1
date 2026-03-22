"""FSM states for Max bot multi-step flows."""
from maxapi.fsm import State, StatesGroup


class Registration(StatesGroup):
    phone = State()
    full_name = State()
    location = State()
