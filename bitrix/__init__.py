# ##bitrix/__init__.py
"""
Пакет Bitrix24.

BitrixSync грузится лениво: иначе `from bitrix.ops import ...` в хендлерах
требовал бы fast_bitrix24 и apscheduler уже на этапе импорта модуля.
"""
from importlib import import_module

__all__ = ['BitrixSync']


def __getattr__(name):
    if name == 'BitrixSync':
        return getattr(import_module('.sync', __name__), 'BitrixSync')
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")