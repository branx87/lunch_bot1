"""
Backfill: проставить orders.source для заказов, созданных до появления колонки.

Сам канал в БД раньше не сохранялся, поэтому источник восстанавливается эвристикой
по данным, которые остались в базе:

  is_from_bitrix = TRUE   → заказ создал человек в Битриксе (форма CRM)
                             → «Битрикс (форма)»
  is_from_bitrix = FALSE  → заказ сделал один из наших чат-ботов:
      * привязан ровно один чат-канал (telegram_id / vk_id / max_id) → этот канал
        («Телеграм» / «ВК» / «Max»);
      * не привязан ни одного чат-канала → заказ мог сделать только чат-бот
        Битрикс24 (он ищет сотрудника по bitrix_id) → «Б24»;
      * привязано несколько чат-каналов → канал определить нельзя,
        source остаётся NULL, в отчёте будет прежнее «Бот».

Запуск:
    python backfill_order_source.py            # dry-run: только показать сводку
    python backfill_order_source.py --apply    # записать изменения

Новые заказы (source уже проставлен) скрипт не трогает.
"""
import logging
import os
import sys

from database import db
from sqlalchemy import text

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

SOURCE_BY_MESSENGER = {
    'telegram_id': 'Телеграм',
    'vk_id': 'ВК',
    'max_id': 'Max',
}
SOURCE_BITRIX_FORM = 'Битрикс (форма)'
SOURCE_BITRIX24 = 'Б24'

SELECT_ORDERS = text("""
    SELECT o.id, o.target_date, o.is_from_bitrix, o.is_for_inspector,
           u.id, u.full_name,
           u.telegram_id, u.vk_id, u.max_id
    FROM orders o JOIN users u ON o.user_id = u.id
    WHERE o.source IS NULL
    ORDER BY o.target_date DESC, o.id DESC
""")

UPDATE_ORDER = text("""
    UPDATE orders SET source = :source
    WHERE id = :id AND source IS NULL
""")


def detect_source(is_from_bitrix, telegram_id, vk_id, max_id):
    """Восстанавливает источник заказа. Возвращает (source, rule) либо (None, rule)."""
    if is_from_bitrix:
        return SOURCE_BITRIX_FORM, 'is_from_bitrix'

    channels = [name for name, value in (
        ('telegram_id', telegram_id),
        ('vk_id', vk_id),
        ('max_id', max_id),
    ) if value is not None]

    if len(channels) == 1:
        return SOURCE_BY_MESSENGER[channels[0]], f'один канал: {channels[0]}'
    if not channels:
        return SOURCE_BITRIX24, 'нет чат-каналов → чат-бот Б24'
    return None, f'каналов несколько: {", ".join(channels)}'


def backfill(apply_changes=False, examples=10):
    """Проставляет source старым заказам. apply_changes=False — только сводка."""
    with db.get_session() as session:
        rows = session.execute(SELECT_ORDERS).fetchall()

        by_source = {}
        by_rule = {}
        unresolved = []
        examples_rows = []

        for row in rows:
            (order_id, target_date, is_from_bitrix, is_for_inspector,
             _user_id, full_name, telegram_id, vk_id, max_id) = row

            source, rule = detect_source(is_from_bitrix, telegram_id, vk_id, max_id)

            if source is None:
                by_rule[rule] = by_rule.get(rule, 0) + 1
                unresolved.append((order_id, full_name, target_date))
                continue

            by_source[source] = by_source.get(source, 0) + 1
            by_rule[rule] = by_rule.get(rule, 0) + 1

            if len(examples_rows) < examples:
                date_str = target_date.strftime('%d.%m.%Y') if hasattr(target_date, 'strftime') else str(target_date)
                flag = ' 🕵️' if is_for_inspector else ''
                examples_rows.append((date_str, full_name, source, flag))

            if apply_changes:
                session.execute(UPDATE_ORDER, {'source': source, 'id': order_id})

        if apply_changes:
            session.commit()

    total = len(rows)
    print('=' * 66)
    print(f'Заказов без source: {total}')
    print('=' * 66)
    if by_source:
        print('Распределение по каналу:')
        for source, count in sorted(by_source.items(), key=lambda x: -x[1]):
            print(f'  {source:<18} {count:>6}')
    else:
        print('Распределение по каналу: —')

    print('Почему так:')
    for rule, count in sorted(by_rule.items(), key=lambda x: -x[1]):
        print(f'  {rule:<28} {count:>6}')

    if examples_rows:
        print('-' * 66)
        print('Примеры:')
        for date_str, full_name, source, flag in examples_rows:
            print(f'  {date_str}  {full_name[:32]:<32} → {source}{flag}')

    if unresolved:
        print('-' * 66)
        print(f'Не определён канал у {len(unresolved)} заказов (остаются «Бот»):')
        for order_id, full_name, target_date in unresolved[:examples]:
            date_str = target_date.strftime('%d.%m.%Y') if hasattr(target_date, 'strftime') else str(target_date)
            print(f'  {date_str}  {full_name[:32]:<32} заказ #{order_id}')

    if not apply_changes:
        print('-' * 66)
        print('Это был DRY-RUN. Изменения НЕ записаны.')
        print('Повторите с --apply, если сводка устраивает.')
    else:
        print('-' * 66)
        print(f'Записано: {sum(by_source.values())} заказ(ов).')

    return {
        'total': total,
        'by_source': by_source,
        'unresolved': len(unresolved),
    }


if __name__ == '__main__':
    apply_changes = '--apply' in sys.argv[1:]
    backfill(apply_changes=apply_changes)
