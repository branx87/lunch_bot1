#!/bin/bash
# Регистрация команд бота в Bitrix24.
# Запускать ОДИН РАЗ после деплоя.
#
# Использование:
#   chmod +x register_commands.sh
#   ./register_commands.sh https://b24.epc.su/rest/788/ТОКЕН YOUR_BOT_ID
#
# BOT_ID берётся из ответа imbot.register (поле result)
# или из настроек бота в Bitrix24 → Разработчикам → Чат-боты

BASE_URL="${1:?Usage: $0 <REST_BASE_URL> <BOT_ID>}"
BOT_ID="${2:?Usage: $0 <REST_BASE_URL> <BOT_ID>}"

# Все команды бота (без пробелов, ACTION=COMMAND в клавиатуре)
COMMANDS=(
    # Навигация
    main_menu
    # Отчёты (admin/provider/accountant)
    reports
    orders_today
    period_day
    period_month
    rtype_admin
    rtype_accounting
    rtype_provider
    rtype_auto
    month_current
    month_prev
    # Заказы (employee/admin)
    order
    my_orders
    menu
    order_portion
    add_portion
    remove_portion
    cancel_order
    # Выбор дня (0=сегодня ... 6)
    day_0 day_1 day_2 day_3 day_4 day_5 day_6
    # Отмена заказа по индексу (из списка "мои заказы", макс ~5 активных)
    cancel_0 cancel_1 cancel_2 cancel_3 cancel_4 cancel_5 cancel_6
)

echo "Регистрация ${#COMMANDS[@]} команд для BOT_ID=${BOT_ID}..."
echo "REST URL: ${BASE_URL}"
echo ""

OK=0
FAIL=0

for CMD in "${COMMANDS[@]}"; do
    RESPONSE=$(curl -s "${BASE_URL}/imbot.command.register" \
        --data-urlencode "BOT_ID=${BOT_ID}" \
        --data-urlencode "COMMAND=${CMD}" \
        --data-urlencode "HIDDEN=Y" \
        --data-urlencode "COMMON=N" \
        --data-urlencode "LANG[0][LANGUAGE_ID]=ru" \
        --data-urlencode "LANG[0][TITLE]=${CMD}" \
    )
    if echo "$RESPONSE" | grep -q '"result"'; then
        echo "  ✅ ${CMD}"
        OK=$((OK + 1))
    else
        echo "  ❌ ${CMD} → ${RESPONSE}"
        FAIL=$((FAIL + 1))
    fi
done

echo ""
echo "Готово: ${OK} успешно, ${FAIL} ошибок."
