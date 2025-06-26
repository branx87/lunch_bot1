# ##notifications.py
from telegram import Update
from telegram.constants import ParseMode
import logging

logger = logging.getLogger(__name__)

# async def show_access_denied(update_or_query) -> None:
#     """Универсальное уведомление о запрете доступа"""
#     try:
#         # Если это CallbackQuery (напрямую)
#         if hasattr(update_or_query, 'answer'):
#             await update_or_query.answer(
#                 "⛔ Доступ запрещён. Ваш аккаунт деактивирован",
#                 show_alert=True
#             )
#         # Если это Update объект
#         elif hasattr(update_or_query, 'callback_query'):
#             if update_or_query.callback_query:
#                 await update_or_query.callback_query.answer(
#                     "⛔ Доступ запрещён. Ваш аккаунт деактивирован",
#                     show_alert=True
#                 )
#             elif update_or_query.message:
#                 await update_or_query.message.reply_text(
#                     "⛔ Ваш аккаунт деактивирован",
#                     parse_mode=ParseMode.HTML
#                 )
#         else:
#             logger.error(f"Неизвестный тип для show_access_denied: {type(update_or_query)}")
#     except Exception as e:
#         logger.error(f"Ошибка в show_access_denied: {e}")

async def show_access_denied(update_or_query) -> None:
    """Уведомление о временной недоступности бота"""
    try:
        message = (
            "🔧 Бот временно недоступен\n"
            "Идёт обновление функционала. Приношу свои извинения.\n"
            "Пожалуйста, заказывайте обед через Битрикс по ссылке."
        )
        
        if hasattr(update_or_query, 'answer'):
            await update_or_query.answer(message, show_alert=True)
        elif hasattr(update_or_query, 'callback_query'):
            if update_or_query.callback_query:
                await update_or_query.callback_query.answer(message, show_alert=True)
            elif update_or_query.message:
                await update_or_query.message.reply_text(message)
        else:
            logger.error(f"Неизвестный тип для show_access_denied: {type(update_or_query)}")
    except Exception as e:
        logger.error(f"Ошибка в show_access_denied: {e}")