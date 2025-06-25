# ##handlers/message_handlers.py
import logging
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import ConversationHandler, MessageHandler, filters, CommandHandler
from telegram.ext import ContextTypes
import asyncio

from config import CONFIG
from constants import AWAIT_MESSAGE_TEXT, AWAIT_USER_SELECTION
from db import Database
from bot_keyboards import create_admin_keyboard, create_main_menu_keyboard


logger = logging.getLogger(__name__)

async def start_user_to_admin_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало диалога пользователя с админом"""
    user = update.effective_user
    
    # Проверяем, есть ли пользователь в базе (даже если не завершил регистрацию)
    user_data = db.get_user(user.id)
    is_registered = bool(user_data and user_data.get('is_verified'))
    
    context.user_data.update({
        'is_registered': is_registered,
        'user_name': user_data.get('full_name') if user_data else user.full_name
    })
    
    await update.message.reply_text(
        "✍️ Введите ваше сообщение администратору:",
        reply_markup=ReplyKeyboardMarkup([["❌ Отменить"]], resize_keyboard=True)
    )
    return AWAIT_MESSAGE_TEXT

async def handle_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает сообщение пользователя администраторам"""
    try:
        user = update.effective_user
        message_text = update.message.text.strip()
        
        if message_text.lower() in ["отменить", "❌ отменить"]:
            await update.message.reply_text("❌ Отправка отменена")
            return ConversationHandler.END

        # Проверяем регистрацию пользователя
        user_data = db.get_user(user.id)
        is_registered = bool(user_data and user_data.get('is_verified'))
        user_name = user_data.get('full_name') if user_data else user.full_name

        # Формируем информативное сообщение для админов
        status = "НЕзарегистрированный" if not is_registered else "зарегистрированный"
        admin_message = (
            f"✉️ Сообщение от {status} пользователя:\n"
            f"👤 Имя: {user_name}\n"
            f"👤 Телеграм: @{user.username if user.username else 'нет'}\n"
            f"🆔 ID: {user.id}\n"
            f"📝 Текст: {message_text}"
        )

        # Сохраняем в БД
        db.cursor.execute(
            "INSERT INTO admin_messages (user_id, message_text, is_unregistered) "
            "VALUES (?, ?, ?)",
            (user.id, message_text, not is_registered)
        )
        db.conn.commit()

        # Отправляем всем админам
        sent_count = 0
        for admin_id in CONFIG.admin_ids:
            try:
                await context.bot.send_message(chat_id=admin_id, text=admin_message)
                sent_count += 1
            except Exception as e:
                logger.error(f"Ошибка отправки админу {admin_id}: {e}")

        await update.message.reply_text(
            f"✅ Сообщение отправлено {sent_count} администраторам",
            reply_markup=ReplyKeyboardRemove()
        )
        
    except Exception as e:
        logger.error(f"Ошибка обработки сообщения: {e}")
        await update.message.reply_text("❌ Ошибка при отправке")
    
    return ConversationHandler.END

# async def handle_admin_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     """Админ отвечает пользователю (в том числе незарегистрированному)"""
#     try:
#         admin_id = update.effective_user.id
#         reply_text = update.message.text.strip()
#         user_id = context.user_data.get('reply_to_user_id')

#         if not user_id:
#             await update.message.reply_text("❌ Не выбран пользователь для ответа")
#             return ConversationHandler.END

#         # Отправляем сообщение пользователю
#         try:
#             await context.bot.send_message(
#                 chat_id=user_id,
#                 text=f"✉️ Ответ администратора:\n{reply_text}"
#             )
#             await update.message.reply_text("✅ Ответ отправлен")
#         except Exception as e:
#             await update.message.reply_text(f"❌ Не удалось отправить ответ: {e}")

#     except Exception as e:
#         logger.error(f"Ошибка ответа админа: {e}")
#         await update.message.reply_text("❌ Ошибка при отправке ответа")
    
#     return ConversationHandler.END

# async def handle_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     """
#     Обрабатывает текст сообщения от пользователя администраторам.
#     Работает как для зарегистрированных, так и для незарегистрированных пользователей.
#     """
#     try:
#         user = update.effective_user
#         message_text = update.message.text
        
#         if message_text.strip().lower() == "отменить":
#             await update.message.reply_text(
#                 "❌ Отправка отменена",
#                 reply_markup=create_main_menu_keyboard(user.id)
#             )
#             return ConversationHandler.END

#         is_registered = context.user_data.get('is_registered', False)
#         full_name = context.user_data.get('user_name', "Неизвестный пользователь")

#         # Формируем сообщение для админов с пометкой о статусе регистрации
#         reg_status = " (зарегистрирован)" if is_registered else " (НЕ зарегистрирован)"
#         admin_message = (
#             f"✉️ Сообщение от пользователя{reg_status}:\n"
#             f"👤 Имя: {full_name}\n"
#             f"👤 Телеграм: @{user.username if user.username else 'нет'}\n"
#             f"🆔 ID: {user.id}\n"
#             f"📝 Текст: {message_text}"
#         )

#         # Сохраняем сообщение в БД (даже для незарегистрированных)
#         db.cursor.execute(
#             "INSERT INTO admin_messages (user_id, message_text, is_unregistered) "
#             "VALUES (?, ?, ?)",
#             (user.id, message_text, not is_registered)
#         )
#         db.conn.commit()

#         # Отправляем всем админам
#         sent_count = 0
#         for admin_id in CONFIG.admin_ids:
#             try:
#                 await context.bot.send_message(
#                     chat_id=admin_id,
#                     text=admin_message
#                 )
#                 sent_count += 1
#             except Exception as e:
#                 logger.error(f"Ошибка отправки админу {admin_id}: {e}")

#         await update.message.reply_text(
#             f"✅ Сообщение отправлено {sent_count} администраторам",
#             reply_markup=create_main_menu_keyboard(user.id)
#         )
        
#         return ConversationHandler.END

#     except Exception as e:
#         logger.error(f"Ошибка обработки сообщения: {e}")
#         await update.message.reply_text(
#             "❌ Произошла ошибка при отправке. Попробуйте позже.",
#             reply_markup=create_main_menu_keyboard(update.effective_user.id)
#         )
#         return ConversationHandler.END

async def start_admin_to_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало диалога отправки сообщения пользователю"""
    if update.effective_user.id not in CONFIG.admin_ids:
        await update.message.reply_text("❌ У вас нет прав")
        return ConversationHandler.END

    context.user_data.clear()
    
    await update.message.reply_text(
        "Выберите получателя:\n"
        "1. Введите ID пользователя (только цифры)\n"
        "2. Введите @username\n"
        "3. Введите часть ФИО (для поиска в базе)\n\n"
        "Для ответа на сообщение - просто ответьте на него",
        reply_markup=ReplyKeyboardMarkup([["❌ Отмена"]], resize_keyboard=True)
    )
    return AWAIT_USER_SELECTION

async def handle_user_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает выбор пользователя администратором"""
    user_input = update.message.text.strip()
    
    if user_input.lower() in ["отмена", "❌ отмена"]:
        await update.message.reply_text("❌ Отправка отменена", reply_markup=create_admin_keyboard())
        return ConversationHandler.END

    # Если ввели чистый ID (только цифры)
    if user_input.isdigit():
        context.user_data['recipient_id'] = int(user_input)
        context.user_data['recipient_name'] = "Пользователь (не в базе)"
        
        await update.message.reply_text(
            f"Выбран пользователь (ID: {user_input})\nВведите сообщение:",
            reply_markup=ReplyKeyboardMarkup([["❌ Отмена"]], resize_keyboard=True)
        )
        return AWAIT_MESSAGE_TEXT

    # Если ввели @username
    if user_input.startswith('@'):
        username = user_input[1:]
        db.cursor.execute(
            "SELECT telegram_id, full_name FROM users WHERE username = ?", 
            (username,)
        )
        result = db.cursor.fetchone()
        
        if result:
            context.user_data['recipient_id'] = result[0]
            context.user_data['recipient_name'] = result[1]
            await update.message.reply_text(
                f"Выбран пользователь: {result[1]}\nВведите сообщение:",
                reply_markup=ReplyKeyboardMarkup([["❌ Отмена"]], resize_keyboard=True)
            )
            return AWAIT_MESSAGE_TEXT
        else:
            await update.message.reply_text(
                "❌ Пользователь с таким username не найден. Можно ввести ID напрямую",
                reply_markup=ReplyKeyboardMarkup([["❌ Отмена"]], resize_keyboard=True)
            )
            return AWAIT_USER_SELECTION

    # Поиск по ФИО (только для зарегистрированных)
    db.cursor.execute(
        "SELECT telegram_id, full_name FROM users WHERE full_name LIKE ?",
        (f"%{user_input}%",)
    )
    recipients = db.cursor.fetchall()

    if not recipients:
        await update.message.reply_text(
            "❌ Пользователь не найден. Введите ID напрямую (только цифры)",
            reply_markup=ReplyKeyboardMarkup([["❌ Отмена"]], resize_keyboard=True)
        )
        return AWAIT_USER_SELECTION

    if len(recipients) > 1:
        keyboard = [[f"{name} (ID: {id})"] for id, name in recipients[:10]]
        keyboard.append(["❌ Отмена"])
        
        await update.message.reply_text(
            "Найдено несколько пользователей. Выберите одного:",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        )
        context.user_data['found_users'] = recipients
        return AWAIT_USER_SELECTION

    # Если нашли одного пользователя
    recipient = recipients[0]
    context.user_data['recipient_id'] = recipient[0]
    context.user_data['recipient_name'] = recipient[1]
    
    await update.message.reply_text(
        f"Выбран пользователь: {recipient[1]}\nВведите сообщение:",
        reply_markup=ReplyKeyboardMarkup([["❌ Отмена"]], resize_keyboard=True)
    )
    return AWAIT_MESSAGE_TEXT

async def handle_admin_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Отправляет сообщение от администратора ЛЮБОМУ пользователю.
    """
    try:
        text = update.message.text.strip()
        
        if text.lower() in ["отмена", "❌ отмена"]:
            await update.message.reply_text("❌ Отправка отменена", reply_markup=create_admin_keyboard())
            return ConversationHandler.END

        recipient_id = context.user_data.get('recipient_id')
        recipient_name = context.user_data.get('recipient_name')

        if not recipient_id:
            await update.message.reply_text("❌ Получатель не выбран", reply_markup=create_admin_keyboard())
            return ConversationHandler.END

        try:
            # Пытаемся отправить сообщение
            await context.bot.send_message(
                chat_id=recipient_id,
                text=f"✉️ Сообщение от администратора:\n{text}"
            )
            
            # Сохраняем в БД (даже если пользователь не в системе)
            db.cursor.execute(
                "INSERT INTO admin_messages (admin_id, user_id, message_text) VALUES (?, ?, ?)",
                (update.effective_user.id, recipient_id, text)
            )
            db.conn.commit()

            await update.message.reply_text(
                f"✅ Сообщение отправлено (ID: {recipient_id})",
                reply_markup=create_admin_keyboard()
            )
        except Exception as e:
            logger.error(f"Ошибка отправки: {e}")
            await update.message.reply_text(
                f"❌ Не удалось отправить. Пользователь мог заблокировать бота.",
                reply_markup=create_admin_keyboard()
            )

    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await update.message.reply_text("❌ Произошла ошибка", reply_markup=create_admin_keyboard())
    
    return ConversationHandler.END

async def handle_broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Инициирует процесс массовой рассылки сообщений.
    Проверяет права администратора и переводит в состояние ожидания текста рассылки.
    """
    if update.effective_user.id not in CONFIG.admin_ids:
        logger.warning(f"Попытка рассылки от неадмина: {update.effective_user.id}")
        await update.message.reply_text("❌ У вас нет прав для этой команды")
        return ConversationHandler.END
    
    logger.info(f"Начало рассылки админом {update.effective_user.id}")
    await update.message.reply_text(
        "Введите сообщение для рассылки:",
        reply_markup=ReplyKeyboardMarkup([["❌ Отмена"]], resize_keyboard=True)
    )
    return AWAIT_MESSAGE_TEXT

async def process_broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Выполняет массовую рассылку сообщения всем верифицированным пользователям.
    Формирует отчет об успешных/неудачных отправках.
    Обрабатывает отмену рассылки.
    """
    text = update.message.text
    logger.info(f"Получен текст для рассылки: {text}")
    
    if text.lower() in ["отмена", "❌ отмена"]:
        logger.info("Рассылка отменена")
        await update.message.reply_text(
            "❌ Рассылка отменена",
            reply_markup=create_admin_keyboard()
        )
        return ConversationHandler.END
    
    try:
        db.cursor.execute("SELECT telegram_id, full_name FROM users WHERE is_verified = TRUE")
        users = db.cursor.fetchall()
        
        if not users:
            logger.warning("Нет верифицированных пользователей для рассылки")
            await update.message.reply_text("❌ Нет пользователей для рассылки")
            return ConversationHandler.END
        
        logger.info(f"Начало рассылки для {len(users)} пользователей")
        msg = await update.message.reply_text(f"⏳ Рассылка для {len(users)} пользователей...")
        
        success = 0
        failed = []
        
        for user_id, full_name in users:
            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text=f"📢 Сообщение от администратора:\n\n{text}"
                )
                success += 1
                await asyncio.sleep(0.1)
            except Exception as e:
                failed.append(f"{full_name} (ID: {user_id})")
                logger.error(f"Ошибка отправки {user_id}: {e}")
        
        try:
            await msg.delete()
        except Exception as e:
            logger.error(f"Ошибка удаления сообщения: {e}")
        
        report = f"✅ Успешно: {success}/{len(users)}"
        if failed:
            report += f"\n❌ Ошибки: {len(failed)}"
        
        logger.info(f"Результат рассылки: {report}")
        await update.message.reply_text(
            report,
            reply_markup=create_admin_keyboard()
        )
        
    except Exception as e:
        logger.error(f"Ошибка рассылки: {e}", exc_info=True)
        await update.message.reply_text("❌ Ошибка при рассылке")
    
    return ConversationHandler.END

def setup_message_handlers(application):
    """
    Настраивает и добавляет обработчики сообщений в приложение:
    - Диалог пользователя с администраторами
    - Диалог администратора с пользователями
    Обеспечивает обработку отмены и повторного входа в диалог.
    """
    # Диалог пользователя с админами
    user_conv = ConversationHandler(
        entry_points=[MessageHandler(
            filters.Regex("^Написать администратору$") & filters.TEXT,
            start_user_to_admin_message
        )],
        states={
            AWAIT_MESSAGE_TEXT: [MessageHandler(filters.TEXT, handle_user_message)]
        },
        fallbacks=[
            CommandHandler('cancel', lambda u, c: ConversationHandler.END),
            MessageHandler(filters.Regex("^❌ Отменить$"), lambda u, c: ConversationHandler.END)
        ],
        allow_reentry=True
    )

    # Диалог админа с пользователем
    admin_conv = ConversationHandler(
        entry_points=[MessageHandler(
            filters.Regex("^✉️ Написать пользователю$") & filters.TEXT,
            start_admin_to_user_message
        )],
        states={
            AWAIT_USER_SELECTION: [MessageHandler(filters.TEXT, handle_user_selection)],
            AWAIT_MESSAGE_TEXT: [MessageHandler(filters.TEXT, handle_admin_message)]
        },
        fallbacks=[
            CommandHandler('cancel', lambda u, c: ConversationHandler.END),
            MessageHandler(filters.Regex("^❌ Отменить$"), lambda u, c: ConversationHandler.END)
        ],
        allow_reentry=True
    )

    application.add_handler(user_conv)
    application.add_handler(admin_conv)