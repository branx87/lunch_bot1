## handlers/message_handlers.py
import logging
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import ConversationHandler, MessageHandler, filters, CommandHandler
from datetime import datetime, date, timedelta
from telegram.ext import ContextTypes
import asyncio

from database import db
from models import User, AdminMessage
from config import CONFIG
from constants import AWAIT_MESSAGE_TEXT, AWAIT_USER_SELECTION
from bot_keyboards import create_admin_keyboard, create_admin_keyboard_for, create_main_menu_keyboard

logger = logging.getLogger(__name__)

async def start_user_to_admin_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало диалога пользователя с админом"""
    user = update.effective_user
    
    # Проверяем, есть ли пользователь в базе через SQLAlchemy
    user_data = db.session.query(User).filter(User.telegram_id == user.id).first()
    is_registered = bool(user_data and user_data.is_verified)
    
    context.user_data.update({
        'is_registered': is_registered,
        'user_name': user_data.full_name if user_data else user.full_name
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

        # Проверяем регистрацию пользователя через SQLAlchemy
        user_data = db.session.query(User).filter(User.telegram_id == user.id).first()
        is_registered = bool(user_data and user_data.is_verified)
        user_name = user_data.full_name if user_data else user.full_name

        # Формируем информативное сообщение для админов
        status = "НЕзарегистрированный" if not is_registered else "зарегистрированный"
        admin_message = (
            f"✉️ Сообщение от {status} пользователя:\n"
            f"👤 Имя: {user_name}\n"
            f"👤 Телеграм: @{user.username if user.username else 'нет'}\n"
            f"🆔 ID: {user.id}\n"
            f"📝 Текст: {message_text}"
        )

        # Сохраняем в БД через SQLAlchemy - ИСПРАВЛЕНО: сохраняем telegram_id напрямую
        new_message = AdminMessage(
            user_telegram_id=user.id,  # Сохраняем telegram_id напрямую
            message_text=message_text,
            is_unregistered=not is_registered
        )
        db.session.add(new_message)
        db.session.commit()

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
        await update.message.reply_text("❌ Отправка отменена", reply_markup=create_admin_keyboard_for(update.effective_user.id))
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
        # Поиск по username через SQLAlchemy
        user = db.session.query(User).filter(User.username == username).first()
        
        if user:
            context.user_data['recipient_id'] = user.telegram_id
            context.user_data['recipient_name'] = user.full_name
            await update.message.reply_text(
                f"Выбран пользователь: {user.full_name}\nВведите сообщение:",
                reply_markup=ReplyKeyboardMarkup([["❌ Отмена"]], resize_keyboard=True)
            )
            return AWAIT_MESSAGE_TEXT
        else:
            await update.message.reply_text(
                "❌ Пользователь с таким username не найден. Можно ввести ID напрямую",
                reply_markup=ReplyKeyboardMarkup([["❌ Отмена"]], resize_keyboard=True)
            )
            return AWAIT_USER_SELECTION

    # Поиск по ФИО через SQLAlchemy (только для зарегистрированных)
    recipients = db.session.query(User).filter(
        User.full_name.ilike(f"%{user_input}%")
    ).all()

    if not recipients:
        await update.message.reply_text(
            "❌ Пользователь не найден. Введите ID напрямую (только цифры)",
            reply_markup=ReplyKeyboardMarkup([["❌ Отмена"]], resize_keyboard=True)
        )
        return AWAIT_USER_SELECTION

    if len(recipients) > 1:
        keyboard = [[f"{user.full_name} (ID: {user.telegram_id})"] for user in recipients[:10]]
        keyboard.append(["❌ Отмена"])
        
        await update.message.reply_text(
            "Найдено несколько пользователей. Выберите одного:",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        )
        context.user_data['found_users'] = [(user.telegram_id, user.full_name) for user in recipients]
        return AWAIT_USER_SELECTION

    # Если нашли одного пользователя
    recipient = recipients[0]
    context.user_data['recipient_id'] = recipient.telegram_id
    context.user_data['recipient_name'] = recipient.full_name
    
    await update.message.reply_text(
        f"Выбран пользователь: {recipient.full_name}\nВведите сообщение:",
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
            await update.message.reply_text("❌ Отправка отменена", reply_markup=create_admin_keyboard_for(update.effective_user.id))
            return ConversationHandler.END

        recipient_id = context.user_data.get('recipient_id')
        
        if not recipient_id:
            await update.message.reply_text("❌ Получатель не выбран", reply_markup=create_admin_keyboard_for(update.effective_user.id))
            return ConversationHandler.END

        try:
            # Отправляем сообщение
            await context.bot.send_message(
                chat_id=recipient_id,
                text=f"✉️ Сообщение от администратора:\n{text}"
            )
            
            # 🔥 ИСПРАВЛЕННОЕ СОХРАНЕНИЕ В admin_messages
            # Сохраняем telegram_id напрямую, а не ID из таблицы users
            admin_message = AdminMessage(
                admin_telegram_id=update.effective_user.id,  # Сохраняем telegram_id админа
                user_telegram_id=recipient_id,              # Сохраняем telegram_id получателя
                message_text=text,
                is_broadcast=False,
                is_unregistered=True  # Отмечаем как незарегистрированного, если не найден в базе
            )
            
            # Проверяем, есть ли получатель в базе
            recipient_user = db.session.query(User).filter(User.telegram_id == recipient_id).first()
            if recipient_user:
                admin_message.is_unregistered = False
            
            db.session.add(admin_message)
            db.session.commit()

            await update.message.reply_text(
                f"✅ Сообщение отправлено (ID: {recipient_id})",
                reply_markup=create_admin_keyboard_for(update.effective_user.id)
            )
            
        except Exception as e:
            logger.error(f"Ошибка отправки: {e}")
            await update.message.reply_text(
                f"❌ Не удалось отправить. Пользователь мог заблокировать бота.",
                reply_markup=create_admin_keyboard_for(update.effective_user.id)
            )

    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await update.message.reply_text("❌ Произошла ошибка", reply_markup=create_admin_keyboard_for(update.effective_user.id))
    
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
            reply_markup=create_admin_keyboard_for(update.effective_user.id)
        )
        return ConversationHandler.END
    
    try:
        # Получаем верифицированных пользователей через SQLAlchemy
        users = db.session.query(User).filter(User.is_verified == True).all()
        
        if not users:
            logger.warning("Нет верифицированных пользователей для рассылки")
            await update.message.reply_text("❌ Нет пользователей для рассылки")
            return ConversationHandler.END
        
        logger.info(f"Начало рассылки для {len(users)} пользователей")
        msg = await update.message.reply_text(f"⏳ Рассылка для {len(users)} пользователей...")
        
        success = 0
        failed = []
        
        for user in users:
            try:
                await context.bot.send_message(
                    chat_id=user.telegram_id,
                    text=f"📢 Сообщение от администратора:\n\n{text}"
                )
                success += 1
                await asyncio.sleep(0.1)
                
                # Сохраняем в историю рассылки
                broadcast_message = AdminMessage(
                    admin_telegram_id=update.effective_user.id,
                    user_telegram_id=user.telegram_id,
                    message_text=text,
                    is_broadcast=True,
                    is_unregistered=False
                )
                db.session.add(broadcast_message)
                
            except Exception as e:
                failed.append(f"{user.full_name} (ID: {user.telegram_id})")
                logger.error(f"Ошибка отправки {user.telegram_id}: {e}")
        
        # Сохраняем все сообщения рассылки
        db.session.commit()
        
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
            reply_markup=create_admin_keyboard_for(update.effective_user.id)
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