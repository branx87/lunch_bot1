# ##handlers/provider_handlers.py
from telegram import ReplyKeyboardMarkup, Update
from telegram.ext import ConversationHandler, MessageHandler, filters
from telegram.ext import ContextTypes
from database import db
from models import Menu
from config import CONFIG
from constants import (
    EDIT_MENU_DAY, 
    EDIT_MENU_FIRST, 
    EDIT_MENU_MAIN, 
    EDIT_MENU_SALAD
)
from handlers.admin_config_handlers import cancel_config
from bot_keyboards import create_provider_menu_keyboard

async def edit_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Начинает процесс редактирования меню.
    Показывает клавиатуру с днями недели для выбора.
    """
    await update.message.reply_text(
        "Выберите день недели для редактирования (или 'Отмена' для выхода):",
        reply_markup=ReplyKeyboardMarkup([
            ["Понедельник", "Вторник", "Среда"],
            ["Четверг", "Пятница", "Суббота"],
            ["Воскресенье", "❌ Отмена"]
        ], resize_keyboard=True)
    )
    return EDIT_MENU_DAY

async def handle_menu_day(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обрабатывает выбор дня недели для редактирования.
    """
    day = update.message.text
    
    # Проверяем команду отмены
    if day in ["❌ Отмена", "Отмена"]:
        await update.message.reply_text(
            "Редактирование меню отменено",
            reply_markup=create_provider_menu_keyboard()
        )
        return ConversationHandler.END
    
    days_of_week = ["Понедельник", "Вторник", "Среда", 
                   "Четверг", "Пятница", "Суббота", "Воскресенье"]
    
    if day not in days_of_week:
        await update.message.reply_text("❌ Выберите день из списка")
        return EDIT_MENU_DAY
    
    context.user_data['edit_menu_day'] = day
    
    # Загружаем текущее меню для этого дня через SQLAlchemy
    current_menu = db.session.query(Menu).filter(Menu.day == day).first()
    if current_menu:
        context.user_data['current_first'] = current_menu.first_course
        context.user_data['current_main'] = current_menu.main_course
        context.user_data['current_salad'] = current_menu.salad
    
    await update.message.reply_text(
        f"Текущее первое блюдо: {context.user_data.get('current_first', 'не указано')}\n"
        "Введите новое первое блюдо (или 'Отмена' для выхода):"
    )
    return EDIT_MENU_FIRST

async def handle_menu_first(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обрабатывает ввод первого блюда.
    """
    text = update.message.text
    
    # Проверяем команду отмены
    if text in ["❌ Отмена", "Отмена"]:
        await update.message.reply_text(
            "Редактирование меню отменено",
            reply_markup=create_provider_menu_keyboard()
        )
        return ConversationHandler.END
    
    context.user_data['first'] = text
    await update.message.reply_text(
        f"Текущее основное блюдо: {context.user_data.get('current_main', 'не указано')}\n"
        "Введите новое основное блюдо (или 'Отмена' для выхода):"
    )
    return EDIT_MENU_MAIN

async def handle_menu_main(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обрабатывает ввод основного блюда.
    """
    text = update.message.text
    
    # Проверяем команду отмены
    if text in ["❌ Отмена", "Отмена"]:
        await update.message.reply_text(
            "Редактирование меню отменено",
            reply_markup=create_provider_menu_keyboard()
        )
        return ConversationHandler.END
    
    context.user_data['main'] = text
    await update.message.reply_text(
        f"Текущий салат: {context.user_data.get('current_salad', 'не указано')}\n"
        "Введите новый салат (или 'Отмена' для выхода):"
    )
    return EDIT_MENU_SALAD

async def handle_menu_salad(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Завершает процесс редактирования меню.
    Сохраняет изменения в базе данных и обновляет конфиг.
    """
    day = context.user_data['edit_menu_day']
    first = context.user_data['first']
    main = context.user_data['main']
    salad = update.message.text
    
    try:
        # Сохраняем в базу данных через SQLAlchemy
        # Сначала проверяем, существует ли уже меню для этого дня
        existing_menu = db.session.query(Menu).filter(Menu.day == day).first()
        
        if existing_menu:
            # Обновляем существующее меню
            existing_menu.first_course = first
            existing_menu.main_course = main
            existing_menu.salad = salad
        else:
            # Создаем новое меню
            new_menu = Menu(
                day=day,
                first_course=first,
                main_course=main,
                salad=salad
            )
            db.session.add(new_menu)
        
        db.session.commit()
        
        # Обновляем конфиг в памяти
        CONFIG.menu[day] = {
            "first": first,
            "main": main,
            "salad": salad
        }
        
        await update.message.reply_text(
            f"✅ Меню на {day} успешно обновлено!\n"
            f"Первое: {first}\n"
            f"Основное: {main}\n"
            f"Салат: {salad}",
            reply_markup=create_provider_menu_keyboard()
        )
    except Exception as e:
        await update.message.reply_text(
            "❌ Ошибка при сохранении меню. Попробуйте позже.",
            reply_markup=create_provider_menu_keyboard()
        )
    
    return ConversationHandler.END

def setup_provider_handlers(application):
    """
    Настраивает обработчики для функционала поставщиков.
    """
    conv_handler = ConversationHandler(
        entry_points=[MessageHandler(
            filters.Regex("^✏️ Изменить меню$") & 
            (filters.User(user_id=CONFIG.provider_ids) | filters.User(user_id=CONFIG.admin_ids)),  # Добавлены админы
            edit_menu
        )],
        states={
            EDIT_MENU_DAY: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_menu_day)],
            EDIT_MENU_FIRST: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_menu_first)],
            EDIT_MENU_MAIN: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_menu_main)],
            EDIT_MENU_SALAD: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_menu_salad)]
        },
        fallbacks=[
            MessageHandler(filters.Regex("^(❌ Отмена|Отмена)$"), cancel_config)
        ],
        allow_reentry=True
    )
    application.add_handler(conv_handler)