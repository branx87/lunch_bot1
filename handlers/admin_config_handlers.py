# ##handlers/admin_config_handlers.py
import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List
from telegram import (
    InlineKeyboardButton, 
    InlineKeyboardMarkup, 
    Update, 
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove
)
from telegram.ext import (
    ConversationHandler, 
    MessageHandler, 
    filters, 
    CallbackQueryHandler, 
    CommandHandler,
    ContextTypes
)
from dotenv import load_dotenv
import os
from telegram.error import BadRequest
from pathlib import Path

from database import db
from models import User, Holiday
from config import CONFIG
from constants import (
    ADD_ACCOUNTANT, ADD_ADMIN, ADD_HOLIDAY_DATE, ADD_HOLIDAY_NAME,
    ADD_PROVIDER, ADD_STAFF, CONFIG_MENU, DELETE_ACCOUNTANT, DELETE_ADMIN,
    DELETE_HOLIDAY, DELETE_PROVIDER, DELETE_STAFF, PAGE_SIZE
)
from bot_keyboards import create_admin_config_keyboard, create_admin_keyboard, create_main_menu_keyboard, get_cancel_button
from decorators import admin_filter

ENV_PATH = Path("data/configs/.env")

logger = logging.getLogger(__name__)
load_dotenv()

# В начале файла добавим новый паттерн для пагинации
PAGINATION_PATTERN = r'^(admin|provider|accountant|staff|holiday)_(prev|next)_\d+$'

# Глобальная переменная для хранения текущих страниц
current_pages: Dict[int, Dict[str, int]] = {}
PAGE_SIZE = 15  # количество сотрудников на одной странице

# Глобальная переменная для хранения данных пагинации и времени последней активности
current_pages = {}  # {user_id: {'page': int, 'items': list, 'timestamp': datetime}}

# Вспомогательные функции
async def _send_or_edit_message(update: Update, text: str, reply_markup=None):
    """Универсальная функция отправки/редактирования сообщения"""
    if update.callback_query:
        try:
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        except BadRequest:
            await update.callback_query.message.reply_text(text, reply_markup=reply_markup)
    else:
        await update.message.reply_text(text, reply_markup=reply_markup)

async def _send_response(update: Update, text: str):
    """Универсальная отправка ответа"""
    if update.callback_query:
        await update.callback_query.edit_message_text(text)
    else:
        await update.message.reply_text(text)

async def _send_error(update: Update, text: str):
    """Отправка сообщения об ошибке"""
    if update.callback_query:
        await update.callback_query.answer(text, show_alert=True)
    else:
        await update.message.reply_text(text)

def update_env_file(key: str, value: str):
    """Обновляет переменную окружения в .env файле"""
    # Читаем текущие данные
    with open(ENV_PATH, 'r') as file:
        lines = file.readlines()

    updated = False
    new_lines = []

    for line in lines:
        if line.startswith(f"{key}="):
            new_lines.append(f"{key}={value}\n")
            updated = True
        else:
            new_lines.append(line)

    if not updated:
        new_lines.append(f"{key}={value}\n")

    with open(ENV_PATH, 'w') as file:
        file.writelines(new_lines)

    # Перезагружаем конфиг в памяти
    load_dotenv(ENV_PATH, override=True)
    from database import db
    from config import CONFIG
    CONFIG.reload()

async def config_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Главное меню управления конфигурацией"""
    user_id = update.effective_user.id
    current_pages[user_id] = {'page': 0}  # Сброс страниц
    
    if user_id not in CONFIG.admin_ids:
        await update.message.reply_text("❌ У вас нет прав")
        return ConversationHandler.END
    
    await update.message.reply_text(
        "⚙️ Управление конфигурацией:",
        reply_markup=create_admin_config_keyboard()
    )
    return CONFIG_MENU

# ===== Обработчики добавления =====
async def start_add_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Введите Telegram ID нового администратора:",
        reply_markup=ReplyKeyboardMarkup([["❌ Отмена"]], resize_keyboard=True)
    )
    return ADD_ADMIN

async def handle_add_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        new_id = update.message.text.strip()
        if not re.match(r'^\d+$', new_id):
            raise ValueError

        current_ids = [str(id) for id in CONFIG.admin_ids]

        if new_id in current_ids:
            await update.message.reply_text("⚠️ Этот ID уже есть в списке")
            return CONFIG_MENU

        current_ids.append(new_id)
        update_env_file('ADMIN_IDS', ','.join(current_ids))

        await update.message.reply_text(
            f"✅ ID {new_id} добавлен в администраторы",
            reply_markup=create_admin_config_keyboard()
        )

        return CONFIG_MENU

    except ValueError:
        await update.message.reply_text("❌ Введите корректный Telegram ID (только цифры)")
        return ADD_ADMIN

# ===== Обработчики добавления =====
async def start_add_provider(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Введите Telegram ID нового поставщика:",
        reply_markup=ReplyKeyboardMarkup([["❌ Отмена"]], resize_keyboard=True, one_time_keyboard=True)
    )
    return ADD_PROVIDER

async def handle_add_provider(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        new_id = update.message.text.strip()
        if not re.match(r'^\d+$', new_id):
            raise ValueError
        
        current_ids = [str(id) for id in CONFIG.provider_ids]
        
        if new_id in current_ids:
            await update.message.reply_text("⚠️ Этот ID уже есть в списке")
            return ADD_PROVIDER
        
        current_ids.append(new_id)
        update_env_file('PROVIDER_IDS', ','.join(current_ids))
        
        await update.message.reply_text(
            f"✅ ID {new_id} добавлен в поставщики",
            reply_markup=create_admin_config_keyboard()
        )
        return CONFIG_MENU
    except ValueError:
        await update.message.reply_text("❌ Введите корректный Telegram ID (только цифры)")
        return ADD_PROVIDER

async def start_add_accountant(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Введите Telegram ID нового бухгалтера:",
        reply_markup=ReplyKeyboardMarkup([["❌ Отмена"]], resize_keyboard=True)
    )
    return ADD_ACCOUNTANT

async def handle_add_accountant(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        new_id = update.message.text.strip()
        if not re.match(r'^\d+$', new_id):
            raise ValueError
        
        current_ids = [str(id) for id in CONFIG.accounting_ids]
        
        if new_id in current_ids:
            await update.message.reply_text("⚠️ Этот ID уже есть в списке")
            return ADD_ACCOUNTANT
        
        current_ids.append(new_id)
        update_env_file('ACCOUNTING_IDS', ','.join(current_ids))
        
        await update.message.reply_text(
            f"✅ ID {new_id} добавлен в бухгалтерию",
            reply_markup=create_admin_config_keyboard()
        )
        return CONFIG_MENU
    except ValueError:
        await update.message.reply_text("❌ Введите корректный Telegram ID (только цифры)")
        return ADD_ACCOUNTANT

# ===== Обработчики удаления с пагинацией =====
def create_pagination_buttons(page: int, total_items: int, prefix: str) -> List[InlineKeyboardButton]:
    """Создает кнопки пагинации"""
    buttons = []
    if page > 0:
        buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"{prefix}_prev_{page}"))
    if (page + 1) * PAGE_SIZE < total_items:
        buttons.append(InlineKeyboardButton("Вперед ➡️", callback_data=f"{prefix}_next_{page}"))
    return buttons

async def start_delete_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    current_ids = [str(id) for id in CONFIG.admin_ids]
    
    if not current_ids:
        await update.message.reply_text("❌ В списке нет администраторов для удаления")
        return CONFIG_MENU
    
    current_pages[user_id] = {'page': 0, 'items': current_ids}
    return await show_admin_page(update, context)

async def show_admin_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    page_data = current_pages.get(user_id, {'page': 0, 'items': []})
    page = page_data['page']
    items = page_data['items']
    
    start_idx = page * PAGE_SIZE
    end_idx = start_idx + PAGE_SIZE
    page_items = items[start_idx:end_idx]
    
    keyboard = [
        [InlineKeyboardButton(f"ID: {admin_id}", callback_data=f"del_admin_{admin_id}")]
        for admin_id in page_items
    ]
    
    # Добавляем кнопки пагинации
    pagination_buttons = create_pagination_buttons(page, len(items), "admin")
    if pagination_buttons:
        keyboard.append(pagination_buttons)
    
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel_delete")])
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
            "Выберите администратора для удаления:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    else:
        await update.message.reply_text(
            "Выберите администратора для удаления:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    return DELETE_ADMIN

async def start_delete_provider(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    current_ids = [str(id) for id in CONFIG.provider_ids]
    
    if not current_ids:
        await update.message.reply_text("❌ В списке нет поставщиков для удаления")
        return CONFIG_MENU
    
    current_pages[user_id] = {'page': 0, 'items': current_ids}
    return await show_provider_page(update, context)

async def show_provider_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    page_data = current_pages.get(user_id, {'page': 0, 'items': []})
    page = page_data['page']
    items = page_data['items']
    
    start_idx = page * PAGE_SIZE
    end_idx = start_idx + PAGE_SIZE
    page_items = items[start_idx:end_idx]
    
    keyboard = [
        [InlineKeyboardButton(f"ID: {provider_id}", callback_data=f"del_provider_{provider_id}")]
        for provider_id in page_items
    ]
    
    pagination_buttons = create_pagination_buttons(page, len(items), "provider")
    if pagination_buttons:
        keyboard.append(pagination_buttons)
    
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel_delete")])
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
            "Выберите поставщика для удаления:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    else:
        await update.message.reply_text(
            "Выберите поставщика для удаления:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    return DELETE_PROVIDER

async def start_delete_accountant(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    current_ids = [str(id) for id in CONFIG.accounting_ids]
    
    if not current_ids:
        await update.message.reply_text("❌ В списке нет бухгалтеров для удаления")
        return CONFIG_MENU
    
    current_pages[user_id] = {'page': 0, 'items': current_ids}
    return await show_accountant_page(update, context)

async def show_accountant_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    page_data = current_pages.get(user_id, {'page': 0, 'items': []})
    page = page_data['page']
    items = page_data['items']
    
    start_idx = page * PAGE_SIZE
    end_idx = start_idx + PAGE_SIZE
    page_items = items[start_idx:end_idx]
    
    keyboard = [
        [InlineKeyboardButton(f"ID: {accountant_id}", callback_data=f"del_accountant_{accountant_id}")]
        for accountant_id in page_items
    ]
    
    pagination_buttons = create_pagination_buttons(page, len(items), "accountant")
    if pagination_buttons:
        keyboard.append(pagination_buttons)
    
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel_delete")])
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
            "Выберите бухгалтера для удаления:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    else:
        await update.message.reply_text(
            "Выберите бухгалтера для удаления:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    return DELETE_ACCOUNTANT

async def start_delete_staff(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало процесса удаления сотрудника с выбором режима"""
    keyboard = [
        [InlineKeyboardButton("🔍 Поиск по имени", callback_data="staff_search_mode")],
        [InlineKeyboardButton("📋 Полный список", callback_data="staff_full_list")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_delete")]
    ]
    
    await _send_or_edit_message(
        update,
        "Выберите способ поиска сотрудника:",
        InlineKeyboardMarkup(keyboard)
    )
    
    return DELETE_STAFF

async def handle_staff_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает выбор режима поиска/просмотра"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "staff_search_mode":
        await query.edit_message_text(
            "Введите часть имени или фамилии для поиска:",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="cancel_delete")]])
        )
        context.user_data['mode'] = 'search'
        return DELETE_STAFF
        
    elif query.data == "staff_full_list":
        context.user_data['mode'] = 'list'
        return await show_staff_list(update, context)

async def handle_staff_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message:  # Если это сообщение с текстом поиска
        search_query = update.message.text.strip()
        context.user_data['search_query'] = search_query
        return await show_staff_page(update, context)
    else:  # Если это callback_query
        query = update.callback_query
        await query.answer()
        
        if query.data == "staff_search":
            await query.edit_message_text(
                "Введите часть имени для поиска:",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="cancel_delete")]])
            )
            return DELETE_STAFF

async def show_staff_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Основная функция отображения списка сотрудников"""
    try:
        # Определяем режим работы (поиск или полный список)
        if 'search_text' in context.user_data:
            return await show_staff_search(update, context)
        else:
            return await show_staff_list(update, context)
    except Exception as e:
        logger.error(f"Ошибка в show_staff_page: {e}")
        await _send_error(update, "❌ Ошибка при загрузке списка")
        return CONFIG_MENU

async def show_staff_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает список сотрудников с пагинацией"""
    try:
        user_id = update.effective_user.id
        
        # Получаем всех сотрудников через SQLAlchemy
        employees = db.get_employees(active_only=False)
        if not employees:
            await _send_response(update, "❌ В базе нет сотрудников")
            return CONFIG_MENU

        # Преобразуем в список словарей для совместимости
        employees_list = []
        for emp in employees:
            employees_list.append({
                'id': emp.id,
                'full_name': emp.full_name,
                'is_deleted': emp.is_deleted
            })
        
        # Сохраняем в кеш пагинации
        current_pages[user_id] = {
            'page': current_pages.get(user_id, {}).get('page', 0),
            'items': employees_list,
            'timestamp': datetime.now()
        }
        
        return await _display_staff_page(update, context)
        
    except Exception as e:
        logger.error(f"Ошибка показа списка: {e}")
        await _send_error(update, "❌ Ошибка загрузки списка")
        return CONFIG_MENU

async def show_staff_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает результаты поиска"""
    try:
        if not update.message or not update.message.text:
            await _send_error(update, "❌ Не получен текст для поиска")
            return await start_delete_staff(update, context)
        
        user_id = update.effective_user.id
        search_text = update.message.text.strip().lower()
        
        # Сохраняем поисковый запрос
        context.user_data['search_text'] = search_text
        
        # Ищем в базе через SQLAlchemy
        all_employees = db.get_employees(active_only=False)
        found_employees = []
        
        for emp in all_employees:
            if search_text in emp.full_name.lower():
                found_employees.append({
                    'id': emp.id,
                    'full_name': emp.full_name,
                    'is_deleted': emp.is_deleted
                })
        
        if not found_employees:
            await update.message.reply_text(f"❌ По запросу '{search_text}' ничего не найдено")
            return await start_delete_staff(update, context)
        
        # Сохраняем результаты поиска
        current_pages[user_id] = {
            'page': 0,  # Сбрасываем на первую страницу
            'items': found_employees,
            'timestamp': datetime.now()
        }
        
        return await _display_staff_page(update, context, search_text)
        
    except Exception as e:
        logger.error(f"Ошибка поиска: {str(e)}", exc_info=True)
        await _send_error(update, "❌ Ошибка при поиске")
        return await start_delete_staff(update, context)

async def _display_staff_page(update: Update, context: ContextTypes.DEFAULT_TYPE, search_text=None):
    """Отображает страницу с сотрудниками"""
    user_id = update.effective_user.id
    page_data = current_pages.get(user_id)
    
    if not page_data:
        return await start_delete_staff(update, context)
    
    page = page_data['page']
    employees = page_data['items']
    page_items = employees[page*PAGE_SIZE : (page+1)*PAGE_SIZE]
    
    # Формируем клавиатуру
    keyboard = []
    for emp in page_items:
        btn_text = f"{emp['full_name']} {'❌' if emp.get('is_deleted') else ''}"
        keyboard.append([InlineKeyboardButton(btn_text, callback_data=f"del_staff_{emp['id']}")])
    
    # Кнопки пагинации
    if pagination_buttons := create_pagination_buttons(page, len(employees), "staff"):
        keyboard.append(pagination_buttons)
    
    # Кнопка возврата если это поиск
    if search_text:
        keyboard.append([InlineKeyboardButton("🔙 Назад к полному списку", callback_data="staff_show_all")])
    
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel_delete")])
    
    # Текст сообщения
    text = f"Результаты поиска по '{search_text}':" if search_text else "Выберите сотрудника:"
    
    await _send_or_edit_message(
        update,
        text,
        InlineKeyboardMarkup(keyboard)
    )
    
    return DELETE_STAFF

async def show_holiday_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        page_data = current_pages.get(user_id)
        
        if not page_data:
            return await start_delete_holiday(update, context)
            
        page = page_data['page']
        holidays = page_data['items']

        start_idx = page * PAGE_SIZE
        end_idx = start_idx + PAGE_SIZE
        page_items = holidays[start_idx:end_idx]

        keyboard = []
        for holiday in page_items:
            date_str = holiday.get('date', '')
            name = holiday.get('name', 'Неизвестный праздник')
            if date_str:
                try:
                    # 🔥 ПРЕОБРАЗУЕМ строку "YYYY-MM-DD" в "DD.MM.YYYY" для отображения
                    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                    display_date = date_obj.strftime("%d.%m.%Y")
                    keyboard.append([InlineKeyboardButton(f"{display_date} — {name}", callback_data=f"del_holiday_{date_str}")])
                except ValueError:
                    # Если формат неправильный, используем как есть
                    keyboard.append([InlineKeyboardButton(f"{date_str} — {name}", callback_data=f"del_holiday_{date_str}")])

        pagination_buttons = create_pagination_buttons(page, len(holidays), "holiday")
        if pagination_buttons:
            keyboard.append(pagination_buttons)

        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel_delete")])
        reply_markup = InlineKeyboardMarkup(keyboard)

        if update.callback_query:
            await update.callback_query.edit_message_text(
                "Выберите праздник для удаления:",
                reply_markup=reply_markup
            )
        else:
            await update.message.reply_text(
                "Выберите праздник для удаления:",
                reply_markup=reply_markup
            )

        return DELETE_HOLIDAY

    except Exception as e:
        logger.error(f"Ошибка при отображении списка праздников: {e}", exc_info=True)
        error_msg = "❌ Произошла ошибка при загрузке списка праздников"
        if update.callback_query:
            await update.callback_query.answer(error_msg, show_alert=True)
        elif update.message:
            await update.message.reply_text(error_msg)
        return CONFIG_MENU

async def handle_deletion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        # Добавляем импорт CONFIG в начале функции
        from database import db
        from config import CONFIG
        
        if query.data == "staff_show_all":
            context.user_data.pop('search_text', None)
            return await show_staff_list(update, context)
        
        if not query or not query.data:
            return CONFIG_MENU

        # Обработка отмены
        if query.data == "cancel_delete":
            try:
                await query.edit_message_text(
                    "❌ Операция отменена",
                    reply_markup=create_admin_config_keyboard()
                )
            except Exception as e:
                await query.message.reply_text(
                    "❌ Операция отменена",
                    reply_markup=create_admin_config_keyboard()
                )
                await query.message.delete()
            return CONFIG_MENU

        # Удаление администратора
        if query.data.startswith("del_admin_"):
            admin_id = query.data.split('_')[2]
            current_ids = [str(id) for id in CONFIG.admin_ids if str(id) != admin_id]
            update_env_file('ADMIN_IDS', ','.join(current_ids))
            CONFIG.reload()  # Обновляем конфиг
            try:
                await query.edit_message_text(
                    f"✅ Администратор {admin_id} удален",
                    reply_markup=create_admin_config_keyboard()
                )
            except Exception as e:
                await query.message.reply_text(
                    f"✅ Администратор {admin_id} удален",
                    reply_markup=create_admin_config_keyboard()
                )
                await query.message.delete()
            return CONFIG_MENU

        # Удаление поставщика
        elif query.data.startswith("del_provider_"):
            provider_id = query.data.split('_')[2]
            current_ids = [str(id) for id in CONFIG.provider_ids if str(id) != provider_id]
            update_env_file('PROVIDER_IDS', ','.join(current_ids))
            CONFIG.reload()  # Обновляем конфиг
            try:
                await query.edit_message_text(
                    f"✅ Поставщик {provider_id} удален",
                    reply_markup=create_admin_config_keyboard()
                )
            except Exception as e:
                await query.message.reply_text(
                    f"✅ Поставщик {provider_id} удален",
                    reply_markup=create_admin_config_keyboard()
                )
                await query.message.delete()
            return CONFIG_MENU

        # Удаление бухгалтера
        elif query.data.startswith("del_accountant_"):
            accountant_id = query.data.split('_')[2]
            current_ids = [str(id) for id in CONFIG.accounting_ids if str(id) != accountant_id]
            update_env_file('ACCOUNTING_IDS', ','.join(current_ids))
            CONFIG.reload()  # Обновляем конфиг
            try:
                await query.edit_message_text(
                    f"✅ Бухгалтер {accountant_id} удален",
                    reply_markup=create_admin_config_keyboard()
                )
            except Exception as e:
                await query.message.reply_text(
                    f"✅ Бухгалтер {accountant_id} удален",
                    reply_markup=create_admin_config_keyboard()
                )
                await query.message.delete()
            return CONFIG_MENU

        # Удаление/восстановление сотрудника
        elif query.data.startswith("del_staff_"):
            staff_id = int(query.data.split('_')[2])
            
            try:
                # Получаем данные из БД через SQLAlchemy
                employee = db.session.query(User).filter(User.id == staff_id).first()
                
                if not employee:
                    await query.answer("❌ Сотрудник не найден", show_alert=True)
                    return CONFIG_MENU

                if employee.is_deleted:
                    # Восстанавливаем сотрудника (с автоматической верификацией)
                    employee.is_deleted = False
                    employee.is_verified = True
                    employee.updated_at = datetime.now()
                    message = f"✅ Сотрудник '{employee.full_name}' восстановлен"
                else:
                    # Деактивируем сотрудника (с автоматической деверификацией)
                    employee.is_deleted = True
                    employee.is_verified = False
                    employee.updated_at = datetime.now()
                    message = f"✅ Сотрудник '{employee.full_name}' деактивирован"

                db.session.commit()

                # Обновляем интерфейс
                try:
                    await query.edit_message_text(
                        message,
                        reply_markup=create_admin_config_keyboard()
                    )
                except BadRequest:
                    await query.message.reply_text(
                        message,
                        reply_markup=create_admin_config_keyboard()
                    )
                    await query.message.delete()

                # Показываем обновленный список
                return await show_staff_page(update, context)

            except Exception as e:
                logger.error(f"Ошибка изменения статуса сотрудника: {e}")
                await query.answer("❌ Ошибка при изменении статуса", show_alert=True)
                return CONFIG_MENU

        # Удаление праздника
        elif query.data.startswith("del_holiday_"):
            holiday_date = query.data.split('_')[2]
            
            # Удаляем праздник через SQLAlchemy
            result = db.session.query(Holiday).filter(Holiday.date == holiday_date).delete()
            db.session.commit()
                
            # ОБНОВЛЯЕМ КОНФИГУРАЦИЮ БЕЗ ПЕРЕЗАПУСКА
            from database import db
            from config import CONFIG
            CONFIG.reload()
                
            try:
                await query.edit_message_text(
                    "✅ Праздник удалён и больше не учитывается в боте",
                    reply_markup=create_admin_config_keyboard()
                )
            except Exception as e:
                await query.message.reply_text(
                    "✅ Праздник удалён и больше не учитывается в боте",
                    reply_markup=create_admin_config_keyboard()
                )
                await query.message.delete()
            return CONFIG_MENU

        return CONFIG_MENU

    except Exception as e:
        logger.error(f"Ошибка при удалении: {e}", exc_info=True)
        try:
            await query.answer("❌ Ошибка при удалении", show_alert=True)
        except:
            pass
        return CONFIG_MENU
    
async def handle_add_staff(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_input = update.message.text.strip()

    if user_input.lower() in ["отмена", "❌ отмена"]:
        await update.message.reply_text(
            "❌ Добавление сотрудника отменено",
            reply_markup=create_admin_config_keyboard()
        )
        return CONFIG_MENU

    name_parts = user_input.split()
    if len(name_parts) < 2 or not all(part.isalpha() for part in name_parts):
        await update.message.reply_text(
            "❌ Введите корректное ФИО (минимум 2 слова, только буквы):",
            reply_markup=ReplyKeyboardMarkup([["❌ Отмена"]], resize_keyboard=True)
        )
        return ADD_STAFF

    full_name = ' '.join(word.capitalize() for word in name_parts)

    # Проверяем, существует ли сотрудник с таким именем через SQLAlchemy
    existing = db.session.query(User).filter(
        User.full_name.ilike(full_name),
        User.is_employee == True
    ).first()

    if existing:
        if existing.is_deleted:
            # Восстанавливаем удаленного сотрудника
            existing.is_deleted = False
            existing.is_verified = False
            existing.telegram_id = None
            existing.updated_at = datetime.now()
            db.session.commit()
            
            await update.message.reply_text(
                f"✅ Сотрудник '{full_name}' восстановлен (ожидает регистрации)",
                reply_markup=create_admin_config_keyboard()
            )
        else:
            await update.message.reply_text(
                "⚠️ Такой сотрудник уже существует",
                reply_markup=create_admin_config_keyboard()
            )
        return CONFIG_MENU

    # Добавляем нового сотрудника без Telegram ID через SQLAlchemy
    new_employee = User(
        full_name=full_name,
        is_employee=True,
        is_verified=False,
        telegram_id=None,
        created_at=datetime.now(),
        updated_at=datetime.now()
    )
    db.session.add(new_employee)
    db.session.commit()

    await update.message.reply_text(
        f"✅ Сотрудник '{full_name}' добавлен. Теперь он может пройти регистрацию.",
        reply_markup=create_admin_config_keyboard()
    )
    return CONFIG_MENU

async def start_delete_holiday(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    try:
        # 🔥 ИСПРАВЛЕНИЕ: поле date уже строка, не нужно вызывать strftime()
        with db.get_session() as session:
            holidays = session.query(Holiday).order_by(Holiday.date).all()
            
            if not holidays:
                await update.message.reply_text("❌ В базе нет праздников для удаления")
                return CONFIG_MENU
            
            # 🔥 date уже строка в формате "YYYY-MM-DD"
            holidays_list = []
            for holiday in holidays:
                holidays_list.append({
                    'date': holiday.date,  # Уже строка "2025-10-13"
                    'name': holiday.name
                })
        
        current_pages[user_id] = {'page': 0, 'items': holidays_list}
        return await show_holiday_page(update, context)
        
    except Exception as e:
        logger.error(f"Ошибка при получении списка праздников: {e}", exc_info=True)
        await update.message.reply_text("❌ Ошибка при загрузке списка праздников")
        return CONFIG_MENU

async def force_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Принудительный выход из любого состояния"""
    user_id = update.effective_user.id
    
    # Удаляем данные пользователя из памяти
    if user_id in current_pages:
        del current_pages[user_id]

    # Отправляем сообщение о выходе
    if update.message:
        await update.message.reply_text(
            "✅ Возврат в главное меню",
            reply_markup=create_admin_keyboard()
        )
    elif update.callback_query:
        try:
            await update.callback_query.edit_message_text(
                "✅ Возврат в главное меню",
                reply_markup=create_admin_keyboard()
            )
        except BadRequest:
            await update.effective_message.reply_text(
                "✅ Возврат в главное меню",
                reply_markup=create_admin_keyboard()
            )

    return ConversationHandler.END

async def start_add_holiday(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запрашивает у пользователя дату праздника для добавления в базу."""
    await update.message.reply_text(
        "Введите дату праздника в формате ДД.ММ.ГГГГ:",
        reply_markup=ReplyKeyboardMarkup([["❌ Отмена"]], resize_keyboard=True)
    )
    return ADD_HOLIDAY_DATE

async def start_add_staff(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало добавления сотрудника"""
    await update.message.reply_text(
        "Введите ФИО нового сотрудника (например, Иванов Иван):",
        reply_markup=ReplyKeyboardMarkup([["❌ Отмена"]], resize_keyboard=True)
    )
    return ADD_STAFF

async def cancel_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Универсальная функция отмены."""
    try:
        reply_markup = create_admin_config_keyboard()
        if update.message:
            await update.message.reply_text(
                "❌ Операция отменена",
                reply_markup=reply_markup
            )
        elif update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(
                "❌ Операция отменена",
                reply_markup=reply_markup
            )
    except Exception as e:
        logger.error(f"Ошибка при отмене: {e}")
        if update.message:
            await update.message.reply_text(
                "❌ Операция отменена",
                reply_markup=reply_markup
            )
    
    return CONFIG_MENU

async def handle_holiday_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает введенную дату праздника, проверяет ее формат"""
    try:
        date_str = update.message.text.strip()
        
        # Проверка на отмену
        if date_str.lower() in ["отмена", "❌ отмена"]:
            await update.message.reply_text(
                "❌ Добавление праздника отменено",
                reply_markup=create_admin_config_keyboard()
            )
            return CONFIG_MENU
            
        date_obj = datetime.strptime(date_str, "%d.%m.%Y").date()
        context.user_data['holiday_date'] = date_obj.strftime("%Y-%m-%d")
        
        await update.message.reply_text(
            "Введите название праздника:",
            reply_markup=ReplyKeyboardMarkup([["❌ Отмена"]], resize_keyboard=True)
        )
        return ADD_HOLIDAY_NAME
        
    except ValueError:
        await update.message.reply_text(
            "❌ Неверный формат даты. Используйте ДД.ММ.ГГГГ",
            reply_markup=ReplyKeyboardMarkup([["❌ Отмена"]], resize_keyboard=True)
        )
        return ADD_HOLIDAY_DATE

async def handle_holiday_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает введенное название праздника и сохраняет в БД"""
    holiday_name = update.message.text.strip()
    
    # Проверка на отмену
    if holiday_name.lower() in ["отмена", "❌ отмена"]:
        await update.message.reply_text(
            "❌ Добавление праздника отменено",
            reply_markup=create_admin_config_keyboard()
        )
        return CONFIG_MENU
    
    if not holiday_name:
        await update.message.reply_text(
            "❌ Название праздника не может быть пустым",
            reply_markup=ReplyKeyboardMarkup([["❌ Отмена"]], resize_keyboard=True)
        )
        return ADD_HOLIDAY_NAME
    
    date_str = context.user_data['holiday_date']
    
    try:
        # 🔥 ИСПРАВЛЕНИЕ: используем прямой SQLAlchemy запрос вместо db.add_holiday()
        with db.get_session() as session:
            # Проверяем, существует ли уже праздник с такой датой
            existing_holiday = session.query(Holiday).filter(Holiday.date == date_str).first()
            
            if existing_holiday:
                await update.message.reply_text(
                    "⚠️ Такой праздник уже существует",
                    reply_markup=create_admin_config_keyboard()
                )
                return CONFIG_MENU
            
            # Создаем новый праздник
            new_holiday = Holiday(
                date=date_str,
                name=holiday_name,
                is_recurring=False
            )
            session.add(new_holiday)
            session.commit()
            
        # 🔥 ОБНОВЛЯЕМ КОНФИГУРАЦИЮ
        from config import CONFIG
        CONFIG.reload()
        
        await update.message.reply_text(
            f"✅ Праздник '{holiday_name}' на {date_str} добавлен и сразу доступен в боте",
            reply_markup=create_admin_config_keyboard()
        )
        return CONFIG_MENU
        
    except Exception as e:
        logger.error(f"Ошибка при добавлении праздника: {e}", exc_info=True)
        await update.message.reply_text(
            "❌ Произошла ошибка. Попробуйте позже.",
            reply_markup=create_admin_config_keyboard()
        )
        return CONFIG_MENU

async def cancel_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отменяет текущий диалог и возвращает в главное меню"""
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text("Диалог завершен")
    
    await update.message.reply_text(
        "Возврат в главное меню",
        reply_markup=create_main_menu_keyboard(update.effective_user.id)
    )
    
    # Очищаем состояние
    if 'user_data' in context:
        context.user_data.clear()
    
    return ConversationHandler.END

def setup_admin_config_handlers(application):
    conv_handler = ConversationHandler(
        entry_points=[
            MessageHandler(
                filters.Regex("^⚙️ Управление конфигурацией$") &
                admin_filter,
                config_menu
            )
        ],
        states={
            CONFIG_MENU: [
                MessageHandler(filters.Regex("^(Добавить администратора|➕ Добавить администратора)$"), start_add_admin),
                MessageHandler(filters.Regex("^(Добавить поставщика|➕ Добавить поставщика)$"), start_add_provider),
                MessageHandler(filters.Regex("^(Добавить бухгалтера|➕ Добавить бухгалтера)$"), start_add_accountant),
                MessageHandler(filters.Regex("^(Добавить сотрудника|➕ Добавить сотрудника)$"), start_add_staff),
                MessageHandler(filters.Regex("^(Добавить праздник|➕ Добавить праздник)$"), start_add_holiday),
                MessageHandler(filters.Regex("^(Удалить администратора|➖ Удалить администратора)$"), start_delete_admin),
                MessageHandler(filters.Regex("^(Удалить поставщика|➖ Удалить поставщика)$"), start_delete_provider),
                MessageHandler(filters.Regex("^(Удалить бухгалтера|➖ Удалить бухгалтера)$"), start_delete_accountant),
                MessageHandler(filters.Regex("^(Удалить сотрудника|➖ Удалить сотрудника)$"), start_delete_staff),
                MessageHandler(filters.Regex("^(Удалить праздник|➖ Удалить праздник)$"), start_delete_holiday),
                MessageHandler(filters.Regex("^(Назад|❌ Отмена)$"), cancel_config),
            ],
            ADD_ADMIN: [
                MessageHandler(filters.Regex("^(❌ Отмена|Отмена)$"), cancel_config),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_admin)
            ],
            ADD_PROVIDER: [
                MessageHandler(filters.Regex("^(❌ Отмена|Отмена)$"), cancel_config),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_provider)
            ],
            ADD_ACCOUNTANT: [
                MessageHandler(filters.Regex("^(❌ Отмена|Отмена)$"), cancel_config),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_accountant)
            ],
            ADD_STAFF: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_staff),
                MessageHandler(filters.Regex("^(❌ Отмена|Отмена)$"), cancel_config)
            ],
            ADD_HOLIDAY_DATE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_holiday_date),
                MessageHandler(filters.Regex("^(❌ Отмена|Отмена)$"), cancel_config)
            ],
            ADD_HOLIDAY_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_holiday_name),
                MessageHandler(filters.Regex("^(❌ Отмена|Отмена)$"), cancel_config)
            ],
            DELETE_ADMIN: [
                CallbackQueryHandler(handle_deletion, pattern=r'^del_admin_\d+|cancel_delete$'),
                CallbackQueryHandler(handle_pagination, pattern=PAGINATION_PATTERN)
            ],
            DELETE_PROVIDER: [
                CallbackQueryHandler(handle_deletion, pattern=r'^del_provider_\d+|cancel_delete$'),
                CallbackQueryHandler(handle_pagination, pattern=PAGINATION_PATTERN)
            ],
            DELETE_ACCOUNTANT: [
                CallbackQueryHandler(handle_deletion, pattern=r'^del_accountant_\d+|cancel_delete$'),
                CallbackQueryHandler(handle_pagination, pattern=PAGINATION_PATTERN)
            ],
            DELETE_STAFF: [
                CallbackQueryHandler(handle_staff_mode, pattern="^staff_(search_mode|full_list)"),
                CallbackQueryHandler(handle_deletion, pattern=r"^del_staff_\d+|cancel_delete|staff_show_all"),
                CallbackQueryHandler(handle_pagination, pattern=PAGINATION_PATTERN),
                MessageHandler(filters.TEXT & ~filters.COMMAND, show_staff_search)
            ],
            DELETE_HOLIDAY: [
                CallbackQueryHandler(handle_deletion, pattern=r'^del_holiday_|cancel_delete$'),
                CallbackQueryHandler(handle_pagination, pattern=PAGINATION_PATTERN)
            ],
        },
        fallbacks=[
            CommandHandler('cancel', cancel_config),
            MessageHandler(filters.Regex("^Отмена$"), cancel_config),
            CallbackQueryHandler(handle_deletion, pattern=r'^cancel_delete$')
        ],
        allow_reentry=True
    )
    application.add_handler(conv_handler)
    
# Добавим новую функцию для обработки пагинации
async def handle_pagination(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        user_id = update.effective_user.id
        entity_type, action, current_page = query.data.split('_')
        current_page = int(current_page)
        
        # Получаем текущие данные пагинации
        page_data = current_pages.get(user_id, {'page': 0, 'items': []})
        
        # Обновляем страницу
        if action == 'next':
            page_data['page'] = current_page + 1
        elif action == 'prev':
            page_data['page'] = max(0, current_page - 1)
        
        current_pages[user_id] = page_data
        
        # Перенаправляем на соответствующую функцию отображения
        if entity_type == 'admin':
            return await show_admin_page(update, context)
        elif entity_type == 'provider':
            return await show_provider_page(update, context)
        elif entity_type == 'accountant':
            return await show_accountant_page(update, context)
        elif entity_type == 'staff':
            return await show_staff_page(update, context)
        elif entity_type == 'holiday':
            return await show_holiday_page(update, context)
            
    except Exception as e:
        logger.error(f"Ошибка пагинации: {e}")
        await query.answer("Ошибка при переключении страницы", show_alert=True)
        return CONFIG_MENU