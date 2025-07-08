# ##report_generators.py
from openpyxl.styles import Font
from typing import Optional
import openpyxl
from datetime import datetime, date
from telegram import Update
from telegram.ext import ContextTypes
import os
import logging
import matplotlib

from db import CONFIG
matplotlib.use('Agg')  # Устанавливаем бэкенд, не требующий GUI
import matplotlib.pyplot as plt
try:
    from openpyxl.styles import Font
except RuntimeError:  # Для окружений без GUI
    class Font:
        def __init__(self, bold=False):
            self.bold = bold

from admin import ensure_reports_dir
from settings import SETTINGS_CONFIG

from db import db

logger = logging.getLogger(__name__)

async def export_orders_for_provider(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
):
    """
    Формирует текстовый отчёт для поставщиков.
    Объединяет данные по локациям "Офис" и "ПЦ 2".
    Добавляет строку с незарегистрированными сотрудниками, если такие есть.
    """
    try:
        # Если даты не заданы — используем сегодняшнюю
        if not start_date or not end_date:
            today = datetime.now(CONFIG.timezone).date()
            start_date = end_date = today
        else:
            start_date = start_date if isinstance(start_date, date) else start_date.date()
            end_date = end_date if isinstance(end_date, date) else end_date.date()

        # Получаем данные по локациям
        db.cursor.execute('''
            SELECT COALESCE(u.location, 'Не указано'), SUM(o.quantity)
            FROM orders o
            JOIN users u ON o.user_id = u.id
            WHERE o.target_date BETWEEN ? AND ?
              AND o.is_cancelled = FALSE
            GROUP BY COALESCE(u.location, 'Не указано')
        ''', (start_date.isoformat(), end_date.isoformat()))

        location_data = dict(db.cursor.fetchall())

        # Объединяем "Офис" и "ПЦ 2"
        office_portions = location_data.get("Офис", 0) + location_data.get("ПЦ 2", 0)
        pc1_portions = location_data.get("ПЦ 1", 0)
        warehouse_portions = location_data.get("Склад", 0)
        unregistered_portions = location_data.get("Не указано", 0)

        total_portions = office_portions + pc1_portions + warehouse_portions + unregistered_portions

        # Формируем текстовое сообщение
        period_text = (
            f"{start_date.strftime('%d.%m.%Y')}"
            if start_date == end_date
            else f"{start_date.strftime('%d.%m.%Y')} — {end_date.strftime('%d.%m.%Y')}"
        )

        # Основные строки отчета
        message_lines = [
            f"📋 *Заказы на* | {period_text}",
            f"━━━━━━━━━━━━━━━━━━",
            f"📌 Всего: *{total_portions}* порций\n",
            f"• 🏢 Офис: *{office_portions}*",
            f"• 🏭 ПЦ 1: *{pc1_portions}*",
            f"• 📦 Склад: *{warehouse_portions}*"
        ]

        # Добавляем строку с незарегистрированными только если они есть
        if unregistered_portions > 0:
            message_lines.append(f"• ❓ Незарегистрированные: *{unregistered_portions}*")

        # Собираем финальное сообщение
        message = "\n".join(message_lines)

        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=message,
            parse_mode="Markdown"
        )

    except Exception as e:
        logger.error(f"Ошибка при создании отчета для поставщика: {e}", exc_info=True)
        await update.message.reply_text("❌ Ошибка при формировании отчёта.")
        raise
    
async def export_accounting_report(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
):
    try:
        # 1. Настройка форматирования чисел
        import locale
        try:
            locale.setlocale(locale.LC_NUMERIC, 'ru_RU.UTF-8')
        except:
            pass

        # 2. Словарь месяцев
        month_names = {
            1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
            5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
            9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь"
        }

        reports_dir = ensure_reports_dir('accounting')
        now = datetime.now(CONFIG.timezone)
        month_year = f"{month_names[now.month]} {now.year}"

        # Обработка дат
        if not start_date or not end_date:
            start_date = end_date = now.date()
        else:
            start_date = start_date if isinstance(start_date, date) else start_date.date()
            end_date = end_date if isinstance(end_date, date) else end_date.date()

        if start_date > end_date:
            start_date, end_date = end_date, start_date

        # Создаем Excel файл
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Удержания за обеды"
        
        # Заголовки
        ws.append(["Список сотрудников на удержание обедов из ежемесячной премии"])
        ws.append([f"за {month_year} г."])
        ws.append([])
        ws.append(["", "удержание стоимости 1 обеда составляет", "150,00 руб. (без НДФЛ)"])
        ws.append(["", "", "172,41 руб. (с НДФЛ 13%)"])
        ws.append([])
        
        # Заголовки таблицы
        headers = [
            "Подразделение",
            "ФИО",
            "Кол-во обедов",
            "Должность",
            "Территория",
            "Дата приема",
            "Сумма удержания без НДФЛ",
            "Сумма удержания с НДФЛ"
        ]
        ws.append(headers)
        ws.auto_filter.ref = f"A{ws.max_row}:H{ws.max_row}"

        # Запрос с заглушками для всех недостающих полей
        query = '''
            SELECT 
                'Основной офис' as department,
                u.full_name,
                SUM(o.quantity) as portions,
                'Сотрудник' as position,
                'Не указана' as location,
                'Не указана' as hire_date
            FROM orders o
            JOIN users u ON o.user_id = u.id
            WHERE o.target_date BETWEEN ? AND ?
              AND o.is_cancelled = FALSE
            GROUP BY u.id
            ORDER BY u.full_name
        '''

        # Инициализация переменных
        total_portions = 0
        total_without_ndfl = 0
        total_with_ndfl = 0

        # Выполнение запроса
        db.cursor.execute(query, (start_date.isoformat(), end_date.isoformat()))
        rows = db.cursor.fetchall()

        if not rows:
            ws.append(["Нет данных за выбранный период", "", "", "", "", "", "", ""])
        else:
            for row in rows:
                portions = row[2]
                amount_without_ndfl = portions * 150
                amount_with_ndfl = round(amount_without_ndfl / 0.87, 2)
                
                ws.append([
                    row[0],  # department
                    row[1],  # full_name
                    portions,
                    row[3],  # position
                    row[4],  # location
                    row[5],  # hire_date
                    amount_without_ndfl,  # числовое значение
                    amount_with_ndfl      # числовое значение
                ])
                
                total_portions += portions
                total_without_ndfl += amount_without_ndfl
                total_with_ndfl += amount_with_ndfl

            # Итоговая строка
            ws.append([
                "ВСЕГО",
                "",
                total_portions,
                "",
                "",
                "",
                total_without_ndfl,  # числовое значение
                total_with_ndfl      # числовое значение
            ])

        # Форматирование
        bold_font = Font(bold=True)
        money_format = '# ##0.00'
        
        # Применение стилей
        for row in ws.iter_rows(min_row=1, max_row=6):
            for cell in row:
                cell.font = bold_font
                
        for cell in ws[7]:
            cell.font = bold_font
            
        for cell in ws[ws.max_row]:
            cell.font = bold_font
        
        # Формат денежных значений (применяем к числовым ячейкам)
        for row in ws.iter_rows(min_row=8, max_row=ws.max_row):
            for cell in row[6:8]:
                cell.number_format = money_format
                # Для правильного отображения чисел в Excel
                if isinstance(cell.value, (int, float)):
                    cell.value = float(cell.value)
        
        # Автоподбор ширины столбцов
        column_widths = {}
        for row in ws.iter_rows():
            for cell in row:
                length = len(str(cell.value)) * 1.2
                if cell.column_letter not in column_widths or length > column_widths[cell.column_letter]:
                    column_widths[cell.column_letter] = length
        
        for col, width in column_widths.items():
            ws.column_dimensions[col].width = min(width, 50)
        
        # Сохранение файла
        file_name = f"salary_deductions_{now.strftime('%Y%m')}.xlsx"
        file_path = os.path.join(reports_dir, file_name)
        wb.save(file_path)
        
        # Функция форматирования валюты для вывода в сообщении
        def format_currency(amount):
            return f"{float(amount):,.2f}".replace(",", " ").replace(".", ",")

        # Отправка файла
        caption = (
            f"📋 Отчет для удержаний из зарплаты\n"
            f"📅 Период: {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}\n"
            f"🍽 Всего обедов: {total_portions}\n"
            f"💰 Сумма удержания: {format_currency(total_with_ndfl)} руб. (с НДФЛ)"
        )

        with open(file_path, 'rb') as file:
            await context.bot.send_document(
                chat_id=update.effective_chat.id,
                document=file,
                caption=caption,
                filename=file_name
            )

        return file_path

    except Exception as e:
        logger.error(f"Ошибка формирования отчета: {e}", exc_info=True)
        await update.message.reply_text("❌ Произошла ошибка при создании отчета.")
        raise
    
async def export_monthly_report(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
):
    """Генерация отчета для удержания из зарплаты"""
    try:
        if update.effective_user.id not in CONFIG.admin_ids:
            await update.message.reply_text("❌ У вас нет прав для выполнения этой команды.")
            return

        now = datetime.now(CONFIG.timezone)
        
        # Если даты не переданы - используем текущий месяц
        if not start_date or not end_date:
            start_date = now.replace(day=1).date()
            end_date = now.date()
        else:
            # Проверяем, что start_date <= end_date
            if start_date > end_date:
                start_date, end_date = end_date, start_date

        reports_dir = ensure_reports_dir('salary_deductions')
        
        # Создаем Excel файл
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Удержания за обед"
        
        # Заголовок отчета
        ws.append(["Список сотрудников на удержание обедов из ежем.премии сотрудников"])
        ws.append([f"за {start_date.strftime('%B %Y')} г."])
        ws.append([])
        ws.append(["", "удержание стоимости 1 обеда составляет", "150"])
        ws.append([])
        
        # Заголовки таблицы
        headers = [
            "Подразделение",
            "ФИО Битрикс",
            "Кол-во обедов",
            "Должность",
            "Территория",
            "Дата приема",
            "Сумма удержания без НДФЛ",
            "Сумма удержания с НДФЛ"
        ]
        ws.append(headers)
        
        # Получаем данные из БД
        query = '''
            SELECT 
                COALESCE(u.department, 'Не указано') as department,
                u.full_name,
                SUM(o.quantity) as total_portions,
                COALESCE(u.position, 'Не указано') as position,
                COALESCE(u.location, 'Не указано') as location,
                CASE 
                    WHEN u.hire_date IS NULL THEN 'Не указана'
                    ELSE u.hire_date 
                END as hire_date
            FROM orders o
            JOIN users u ON o.user_id = u.id
            WHERE o.target_date BETWEEN ? AND ?
            AND o.is_cancelled = FALSE
            AND u.is_deleted = FALSE
            GROUP BY u.id
            ORDER BY department, u.full_name
        '''
        db.cursor.execute(query, (start_date.isoformat(), end_date.isoformat()))
        
        total_portions = 0
        total_without_ndfl = 0
        total_with_ndfl = 0
        
        for row in db.cursor.fetchall():
            department, full_name, portions, position, location, hire_date = row
            
            # Рассчитываем суммы
            amount_without_ndfl = portions * 150  # 150 руб за обед
            amount_with_ndfl = round(amount_without_ndfl * 1.13)  # НДФЛ 13%
            
            # Форматируем дату приема
            hire_date_formatted = datetime.strptime(hire_date, "%Y-%m-%d").strftime("%d.%m.%Y") if hire_date else ""
            
            # Добавляем строку в отчет
            ws.append([
                department,
                full_name,
                portions,
                position,
                location,
                hire_date_formatted,
                f"{amount_without_ndfl:,.2f}".replace(",", " ").replace(".", ","),
                f"{amount_with_ndfl:,.2f}".replace(",", " ").replace(".", ",")
            ])
            
            # Суммируем итоги
            total_portions += portions
            total_without_ndfl += amount_without_ndfl
            total_with_ndfl += amount_with_ndfl
        
        # Добавляем итоговую строку
        ws.append([
            "ВСЕГО",
            "",
            total_portions,
            "",
            "",
            "",
            f"{total_without_ndfl:,.2f}".replace(",", " ").replace(".", ","),
            f"{total_with_ndfl:,.2f}".replace(",", " ").replace(".", ",")
        ])
        
        # Форматирование
        bold_font = Font(bold=True)
        money_format = '# ##0,00'
        
        # Заголовок отчета
        for row in ws.iter_rows(min_row=1, max_row=5):
            for cell in row:
                cell.font = bold_font
        
        # Заголовки таблицы
        for cell in ws[6]:
            cell.font = bold_font
        
        # Итоговая строка
        for cell in ws[ws.max_row]:
            cell.font = bold_font
        
        # Формат денежных значений
        for row in ws.iter_rows(min_row=7, max_row=ws.max_row):
            for cell in row[6:8]:  # Колонки с суммами
                cell.number_format = money_format
        
        # Автоподбор ширины столбцов
        for col in ws.columns:
            max_length = max(len(str(cell.value)) for cell in col)
            ws.column_dimensions[col[0].column_letter].width = max_length + 2
        
        # Сохраняем файл
        timestamp = now.strftime("%Y%m%d_%H%M%S")
        file_name = f"salary_deductions_{start_date.strftime('%Y%m')}.xlsx"
        file_path = os.path.join(reports_dir, file_name)
        wb.save(file_path)
        
        # Отправляем файл
        caption = (
            f"📋 Отчет для удержаний из зарплаты\n"
            f"📅 Период: {start_date.strftime('%B %Y')}\n"
            f"🍽 Всего обедов: {total_portions}\n"
            f"💰 Общая сумма удержаний: {total_with_ndfl:,.2f} руб."
        )
        
        with open(file_path, 'rb') as file:
            await context.bot.send_document(
                chat_id=update.effective_chat.id,
                document=file,
                caption=caption,
                filename=file_name
            )

    except Exception as e:
        logger.error(f"Ошибка формирования отчета для удержаний: {e}", exc_info=True)
        await update.message.reply_text(
            "❌ Произошла ошибка при создании отчета для удержаний."
        )
        raise
        
async def export_daily_admin_report(
    update: Update, 
    context: ContextTypes.DEFAULT_TYPE, 
    target_date: Optional[date] = None
):
    """Формирует дневной административный отчет"""
    logger.info(f"Создание дневного админ отчета, дата: {target_date}")
    if not target_date:
        target_date = datetime.now(CONFIG.timezone).date()
    return await export_monthly_report(
        update, 
        context, 
        target_date, 
        target_date,
        is_daily=True  # Передаём флаг, что это дневной отчёт
    )
    
async def export_daily_orders_for_provider(update: Update, context: ContextTypes.DEFAULT_TYPE, target_date: Optional[date] = None):
    """Формирует дневной отчет для поставщиков"""
    logger.info(f"Создание дневного отчета для поставщика, дата: {target_date}")
    if not target_date:
        target_date = datetime.now(CONFIG.timezone).date()
    return await export_orders_for_provider(update, context, target_date, target_date)