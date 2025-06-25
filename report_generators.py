# ##report_generators.py
from openpyxl.styles import Font
from typing import Optional
import openpyxl
from datetime import datetime, date
from telegram import Update
from telegram.ext import ContextTypes
import logging
import matplotlib
from pathlib import Path

from config import CONFIG, LOCATIONS, TIMEZONE
matplotlib.use('Agg')  # Устанавливаем бэкенд, не требующий GUI
import matplotlib.pyplot as plt

logger = logging.getLogger(__name__)

class ReportGenerator:
    def __init__(self, db_connection):
        self.db = db_connection

    def ensure_reports_dir(self, report_type: str = 'accounting') -> Path:
        """Создает папку для отчетов если ее нет и возвращает путь к ней"""
        reports_dir = CONFIG.reports_dir / report_type
        reports_dir.mkdir(parents=True, exist_ok=True)
        
        # Очищаем старые отчеты (оставляем 5 последних)
        report_files = sorted(
            reports_dir.glob('*.xlsx'),
            key=lambda x: x.stat().st_mtime,
            reverse=True
        )
        for old_file in report_files[5:]:
            try:
                old_file.unlink()
            except Exception as e:
                logger.error(f"Ошибка удаления старого отчета {old_file}: {e}")
        
        return reports_dir

    async def export_orders_for_provider(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ):
        """Формирует текстовый отчёт для поставщиков"""
        try:
            # Если даты не заданы — используем сегодняшнюю
            if not start_date or not end_date:
                today = datetime.now(TIMEZONE).date()
                start_date = end_date = today

            # Получаем данные по локациям
            self.db.cursor.execute('''
                SELECT u.location, SUM(o.quantity)
                FROM orders o
                JOIN users u ON o.user_id = u.id
                WHERE o.target_date BETWEEN ? AND ?
                  AND o.is_cancelled = FALSE
                GROUP BY u.location
            ''', (start_date.isoformat(), end_date.isoformat()))

            location_data = dict(self.db.cursor.fetchall())

            # Объединяем "Офис" и "ПЦ 2"
            office_portions = location_data.get("Офис", 0) + location_data.get("ПЦ 2", 0)
            pc1_portions = location_data.get("ПЦ 1", 0)
            warehouse_portions = location_data.get("Склад", 0)

            total_portions = office_portions + pc1_portions + warehouse_portions

            # Формируем текстовое сообщение
            period_text = (
                f"{start_date.strftime('%d.%m.%Y')}"
                if start_date == end_date
                else f"{start_date.strftime('%d.%m.%Y')} — {end_date.strftime('%d.%m.%Y')}"
            )

            message = (
                f"📋 *Заказы на* | {period_text}\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📌 Всего: *{total_portions}* порций\n\n"
                
                f"• 🏢 Офис: *{office_portions}*\n"
                f"• 🏭 ПЦ 1: *{pc1_portions}*\n"
                f"• 📦 Склад: *{warehouse_portions}*"
            )

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
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ):
        """Генерирует детализированный бухгалтерский отчет в формате Excel"""
        try:
            reports_dir = self.ensure_reports_dir('accounting')
            now = datetime.now(TIMEZONE)
            
            # Обработка дат
            if not start_date or not end_date:
                start_date = end_date = now.date()

            if start_date > end_date:
                start_date, end_date = end_date, start_date

            # Создаем Excel файл
            wb = openpyxl.Workbook()
            
            # 1. Лист "Детализация"
            ws_detailed = wb.active
            ws_detailed.title = "Детализация"
            detailed_headers = ["ФИО", "Объект", "Дата заказа", "Время заказа", "Дата обеда", "Количество", "Тип заказа"]
            ws_detailed.append(detailed_headers)
            ws_detailed.auto_filter.ref = "A1:G1"
            
            # Получаем данные
            self.db.cursor.execute('''
                SELECT 
                    u.full_name,
                    u.location,
                    date(o.created_at) as order_date,
                    time(o.created_at) as order_time,
                    o.target_date,
                    o.quantity,
                    CASE WHEN o.is_preliminary THEN 'Предзаказ' ELSE 'Обычный' END
                FROM orders o
                JOIN users u ON o.user_id = u.id
                WHERE o.target_date BETWEEN ? AND ?
                  AND o.is_cancelled = FALSE
                  AND u.is_deleted = FALSE
                ORDER BY o.target_date, u.full_name
            ''', (start_date.isoformat(), end_date.isoformat()))
            
            total_portions = 0
            orders_count = 0
            for row in self.db.cursor.fetchall():
                order_date = datetime.strptime(row[2], "%Y-%m-%d").strftime("%d.%m.%Y")
                target_date = datetime.strptime(row[4], "%Y-%m-%d").strftime("%d.%m.%Y")
                ws_detailed.append([
                    row[0], row[1], order_date, row[3], target_date, row[5], row[6]
                ])
                total_portions += row[5]
                orders_count += 1

            # 2. Лист "Сводка по сотрудникам"
            ws_summary_users = wb.create_sheet("Сводка по сотрудникам")
            summary_headers = ["ФИО", "Объект", "Всего порций"]
            ws_summary_users.append(summary_headers)
            ws_summary_users.auto_filter.ref = "A1:C1"
            
            self.db.cursor.execute('''
                SELECT 
                    u.full_name,
                    u.location,
                    SUM(o.quantity)
                FROM orders o
                JOIN users u ON o.user_id = u.id
                WHERE o.target_date BETWEEN ? AND ?
                  AND o.is_cancelled = FALSE
                GROUP BY u.full_name, u.location
                ORDER BY SUM(o.quantity) DESC
            ''', (start_date.isoformat(), end_date.isoformat()))
            
            for row in self.db.cursor.fetchall():
                ws_summary_users.append(row)

            # 3. Лист "Сводка по объектам"
            ws_summary_locations = wb.create_sheet("Сводка по объектам")
            loc_headers = ["Объект", "Порции"]
            ws_summary_locations.append(loc_headers)
            ws_summary_locations.auto_filter.ref = "A1:B1"
            
            self.db.cursor.execute('''
                SELECT u.location, SUM(o.quantity)
                FROM orders o
                JOIN users u ON o.user_id = u.id
                WHERE o.target_date BETWEEN ? AND ?
                  AND o.is_cancelled = FALSE
                GROUP BY u.location
                ORDER BY SUM(o.quantity) DESC
            ''', (start_date.isoformat(), end_date.isoformat()))
            
            for row in self.db.cursor.fetchall():
                ws_summary_locations.append(row)
            ws_summary_locations.append(["ВСЕГО", total_portions])

            # 4. Лист "Итоги"
            ws_stats = wb.create_sheet("Итоги")
            stats_headers = ["Показатель", "Значение"]
            ws_stats.append(stats_headers)
            
            self.db.cursor.execute('''
                SELECT COUNT(DISTINCT u.id)
                FROM orders o
                JOIN users u ON o.user_id = u.id
                WHERE o.target_date BETWEEN ? AND ?
                  AND o.is_cancelled = FALSE
            ''', (start_date.isoformat(), end_date.isoformat()))
            unique_users = self.db.cursor.fetchone()[0]

            stats_data = [
                ["Период", f"{start_date.strftime('%d.%m.%Y')} — {end_date.strftime('%d.%m.%Y')}"],
                ["Всего заказов", orders_count],
                ["Всего порций", total_portions],
                ["Уникальных сотрудников", unique_users],
                ["Дата формирования", now.strftime("%d.%m.%Y %H:%M")]
            ]
            for row in stats_data:
                ws_stats.append(row)

            # Форматирование
            bold_font = Font(bold=True)
            for sheet in wb.worksheets:
                # Заголовки жирным
                for row in sheet.iter_rows(min_row=1, max_row=1):
                    for cell in row:
                        cell.font = bold_font
                
                # Автоподбор ширины столбцов
                for col in sheet.columns:
                    max_length = max(len(str(cell.value)) for cell in col)
                    sheet.column_dimensions[col[0].column_letter].width = max_length + 2

            # Сохраняем файл
            timestamp = now.strftime("%Y%m%d_%H%M%S")
            file_name = f"accounting_report_{timestamp}.xlsx"
            file_path = reports_dir / file_name
            wb.save(file_path)

            # Отправляем файл
            caption = (
                f"📊 Бухгалтерский отчет\n"
                f"📅 Период: {start_date.strftime('%d.%m.%Y')} — {end_date.strftime('%d.%m.%Y')}\n"
                f"🍽 Всего порций: {total_portions}\n"
                f"👥 Уникальных сотрудников: {unique_users}"
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
            await update.message.reply_text(
                "❌ Произошла ошибка при создании отчета. Подробности в логах."
            )
            raise
    
    async def export_monthly_report(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        is_daily: bool = False
    ):
        """Генерация административного отчёта с возможностью указания дат"""
        try:
            if update.effective_user.id not in CONFIG.admin_ids:
                await update.message.reply_text("❌ У вас нет прав для выполнения этой команды.")
                return

            now = datetime.now(TIMEZONE)
            
            # Если даты не переданы - используем текущий месяц
            if not start_date or not end_date:
                month_start = now.replace(day=1).date()
                end_date = now.date()
            else:
                # Проверяем, что start_date <= end_date
                if start_date > end_date:
                    start_date, end_date = end_date, start_date

            reports_dir = self.ensure_reports_dir('admin')
            wb = openpyxl.Workbook()
            
            # Удаляем лист по умолчанию
            if 'Sheet' in wb.sheetnames:
                del wb['Sheet']
            
            # Создаем листы для каждой локации
            for location in LOCATIONS:
                ws = wb.create_sheet(location)
                headers = ["Дата обеда", "Сотрудник", "Территориальный признак", "Подпись", "Кол-во обедов", "Тип заказа"]
                ws.append(headers)
                ws.auto_filter.ref = "A1:F1"
                
                # Изменяем запрос в зависимости от типа отчёта
                if is_daily:
                    self.db.cursor.execute('''
                        SELECT 
                            o.target_date,
                            u.full_name,
                            u.location,
                            o.quantity,
                            CASE WHEN o.is_preliminary THEN 'Предзаказ' ELSE 'Обычный' END
                        FROM orders o
                        JOIN users u ON o.user_id = u.id
                        WHERE o.target_date = ?
                          AND u.location = ?
                          AND o.is_cancelled = FALSE
                          AND u.is_deleted = FALSE
                        ORDER BY u.full_name
                    ''', (start_date.isoformat(), location))
                else:
                    self.db.cursor.execute('''
                        SELECT 
                            o.target_date,
                            u.full_name,
                            u.location,
                            o.quantity,
                            CASE WHEN o.is_preliminary THEN 'Предзаказ' ELSE 'Обычный' END
                        FROM orders o
                        JOIN users u ON o.user_id = u.id
                        WHERE o.target_date BETWEEN ? AND ?
                          AND u.location = ?
                          AND o.is_cancelled = FALSE
                          AND u.is_deleted = FALSE
                        ORDER BY o.target_date, u.full_name
                    ''', (start_date.isoformat(), end_date.isoformat(), location))
                
                for row in self.db.cursor.fetchall():
                    target_date = datetime.strptime(row[0], "%Y-%m-%d").strftime("%d.%m.%Y")
                    ws.append([target_date, row[1], row[2], "", row[3], row[4]])

            # Лист "Итоги"
            ws_summary = wb.create_sheet("Итоги")
            summary_headers = ["Локация", "Порции"]
            ws_summary.append(summary_headers)
            
            if is_daily:
                self.db.cursor.execute('''
                    SELECT u.location, SUM(o.quantity)
                    FROM orders o
                    JOIN users u ON o.user_id = u.id
                    WHERE o.target_date = ?
                      AND o.is_cancelled = FALSE
                    GROUP BY u.location
                ''', (start_date.isoformat(),))
            else:
                self.db.cursor.execute('''
                    SELECT u.location, SUM(o.quantity)
                    FROM orders o
                    JOIN users u ON o.user_id = u.id
                    WHERE o.target_date BETWEEN ? AND ?
                      AND o.is_cancelled = FALSE
                    GROUP BY u.location
                ''', (start_date.isoformat(), end_date.isoformat()))
            
            total = 0
            for location, portions in self.db.cursor.fetchall():
                ws_summary.append([location, portions])
                total += portions
            
            ws_summary.append(["ВСЕГО", total])
            
            # Форматирование
            bold_font = Font(bold=True)
            for sheet in wb.worksheets:
                for row in sheet.iter_rows(min_row=1, max_row=1):
                    for cell in row:
                        cell.font = bold_font
                
                for col in sheet.columns:
                    max_length = max(len(str(cell.value)) for cell in col)
                    sheet.column_dimensions[col[0].column_letter].width = max_length + 2

            # Сохраняем файл
            timestamp = now.strftime("%Y%m%d_%H%M%S")
            file_name = f"admin_report_{timestamp}.xlsx"
            file_path = reports_dir / file_name
            wb.save(file_path)
            
            # Формируем сообщение
            if is_daily:
                caption = f"📅 Админ отчет за {start_date.strftime('%d.%m.%Y')}\n🍽 Всего порций: {total}"
            else:
                caption = f"📅 Админ отчет за {start_date.strftime('%B %Y')}\n🍽 Всего порций: {total}"
            
            with open(file_path, 'rb') as file:
                await context.bot.send_document(
                    chat_id=update.effective_chat.id,
                    document=file,
                    caption=caption,
                    filename=file_name
                )

        except Exception as e:
            logger.error(f"Ошибка формирования админ отчёта: {e}")
            await update.message.reply_text("❌ Ошибка формирования отчёта")
    
    async def export_daily_admin_report(
        self,
        update: Update, 
        context: ContextTypes.DEFAULT_TYPE, 
        target_date: Optional[date] = None
    ):
        """Формирует дневной административный отчет"""
        logger.info(f"Создание дневного админ отчета, дата: {target_date}")
        target_date = target_date or datetime.now(TIMEZONE).date()
        return await self.export_monthly_report(
            update, 
            context, 
            target_date, 
            target_date,
            is_daily=True
        )
        
    async def export_daily_orders_for_provider(
        self,
        update: Update, 
        context: ContextTypes.DEFAULT_TYPE, 
        target_date: Optional[date] = None
    ):
        """Формирует дневной отчет для поставщиков"""
        logger.info(f"Создание дневного отчета для поставщика, дата: {target_date}")
        target_date = target_date or datetime.now(TIMEZONE).date()
        return await self.export_orders_for_provider(update, context, target_date, target_date)
    
# В конце файла report_generators.py
from db import Database
from config import CONFIG

# Создаем глобальный экземпляр для обратной совместимости
db = Database(str(CONFIG.db_path))
report_generator = ReportGenerator(db)

# Делаем функции доступными для импорта
export_daily_admin_report = report_generator.export_daily_admin_report
export_daily_orders_for_provider = report_generator.export_daily_orders_for_provider
export_monthly_report = report_generator.export_monthly_report
export_accounting_report = report_generator.export_accounting_report
export_orders_for_provider = report_generator.export_orders_for_provider