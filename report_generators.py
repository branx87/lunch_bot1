# ##report_generators.py
from typing import Optional
from datetime import datetime, date
from telegram import Update
from telegram.error import Forbidden
from telegram.ext import ContextTypes
import logging

import asyncio
from telegram.error import TimedOut, NetworkError

from database import db
from config import CONFIG


async def _send_document_with_retry(bot, chat_id, file_path, caption, filename,
                                    retries=3, attempt_timeout=90):
    """Отправка документа с повторными попытками при сетевых ошибках.

    attempt_timeout — жёсткий предел на одну попытку. Через SOCKS-прокси
    соединение может висеть 15-20 минут (таймаут httpx не покрывает
    SOCKS-хендшейк), и без предела три попытки растягивали отправку
    отчёта почти на час вместо нескольких минут.
    """
    for attempt in range(1, retries + 1):
        try:
            with open(file_path, 'rb') as file:
                await asyncio.wait_for(
                    bot.send_document(chat_id=chat_id, document=file,
                                      caption=caption, filename=filename),
                    timeout=attempt_timeout,
                )
            return True
        except asyncio.TimeoutError:
            err = f"превышено время ожидания ({attempt_timeout} с)"
            if attempt == retries:
                raise NetworkError(err)
            wait = attempt * 5
            logger.warning(f"Ошибка отправки файла (попытка {attempt}/{retries}), повтор через {wait}с: {err}")
            await asyncio.sleep(wait)
        except (TimedOut, NetworkError) as e:
            if attempt == retries:
                raise
            wait = attempt * 5
            logger.warning(f"Ошибка отправки файла (попытка {attempt}/{retries}), повтор через {wait}с: {e}")
            await asyncio.sleep(wait)
    return False

logger = logging.getLogger(__name__)

async def export_orders_for_provider(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    caption: Optional[str] = None
):
    """
    Формирует текстовый отчёт для поставщиков.
    Объединяет данные по локациям "Офис" и "ПЦ 2".
    Добавляет строку с незарегистрированными сотрудниками, если такие есть.
    """
    try:
        with db.get_session() as session:
            from services.report_service import generate_provider_report_text
            message, _ = generate_provider_report_text(start_date, end_date, session, caption)

        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=message,
            parse_mode="Markdown"
        )
        return True

    except Forbidden:
        raise
    except (TimedOut, NetworkError) as e:
        logger.error(f"Не удалось отправить отчёт поставщику (Telegram API недоступен): {e}", exc_info=True)
        try:
            await update.message.reply_text(
                "⚠️ Отчёт не отправлен — Telegram API недоступен "
                "(таймаут сети или прокси). Попробуйте позже."
            )
        except Forbidden:
            pass
        return False
    except Exception as e:
        logger.error(f"Ошибка при создании отчета для поставщика: {e}", exc_info=True)
        try:
            await update.message.reply_text("❌ Ошибка при формировании отчёта.")
        except Forbidden:
            pass
        raise
    
async def export_accounting_report(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
):
    try:
        with db.get_session() as session:
            from services.report_service import generate_accounting_report_file
            file_path, file_name, caption = generate_accounting_report_file(start_date, end_date, session)

        await _send_document_with_retry(context.bot, update.effective_chat.id, file_path, caption, file_name)

        return file_path

    except Forbidden:
        raise
    except (TimedOut, NetworkError) as e:
        logger.error(f"Не удалось отправить файл отчёта (Telegram API недоступен): {e}", exc_info=True)
        try:
            await update.message.reply_text(
                "⚠️ Файл отчёта не удалось отправить — Telegram API недоступен "
                "(таймаут сети или прокси). Попробуйте позже."
            )
        except Forbidden:
            pass
        return False
    except Exception as e:
        logger.error(f"Ошибка формирования отчета: {e}", exc_info=True)
        try:
            await update.message.reply_text("❌ Произошла ошибка при создании отчета.")
        except Forbidden:
            pass
        raise
    
async def export_monthly_report(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    is_daily: bool = False,
    caption: Optional[str] = None
):
    """Генерация административного отчёта с возможностью указания дат"""
    try:
        if update.effective_user.id not in CONFIG.admin_ids:
            await update.message.reply_text("❌ У вас нет прав для выполнения этой команды.")
            return False

        with db.get_session() as session:
            from services.report_service import generate_admin_report_file
            file_path, file_name, caption_text = generate_admin_report_file(
                start_date, end_date, session, is_daily, caption
            )

        if file_path is None:
            await update.message.reply_text(caption_text)
            return True

        await _send_document_with_retry(
            context.bot, update.effective_chat.id, file_path, caption_text, file_name
        )
        return True

    except Forbidden:
        raise
    except (TimedOut, NetworkError) as e:
        logger.error(f"Не удалось отправить файл админ отчёта (Telegram API недоступен): {e}", exc_info=True)
        try:
            await update.message.reply_text(
                "⚠️ Файл отчёта не удалось отправить — Telegram API недоступен "
                "(таймаут сети или прокси).\n\nОтчёт можно запросить заново: "
                "📊 Отчеты → выбрать день."
            )
        except Forbidden:
            pass
        return False
    except Exception as e:
        logger.error(f"Ошибка формирования админ отчёта: {e}", exc_info=True)
        try:
            await update.message.reply_text("❌ Ошибка формирования отчёта")
        except Forbidden:
            pass
        return False
        
async def export_daily_admin_report(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    target_date: Optional[date] = None,
    caption: Optional[str] = None
):
    """Формирует дневной административный отчет"""
    logger.info(f"Создание дневного админ отчета, дата: {target_date}")
    if not target_date:
        target_date = datetime.now(CONFIG.timezone).date()
    if caption is None:
        from services.report_service import date_caption
        caption = date_caption(target_date)
    return await export_monthly_report(
        update,
        context,
        target_date,
        target_date,
        is_daily=True,  # Передаём флаг, что это дневной отчёт
        caption=caption
    )
        
    
async def export_daily_orders_for_provider(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    target_date: Optional[date] = None,
    caption: Optional[str] = None
):
    """Формирует дневной отчет для поставщиков"""
    logger.info(f"Создание дневного отчета для поставщика, дата: {target_date}")
    if not target_date:
        target_date = datetime.now(CONFIG.timezone).date()
    if caption is None:
        from services.report_service import date_caption
        caption = date_caption(target_date)
    return await export_orders_for_provider(update, context, target_date, target_date, caption)