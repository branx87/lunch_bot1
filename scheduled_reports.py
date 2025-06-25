# # ##scheduled_reports.py
import logging
from datetime import datetime
from telegram.ext import Application
from config import CONFIG, TIMEZONE
from settings import SETTINGS_CONFIG

from report_generators import ReportGenerator
# from report_generators import report_generator
logger = logging.getLogger(__name__)

class FakeUpdate:
    """Имитация объекта Update для scheduled задач"""
    def __init__(self, user_id, bot):
        self.effective_user = type('', (), {'id': user_id, 'username': 'system'})()
        self.effective_chat = type('', (), {'id': user_id})()
        self.message = self.FakeMessage(bot, user_id)
    
    class FakeMessage:
        def __init__(self, bot, chat_id):
            self.bot = bot
            self.chat_id = chat_id
        
        async def reply_text(self, text, **kwargs):
            await self.bot.send_message(chat_id=self.chat_id, text=text, **kwargs)
        
        async def reply_document(self, document, **kwargs):
            await self.bot.send_document(chat_id=self.chat_id, document=document, **kwargs)

async def send_scheduled_reports(application: Application, report_types: list):
    """Основная функция отправки отчетов"""
    logger.info("Начало отправки отчетов: %s", report_types)
    
    today = datetime.now(TIMEZONE).date()
    
    # Используем прямое обращение к атрибутам CONFIG вместо .get()
    reports = {
        'admins': {
            'ids': getattr(CONFIG, 'admin_ids', []),
            'func': export_daily_admin_report,
            'args': [today]
        },
        'providers': {
            'ids': getattr(CONFIG, 'provider_ids', []),
            'func': export_daily_orders_for_provider,
            'args': [today]
        },
        'accounting': {
            'ids': getattr(CONFIG, 'accounting_ids', []),
            'func': export_accounting_report,
            'args': [today.replace(day=1), today]
        }
    }

    for report in report_types:
        if report not in reports:
            logger.warning("Неизвестный тип отчета: %s", report)
            continue
            
        config = reports[report]
        if not config['ids']:
            logger.warning("Нет получателей для отчета: %s", report)
            continue
            
        logger.info("Отправка отчета '%s' для %d получателей", report, len(config['ids']))
        
        for user_id in config['ids']:
            try:
                fake_update = FakeUpdate(user_id, application.bot)
                await config['func'](fake_update, application, *config['args'])
                logger.info("Отчет '%s' отправлен пользователю %d", report, user_id)
            except Exception as e:
                logger.error("Ошибка отправки отчета '%s' пользователю %d: %s", 
                           report, user_id, str(e))