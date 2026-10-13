# ##logging_config.py
"""
Единая настройка логирования для всех точек входа.

Раньше setup_logging был скопирован в 4 файла (main.py, vk_bot, max_bot,
bitrix24_bot) и отличался только именем лог-файла.
"""
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

DEFAULT_LOGS_DIR = Path('data/logs')
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


def setup_logging(log_file: str = 'bot.log', logs_dir: Path = DEFAULT_LOGS_DIR, telegram_debug: bool = False):
    """Настраивает rotating-лог в data/logs и вывод в консоль.

    log_file       — имя файла, у каждого бота своё (bot.log, vk_bot.log, ...).
    telegram_debug — подробный лог библиотеки telegram, нужен только основному боту.
    """
    logs_dir.mkdir(parents=True, exist_ok=True)

    handler = RotatingFileHandler(
        logs_dir / log_file,
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding='utf-8'
    )
    logging.basicConfig(
        level=logging.INFO,
        format=LOG_FORMAT,
        handlers=[handler, logging.StreamHandler()]
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)

    if telegram_debug:
        # DEBUG: временно включаем для диагностики polling
        logging.getLogger("telegram.ext._updater").setLevel(logging.DEBUG)
        logging.getLogger("telegram.ext._application").setLevel(logging.DEBUG)