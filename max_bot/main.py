"""
Max messenger bot entry point.
Shares the same PostgreSQL database and business logic with the Telegram bot.
"""
import asyncio
import logging
import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from logging.handlers import RotatingFileHandler


def setup_logging():
    logs_dir = Path('data/logs')
    logs_dir.mkdir(parents=True, exist_ok=True)

    handler = RotatingFileHandler(
        logs_dir / 'max_bot.log',
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding='utf-8'
    )
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[handler, logging.StreamHandler()]
    )


setup_logging()
logger = logging.getLogger(__name__)


async def main():
    try:
        from maxapi import Bot, Dispatcher

        # Initialize database and models
        from database import db
        from models import Base
        from config import CONFIG

        if db is None:
            logger.error("Database not initialized")
            sys.exit(1)

        Base.metadata.create_all(bind=db.engine)

        # Auto-migration: add max_id column if missing
        try:
            from migrate_add_max_id import migrate
            migrate()
        except Exception as e:
            logger.warning(f"Migration check: {e}")

        token = os.getenv('MAX_BOT_TOKEN')
        if not token:
            logger.error("MAX_BOT_TOKEN not set in environment")
            sys.exit(1)

        bot = Bot(token=token)
        dp = Dispatcher()

        # Register handlers
        from max_bot.handlers import setup_routers
        setup_routers(dp)

        logger.info("=== Max bot starting ===")
        me = await bot.get_me()
        logger.info(f"Bot info: {me}")

        await dp.start_polling(bot)

    except KeyboardInterrupt:
        logger.info("Max bot stopped by user")
    except Exception as e:
        logger.error(f"Max bot fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    asyncio.run(main())
