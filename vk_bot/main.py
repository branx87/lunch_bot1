"""
VK messenger bot entry point.
Shares the same PostgreSQL database and business logic with the Telegram bot.
"""
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
        logs_dir / 'vk_bot.log',
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


def main():
    try:
        from vkbottle import BuiltinStateDispenser
        from vkbottle.bot import Bot, BotLabeler

        # Initialize database and models
        from database import db
        from models import Base
        from config import CONFIG

        if db is None:
            logger.error("Database not initialized")
            sys.exit(1)

        Base.metadata.create_all(bind=db.engine)

        # Auto-migration: add vk_id column if missing
        try:
            from migrate_add_max_id import migrate
            migrate()
        except Exception as e:
            logger.warning(f"Migration check: {e}")

        token = os.getenv('VK_BOT_TOKEN')
        if not token:
            logger.error("VK_BOT_TOKEN not set in environment")
            sys.exit(1)

        labeler = BotLabeler()
        state_dispenser = BuiltinStateDispenser()

        # Share state_dispenser with handlers via vk_bot package
        import vk_bot
        vk_bot.state_dispenser = state_dispenser

        # Register handlers
        from vk_bot.handlers import setup_labelers
        setup_labelers(labeler)

        global bot
        bot = Bot(token=token, labeler=labeler, state_dispenser=state_dispenser)

        logger.info("=== VK bot starting ===")
        bot.run_forever()

    except KeyboardInterrupt:
        logger.info("VK bot stopped by user")
    except Exception as e:
        logger.error(f"VK bot fatal error: {e}", exc_info=True)
        sys.exit(1)


# Global bot reference (used by registration handler for state_dispenser access)
bot = None

if __name__ == '__main__':
    main()
