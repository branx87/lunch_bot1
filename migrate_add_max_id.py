"""
Migration: Add max_id column to users table for Max messenger support.
Also changes telegram_id from Integer to BigInteger.
"""
import logging
from database import db
from sqlalchemy import text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def migrate():
    with db.get_session() as session:
        # Check if max_id column already exists
        result = session.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'users' AND column_name = 'max_id'
        """))
        if result.fetchone():
            logger.info("Column max_id already exists, skipping")
        else:
            session.execute(text("ALTER TABLE users ADD COLUMN max_id BIGINT UNIQUE"))
            logger.info("Added max_id column to users table")

        # Change telegram_id from INTEGER to BIGINT if needed
        result = session.execute(text("""
            SELECT data_type FROM information_schema.columns
            WHERE table_name = 'users' AND column_name = 'telegram_id'
        """))
        row = result.fetchone()
        if row and row[0] == 'integer':
            session.execute(text("ALTER TABLE users ALTER COLUMN telegram_id TYPE BIGINT"))
            logger.info("Changed telegram_id column type to BIGINT")
        else:
            logger.info("telegram_id is already BIGINT, skipping")

        # Check if vk_id column already exists
        result = session.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'users' AND column_name = 'vk_id'
        """))
        if result.fetchone():
            logger.info("Column vk_id already exists, skipping")
        else:
            session.execute(text("ALTER TABLE users ADD COLUMN vk_id BIGINT UNIQUE"))
            logger.info("Added vk_id column to users table")

        session.commit()
        logger.info("Migration completed successfully")


if __name__ == '__main__':
    migrate()
