"""
Migration: Add source column to orders table.

В колонке хранится канал, где оформлен заказ (Телеграм / ВК / Max / Б24 /
Битрикс (форма)) — по ней в отчёте заполняется «Источник заказа».
Старые заказы останутся с NULL и будут показываться как раньше.
"""
import logging
from database import db
from sqlalchemy import text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def migrate():
    with db.get_session() as session:
        result = session.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'orders' AND column_name = 'source'
        """))
        if result.fetchone():
            logger.info("Column source already exists, skipping")
        else:
            session.execute(text("ALTER TABLE orders ADD COLUMN source VARCHAR(20)"))
            logger.info("Added source column to orders table")

        session.commit()
        logger.info("Migration completed successfully")


if __name__ == '__main__':
    migrate()
