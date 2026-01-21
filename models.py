from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text, ForeignKey, UniqueConstraint, CheckConstraint, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from sqlalchemy import Date
from datetime import datetime

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    bitrix_id = Column(Integer, nullable=True)
    crm_employee_id = Column(Integer, nullable=True)
    telegram_id = Column(Integer, unique=True, nullable=True)
    full_name = Column(String(255), nullable=False)
    position = Column(String(255), nullable=True)
    department = Column(String(255), nullable=True)
    phone = Column(String(50), nullable=True)
    location = Column(String(100), nullable=True)
    city = Column(String(100), nullable=True)
    is_verified = Column(Boolean, default=False)
    is_employee = Column(Boolean, default=False)
    username = Column(String(100), nullable=True)
    is_deleted = Column(Boolean, default=False)
    notifications_enabled = Column(Boolean, default=True)
    bitrix_entity_type = Column(String(50), nullable=True)
    
    # 🔥 ДОБАВЬТЕ ЭТО ПОЛЕ
    employment_date = Column(Date, nullable=True)  # Дата трудоустройства
    
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

class BitrixMapping(Base):
    __tablename__ = 'bitrix_mapping'
    
    local_id = Column(Integer, primary_key=True)
    local_type = Column(String(50), primary_key=True)
    bitrix_id = Column(Integer, nullable=False)
    bitrix_entity_type = Column(String(50), nullable=False)
    last_sync = Column(DateTime, server_default=func.now())

class Order(Base):
    __tablename__ = 'orders'
    
    id = Column(Integer, primary_key=True)
    bitrix_order_id = Column(String(100), unique=True, nullable=True)
    is_active = Column(Boolean, default=True)
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    target_date = Column(Date, nullable=False)
    order_time = Column(String(8), nullable=False)
    quantity = Column(Integer, CheckConstraint('quantity BETWEEN 1 AND 5'), nullable=False)
    bitrix_quantity_id = Column(String(100), nullable=True)
    is_cancelled = Column(Boolean, default=False)
    is_from_bitrix = Column(Boolean, default=False)
    is_sent_to_bitrix = Column(Boolean, default=False)
    is_preliminary = Column(Boolean, default=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    last_synced_at = Column(DateTime, nullable=True)  # 🔥 УБРАТЬ server_default=func.now()
    
    user = relationship("User")

class Holiday(Base):
    __tablename__ = 'holidays'
    
    id = Column(Integer, primary_key=True)
    date = Column(String(10), nullable=False)
    name = Column(String(255), nullable=False)
    is_recurring = Column(Boolean, default=False)
    created_at = Column(DateTime, server_default=func.now())
    
    __table_args__ = (
        UniqueConstraint('date', 'name', name='uq_holiday_date_name'),
    )

class Menu(Base):
    __tablename__ = 'menu'
    
    id = Column(Integer, primary_key=True)
    day = Column(String(20), unique=True, nullable=False)
    first_course = Column(Text, nullable=False)
    main_course = Column(Text, nullable=False)
    salad = Column(Text, nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

class AdminMessage(Base):
    __tablename__ = 'admin_messages'
    
    id = Column(Integer, primary_key=True)
    # Старые поля для связи с таблицей users (опционально)
    admin_id = Column(Integer, ForeignKey('users.id'), nullable=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=True)
    # Новые поля для хранения telegram_id напрямую
    admin_telegram_id = Column(BigInteger, nullable=True)
    user_telegram_id = Column(BigInteger, nullable=True)
    message_text = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    is_broadcast = Column(Boolean, default=False)
    is_unregistered = Column(Boolean, default=False)
    
class FeedbackMessage(Base):
    __tablename__ = 'feedback_messages'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    provider_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    message_text = Column(Text, nullable=False)
    sent_at = Column(DateTime, server_default=func.now())
    is_processed = Column(Boolean, default=False)

class BotSetting(Base):
    __tablename__ = 'bot_settings'
    
    id = Column(Integer, primary_key=True)
    setting_name = Column(String(100), unique=True, nullable=False)
    setting_value = Column(Text, nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())