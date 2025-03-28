"""SQLAlchemy models for the Rental Marketplace database."""

from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    Boolean,
    DateTime,
    ForeignKey,
    Text,
    Numeric,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

# Constants for foreign key references
APARTMENT_ID_FK = "apartments.id"


class Apartment(Base):
    """Core entity representing property listings in the marketplace."""

    __tablename__ = "apartments"

    id = Column(Integer, primary_key=True)
    title = Column(String(255), nullable=False)
    source = Column(String(100))
    price = Column(Numeric(10, 2))
    currency = Column(String(10))
    listing_created_on = Column(DateTime, nullable=False)
    is_active = Column(Boolean, default=True)
    last_modified_timestamp = Column(DateTime, nullable=False)

    # Relationships
    attributes = relationship(
        "ApartmentAttribute", back_populates="apartment", uselist=False
    )
    viewings = relationship("UserViewing", back_populates="apartment")
    bookings = relationship("Booking", back_populates="apartment")


class ApartmentAttribute(Base):
    """Detailed characteristics and features of an apartment."""

    __tablename__ = "apartment_attributes"

    id = Column(Integer, ForeignKey(APARTMENT_ID_FK), primary_key=True)
    category = Column(String(100))
    body = Column(Text)
    amenities = Column(Text)
    bathrooms = Column(Float)
    bedrooms = Column(Integer)
    fee = Column(Numeric(10, 2))
    has_photo = Column(Boolean, default=False)
    pets_allowed = Column(Boolean, default=False)
    price_display = Column(String(100))
    price_type = Column(String(50))
    square_feet = Column(Integer)
    address = Column(String(255))
    cityname = Column(String(100))
    state = Column(String(50))
    latitude = Column(Numeric(10, 8))
    longitude = Column(Numeric(11, 8))

    # Relationship
    apartment = relationship("Apartment", back_populates="attributes")


class UserViewing(Base):
    """Records of user interactions with apartment listings."""

    __tablename__ = "user_viewings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False)
    apartment_id = Column(Integer, ForeignKey(APARTMENT_ID_FK), nullable=False)
    viewed_at = Column(DateTime, nullable=False)
    is_wishlisted = Column(Boolean, default=False)
    call_to_action = Column(String(100))

    # Relationship
    apartment = relationship("Apartment", back_populates="viewings")


class Booking(Base):
    """Reservation transactions for apartments."""

    __tablename__ = "bookings"

    booking_id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False)
    apartment_id = Column(Integer, ForeignKey(APARTMENT_ID_FK), nullable=False)
    booking_date = Column(DateTime, nullable=False)
    checkin_date = Column(DateTime, nullable=False)
    checkout_date = Column(DateTime, nullable=False)
    total_price = Column(Numeric(10, 2))
    currency = Column(String(10))
    booking_status = Column(String(50))

    # Relationship
    apartment = relationship("Apartment", back_populates="bookings")
