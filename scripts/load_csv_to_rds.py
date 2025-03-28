"""
Script to load CSV data into RDS database using SQLAlchemy models.

This script handles the ETL process for loading rental marketplace data from CSV files
into a PostgreSQL RDS database. It processes four main entities:
1. Apartments - Core property listings
2. Apartment Attributes - Detailed property characteristics
3. User Viewings - User interaction tracking
4. Bookings - Reservation transactions
"""

import pandas as pd
from pathlib import Path
from decimal import Decimal
from sqlalchemy.orm import Session

from src.database.connection import db
from src.database.models import Apartment, ApartmentAttribute, UserViewing, Booking


def load_apartments(session: Session, df: pd.DataFrame) -> None:
    """Load apartments data from DataFrame into database."""
    print(f"Loading {len(df)} apartments...")
    for _, row in df.iterrows():
        apartment = Apartment(
            id=row["id"],
            title=row["title"],
            source=row["source"],
            price=Decimal(str(row["price"])) if pd.notna(row["price"]) else None,
            currency=row["currency"],
            listing_created_on=pd.to_datetime(row["listing_created_on"], dayfirst=True),
            is_active=row["is_active"],
            last_modified_timestamp=pd.to_datetime(
                row["last_modified_timestamp"], dayfirst=True
            ),
        )
        session.add(apartment)
        print(f"Loaded apartment ID {row['id']}")


def load_apartment_attributes(session: Session, df: pd.DataFrame) -> None:
    """Load apartment attributes data from DataFrame into database."""
    print(f"Loading {len(df)} apartment attributes...")
    for _, row in df.iterrows():
        attribute = ApartmentAttribute(
            id=row["id"],
            category=row["category"],
            body=row["body"],
            amenities=row["amenities"],
            bathrooms=float(row["bathrooms"]) if pd.notna(row["bathrooms"]) else None,
            bedrooms=int(row["bedrooms"]) if pd.notna(row["bedrooms"]) else None,
            fee=Decimal(str(row["fee"])) if pd.notna(row["fee"]) else None,
            has_photo=row["has_photo"],
            pets_allowed=row["pets_allowed"],
            price_display=row["price_display"],
            price_type=row["price_type"],
            square_feet=int(row["square_feet"])
            if pd.notna(row["square_feet"])
            else None,
            address=row["address"],
            cityname=row["cityname"],
            state=row["state"],
            latitude=Decimal(str(row["latitude"]))
            if pd.notna(row["latitude"])
            else None,
            longitude=Decimal(str(row["longitude"]))
            if pd.notna(row["longitude"])
            else None,
        )
        session.add(attribute)
        print(f"Loaded attribute for apartment ID {row['id']}")


def load_user_viewings(session: Session, df: pd.DataFrame) -> None:
    """Load user viewings data from DataFrame into database."""
    print(f"Loading {len(df)} user viewings...")
    for _, row in df.iterrows():
        viewing = UserViewing(
            user_id=row["user_id"],
            apartment_id=row["apartment_id"],
            viewed_at=pd.to_datetime(row["viewed_at"], dayfirst=True),
            is_wishlisted=row["is_wishlisted"],
            call_to_action=row["call_to_action"],
        )
        session.add(viewing)
        print(
            f"Loaded viewing for user ID {row['user_id']} and apartment ID {row['apartment_id']}"
        )


def load_bookings(session: Session, df: pd.DataFrame) -> None:
    """Load bookings data from DataFrame into database."""
    print(f"Loading {len(df)} bookings...")
    for _, row in df.iterrows():
        booking = Booking(
            booking_id=row["booking_id"],
            user_id=row["user_id"],
            apartment_id=row["apartment_id"],
            booking_date=pd.to_datetime(row["booking_date"], dayfirst=True),
            checkin_date=pd.to_datetime(row["checkin_date"], dayfirst=True),
            checkout_date=pd.to_datetime(row["checkout_date"], dayfirst=True),
            total_price=Decimal(str(row["total_price"]))
            if pd.notna(row["total_price"])
            else None,
            currency=row["currency"],
            booking_status=row["booking_status"],
        )
        session.add(booking)
        print(
            f"Loaded booking for user ID {row['user_id']} and apartment ID {row['apartment_id']}"
        )


def load_csv_data():
    """
    Load CSV files into RDS database.

    This function coordinates the loading of all data files into the database,
    maintaining proper order to satisfy foreign key constraints.
    """
    project_root = Path(__file__).parent.parent
    data_dir = project_root / "data"

    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory not found at {data_dir}")

    try:
        with db.get_session() as session:
            # Load data in order of dependencies
            load_apartments(session, pd.read_csv(data_dir / "apartments.csv"))
            load_apartment_attributes(
                session, pd.read_csv(data_dir / "apartment_attributes.csv")
            )
            load_user_viewings(session, pd.read_csv(data_dir / "user_viewing.csv"))
            load_bookings(session, pd.read_csv(data_dir / "bookings.csv"))

            session.commit()
            print("All data loaded successfully!")

    except Exception as e:
        print(f"Error loading data: {str(e)}")
        raise


if __name__ == "__main__":
    load_csv_data()
