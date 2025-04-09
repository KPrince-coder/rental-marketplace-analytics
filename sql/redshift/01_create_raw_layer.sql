-- Create Raw Layer tables in Redshift
-- This layer stores data as close as possible to the source format
-- Raw Apartments table
CREATE TABLE IF NOT EXISTS raw_data.apartments (
    id INTEGER PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    source VARCHAR(50),
    price FLOAT NOT NULL,
    currency VARCHAR(3),
    listing_created_on TIMESTAMP,
    is_active BOOLEAN,
    last_modified_timestamp TIMESTAMP,
    etl_inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Raw Apartment Attributes table
CREATE TABLE IF NOT EXISTS raw_data.apartment_attributes (
    id INTEGER PRIMARY KEY,
    apartment_id INTEGER REFERENCES raw_data.apartments(id),
    category VARCHAR(50),
    body TEXT,
    amenities TEXT,
    bathrooms INTEGER,
    bedrooms INTEGER,
    fee FLOAT,
    has_photo BOOLEAN,
    pets_allowed BOOLEAN,
    square_feet INTEGER,
    address VARCHAR(255),
    cityname VARCHAR(100),
    state VARCHAR(50),
    latitude FLOAT,
    longitude FLOAT,
    etl_inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Raw User Viewings table
CREATE TABLE IF NOT EXISTS raw_data.user_viewings (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    apartment_id INTEGER REFERENCES raw_data.apartments(id),
    viewed_at TIMESTAMP,
    is_wishlisted BOOLEAN,
    call_to_action VARCHAR(50),
    etl_inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Raw Bookings table
CREATE TABLE IF NOT EXISTS raw_data.bookings (
    id INTEGER PRIMARY KEY,
    booking_id INTEGER UNIQUE,
    user_id INTEGER NOT NULL,
    apartment_id INTEGER REFERENCES raw_data.apartments(id),
    booking_date TIMESTAMP NOT NULL,
    checkin_date TIMESTAMP,
    checkout_date TIMESTAMP,
    total_price FLOAT,
    currency VARCHAR(3),
    booking_status VARCHAR(20),
    payment_status VARCHAR(20),
    num_guests INTEGER,
    etl_inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Create distribution keys and sort keys for performance
-- Note: In Redshift, distribution and sort keys should be defined during table creation
-- These tables were already created, so we'll use ALTER TABLE to modify them
-- Set distribution and sort keys for raw_apartments
-- We'll skip these ALTER statements since the tables might already have these keys
-- If you need to set these keys, uncomment the following lines:
-- ALTER TABLE raw_data.apartments ALTER DISTKEY id;
-- ALTER TABLE raw_data.apartments ALTER SORTKEY (id, last_modified_timestamp);
-- ALTER TABLE raw_data.apartment_attributes ALTER DISTKEY apartment_id;
-- ALTER TABLE raw_data.apartment_attributes ALTER SORTKEY (apartment_id);
-- ALTER TABLE raw_data.user_viewings ALTER DISTKEY apartment_id;
-- ALTER TABLE raw_data.user_viewings ALTER SORTKEY (viewed_at, apartment_id);
-- ALTER TABLE raw_data.bookings ALTER DISTKEY apartment_id;
-- ALTER TABLE raw_data.bookings ALTER SORTKEY (booking_date, apartment_id);