-- Create Curated Layer tables in Redshift
-- This layer applies transformations and standardizations to the raw data
-- Curated Apartments table
CREATE TABLE IF NOT EXISTS curated.curated_apartments (
    apartment_id INTEGER PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    source VARCHAR(50),
    price FLOAT NOT NULL,
    currency VARCHAR(3),
    listing_created_on TIMESTAMP,
    is_active BOOLEAN,
    last_modified_timestamp TIMESTAMP,
    days_listed INTEGER,
    -- Derived field
    etl_inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Curated Apartment Details table
CREATE TABLE IF NOT EXISTS curated.curated_apartment_details (
    apartment_id INTEGER PRIMARY KEY REFERENCES curated.curated_apartments(apartment_id),
    category VARCHAR(50),
    bathrooms INTEGER,
    bedrooms INTEGER,
    fee FLOAT,
    has_photo BOOLEAN,
    pets_allowed BOOLEAN,
    square_feet INTEGER,
    price_per_sqft FLOAT,
    -- Derived field
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    latitude FLOAT,
    longitude FLOAT,
    amenities_text VARCHAR(4000),
    -- Stored as a delimited string (e.g., comma-separated)
    etl_inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Curated User Activity table
CREATE TABLE IF NOT EXISTS curated.curated_user_activity (
    activity_id INTEGER IDENTITY(1, 1) PRIMARY KEY,
    user_id INTEGER NOT NULL,
    apartment_id INTEGER REFERENCES curated.curated_apartments(apartment_id),
    activity_type VARCHAR(20),
    -- 'viewing' or 'booking'
    activity_date TIMESTAMP,
    is_wishlisted BOOLEAN,
    call_to_action VARCHAR(50),
    booking_id INTEGER,
    checkin_date TIMESTAMP,
    checkout_date TIMESTAMP,
    stay_duration INTEGER,
    -- Derived field
    total_price FLOAT,
    booking_status VARCHAR(20),
    payment_status VARCHAR(20),
    num_guests INTEGER,
    etl_inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Create distribution keys and sort keys for performance
-- Set distribution and sort keys for curated_apartments
-- We'll skip these ALTER statements since the tables might already have these keys
-- If you need to set these keys, uncomment the following lines:
-- ALTER TABLE curated.curated_apartments ALTER DISTKEY apartment_id;
-- ALTER TABLE curated.curated_apartments ALTER SORTKEY (apartment_id, is_active, last_modified_timestamp);
-- ALTER TABLE curated.curated_apartment_details ALTER DISTKEY apartment_id;
-- ALTER TABLE curated.curated_apartment_details ALTER SORTKEY (apartment_id, city, state);
-- ALTER TABLE curated.curated_user_activity ALTER DISTKEY apartment_id;
-- ALTER TABLE curated.curated_user_activity ALTER SORTKEY (activity_date, user_id, apartment_id);