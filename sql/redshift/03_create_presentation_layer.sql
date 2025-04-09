-- Create Presentation Layer tables in Redshift
-- This layer contains aggregated data and metrics for reporting
-- Weekly Rental Performance Metrics
CREATE TABLE IF NOT EXISTS presentation.pres_rental_performance_weekly (
    week_start_date DATE PRIMARY KEY,
    avg_listing_price FLOAT,
    -- Average price of active rental listings
    median_listing_price FLOAT,
    -- Median price of active rental listings
    total_active_listings INTEGER,
    -- Count of active listings
    new_listings INTEGER,
    -- Count of new listings created that week
    occupancy_rate FLOAT,
    -- Percentage of available rental nights that were booked
    avg_price_per_sqft FLOAT,
    -- Average price per square foot
    etl_inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Monthly Rental Performance Metrics
CREATE TABLE IF NOT EXISTS presentation.pres_rental_performance_monthly (
    month_start_date DATE PRIMARY KEY,
    avg_listing_price FLOAT,
    median_listing_price FLOAT,
    total_active_listings INTEGER,
    new_listings INTEGER,
    occupancy_rate FLOAT,
    avg_price_per_sqft FLOAT,
    etl_inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Weekly Popular Locations
CREATE TABLE IF NOT EXISTS presentation.pres_popular_locations_weekly (
    week_start_date DATE,
    city VARCHAR(100),
    state VARCHAR(50),
    total_bookings INTEGER,
    total_viewings INTEGER,
    avg_price FLOAT,
    booking_conversion_rate FLOAT,
    -- Bookings / Viewings
    PRIMARY KEY (week_start_date, city, state),
    etl_inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Weekly Top Performing Listings
CREATE TABLE IF NOT EXISTS presentation.pres_top_listings_weekly (
    week_start_date DATE,
    apartment_id INTEGER,
    title VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    total_revenue FLOAT,
    total_bookings INTEGER,
    avg_stay_duration FLOAT,
    occupancy_rate FLOAT,
    PRIMARY KEY (week_start_date, apartment_id),
    etl_inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Weekly User Engagement Metrics
CREATE TABLE IF NOT EXISTS presentation.pres_user_engagement_weekly (
    week_start_date DATE,
    total_users INTEGER,
    active_users INTEGER,
    -- Users with at least one viewing or booking
    new_users INTEGER,
    -- Users who made their first viewing/booking this week
    total_bookings INTEGER,
    avg_bookings_per_user FLOAT,
    avg_booking_duration FLOAT,
    repeat_customer_rate FLOAT,
    -- Users who booked more than once in 30 days
    PRIMARY KEY (week_start_date),
    etl_inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- User Booking History
CREATE TABLE IF NOT EXISTS presentation.pres_user_booking_history (
    user_id INTEGER,
    week_start_date DATE,
    total_bookings INTEGER,
    total_spend FLOAT,
    avg_booking_duration FLOAT,
    favorite_city VARCHAR(100),
    -- Most booked city
    is_repeat_customer BOOLEAN,
    -- Booked more than once in 30 days
    PRIMARY KEY (user_id, week_start_date),
    etl_inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Create distribution and sort keys for performance
-- We'll skip these ALTER statements since the tables might already have these keys
-- If you need to set these keys, uncomment the following lines:
-- Set distribution and sort keys for pres_rental_performance_weekly
-- ALTER TABLE presentation.pres_rental_performance_weekly ALTER DISTSTYLE ALL;
-- ALTER TABLE presentation.pres_rental_performance_weekly ALTER SORTKEY (week_start_date);
-- Set distribution and sort keys for pres_rental_performance_monthly
-- ALTER TABLE presentation.pres_rental_performance_monthly ALTER DISTSTYLE ALL;
-- ALTER TABLE presentation.pres_rental_performance_monthly ALTER SORTKEY (month_start_date);
-- Set distribution and sort keys for pres_popular_locations_weekly
-- ALTER TABLE presentation.pres_popular_locations_weekly ALTER DISTKEY city;
-- ALTER TABLE presentation.pres_popular_locations_weekly ALTER SORTKEY (week_start_date, city, state);
-- Set distribution and sort keys for pres_top_listings_weekly
-- ALTER TABLE presentation.pres_top_listings_weekly ALTER DISTKEY apartment_id;
-- ALTER TABLE presentation.pres_top_listings_weekly ALTER SORTKEY (week_start_date, total_revenue);
-- Set distribution and sort keys for pres_user_engagement_weekly
-- ALTER TABLE presentation.pres_user_engagement_weekly ALTER DISTSTYLE ALL;
-- ALTER TABLE presentation.pres_user_engagement_weekly ALTER SORTKEY (week_start_date);
-- Set distribution and sort keys for pres_user_booking_history
-- ALTER TABLE presentation.pres_user_booking_history ALTER DISTKEY user_id;
-- ALTER TABLE presentation.pres_user_booking_history ALTER SORTKEY (user_id, week_start_date);