-- Populate Curated Layer from Raw Layer
-- Populate curated_apartments
INSERT INTO curated_apartments (
        apartment_id,
        title,
        source,
        price,
        currency,
        listing_created_on,
        is_active,
        last_modified_timestamp,
        days_listed
    )
SELECT id AS apartment_id,
    title,
    source,
    price,
    currency,
    listing_created_on,
    is_active,
    last_modified_timestamp,
    DATEDIFF(day, listing_created_on, CURRENT_DATE) AS days_listed
FROM raw_apartments
WHERE NOT EXISTS (
        SELECT 1
        FROM curated_apartments ca
        WHERE ca.apartment_id = raw_apartments.id
    );
-- Populate curated_apartment_details
INSERT INTO curated_apartment_details (
        apartment_id,
        category,
        bathrooms,
        bedrooms,
        fee,
        has_photo,
        pets_allowed,
        square_feet,
        price_per_sqft,
        address,
        city,
        state,
        latitude,
        longitude,
        amenities_text
    )
SELECT ra.apartment_id,
    raa.category,
    raa.bathrooms,
    raa.bedrooms,
    raa.fee,
    raa.has_photo,
    raa.pets_allowed,
    raa.square_feet,
    CASE
        WHEN raa.square_feet > 0 THEN ra.price / raa.square_feet
        ELSE NULL
    END AS price_per_sqft,
    raa.address,
    raa.cityname AS city,
    raa.state,
    raa.latitude,
    raa.longitude,
    -- Store amenities as a delimited string instead of an array
    raa.amenities AS amenities_text
FROM curated_apartments ra
    JOIN raw_apartment_attributes raa ON ra.apartment_id = raa.apartment_id
WHERE NOT EXISTS (
        SELECT 1
        FROM curated_apartment_details cad
        WHERE cad.apartment_id = ra.apartment_id
    );
-- Populate curated_user_activity with viewings
INSERT INTO curated_user_activity (
        user_id,
        apartment_id,
        activity_type,
        activity_date,
        is_wishlisted,
        call_to_action
    )
SELECT ruv.user_id,
    ruv.apartment_id,
    'viewing' AS activity_type,
    ruv.viewed_at AS activity_date,
    ruv.is_wishlisted,
    ruv.call_to_action
FROM raw_user_viewings ruv
    JOIN curated_apartments ca ON ruv.apartment_id = ca.apartment_id
WHERE NOT EXISTS (
        SELECT 1
        FROM curated_user_activity cua
        WHERE cua.user_id = ruv.user_id
            AND cua.apartment_id = ruv.apartment_id
            AND cua.activity_type = 'viewing'
            AND cua.activity_date = ruv.viewed_at
    );
-- Populate curated_user_activity with bookings
INSERT INTO curated_user_activity (
        user_id,
        apartment_id,
        activity_type,
        activity_date,
        booking_id,
        checkin_date,
        checkout_date,
        stay_duration,
        total_price,
        booking_status,
        payment_status,
        num_guests
    )
SELECT rb.user_id,
    rb.apartment_id,
    'booking' AS activity_type,
    rb.booking_date AS activity_date,
    rb.booking_id,
    rb.checkin_date,
    rb.checkout_date,
    DATEDIFF(day, rb.checkin_date, rb.checkout_date) AS stay_duration,
    rb.total_price,
    rb.booking_status,
    rb.payment_status,
    rb.num_guests
FROM raw_bookings rb
    JOIN curated_apartments ca ON rb.apartment_id = ca.apartment_id
WHERE NOT EXISTS (
        SELECT 1
        FROM curated_user_activity cua
        WHERE cua.booking_id = rb.booking_id
            AND cua.activity_type = 'booking'
    );