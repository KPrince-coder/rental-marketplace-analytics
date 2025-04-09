-- Create stored procedures for Redshift to populate the curated and presentation layers
-- Stored procedure to populate curated apartments
CREATE OR REPLACE PROCEDURE sp_populate_curated_apartments() AS $$ BEGIN -- Execute the SQL to populate curated_apartments
    EXECUTE '
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
    SELECT
        id AS apartment_id,
        title,
        source,
        price,
        currency,
        listing_created_on,
        is_active,
        last_modified_timestamp,
        DATEDIFF(day, listing_created_on, CURRENT_DATE) AS days_listed
    FROM
        raw_apartments
    WHERE
        NOT EXISTS (
            SELECT 1 FROM curated_apartments ca
            WHERE ca.apartment_id = raw_apartments.id
        );
    ';
-- Execute the SQL to populate curated_apartment_details
EXECUTE '
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
    SELECT
        ra.apartment_id,
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
    FROM
        curated_apartments ra
    JOIN
        raw_apartment_attributes raa ON ra.apartment_id = raa.apartment_id
    WHERE
        NOT EXISTS (
            SELECT 1 FROM curated_apartment_details cad
            WHERE cad.apartment_id = ra.apartment_id
        );
    ';
END;
$$ LANGUAGE plpgsql;
-- Stored procedure to populate curated user activity
CREATE OR REPLACE PROCEDURE sp_populate_curated_user_activity() AS $$ BEGIN -- Execute the SQL to populate curated_user_activity with viewings
    EXECUTE '
    INSERT INTO curated_user_activity (
        user_id,
        apartment_id,
        activity_type,
        activity_date,
        is_wishlisted,
        call_to_action
    )
    SELECT
        ruv.user_id,
        ruv.apartment_id,
        ''viewing'' AS activity_type,
        ruv.viewed_at AS activity_date,
        ruv.is_wishlisted,
        ruv.call_to_action
    FROM
        raw_user_viewings ruv
    JOIN
        curated_apartments ca ON ruv.apartment_id = ca.apartment_id
    WHERE
        NOT EXISTS (
            SELECT 1 FROM curated_user_activity cua
            WHERE cua.user_id = ruv.user_id
            AND cua.apartment_id = ruv.apartment_id
            AND cua.activity_type = ''viewing''
            AND cua.activity_date = ruv.viewed_at
        );
    ';
-- Execute the SQL to populate curated_user_activity with bookings
EXECUTE '
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
    SELECT
        rb.user_id,
        rb.apartment_id,
        ''booking'' AS activity_type,
        rb.booking_date AS activity_date,
        rb.booking_id,
        rb.checkin_date,
        rb.checkout_date,
        DATEDIFF(day, rb.checkin_date, rb.checkout_date) AS stay_duration,
        rb.total_price,
        rb.booking_status,
        rb.payment_status,
        rb.num_guests
    FROM
        raw_bookings rb
    JOIN
        curated_apartments ca ON rb.apartment_id = ca.apartment_id
    WHERE
        NOT EXISTS (
            SELECT 1 FROM curated_user_activity cua
            WHERE cua.booking_id = rb.booking_id
            AND cua.activity_type = ''booking''
        );
    ';
END;
$$ LANGUAGE plpgsql;
-- Stored procedure to populate presentation layer metrics
CREATE OR REPLACE PROCEDURE sp_populate_presentation_metrics() AS $$ BEGIN -- Execute the SQL to populate weekly rental performance metrics
    EXECUTE '
    INSERT INTO pres_rental_performance_weekly (
        week_start_date,
        avg_listing_price,
        median_listing_price,
        total_active_listings,
        new_listings,
        occupancy_rate,
        avg_price_per_sqft
    )
    WITH weekly_metrics AS (
        SELECT
            get_week_start_date(ca.last_modified_timestamp::DATE) AS week_start_date,
            AVG(ca.price_usd) AS avg_listing_price,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ca.price_usd) AS median_listing_price,
            COUNT(DISTINCT ca.apartment_id) AS total_active_listings,
            COUNT(DISTINCT CASE WHEN get_week_start_date(ca.listing_created_on::DATE) = get_week_start_date(ca.last_modified_timestamp::DATE) THEN ca.apartment_id END) AS new_listings,
            AVG(cad.price_per_sqft) AS avg_price_per_sqft
        FROM
            curated_apartments ca
        JOIN
            curated_apartment_details cad ON ca.apartment_id = cad.apartment_id
        WHERE
            ca.is_active = TRUE
        GROUP BY
            get_week_start_date(ca.last_modified_timestamp::DATE)
    ),
    weekly_bookings AS (
        SELECT
            get_week_start_date(cua.checkin_date::DATE) AS week_start_date,
            COUNT(DISTINCT cua.apartment_id) AS booked_apartments,
            SUM(cua.stay_duration) AS total_booked_nights
        FROM
            curated_user_activity cua
        WHERE
            cua.activity_type = ''booking''
            AND cua.booking_status = ''confirmed''
        GROUP BY
            get_week_start_date(cua.checkin_date::DATE)
    ),
    weekly_availability AS (
        SELECT
            get_week_start_date(ca.last_modified_timestamp::DATE) AS week_start_date,
            COUNT(DISTINCT ca.apartment_id) * 7 AS total_available_nights
        FROM
            curated_apartments ca
        WHERE
            ca.is_active = TRUE
        GROUP BY
            get_week_start_date(ca.last_modified_timestamp::DATE)
    )
    SELECT
        wm.week_start_date,
        wm.avg_listing_price,
        wm.median_listing_price,
        wm.total_active_listings,
        wm.new_listings,
        COALESCE(wb.total_booked_nights / NULLIF(wa.total_available_nights, 0) * 100, 0) AS occupancy_rate,
        wm.avg_price_per_sqft
    FROM
        weekly_metrics wm
    LEFT JOIN
        weekly_bookings wb ON wm.week_start_date = wb.week_start_date
    LEFT JOIN
        weekly_availability wa ON wm.week_start_date = wa.week_start_date
    WHERE
        NOT EXISTS (
            SELECT 1 FROM pres_rental_performance_weekly prpw
            WHERE prpw.week_start_date = wm.week_start_date
        );
    ';
-- Execute the SQL to populate weekly popular locations
EXECUTE '
    INSERT INTO pres_popular_locations_weekly (
        week_start_date,
        city,
        state,
        total_bookings,
        total_viewings,
        avg_price,
        booking_conversion_rate
    )
    WITH location_bookings AS (
        SELECT
            get_week_start_date(cua.activity_date::DATE) AS week_start_date,
            cad.city,
            cad.state,
            COUNT(*) AS total_bookings,
            AVG(cua.total_price_usd) AS avg_price
        FROM
            curated_user_activity cua
        JOIN
            curated_apartment_details cad ON cua.apartment_id = cad.apartment_id
        WHERE
            cua.activity_type = ''booking''
            AND cua.booking_status = ''confirmed''
        GROUP BY
            get_week_start_date(cua.activity_date::DATE),
            cad.city,
            cad.state
    ),
    location_viewings AS (
        SELECT
            get_week_start_date(cua.activity_date::DATE) AS week_start_date,
            cad.city,
            cad.state,
            COUNT(*) AS total_viewings
        FROM
            curated_user_activity cua
        JOIN
            curated_apartment_details cad ON cua.apartment_id = cad.apartment_id
        WHERE
            cua.activity_type = ''viewing''
        GROUP BY
            get_week_start_date(cua.activity_date::DATE),
            cad.city,
            cad.state
    )
    SELECT
        COALESCE(lb.week_start_date, lv.week_start_date) AS week_start_date,
        COALESCE(lb.city, lv.city) AS city,
        COALESCE(lb.state, lv.state) AS state,
        COALESCE(lb.total_bookings, 0) AS total_bookings,
        COALESCE(lv.total_viewings, 0) AS total_viewings,
        lb.avg_price,
        CASE
            WHEN COALESCE(lv.total_viewings, 0) > 0 THEN COALESCE(lb.total_bookings, 0) / COALESCE(lv.total_viewings, 1) * 100
            ELSE 0
        END AS booking_conversion_rate
    FROM
        location_bookings lb
    FULL OUTER JOIN
        location_viewings lv ON lb.week_start_date = lv.week_start_date
                            AND lb.city = lv.city
                            AND lb.state = lv.state
    WHERE
        NOT EXISTS (
            SELECT 1 FROM pres_popular_locations_weekly pplw
            WHERE pplw.week_start_date = COALESCE(lb.week_start_date, lv.week_start_date)
            AND pplw.city = COALESCE(lb.city, lv.city)
            AND pplw.state = COALESCE(lb.state, lv.state)
        );
    ';
-- Execute the SQL to populate user engagement metrics
EXECUTE '
    INSERT INTO pres_user_engagement_weekly (
        week_start_date,
        total_users,
        active_users,
        new_users,
        total_bookings,
        avg_bookings_per_user,
        avg_booking_duration,
        repeat_customer_rate
    )
    WITH weekly_users AS (
        SELECT
            get_week_start_date(cua.activity_date::DATE) AS week_start_date,
            COUNT(DISTINCT cua.user_id) AS total_users,
            COUNT(DISTINCT CASE WHEN cua.activity_type IN (''viewing'', ''booking'') THEN cua.user_id END) AS active_users
        FROM
            curated_user_activity cua
        GROUP BY
            get_week_start_date(cua.activity_date::DATE)
    ),
    new_users AS (
        SELECT
            get_week_start_date(cua.activity_date::DATE) AS week_start_date,
            COUNT(DISTINCT cua.user_id) AS new_users
        FROM
            curated_user_activity cua
        WHERE
            NOT EXISTS (
                SELECT 1 FROM curated_user_activity cua2
                WHERE cua2.user_id = cua.user_id
                AND cua2.activity_date < cua.activity_date
            )
        GROUP BY
            get_week_start_date(cua.activity_date::DATE)
    ),
    weekly_bookings AS (
        SELECT
            get_week_start_date(cua.activity_date::DATE) AS week_start_date,
            COUNT(*) AS total_bookings,
            COUNT(DISTINCT cua.user_id) AS booking_users,
            AVG(cua.stay_duration) AS avg_booking_duration
        FROM
            curated_user_activity cua
        WHERE
            cua.activity_type = ''booking''
            AND cua.booking_status = ''confirmed''
        GROUP BY
            get_week_start_date(cua.activity_date::DATE)
    ),
    repeat_customers AS (
        SELECT
            get_week_start_date(cua.activity_date::DATE) AS week_start_date,
            COUNT(DISTINCT cua.user_id) AS repeat_customers
        FROM
            curated_user_activity cua
        WHERE
            cua.activity_type = ''booking''
            AND cua.booking_status = ''confirmed''
            AND EXISTS (
                SELECT 1 FROM curated_user_activity cua2
                WHERE cua2.user_id = cua.user_id
                AND cua2.activity_type = ''booking''
                AND cua2.booking_status = ''confirmed''
                AND cua2.activity_date BETWEEN cua.activity_date - INTERVAL ''30 days'' AND cua.activity_date - INTERVAL ''1 day''
            )
        GROUP BY
            get_week_start_date(cua.activity_date::DATE)
    )
    SELECT
        wu.week_start_date,
        wu.total_users,
        wu.active_users,
        COALESCE(nu.new_users, 0) AS new_users,
        COALESCE(wb.total_bookings, 0) AS total_bookings,
        CASE
            WHEN COALESCE(wb.booking_users, 0) > 0 THEN COALESCE(wb.total_bookings, 0) / COALESCE(wb.booking_users, 1)
            ELSE 0
        END AS avg_bookings_per_user,
        COALESCE(wb.avg_booking_duration, 0) AS avg_booking_duration,
        CASE
            WHEN COALESCE(wb.booking_users, 0) > 0 THEN COALESCE(rc.repeat_customers, 0) / COALESCE(wb.booking_users, 1) * 100
            ELSE 0
        END AS repeat_customer_rate
    FROM
        weekly_users wu
    LEFT JOIN
        new_users nu ON wu.week_start_date = nu.week_start_date
    LEFT JOIN
        weekly_bookings wb ON wu.week_start_date = wb.week_start_date
    LEFT JOIN
        repeat_customers rc ON wu.week_start_date = rc.week_start_date
    WHERE
        NOT EXISTS (
            SELECT 1 FROM pres_user_engagement_weekly puew
            WHERE puew.week_start_date = wu.week_start_date
        );
    ';
END;
$$ LANGUAGE plpgsql;