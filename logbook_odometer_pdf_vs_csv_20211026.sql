-- car id 239300988
-- trip id 340275600
-- odo inq at 2020-12-11 16:21:04
-- trip start 2020-12-11 16:20:39
-- trip end 2020-12-11 16:26:33

----------------------------------------------------------------------------------------------------------------------------------------
-- WORKING SOLUTION:
WITH odo_inq AS (
    SELECT DISTINCT
        doi.car_id
                  , doi.odometer_inquiry_ts AS odo_inq_ts_valid_from
                  , coalesce(LEAD(doi.odometer_inquiry_ts, 1) OVER (ORDER BY doi.odometer_inquiry_ts), now()) AS odo_inq_ts_valid_until
                  , doi.total_odometer_value_km
    FROM c_tmp.dm_odometerinquiry doi
    WHERE doi.car_id = ${car_id}
      AND doi.total_odometer_value_km IS NOT NULL
      AND doi.odometer_inquiry_is_valid IS TRUE
), odo_calc AS (
    SELECT
        trip_id
         , trip_start_ts
         , ROW_NUMBER () OVER (PARTITION BY cte.total_odometer_value_km ORDER BY trip_start_ts) AS trips_since_odo_inq
         , SUM ((trip_distance + distance_adjustment) / 1000::decimal) OVER (PARTITION BY cte.total_odometer_value_km ORDER BY trip_start_ts) AS rolling_sum
         , cte.total_odometer_value_km
         , cte.total_odometer_value_km + SUM ((trip_distance + distance_adjustment) / 1000::decimal) OVER (PARTITION BY cte.total_odometer_value_km ORDER BY trip_start_ts) AS "Ankunft_kmstand"
    FROM c_tmp.dm_trip dt
             JOIN odo_inq cte
                  ON cte.car_id = dt.car_id

                      AND dt.trip_start_ts + '3 Minutes'::INTERVAL >= cte.odo_inq_ts_valid_from
                      AND dt.trip_start_ts + '3 Minutes'::INTERVAL < cte.odo_inq_ts_valid_until
                      AND dt.trip_end_ts - '3 Minutes'::INTERVAL >= cte.odo_inq_ts_valid_from
                      AND dt.trip_end_ts - '3 Minutes'::INTERVAL < cte.odo_inq_ts_valid_until

         -- AND dt.trip_start_ts >= cte.odo_inq_ts_valid_from - '3 Minutes'::INTERVAL AND dt.trip_start_ts < cte.odo_inq_ts_valid_until - '3 Minutes'::INTERVAL  -- working solution 1
         -- AND (dt.trip_start_ts + '3 Minutes'::INTERVAL >= cte.odo_inq_ts_valid_from AND dt.trip_start_ts + '3 Minutes'::INTERVAL < cte.odo_inq_ts_valid_until) -- working solution 2
         -- odometer input within first 3 minutes of a trip is treated as happened before the trip
         -- odometer input within last 3 minutes of a trip is treated as happened after the trip

    WHERE dt.car_id = ${car_id}
)
SELECT
    dt.domain_name
     , dt.trip_id
     , 'Fahrt' AS type
     , NULL::date AS "Änderungszeitpunkt"
     , trip_category AS "Kategorie"
     , dc.car_licence_plate AS "Kennzeichen"
     , (dt.trip_start_ts AT TIME ZONE 'Europe/Berlin')::date AS "Abfahrt_datum"
     , (trip_end_ts AT TIME ZONE 'Europe/Berlin')::date AS "Ankunft_datum"
     , (CASE
            WHEN cte2.trips_since_odo_inq = 1 THEN cte2.total_odometer_value_km
            ELSE round(cte2.total_odometer_value_km + LAG(rolling_sum, 1) OVER (PARTITION BY cte2.total_odometer_value_km ORDER BY cte2.trip_start_ts))
    END)::int AS "Abfahrt_kmstand"
     , round(cte2."Ankunft_kmstand")::int AS "Ankunft_kmstand"
     , round(trip_distance_business / 1000::decimal)::smallint AS "Betrieb_km"
     , round(trip_distance_commute / 1000::decimal)::smallint AS "Arbeitsweg_km"
     , round(trip_distance_private / 1000::decimal)::smallint AS "Privat_km"
     , CASE is_merged_trip WHEN TRUE THEN 'Ja' ELSE 'Nein' END AS "Zusammengeführt"
     , CASE
           WHEN trip_category <> 'PRIVATE' AND trip_start_address ->> 'streetName' IS NOT NULL
               THEN (trip_start_address ->> 'streetName') || ', ' || (trip_start_address ->> 'postalCode') || ' - ' || (trip_start_address ->> 'city') || ', ' || (trip_start_address ->> 'country')
           WHEN trip_category <> 'PRIVATE' AND trip_start_address ->> 'streetName' IS NULL
               THEN trip_start_address ->> 'venueName'
    END AS "Start Adresse"
     , CASE
           WHEN trip_category <> 'PRIVATE' AND trip_end_address ->> 'streetName' IS NOT NULL
               THEN (trip_end_address ->> 'streetName') || ', ' || (trip_end_address ->> 'postalCode') || ' - ' || (trip_end_address ->> 'city') || ', ' || (trip_end_address ->> 'country')
           WHEN trip_category <> 'PRIVATE' AND trip_end_address ->> 'streetName' IS NULL
               THEN trip_end_address ->> 'venueName'
    END AS "End Adresse"
     , trip_contact_name AS "Geschäftspartner"
     , trip_contact_company AS "Firma"
     , trip_reason AS "Anlass"
     , trip_driver_information AS "Fahrer"
     , trip_comment AS "Bemerkung"
FROM c_tmp.dm_trip dt
         JOIN c_tmp.dm_car dc
              ON dc.car_id = dt.car_id
         JOIN odo_calc cte2
              ON cte2.trip_id = dt.trip_id
WHERE dt.car_id = ${car_id}
ORDER BY trip_id;




----------------------------------------------------------------------------------------------------------------------------------------
-- TESTS
WITH odo_inq AS (
    SELECT DISTINCT
        doi.car_id
                  , doi.odometer_inquiry_ts AS odo_inq_ts_valid_from
                  , coalesce(LEAD(doi.odometer_inquiry_ts, 1) OVER (ORDER BY doi.odometer_inquiry_ts), now()) AS odo_inq_ts_valid_until
                  , doi.total_odometer_value_km
    FROM c_tmp.dm_odometerinquiry doi
    WHERE doi.car_id = 239300988
      AND doi.total_odometer_value_km IS NOT NULL
      AND doi.odometer_inquiry_is_valid IS TRUE
)
   , test AS (
    SELECT
        dt.trip_id
         , dt.trip_start_ts
         , dt.trip_end_ts
         , dt.trip_end_ts - '3 Minutes'::INTERVAL AS trip_end_minus_3m
         , cte.odo_inq_ts_valid_from
         , cte.odo_inq_ts_valid_until
         , CASE WHEN TRUE
        AND dt.trip_start_ts + '3 Minutes'::INTERVAL >= cte.odo_inq_ts_valid_from
        AND dt.trip_start_ts + '3 Minutes'::INTERVAL < cte.odo_inq_ts_valid_until
                    --     AND dt.trip_end_ts - '3 Minutes'::INTERVAL >= cte.odo_inq_ts_valid_from
--     AND dt.trip_end_ts - '3 Minutes'::INTERVAL < cte.odo_inq_ts_valid_until
                    THEN TRUE
        END flag_join_1
         , CASE WHEN TRUE
        --     AND dt.trip_start_ts + '3 Minutes'::INTERVAL >= cte.odo_inq_ts_valid_from
--     AND dt.trip_start_ts + '3 Minutes'::INTERVAL < cte.odo_inq_ts_valid_until
        AND dt.trip_end_ts - '3 Minutes'::INTERVAL >= cte.odo_inq_ts_valid_from
        AND dt.trip_end_ts - '3 Minutes'::INTERVAL < cte.odo_inq_ts_valid_until
                    THEN TRUE
        END flag_join_2
         , CASE
               WHEN cte.odo_inq_ts_valid_from BETWEEN dt.trip_end_ts - '3 Minutes'::INTERVAL AND dt.trip_end_ts
                   OR cte.odo_inq_ts_valid_until BETWEEN dt.trip_end_ts - '3 Minutes'::INTERVAL AND dt.trip_end_ts
                   THEN TRUE
        END tru_if_odoming_within_last_3_mins_of_trip
    FROM c_tmp.dm_trip dt
             JOIN odo_inq cte
                  ON cte.car_id = dt.car_id
                      AND trip_start_ts BETWEEN cte.odo_inq_ts_valid_from AND cte.odo_inq_ts_valid_until
--                   AND dt.trip_start_ts + '3 Minutes'::INTERVAL >= cte.odo_inq_ts_valid_from
--                   AND dt.trip_start_ts + '3 Minutes'::INTERVAL < cte.odo_inq_ts_valid_until
--                   AND dt.trip_end_ts - '3 Minutes'::INTERVAL >= cte.odo_inq_ts_valid_from
--                   AND dt.trip_end_ts - '3 Minutes'::INTERVAL < cte.odo_inq_ts_valid_until
)
SELECT * FROM test
WHERE TRUE
    AND flag_join_1 is NULL OR flag_join_2 is NULL
--   AND flag_join is null
--     AND flag_join = true and tru_if_odoming_within_last_3_mins_of_trip = true
