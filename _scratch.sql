SELECT
    f.customer_id
     , count(trip_id) FILTER ( WHERE t.trip_start_ts AT TIME ZONE 'Europe/Berlin' BETWEEN f.reference_dt - INTERVAL '60 days' AND f.reference_dt - INTERVAL '31 days') AS trips_2_mths_ago
     , count(trip_id) FILTER ( WHERE t.trip_start_ts AT TIME ZONE 'Europe/Berlin' BETWEEN f.reference_dt - INTERVAL '30 days' AND f.reference_dt) AS trips_last_30_days
     , count(trip_id) FILTER ( WHERE t.trip_start_ts AT TIME ZONE 'Europe/Berlin' BETWEEN f.reference_dt - INTERVAL '30 days' AND f.reference_dt)
    - count(trip_id) FILTER ( WHERE t.trip_start_ts AT TIME ZONE 'Europe/Berlin' BETWEEN f.reference_dt - INTERVAL '60 days' AND f.reference_dt - INTERVAL '31 days') AS trips_development_abs
     , coalesce(
        round(((coalesce(sum(trip_distance) FILTER ( WHERE t.trip_start_ts AT TIME ZONE 'Europe/Berlin' BETWEEN f.reference_dt - INTERVAL '30 days' AND f.reference_dt),0)
            - coalesce(sum(trip_distance) FILTER ( WHERE t.trip_start_ts AT TIME ZONE 'Europe/Berlin' BETWEEN f.reference_dt - INTERVAL '60 days' AND f.reference_dt - INTERVAL '31 days'),0))
            / nullif(sum(trip_distance) FILTER ( WHERE t.trip_start_ts AT TIME ZONE 'Europe/Berlin' BETWEEN f.reference_dt - INTERVAL '30 days' AND f.reference_dt),0)::numeric) *100)
    , -100 ) AS distance_development_pct
FROM data_mart_internal_reporting.ir_ml_customer_revenue f
         JOIN dwh_main.map_customer_to_foxbox_domain m
              ON m.map_unified_customer_id = f.customer_id
         JOIN dwh_main.dim_trip t
              ON t.domain_name = m.map_fb_domain_name
WHERE TRUE
  AND f.customer_is_fleet = TRUE
  AND t.trip_start_ts AT TIME ZONE 'Europe/Berlin' BETWEEN f.reference_dt - INTERVAL '60 days' AND f.reference_dt
--         AND f.domain_name = 'com.vimcar.bauunion-danko'
GROUP BY 1
;
SELECT *
FROM dwh_main.dim_v_gl_trip LIMIT 10