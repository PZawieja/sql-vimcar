-- 80% cars threshold calc:
WITH foxbox_chargebee_map AS (
    SELECT
        m.map_fb_domain_name AS foxbox_domain_name
         --, i.customer_id  -- uncomment to see detailed view
         , CASE
               WHEN array_agg(customer_status) @> '{"active"}' THEN 'active'
               WHEN array_agg(customer_status) @> '{"future"}' THEN 'future'
               WHEN array_agg(customer_status) @> '{"in_trial"}' THEN 'in_trial'
               WHEN array_agg(customer_status) @> '{"non_renewing"}' THEN 'non_renewing'
               WHEN array_agg(customer_status) @> '{"cancelled"}' THEN 'cancelled'
        END AS customer_subscription_status
         , MIN(fi.first_subscription_start_dt) AS first_subscription_start_dt
         , round(sum(i.item_quantity) * (1-(sum(i.refund_mrr_eur) / sum(nullif(i.mrr_eur,0)))), 1) AS order_item_quantity
--      , sum(i.mrr_eur - i.refund_mrr_eur) AS mrr_eur -- only Geo product
    FROM dwh_main.dim_combined_invoice_line i
             JOIN (SELECT i.customer_id
                        , c.customer_status
                        , MIN(i.invoice_provisioning_start_dt) AS first_subscription_start_dt
                   FROM dwh_main.dim_combined_invoice_line i
                            JOIN dwh_main.dim_combined_customer c
                                 ON c.customer_id = i.customer_id
                   WHERE 1=1
                     AND i.invoice_status <> 'voided'
                     AND i.full_refund_flag = false
                     AND c.customer_id_shop IS NULL -- only new customers created in CB
                   GROUP BY i.customer_id, c.customer_status) AS fi
                  ON fi.customer_id = i.customer_id
             JOIN dwh_main.map_customer_to_foxbox_domain m
                  ON i.customer_id = m.map_unified_customer_id
                      AND m.record_nbr_per_unified_customer_id = 1
                      AND m.cnt_foxbox_domains_per_unified_customer_id > 0
    WHERE 1=1
      AND i.invoice_status <> 'voided'
      AND i.full_refund_flag = false
      AND i.product_group = 'Geo'
      AND fi.first_subscription_start_dt = i.invoice_provisioning_start_dt -- only the 1st customer's purchase
--       AND m.map_fb_domain_name = 'com.vimcar.de.egloimmobilien'  -- of the FB domain is linked to multiple CB domains then we take the minimum first invoice created date of all CB customer IDs
    GROUP BY m.map_fb_domain_name --, i.customer_id -- uncomment to see detailed view
)
   , cars_raw AS (
    SELECT
        domain_name
         , (car_created_ts AT TIME ZONE 'Europe/Berlin') AS car_created_ts
         , (car_first_trip_start_ts AT TIME ZONE 'Europe/Berlin') AS car_first_trip_start_ts
         , (car_first_trip_categorization_ts AT TIME ZONE 'Europe/Berlin') AS car_first_trip_categorization_ts
         , CASE WHEN row_number() OVER (PARTITION BY domain_name ORDER BY car_created_ts) > order_item_quantity * 0.8 THEN TRUE END flag_cars_threshold_reached
--          , row_number() OVER (PARTITION BY domain_name ORDER BY car_created_ts)
--          , car_created_ts
--          , m.first_invoice_created_dt
--          , m.item_quantity
    FROM dwh_main.dim_car c
             JOIN foxbox_chargebee_map m
                  ON m.foxbox_domain_name = c.domain_name
    ORDER BY domain_name)
   , users AS (
    SELECT
        u.v_id_user_domain AS user_domain_name
         , MIN(u.v_id_user_created_ts AT TIME ZONE 'Europe/Berlin')::DATE AS first_user_created_dt
         , COUNT(u.principal_id) AS cnt_users_created
         , COUNT(u.principal_id) FILTER ( WHERE v_id_user_deleted_ts IS NOT NULL ) AS cnt_users_deleted
         , MIN(u.v_id_user_password_created_ts AT TIME ZONE 'Europe/Berlin') AS first_user_activated_ts
         , MIN(u.v_id_user_password_created_ts AT TIME ZONE 'Europe/Berlin')::DATE AS first_user_activated_dt
         , COUNT(u.principal_id) FILTER ( WHERE u.v_id_user_password_created_ts IS NOT NULL) AS cnt_users_activated -- users who set up the password
         , COUNT(u.principal_id) FILTER ( WHERE EXTRACT(EPOCH FROM (u.v_id_user_password_created_ts - u.v_id_user_created_ts ))/3600 <= 72) AS cnt_users_activated_w72h
    FROM dwh_main.dim_v_id_user u
             JOIN foxbox_chargebee_map m
                  ON m.foxbox_domain_name = u.v_id_user_domain
    WHERE v_id_user_deleted_ts IS NULL
       OR v_id_user_deleted_ts::DATE - v_id_user_created_ts::DATE > 2 -- filter out users deleted within 2 days since created date
    GROUP BY v_id_user_domain)

SELECT
    m.foxbox_domain_name
     , u.first_user_created_dt
     , u.cnt_users_created
     , u.cnt_users_deleted
     , u.first_user_activated_dt
     , u.cnt_users_activated
     , u.cnt_users_activated_w72h
     , round(u.cnt_users_activated / u.cnt_users_created::DECIMAL(10,2),2) AS pct_users_activated
     , round(u.cnt_users_activated_w72h / u.cnt_users_created::DECIMAL(10,2),2) AS pct_users_activated_w72h
     , m.first_subscription_start_dt
     , m.customer_subscription_status
     , m.order_item_quantity
     , car.first_car_created_ts
     , CASE
           WHEN first_car_created_ts IS NOT NULL
               THEN GREATEST(first_car_created_ts::DATE - first_subscription_start_dt, 0)
        END AS days_first_subscr_to_first_car_created  -- if the cars were created before the first invoice then show 0 instead of negative nbr of days
     , CASE
           WHEN first_car_created_ts IS NOT NULL
               THEN GREATEST(cars_threshold_reached_dt::DATE - first_subscription_start_dt, 0)
        END AS days_first_subscr_to_cars_created_threshold
     , CASE
           WHEN first_user_activated_ts IS NOT NULL
               AND first_car_created_ts IS NOT NULL
            THEN GREATEST(DATE_PART('day', first_car_created_ts - first_user_activated_ts) * 24 +
                        DATE_PART('hour', first_car_created_ts - first_user_activated_ts), 0)
--                THEN first_car_created_ts - first_user_activated_ts
        END AS hours_first_user_activated_to_first_car_created
     , CASE
           WHEN first_user_activated_ts IS NOT NULL
               AND car_first_trip_start_ts IS NOT NULL
               THEN GREATEST(DATE_PART('day', car_first_trip_start_ts - first_user_activated_ts) * 24 +
                             DATE_PART('hour', car_first_trip_start_ts - first_user_activated_ts), 0)
--                THEN first_car_created_ts - first_user_activated_ts
    END AS hours_first_user_activated_to_first_trip
FROM foxbox_chargebee_map m
         LEFT JOIN users u
                   ON u.user_domain_name = m.foxbox_domain_name
         LEFT JOIN (SELECT domain_name
                         , MIN(car_created_ts) AS first_car_created_ts
                         , MIN(car_first_trip_start_ts) AS car_first_trip_start_ts
                         , MIN(car_first_trip_categorization_ts) AS car_first_trip_categorization_ts
                         , MIN(car_created_ts) FILTER ( WHERE flag_cars_threshold_reached = TRUE ) AS cars_threshold_reached_dt  -- 80% of the cars since first Chargebee invoice created date
                    FROM cars_raw
                    GROUP BY domain_name) car
                   ON m.foxbox_domain_name = car.domain_name
-- WHERE m.foxbox_domain_name = 'com.vimcar.gb.88f000241a324d34ad995ed6d2f2c2b6'
;


----------------------------------------------------------------------------------------------------------------
-- RETENTION:
-- Have the map view open for more than 30min in total
WITH users_in_scope AS (
    SELECT DISTINCT
        p.principal_uuid
        , p.domain_name AS foxbox_domain_name
    FROM dwh_main.dim_combined_invoice_line i
             JOIN (SELECT i.customer_id, MIN(i.invoice_created_dt) AS first_invoice_created_dt
                   FROM dwh_main.dim_combined_invoice_line i
                            JOIN dwh_main.dim_combined_customer c
                                 ON c.customer_id = i.customer_id
                   WHERE 1=1
                     AND i.invoice_status <> 'voided'
                     AND i.full_refund_flag = false
                     AND c.customer_id_shop IS NULL -- only new customers created in CB
                   GROUP BY i.customer_id) AS fi
                  ON fi.customer_id = i.customer_id
             JOIN dwh_main.map_customer_to_foxbox_domain m
                  ON i.customer_id = m.map_unified_customer_id
                      AND m.record_nbr_per_unified_customer_id = 1
             JOIN dwh_main.dim_principal p
                  ON p.domain_name = m.map_fb_domain_name
    WHERE 1=1
      AND i.invoice_status <> 'voided'
      AND i.full_refund_flag = false
      AND i.product_group = 'Geo'
      AND fi.first_invoice_created_dt = i.invoice_provisioning_start_dt -- only the 1st customer's purchase
-- AND m.map_fb_domain_name = 'com.vimcar.gb.ee60fd66db7a484aaed7ed6fc803e8f9'
)
, event_map_view_raw AS (
SELECT
    a.principal_uuid
    , u.foxbox_domain_name
    , date_trunc('day', amp_event_ts AT TIME ZONE 'Europe/Berlin')::DATE AS date_key
    , amp_event_ts AS from_event_ts
    , lead(amp_event_ts) OVER (PARTITION BY a.principal_uuid ORDER BY a.principal_uuid, amp_event_ts) AS until_event_ts
    , date_part('second', lead(amp_event_ts::timestamp) OVER (PARTITION BY a.principal_uuid ORDER BY a.principal_uuid, amp_event_ts) - amp_event_ts::timestamp) AS event_duration
    , CASE
        WHEN concat_ws(',', amp_event_type, lead(amp_event_type) OVER (PARTITION BY a.principal_uuid ORDER BY a.principal_uuid, amp_event_ts)) = 'live locations - map loaded,navigation - navigated away'
            THEN TRUE
     END AS flag_from_until
FROM dwh_main.dim_amp_event_fleet a
         JOIN users_in_scope u
              ON u.principal_uuid = a.principal_uuid::UUID
WHERE amp_event_type IN ('live locations - map loaded', 'navigation - navigated away')
--   AND principal_uuid = '0b80b69c-ba98-41b7-92b6-bd217ecc628b'
  AND amp_event_ts > '2021-01-01 00:00:00'
)
-- SELECT * FROM event_map_view_raw WHERE flag_from_until = TRUE ORDER BY from_event_ts;
, result_user_level AS (SELECT e.principal_uuid
                             , foxbox_domain_name
                             , date_key
                             , ROUND(SUM(event_duration) / 60 / 30) AS job_count
                             , ROUND(SUM(event_duration) / 60)      AS total_minutes
                        FROM event_map_view_raw e
                        WHERE flag_from_until = TRUE
                        GROUP BY e.principal_uuid, foxbox_domain_name, date_key)
SELECT foxbox_domain_name
     , date_key
     , 'map_view_open'      AS job_type
     , SUM(job_count)       AS job_count
     , SUM(total_minutes)   AS total_minutes
FROM result_user_level
GROUP BY foxbox_domain_name, date_key
    ;



WITH users_in_scope AS (
    SELECT DISTINCT
        p.principal_uuid
        , p.domain_name AS foxbox_domain_name
        FROM dwh_main.dim_combined_invoice_line i
             JOIN (SELECT i.customer_id, MIN(i.invoice_created_dt) AS first_invoice_created_dt
                   FROM dwh_main.dim_combined_invoice_line i
                            JOIN dwh_main.dim_combined_customer c
                                 ON c.customer_id = i.customer_id
                   WHERE 1=1
                     AND i.invoice_status <> 'voided'
                     AND i.full_refund_flag = false
                     AND c.customer_id_shop IS NULL -- only new customers created in CB
                   GROUP BY i.customer_id) AS fi
                  ON fi.customer_id = i.customer_id
             JOIN dwh_main.map_customer_to_foxbox_domain m
                  ON i.customer_id = m.map_unified_customer_id
                      AND m.record_nbr_per_unified_customer_id = 1
             JOIN dwh_main.dim_principal p
                ON p.domain_name = m.map_fb_domain_name
    WHERE 1=1
      AND i.invoice_status <> 'voided'
      AND i.full_refund_flag = false
      AND i.product_group = 'Geo'
      AND fi.first_invoice_created_dt = i.invoice_provisioning_start_dt -- only the 1st customer's purchase
-- AND m.map_fb_domain_name = 'com.vimcar.gb.ee60fd66db7a484aaed7ed6fc803e8f9'
)
SELECT
    u.foxbox_domain_name
     , date_trunc('day', amp_event_ts AT TIME ZONE 'Europe/Berlin')::DATE AS date_key
     , CASE
         WHEN amp_event_type = 'custom locations - created'
             THEN 'custom_locations'
         WHEN amp_event_type = 'custom locations - created'
             THEN 'request_trips_export'
         WHEN amp_event_type = 'route tracking - routes loaded'
             THEN 'request_route_history'
         END AS job_type
     , count(*) AS job_count
     , count(DISTINCT u.principal_uuid) AS user_count
FROM dwh_main.dim_amp_event_fleet a
    JOIN users_in_scope u
        ON u.principal_uuid = a.principal_uuid::UUID
WHERE amp_event_type IN ('custom locations - created','route tracking - trips exported','route tracking - routes loaded')
  AND amp_event_ts > '2021-01-01 00:00:00'
GROUP BY u.foxbox_domain_name, a.amp_event_type, date_trunc('day', a.amp_event_ts AT TIME ZONE 'Europe/Berlin')::DATE;

SELECT * FROM dwh_main.dim_v_id_user WHERE v_id_user_domain = 'com.vimcar.gb.be63a477c3f348479d3a3883cf598427'