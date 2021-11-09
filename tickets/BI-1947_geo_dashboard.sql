-- 80% cars treshold calc:
WITH customers_chargebee_foxbox AS (
    SELECT
        i.customer_id
         , fi.first_invoice_created_dt
         , m.map_fb_domain_name AS foxbox_domain_name
         , sum(i.mrr_eur - i.refund_mrr_eur) AS mrr_eur -- only Geo product
         , round(sum(i.item_quantity) * (1-(sum(i.refund_mrr_eur) / sum(nullif(i.mrr_eur,0)))), 1) AS item_quantity
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
    WHERE 1=1
      AND i.invoice_status <> 'voided'
      AND i.full_refund_flag = false
      AND i.product_group = 'Geo'
      AND fi.first_invoice_created_dt = i.invoice_provisioning_start_dt -- only the 1st customer's purchase
-- AND m.map_fb_domain_name = 'com.vimcar.gb.ee60fd66db7a484aaed7ed6fc803e8f9'
    GROUP BY i.customer_id, fi.first_invoice_created_dt, m.map_fb_domain_name
    )
    , cars_raw AS (
                SELECT
                domain_name
                , (car_created_ts AT TIME ZONE 'Europe/Berlin')::DATE AS car_created_dt_cet
                , (car_first_trip_start_ts AT TIME ZONE 'Europe/Berlin')::DATE AS car_first_trip_start_dt_cet
                , (car_first_trip_categorization_ts AT TIME ZONE 'Europe/Berlin')::DATE AS car_first_trip_categorization_dt_cet
                , CASE WHEN row_number() OVER (PARTITION BY domain_name) > item_quantity * 0.8 THEN TRUE END flag_cars_threshold_reached
            FROM dwh_main.dim_car c
                JOIN customers_chargebee_foxbox m
                    ON m.foxbox_domain_name = c.domain_name
            ORDER BY domain_name, car_created_ts)
   , users_raw AS (
    SELECT
        domain_name
         , MIN(principal_created_ts AT TIME ZONE 'Europe/Berlin')::DATE AS user_created_dt_cet
         , COUNT(p.principal_id) AS cnt_users_all
         , COUNT(p.principal_id) FILTER ( WHERE true) AS cnt_users_activated -- TODO: activated users
         , COUNT(p.principal_id) FILTER ( WHERE true) AS cnt_users_activated_w72h -- TODO: activated within 72h after activation link was sent
    FROM dwh_main.dim_principal p
             JOIN customers_chargebee_foxbox m
                  ON m.foxbox_domain_name = p.domain_name
    GROUP BY domain_name)
SELECT
    c.*
    , coalesce(first_car_created_dt, current_date) - first_invoice_created_dt AS cb_inv_days_to_first_car
    , coalesce(cars_threshold_reached_dt, current_date) - first_invoice_created_dt AS cb_inv_days_to_cars_threshold_reached
--     , CASE WHEN cars_treshold_reached_dt - first_invoice_created_dt <= 10 THEN TRUE ELSE FALSE END AS flg_10_days_to_cars_threshold_reached
FROM customers_chargebee_foxbox c
    LEFT JOIN (SELECT domain_name
                , MIN(car_created_dt_cet) AS first_car_created_dt
                , MIN(car_first_trip_start_dt_cet) AS car_first_trip_start_dt_cet
                , MIN(car_first_trip_categorization_dt_cet) AS car_first_trip_categorization_dt_cet
                , MIN(car_created_dt_cet) FILTER ( WHERE flag_cars_threshold_reached = TRUE ) AS cars_threshold_reached_dt  -- 80% of the cars since first Chargebee invoice created date
               FROM cars_raw
               GROUP BY domain_name) car
        ON c.foxbox_domain_name = car.domain_name
;

----------------------------------------------------------------------------------------------------------------
-- RETENTION:
WITH event_map_view_raw AS (
SELECT
    principal_uuid
    , date_trunc('day', amp_event_ts)::DATE AS date_key
    , amp_event_ts AS from_event_ts
    , lead(amp_event_ts) OVER (PARTITION BY principal_uuid ORDER BY principal_uuid, amp_event_ts) AS until_event_ts
    , date_part('second', lead(amp_event_ts::timestamp) OVER (PARTITION BY principal_uuid ORDER BY principal_uuid, amp_event_ts) - amp_event_ts::timestamp) AS event_duration
    , CASE
        WHEN concat_ws(',', amp_event_type, lead(amp_event_type) OVER (PARTITION BY principal_uuid ORDER BY principal_uuid, amp_event_ts)) = 'live locations - map loaded,navigation - navigated away'
            THEN TRUE
     END AS flag_from_until
FROM dwh_main.dim_amp_event_fleet
WHERE amp_event_type IN ('live locations - map loaded', 'navigation - navigated away')
--   AND principal_uuid = '0b80b69c-ba98-41b7-92b6-bd217ecc628b'
  AND amp_event_ts > '2021-01-01 00:00:00'
)
-- SELECT * FROM event_map_view_raw WHERE flag_from_until = TRUE ORDER BY from_event_ts;
SELECT
    principal_uuid
    , date_key
    , round(SUM(event_duration) / 60) AS total_minutes
    , round(SUM(event_duration) / 60 / 30) AS cnt_jbs
FROM event_map_view_raw
WHERE flag_from_until = TRUE
GROUP BY principal_uuid, date_key
;
