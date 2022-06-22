WITH cte_cost AS (SELECT c.domain_name
                       , c.cost_uuid
                       , cost_type
                       , SPLIT_PART(cost_type, ':', 1)                AS cost_type_clean
                       , cost_created_ts AT TIME ZONE 'Europe/Berlin' AS cost_created_ts
                       , cost_ts AT TIME ZONE 'Europe/Berlin'         AS cost_ts
                       , cost_price_gross
                       , cost_price_net
                       , cost_price_currency
                       , c.car_id
                       , car.car_type
                       , m.map_unified_customer_id AS customer_id
                  FROM dwh_main.dim_cost c
                           JOIN dwh_main.dim_car car
                               ON car.car_id = c.car_id
                           JOIN dwh_main.map_customer_to_foxbox_domain m
                              ON m.map_fb_domain_name = c.domain_name
                                AND m.record_nbr_per_unified_customer_id = 1
                  WHERE cost_deleted_ts IS NULL
                    AND (cost_created_ts AT TIME ZONE 'Europe/Berlin')::DATE BETWEEN '2021-12-20' AND '2022-06-20'
                    AND c.cost_uuid = '0104a552-a349-4576-b988-e75c5e7e4cc4'
    )
SELECT cc.*
FROM cte_cost cc
    JOIN dwh_main.dim_combined_invoice_line i
        ON cc.customer_id = i.customer_id
            AND cc.cost_created_ts BETWEEN i.invoice_provisioning_start_dt AND i.invoice_provisioning_end_dt
WHERE i.invoice_status <> 'voided'
  AND i.product_group IN ('Admin', 'Pro')
  AND domain_name = 'com.vimcar.de.k45441392'