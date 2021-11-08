WITH geo_customers_chargebee AS (
    SELECT
        i.customer_id
         , fi.first_invoice_created_dt
         , sum(i.mrr_eur - i.refund_mrr_eur) AS mrr_eur -- only Geo product
         , round(sum(i.item_quantity) * (1-(sum(i.refund_mrr_eur) / sum(nullif(i.mrr_eur,0)))), 1) AS item_quantity
    FROM dwh_main.dim_combined_invoice_line i
             JOIN (SELECT customer_id, MIN(invoice_created_dt) AS first_invoice_created_dt
                   FROM dwh_main.dim_combined_invoice_line
                   GROUP BY customer_id) AS fi
                ON fi.customer_id = i.customer_id
             JOIN dwh_main.dim_combined_customer c
                  ON c.customer_id = i.customer_id
    WHERE 1=1
      AND i.invoice_status <> 'voided'
      AND i.full_refund_flag = false
      AND i.product_group = 'Geo'
      AND fi.first_invoice_created_dt = i.invoice_provisioning_start_dt -- only the 1st customer's purchase
      AND c.customer_id_shop IS NULL -- only new customers created in CB
    GROUP BY i.customer_id, fi.first_invoice_created_dt
    )
, map_customer_to_foxbox AS (SELECT c.customer_id
                                  , c.customer_country
                                  , gc.first_invoice_created_dt
--                                   , m.map_customer_domain_name
--                                   , m.map_fb_main_domain_name
                                  , m.map_fb_domain_name
--                                   , d.domain_configuration_template_match
                                  , gc.item_quantity
                             FROM geo_customers_chargebee gc
                                      JOIN dwh_main.dim_combined_customer c ON c.customer_id = gc.customer_id
                                      JOIN dwh_main.map_customer_to_foxbox_domain m
                                           ON c.customer_id = m.map_unified_customer_id
                                      LEFT JOIN dwh_main.dim_v_dom_domain d ON d.domain_name = m.map_fb_main_domain_name
                             WHERE m.record_nbr_per_unified_customer_id = 1
--                                AND gc.customer_id = '30010461047'
    )
, cars_raw AS (
                SELECT
                domain_name
                , (car_created_ts AT TIME ZONE 'Europe/Berlin')::DATE AS car_created_dt_cet
--                 , row_number() OVER (PARTITION BY domain_name)
--                 , item_quantity * 0.8
                , CASE WHEN row_number() OVER (PARTITION BY domain_name) > item_quantity * 0.8 THEN TRUE END flag_cars_treshold_reached
            FROM dwh_main.dim_car c
                JOIN map_customer_to_foxbox m
                    ON m.map_fb_domain_name = c.domain_name
--             WHERE domain_name = 'com.vimcar.gb.ee60fd66db7a484aaed7ed6fc803e8f9'
            ORDER BY domain_name, car_created_ts
)
SELECT
    c.*
    , first_car_created_dt - first_invoice_created_dt AS days_to_first_car
    , cars_treshold_reached_dt - first_invoice_created_dt AS days_to_cars_treshold_reached
FROM map_customer_to_foxbox c
    LEFT JOIN (SELECT domain_name
                , MIN(car_created_dt_cet) AS first_car_created_dt
                , MIN(car_created_dt_cet) FILTER ( WHERE flag_cars_treshold_reached = TRUE ) AS cars_treshold_reached_dt
               FROM cars_raw
               GROUP BY domain_name) car
        ON c.map_fb_domain_name = car.domain_name
;

30010461047
SELECT * FROM dwh_main.dim_car WHERE domain_name = 'com.vimcar.gb.fine-med-foods-ltd';
