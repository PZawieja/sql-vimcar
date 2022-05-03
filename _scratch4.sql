WITH cte_product AS (
    SELECT
        m.product_uuid
         , m.product_key
         , m.product_name
         , m.product_note
         , m.product_length_and_payment
         , m.product_family
         , p.product_price_net
         , (p.product_created_ts AT TIME ZONE 'Europe/Berlin')::DATE AS product_created_dt
         , CASE
            WHEN (p.product_created_ts AT TIME ZONE 'Europe/Berlin')::DATE >= '2020-01-01'
                OR p.product_uuid IN ('061928ea-042b-4182-a1fe-a711f5134389',
                                     'b16a5957-b5d1-4b2f-860b-331c25fe2989',
                                     '0a64e2b9-406c-4810-a3d2-c9d67dc95b89',
                                     '65a0bfd5-d789-4a7e-aad2-9787d1f1048e',
                                     '78d3a806-e3ab-4eb6-bb81-50d6ba432487',
                                     '65a0bfd5-d789-4a7e-aad2-9787d1f1048e',
                                     '7d6fb064-5df7-4858-b543-bf4cd612897a',
                                     '47d0bd5b-fdaa-492e-90d7-20df81940aea',
                                     'f0622671-3256-41b0-9a49-aee469973257',
                                     '83bc9af9-cc35-4d7e-ab84-e49fc1cea89d',
                                     '552a3e33-c99b-49db-a01c-fe949e73508a',
                                     'dedbe9ed-b0c7-4746-814f-97bbd1c5ba69',
                                     'f0622671-3256-41b0-9a49-aee469973257',
                                      '8c864718-f6cf-4cd9-8fda-71b7aed4fec6',
                                      'e9ff0a5f-fb44-4b2b-8a05-72896f09bdf5',
                                      '9b892ebc-6f1a-4cab-a95e-cac364fd6d49',
                                      '5fc29394-7dcb-4535-9641-5dfbdc503f80',
                                      '2d7da6f5-8539-40bc-b740-fb99ecb76113',
                                      'e9ff0a5f-fb44-4b2b-8a05-72896f09bdf5',
                                      '7df58396-d3ac-46ba-8251-ab1353270a4d',
                                      '65a0bfd5-d789-4a7e-aad2-9787d1f1048e',
                                     'f02d2366-735e-4a64-9fee-8fdf894fe10d',
                                      'b16a5957-b5d1-4b2f-860b-331c25fe2989',
                                      'db7eed7a-7571-46fb-a96c-8cf79aa00c09',
                                      '91e48c6c-d6c0-438c-a04e-3df90e879680',
                                     '6c5c1118-9aa6-42b8-9835-365ec955a3d5',
                                     '07d1f6a2-3415-4f9d-b927-f4b78bcb564a',
                                     '405001c6-d81f-42fa-8bd1-cea26ec3499c')
                THEN FALSE
            ELSE TRUE
            END AS is_old_price_product
    FROM dwh_main.umd_product_map m
             JOIN dwh_main.dim_product p
                  ON p.product_uuid = m.product_uuid
    WHERE m.product_family NOT IN ('Hardware', 'Other')
)
   , customer_product AS (SELECT c.customer_id
                               , ctr.contract_id
                               , ctr.contract_status
                               , upm.product_family     AS product_family
                               , upm.product_uuid       AS product_uuid
                               , upm.product_name       AS product_name
                               , upm.product_created_dt AS product_created_dt
                               , upm.product_note       AS product_note
                               , upm.product_price_net  AS product_price_net
                               , upm.product_length_and_payment
                               , upm.is_old_price_product
                          FROM dwh_main.dim_customer c
                                   JOIN dwh_main.dim_contract ctr
                                       ON ctr.customer_uuid = c.customer_uuid
                                   JOIN dwh_main.fact_contract_line fcl
                                       ON ctr.contract_key = fcl.contract_key
                                   JOIN cte_product upm
                                       ON upm.product_key = fcl.contract_item_product_key
                          WHERE fcl.opcode <> 'D'
                        )
, cte_final AS (
SELECT
cp.customer_id
, CASE
    WHEN cc.customer_is_fleet = TRUE
        THEN 'B2B'
    ELSE 'B2C'
    END AS customer_type
, cc.customer_current_mrr_eur
, cc.customer_contact_email
, cc.customer_contact_first_name
, cc.customer_contact_last_name
, cp.contract_id
, cc.min_subscription_start_dt AS first_subscription_start_dt
, cs.subscription_end_dt
, cs.subscription_coupon_1 AS coupon_code
, dc.coupon_discount_type
, dc.coupon_discount_value
, product_family
, product_uuid
, product_name
, product_length_and_payment
, product_created_dt
, product_note
, product_price_net
, is_old_price_product
FROM customer_product cp
    JOIN dwh_main.dim_combined_customer cc
        ON cc.customer_id_shop = cp.customer_id
    JOIN dwh_main.dim_combined_subscription cs
        ON cs.subscription_id = cp.contract_id
    LEFT JOIN dwh_main.dim_coupon dc
        ON dc.coupon_code = cs.subscription_coupon_1
WHERE cs.subscription_status = 'active'
)
SELECT DISTINCT
product_family
, product_uuid
, product_name
, product_note
, product_created_dt
FROM cte_final c
WHERE is_old_price_product = TRUE
;
SELECT *
FROM cte_final c
    JOIN (SELECT DISTINCT customer_id
          FROM cte_final
          WHERE is_old_price_product = TRUE) t
        ON t.customer_id = c.customer_id
-- WHERE c.customer_id = '75742010'
ORDER BY subscription_end_dt, customer_type, c.customer_id, product_name

;
