WITH prod_map_raw AS (
    SELECT
        product_uuid AS original_product_uuid
         , p.product_name AS original_product_name
         , p.product_price_net AS original_product_price_net
         , CASE
               WHEN p.product_price_net= 290 THEN 790
               WHEN p.product_price_net= 590 THEN 790
               WHEN p.product_price_net= 700 THEN 1000
               WHEN p.product_price_net= 1000 THEN 1700
               WHEN p.product_price_net= 1590 THEN 2290
               WHEN p.product_price_net= 1890 THEN 2290
               WHEN p.product_price_net= 2290 THEN 3290
               WHEN p.product_price_net= 3480 THEN 5880
               WHEN p.product_price_net= 4920 THEN 6120
               WHEN p.product_price_net= 8400 THEN 12000
               WHEN p.product_price_net= 12000 THEN 20400
               WHEN p.product_price_net= 19080 THEN 23880
               WHEN p.product_price_net= 19900 THEN 23880
               WHEN p.product_price_net= 27480 THEN 35880
        END AS map_product_price_net
    FROM dwh_main.dim_product p
)
   , prod_map AS (SELECT *
                       , coalesce(map_product_price_net / original_product_price_net::decimal, 1) AS price_increase_factor
                  FROM prod_map_raw)
   , cte_raw AS (
    SELECT
        i.subscription_id AS shop_contract
         , b.batch_name
         , subscription_status
         , c.customer_id
         , i.invoice_id
         , invoice_created_dt
         , invoice_provisioning_start_dt
         , invoice_provisioning_end_dt
         , CASE
               WHEN b.batch_name = '2021-10_1_B2B (Oct 2021)'
                   AND i.invoice_created_dt BETWEEN '2021-09-18' AND '2021-11-01'
                   THEN TRUE
               WHEN b.batch_name = '2021-11_1_B2B (Nov 2021)'
                   AND i.invoice_created_dt BETWEEN '2021-11-01' AND '2021-11-30'
                   THEN TRUE
               WHEN b.batch_name = '2021-12_1_B2B (Dec 2021)'
                   AND i.invoice_created_dt BETWEEN '2021-12-01' AND '2021-12-31'
                   THEN TRUE
               WHEN b.batch_name = '2022-01_1_B2B (Jan 2022)'
                   AND i.invoice_created_dt BETWEEN '2022-01-01' AND '2022-01-31'
                   THEN TRUE
               WHEN b.batch_name = '2022-02_1_B2B (Feb 2022)'
                   AND i.invoice_created_dt BETWEEN '2022-02-01' AND '2022-02-28'
                   THEN TRUE
               WHEN b.batch_name = '2022-03_1_B2B (Mar 2022)'
                   AND i.invoice_created_dt BETWEEN '2022-03-01' AND '2022-03-31'
                   THEN TRUE
               WHEN b.batch_name = '2022-04_1_B2B (Apr 2022)'
                   AND i.invoice_created_dt BETWEEN '2022-04-01' AND '2022-04-30'
                   THEN TRUE
               WHEN b.batch_name = '2022-05_1_B2B (May 2022)'
                   AND i.invoice_created_dt BETWEEN '2022-05-01' AND '2022-05-31'
                   THEN TRUE
               WHEN b.batch_name = '2022-06_1_B2B (Jun 2022)'
                   AND i.invoice_created_dt BETWEEN '2022-06-01' AND '2022-06-30'
                   THEN TRUE
               WHEN b.batch_name = '2022-07_1_B2B (Jul 2022)'
                   AND i.invoice_created_dt BETWEEN '2022-07-01' AND '2022-07-31'
                   THEN TRUE
               ELSE FALSE END AS flag_renewed_with_price_increase
--                                      , current_date - invoice_created_dt AS days_since_last_invoice
         , CASE WHEN s.subscription_end_dt >= current_date THEN s.subscription_end_dt END AS next_renewal_dt
         , CASE WHEN product_group LIKE 'Logbook%' THEN 'Logbook' ELSE product_group END AS product_group
         , p.original_product_name AS product_name
         , entity_id AS product_uuid
         , p.original_product_price_net AS product_price_net
         , p.price_increase_factor
         , mrr_eur
         , item_quantity
         , round(mrr_eur * price_increase_factor, 2) AS mrr_eur_expected
         , (i2.invoice_total_price_gross_amount / 100::decimal) / count(i2.invoice_id) OVER (PARTITION BY i2.invoice_ID) AS invoice_total_price_gross_amount
         , round((i2.invoice_total_price_gross_amount / count(i2.invoice_id) OVER (PARTITION BY i2.invoice_ID) * price_increase_factor) / 100,2) AS invoice_total_price_gross_amount_expected
    FROM dwh_main.dim_combined_invoice_line i
             JOIN dwh_main.dim_combined_subscription s
                  ON s.subscription_id = i.subscription_id
             JOIN dwh_main.dim_combined_customer c
                  ON c.customer_id = i.customer_id
             JOIN dwh_main.dim_invoice i2
                  ON i.invoice_id = i2.invoice_id
             JOIN prod_map p
                  ON p.original_product_uuid::text = i.entity_id
             JOIN dwh_main.umd_sales_price_increase_batch b
                  ON b.contract_id = i.subscription_id
    WHERE TRUE
      AND i.full_refund_flag = FALSE
      AND i.product_group <> 'Hardware'
      AND i.invoice_provisioning_end_dt >= '2021-09-18'
      AND b.batch_name IN ('2021-10_1_B2B (Oct 2021)',
                           '2021-11_1_B2B (Nov 2021)',
                           '2021-12_1_B2B (Dec 2021)',
                           '2022-01_1_B2B (Jan 2022)',
                           '2022-02_1_B2B (Feb 2022)',
                           '2022-03_1_B2B (Mar 2022)',
                           '2022-04_1_B2B (Apr 2022)',
                           '2022-05_1_B2B (May 2022)',
                           '2022-06_1_B2B (Jun 2022)',
                           '2022-07_1_B2B (Jul 2022)')
--       AND i.subscription_id IN ('V81183643') -- invoice created for 1 item instead of 10, product with 9 dongles didn't trigger a renewal
--       AND i.subscription_id IN ('V81183643','V17304741')
--      AND i.subscription_id = '67383155'

    ORDER BY i.subscription_id, invoice_provisioning_start_dt DESC
)
SELECT DISTINCT ON (shop_contract)
    shop_contract
                                 , invoice_id AS renewal_invoice_id
                                 , invoice_created_dt AS renewal_invoice_created_dt
                                 , mrr_eur AS renewal_mrr
FROM cte_raw
WHERE flag_renewed_with_price_increase = True
ORDER BY shop_contract, invoice_created_dt