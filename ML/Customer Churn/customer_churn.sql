
WITH customers_with_revenue AS (
    SELECT
        c.customer_id
         , c.customer_is_fleet
         , c.customer_country
         , c.customer_status
         , c.min_subscription_start_dt AS first_subscription_start_dt
         , c.max_subscription_cancelled_dt
         , coalesce(c.max_subscription_cancelled_dt, current_date) AS reference_dt
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_length_and_payment LIKE '%36 %' THEN TRUE END))          AS subscription_length_36_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_length_and_payment LIKE '%12 %' THEN TRUE END))          AS subscription_length_12_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_length_and_payment LIKE '%1 %' THEN TRUE END))           AS subscription_length_1_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_length_and_payment LIKE '% yearly%' THEN TRUE END))      AS billing_every_12_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_length_and_payment LIKE '% bi-yearly%' THEN TRUE END))   AS billing_every_6_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_length_and_payment LIKE '% quarterly%' THEN TRUE END))   AS billing_every_3_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_length_and_payment LIKE '% monthly%' THEN TRUE END))     AS billing_every_1_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.invoice_discount_pct >0 THEN TRUE END))                          AS has_discount
         , SUM(mrr_eur) AS mrr
         , SUM(mrr_eur) FILTER ( WHERE r.product_group = 'Logbook' ) AS mrr_logbook
         , SUM(mrr_eur) FILTER ( WHERE r.product_group = 'Admin' ) AS mrr_admin
         , SUM(mrr_eur) FILTER ( WHERE r.product_group = 'Geo' ) AS mrr_geo
         , SUM(mrr_eur) FILTER ( WHERE r.product_group = 'Pro' ) AS mrr_pro
         , SUM(mrr_eur) FILTER ( WHERE r.product_group = 'Other' ) AS mrr_other
    FROM data_mart_internal_reporting.ir_m_investor_report r
             JOIN (SELECT customer_id
                        , MAX(reporting_month) AS last_mth_with_value
                   FROM data_mart_internal_reporting.ir_m_investor_report
                   WHERE mrr_eur > 0
                   GROUP BY customer_id) r2
                  ON r.customer_id = r2.customer_id
                      AND r.reporting_month = r2.last_mth_with_value
             JOIN dwh_main.dim_combined_customer c
                  ON c.customer_id = r.customer_id
    WHERE 1=1
      AND c.customer_is_fleet = True
--       AND c.customer_id = '10052946'
    GROUP BY c.customer_id, c.customer_is_fleet, c.customer_country, c.customer_status, c.min_subscription_start_dt, c.max_subscription_cancelled_dt, coalesce(c.max_subscription_cancelled_dt, current_date)
)
   , customers_without_revenue AS (
    SELECT
        c.customer_id
         , c.customer_is_fleet
         , c.customer_country
         , c.customer_status
         , r.first_invoice_dt_all_invoices AS first_subscription_start_dt
         , c.max_subscription_cancelled_dt
         , coalesce(c.max_subscription_cancelled_dt, current_date) AS reference_dt
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_terms LIKE '%36 %' THEN TRUE END))          AS subscription_length_36_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_terms LIKE '%12 %' THEN TRUE END))          AS subscription_length_12_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_terms LIKE '%1 %' THEN TRUE END))           AS subscription_length_1_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_terms LIKE '% yearly%' THEN TRUE END))      AS billing_every_12_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_terms LIKE '% bi-yearly%' THEN TRUE END))   AS billing_every_6_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_terms LIKE '% quarterly%' THEN TRUE END))   AS billing_every_3_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_terms LIKE '% monthly%' THEN TRUE END))     AS billing_every_1_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.invoice_discount_pct >0 THEN TRUE END))             AS has_discount
         , round(SUM(mrr_without_credit_note / exchange_rate), 2) AS mrr
         , round(SUM(mrr_without_credit_note / exchange_rate) FILTER ( WHERE r.product_group = 'Logbook' ), 2) AS mrr_logbook
         , round(SUM(mrr_without_credit_note / exchange_rate) FILTER ( WHERE r.product_group = 'Admin' ), 2) AS mrr_admin
         , round(SUM(mrr_without_credit_note / exchange_rate) FILTER ( WHERE r.product_group = 'Geo' ), 2) AS mrr_geo
         , round(SUM(mrr_without_credit_note / exchange_rate) FILTER ( WHERE r.product_group = 'Pro' ), 2) AS mrr_pro
         , round(SUM(mrr_without_credit_note / exchange_rate) FILTER ( WHERE r.product_group = 'Other' ), 2) AS mrr_other
    FROM data_mart_internal_reporting.tmp_revenue_reporting_sh_cb r
             JOIN (SELECT customer_id, MAX(reporting_month) AS last_mth_with_value
                   FROM data_mart_internal_reporting.tmp_revenue_reporting_sh_cb
                   WHERE mrr_without_credit_note > 0
                   GROUP BY customer_id) r2
                  ON r.customer_id = r2.customer_id
                      AND r.reporting_month = r2.last_mth_with_value
             JOIN dwh_main.dim_combined_customer c
                  ON c.customer_id = r.customer_id
    WHERE 1=1
      AND c.customer_is_fleet = True
    GROUP BY c.customer_id, c.customer_is_fleet, c.customer_country, c.customer_status, r.first_invoice_dt_all_invoices, c.max_subscription_cancelled_dt, coalesce(c.max_subscription_cancelled_dt, current_date)
)
   , customers_all AS (SELECT FALSE AS is_tester, *
                       FROM customers_with_revenue r
                       UNION ALL
                       SELECT TRUE AS is_tester, *
                       FROM customers_without_revenue nr
                       WHERE NOT EXISTS(SELECT 1 FROM customers_with_revenue r WHERE r.customer_id = nr.customer_id))
SELECT * FROM customers_all
WHERE first_subscription_start_dt > current_date

;
WITH cte_product AS (
    SELECT
        m.product_uuid
         , m.product_key
         , m.product_name
         , m.product_note
         , m.product_length_and_payment
         , m.product_family
         , m.is_recurring
         , m.is_subsequent_recurring
         , p.product_is_shippable
         , p.product_is_for_sale
         , p.product_price_net
         , p.product_weight
         , p.product_tax
         , (p.product_created_ts AT TIME ZONE 'Europe/Berlin')::DATE AS product_created_dt
    FROM dwh_main.umd_product_map m
             JOIN dwh_main.dim_product p
                  ON p.product_uuid = m.product_uuid
    WHERE m.product_family NOT IN ('Hardware', 'Other')
)
   , customer_product AS (
    SELECT
        c.customer_id
         , ctr.contract_id
         , upm.product_uuid AS current_product_uuid
         , upm.product_family AS current_product_family
         , upm.product_name AS current_product_name
         , upm.product_created_dt AS current_product_created_dt
         , upm.product_note AS current_product_note
         , upm.product_price_net AS current_product_price_net
         , CASE WHEN upm.product_price_net = p.max_price_net THEN TRUE ELSE FALSE END AS current_price_is_max_price_for_the_product
         , upm2.product_uuid AS future_product_uuid
         , upm2.product_name AS future_product_name
         , upm2.product_created_dt AS future_product_created_dt
         , upm2.product_note AS future_product_note
         , upm2.product_price_net AS future_product_price_net
    FROM dwh_main.dim_customer c
             JOIN dwh_main.dim_contract ctr
                  ON ctr.customer_uuid = c.customer_uuid
             JOIN dwh_main.fact_contract_line fcl
                  ON ctr.contract_key = fcl.contract_key
             JOIN cte_product upm
                  ON upm.product_key = fcl.contract_item_product_key
             LEFT JOIN cte_product upm2
                       ON upm2.product_name = upm.product_name
                           AND upm2.product_length_and_payment = upm.product_length_and_payment
                           AND upm2.is_recurring = upm.is_recurring
                           AND upm2.is_subsequent_recurring = upm.is_subsequent_recurring
                           AND upm2.product_is_shippable = upm.product_is_shippable
                           AND upm2.product_is_for_sale = upm.product_is_for_sale
                           AND upm2.product_weight = upm.product_weight
                           AND upm2.product_uuid <> upm.product_uuid
                           AND upm2.product_price_net > upm.product_price_net
                           AND upm2.product_tax <> 0.16
             LEFT JOIN
         (SELECT product_name, MAX(product_price_net) AS max_price_net
          FROM cte_product
          GROUP BY product_name) p
         ON p.product_name = upm.product_name

             LEFT JOIN dwh_main.umd_sales_price_increase_batch b
                       ON b.contract_id = ctr.contract_id
    WHERE fcl.opcode <> 'D'
      AND batch_name like '%Jan%' AND batch_name like '%B2B%'
    --     AND ctr.contract_id = 'V12127102'
)
--    SELECT * FROM customer_product; -- CHECK CUSTOMERS
   , cte_1 AS (SELECT DISTINCT
                   current_product_uuid
                             , current_product_family
                             , current_product_name
                             , current_product_created_dt
                             , current_product_note
                             , current_product_price_net
                             , current_price_is_max_price_for_the_product
/*
IMPORTANT NOTE:
1. When running mapping for B2C uncomment B2C mapping and comment out the B2B part.
2. When running mapping for B2B uncomment B2B mapping and comment out the B2C part.
*/                             , CASE
        -- B2C mapped in October https://docs.google.com/spreadsheets/d/1_iiImHOuVW6OjESbl5z9g5jXr0IziKGHgJ9PQ0Vqs8g/edit#gid=278718204
--                                    WHEN current_product_uuid = '4914d433-88cd-4b2e-bbbe-39624e2d7bc6' THEN '061928ea-042b-4182-a1fe-a711f5134389'
--                                    WHEN current_product_uuid = '7fb84572-05ac-4c94-ae9b-f3fcf07c2cf5' THEN '061928ea-042b-4182-a1fe-a711f5134389'
--                                    WHEN current_product_uuid = '83d52040-6f87-428c-83f8-32eb853b6d22' THEN 'b16a5957-b5d1-4b2f-860b-331c25fe2989'
--                                    WHEN current_product_uuid = 'c0e8ec1d-9f72-4309-a377-f5ca0508644f' THEN '0a64e2b9-406c-4810-a3d2-c9d67dc95b89'
--                                    WHEN current_product_uuid = 'efcab066-7216-4948-a765-99d82ba44566' THEN '061928ea-042b-4182-a1fe-a711f5134389'
--                                    WHEN current_product_uuid = '7fb84572-05ac-4c94-ae9b-f3fcf07c2cf5' THEN '061928ea-042b-4182-a1fe-a711f5134389' -- sheet B2C renewal Jan'22
--                                    WHEN current_product_uuid = '576a7be3-004b-410e-b20d-633a1c177120' THEN '061928ea-042b-4182-a1fe-a711f5134389' -- sheet B2C renewal Jan'22
        -- B2B mapped in October https://docs.google.com/spreadsheets/d/1_iiImHOuVW6OjESbl5z9g5jXr0IziKGHgJ9PQ0Vqs8g/edit#gid=278718204
                                     WHEN current_product_uuid = '0c809251-a8d4-4d54-a681-41839f022ef5' THEN '65a0bfd5-d789-4a7e-aad2-9787d1f1048e'
                                     WHEN current_product_uuid = '1c2891ea-2d8d-464b-8f3a-9d22f2ed42d0' THEN '7d6fb064-5df7-4858-b543-bf4cd612897a'
                                     WHEN current_product_uuid = '2ee2f831-999c-44b9-a6ca-66da41bfbfdb' THEN '47d0bd5b-fdaa-492e-90d7-20df81940aea'
                                     WHEN current_product_uuid = '3b94b261-f47a-4393-9faf-a298543664ac' THEN '65a0bfd5-d789-4a7e-aad2-9787d1f1048e'
                                     WHEN current_product_uuid = '44f7e2e7-8e5d-4604-a107-56a10e31a021' THEN 'f0622671-3256-41b0-9a49-aee469973257'
                                     WHEN current_product_uuid = '4914d433-88cd-4b2e-bbbe-39624e2d7bc6' THEN '65a0bfd5-d789-4a7e-aad2-9787d1f1048e'
                                     WHEN current_product_uuid = '68c0319d-36ca-4731-af05-4d9ff589d99b' THEN '83bc9af9-cc35-4d7e-ab84-e49fc1cea89d'
                                     WHEN current_product_uuid = '726bc39b-c3c1-48f0-8544-c391427eaad5' THEN '552a3e33-c99b-49db-a01c-fe949e73508a'
                                     WHEN current_product_uuid = '7fb84572-05ac-4c94-ae9b-f3fcf07c2cf5' THEN '65a0bfd5-d789-4a7e-aad2-9787d1f1048e'
                                     WHEN current_product_uuid = '8a85301e-df82-437f-9b96-11eb46420ae4' THEN 'dedbe9ed-b0c7-4746-814f-97bbd1c5ba69'
                                     WHEN current_product_uuid = 'c0e8ec1d-9f72-4309-a377-f5ca0508644f' THEN 'f0622671-3256-41b0-9a49-aee469973257'
                                     WHEN current_product_uuid = 'cee04f53-d490-4c5e-ad43-ca76f5533b5f' THEN '8c864718-f6cf-4cd9-8fda-71b7aed4fec6'
                                     WHEN current_product_uuid = 'd4bab2dd-1dd4-466e-8a26-3ac5ba984eb0' THEN 'e9ff0a5f-fb44-4b2b-8a05-72896f09bdf5'
                                     WHEN current_product_uuid = 'f2fffb32-6a5a-4a67-908b-cdc52f09fee9' THEN '9b892ebc-6f1a-4cab-a95e-cac364fd6d49'
        -- B2B mapped in November https://docs.google.com/spreadsheets/d/1_iiImHOuVW6OjESbl5z9g5jXr0IziKGHgJ9PQ0Vqs8g/edit#gid=482325070
                                     WHEN current_product_uuid = '408a0153-5567-452c-a10e-242f6fabaaf4' THEN '5fc29394-7dcb-4535-9641-5dfbdc503f80'
                                     WHEN current_product_uuid = 'ade12889-23be-40fa-895a-52cd7b8e0891' THEN 'f0622671-3256-41b0-9a49-aee469973257'
        -- B2B mapped in December https://docs.google.com/spreadsheets/d/1_iiImHOuVW6OjESbl5z9g5jXr0IziKGHgJ9PQ0Vqs8g/edit#gid=643694551
                                     WHEN current_product_uuid = '9f5feb96-ad9d-462b-9060-ddfc508fc190' THEN '2d7da6f5-8539-40bc-b740-fb99ecb76113'
                                     WHEN current_product_uuid = 'e9ff0a5f-fb44-4b2b-8a05-72896f09bdf5' THEN 'e9ff0a5f-fb44-4b2b-8a05-72896f09bdf5'  -- we keep the same product here
                                     WHEN current_product_uuid = '77723376-b408-4508-84eb-425be417cc0c' THEN '7df58396-d3ac-46ba-8251-ab1353270a4d'
                                ELSE future_product_uuid
        END AS future_product_uuid
                             , future_product_name
                             , future_product_created_dt
                             , future_product_note
                             , future_product_price_net
               FROM customer_product cp)
SELECT DISTINCT
    current_product_uuid
              , current_product_family
              , current_product_name
              , current_product_created_dt
              , current_product_note
              , current_product_price_net
              , current_price_is_max_price_for_the_product
              , future_product_uuid
              , coalesce(cp.product_name, future_product_name) AS future_product_name  -- product mapped in the past has priority
              , coalesce(cp.product_created_dt, future_product_created_dt) AS future_product_created_dt
              , coalesce(cp.product_note, future_product_note) AS future_product_note
              , coalesce(cp.product_price_net, future_product_price_net) AS future_product_price_net
FROM cte_1 c1
         LEFT JOIN cte_product cp
                   ON cp.product_uuid = c1.future_product_uuid
ORDER BY current_product_uuid
;


