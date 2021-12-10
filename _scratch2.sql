SELECT * FROM dwh_main.dim_combined_product WHERE product_name LIKE '%Vimcar Fleet Admin (3-jährige Laufzeit)%'


SELECT * FROM dwh_main.umd_sales_price_increase_batch WHERE batch_name like '%Jan%' AND batch_name like '%B2B%' ;
SELECT * FROM dwh_main.umd_sales_price_increase_batch WHERE contract_id = 'V60270003';

SELECT * FROM dwh_main.dim_combined_subscription WHERE subscription_id = 'V60270003' ;


B2C 12.105 contracts
B2B 991 contracts



-- BI-1956 product mapping
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

