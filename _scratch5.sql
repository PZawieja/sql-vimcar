WITH cte_product AS (SELECT m.*
                          , p.price
                          , p.created
                          , CASE
                                WHEN product_uuid = '4914d433-88cd-4b2e-bbbe-39624e2d7bc6' OR
                                     product_uuid = '9cdbdc7e-54c1-47ad-8196-f3d452051740' OR
                                     product_uuid = '7fb84572-05ac-4c94-ae9b-f3fcf07c2cf5' OR
                                     product_uuid = '83d52040-6f87-428c-83f8-32eb853b6d22' OR
                                     product_uuid = 'c0e8ec1d-9f72-4309-a377-f5ca0508644f' OR
                                     product_uuid = 'efcab066-7216-4948-a765-99d82ba44566' OR
                                     product_uuid = '576a7be3-004b-410e-b20d-633a1c177120' OR
                                     product_uuid = '0c809251-a8d4-4d54-a681-41839f022ef5' OR
                                     product_uuid = '1c2891ea-2d8d-464b-8f3a-9d22f2ed42d0' OR
                                     product_uuid = '2ee2f831-999c-44b9-a6ca-66da41bfbfdb' OR
                                     product_uuid = '3b94b261-f47a-4393-9faf-a298543664ac' OR
                                     product_uuid = '44f7e2e7-8e5d-4604-a107-56a10e31a021' OR
                                     product_uuid = '4914d433-88cd-4b2e-bbbe-39624e2d7bc6' OR
                                     product_uuid = '68c0319d-36ca-4731-af05-4d9ff589d99b' OR
                                     product_uuid = '7fb84572-05ac-4c94-ae9b-f3fcf07c2cf5' OR
                                     product_uuid = '726bc39b-c3c1-48f0-8544-c391427eaad5' OR
                                     product_uuid = '8a85301e-df82-437f-9b96-11eb46420ae4' OR
                                     product_uuid = 'cee04f53-d490-4c5e-ad43-ca76f5533b5f' OR
                                     product_uuid = 'd4bab2dd-1dd4-466e-8a26-3ac5ba984eb0' OR
                                     product_uuid = 'f2fffb32-6a5a-4a67-908b-cdc52f09fee9' OR
                                     product_uuid = '408a0153-5567-452c-a10e-242f6fabaaf4' OR
                                     product_uuid = 'ade12889-23be-40fa-895a-52cd7b8e0891' OR
                                     product_uuid = '9f5feb96-ad9d-462b-9060-ddfc508fc190' OR
                                     product_uuid = 'e9ff0a5f-fb44-4b2b-8a05-72896f09bdf5' OR
                                     product_uuid = '7df58396-d3ac-46ba-8251-ab1353270a4d' OR
                                     product_uuid = '77723376-b408-4508-84eb-425be417cc0c' OR
                                     product_uuid = '576a7be3-004b-410e-b20d-633a1c177120' OR
                                     product_uuid = 'e4b91a9a-dc5c-492f-a3c2-37a35e47865b' OR
                                     product_uuid = 'c8467aa9-82b2-4102-8625-f1cc0cc2fce5' OR
                                     product_uuid = 'e26977b4-5e14-4992-a7f8-4d3855c2fa88' OR
                                     product_uuid = 'f02d2366-735e-4a64-9fee-8fdf894fe10d' THEN TRUE
                                ELSE FALSE END AS product_old_price
                     FROM mapping.umd_product_map m
                              JOIN product p ON p.id = m.product_uuid)
-- SELECT cb_plan_id, MIN(created), MAX(created), MAX(CASE WHEN product_old_price =TRUE THEN 1 END)  FROM cte_product GROUP BY 1;
SELECT DISTINCT cb_plan_id, product_old_price  FROM cte_product ORDER BY 1;
SELECT *  FROM cte_product WHERE cb_plan_id = 'fleet-pro-logbook-upgrade_yearly-eur';
, cte_contract_product AS (
    SELECT
        cco.outbound_id::VARCHAR(9) AS shop_contract_id
         , pmap.cb_plan_id AS plan_id
         , cbmap.cb_plan_id_map
         , cbmap.cb_plan_id_map_nbr
         , item_net_unit_price_amount AS unit_price_net
         , pmap.cb_addon_id
         , pmap.cb_addon_price_net
         , pmap.monthly_payment
         , SUM(item_quantity) AS item_quantity
    FROM public.contract cco
             JOIN public.customer cc
                  ON cc.id = cco.customer_id
--              JOIN public.shop_extraction_current_batch b  -- join batch table
--                   ON cc.outbound_id = b.customer_id
--                       AND coalesce(b.comment,'dummy') NOT IN ('Excluded from migration: Ghost customer or internal Vimcar email address.')
       , LATERAL (SELECT (obj.value ->> 'product_id') AS product_id,
                      (obj.value -> 'price' -> 'net' ->> 'amount')::INT AS item_net_unit_price_amount,
                      (obj.value ->> 'quantity')::SMALLINT AS item_quantity
                  FROM jsonb_array_elements(cco.items) obj(value)) items
             JOIN cte_product pmap
                  ON pmap.product_uuid = CASE items.product_id WHEN 'fake' THEN '00000000-0000-0000-0000-000000000000' ELSE items.product_id END::UUID
             LEFT JOIN public.shop_extraction_cb_plan_id_map cbmap
                       ON pmap.cb_plan_id = cbmap.cb_plan_id
    WHERE cc.contact ->> 'email' NOT LIKE '%vimcar.com'
      and cc.outbound_id = 'K78533247'
    GROUP BY cco.outbound_id, pmap.cb_plan_id, cbmap.cb_plan_id_map, cbmap.cb_plan_id_map_nbr, items.product_id, item_net_unit_price_amount
           , pmap.cb_addon_id, pmap.cb_addon_price_net, pmap.monthly_payment
)
   , cte_invoice_product AS (
    SELECT
        ci.contract_outbound_id::VARCHAR(9) AS shop_contract_id
         , pmap.cb_plan_id AS plan_id
         , cbmap.cb_plan_id_map
         , cbmap.cb_plan_id_map_nbr
         , item_net_unit_price_amount AS unit_price_net
         , pmap.cb_addon_id
         , pmap.cb_addon_price_net
         , pmap.monthly_payment
         , MIN(provisioning_start AT TIME ZONE 'Europe/Berlin') AS min_provisioning_start
         , MAX(provisioning_start AT TIME ZONE 'Europe/Berlin') AS max_provisioning_start
         , MAX(provisioning_end AT TIME ZONE 'Europe/Berlin') AS max_provisioning_end
    FROM public.shop_extraction_order_invoice ci
             JOIN public.customer cc
                  ON cc.id = ci.customer_id
--              JOIN public.shop_extraction_current_batch b  -- join batch table
--                   ON cc.outbound_id = b.customer_id
--                       AND coalesce(b.comment,'dummy') NOT IN ('Excluded from migration: Ghost customer or internal Vimcar email address.')
--                               JOIN (SELECT DISTINCT customer_id
--                                     FROM public.shop_extraction_batch
--                                     WHERE coalesce(comment,'dummy') <> 'Not sent for migration - multiple customers with the same email address.') b2  -- excluding the problematic customer from the batch
--                                    ON b2.customer_id = cc.outbound_id
       , LATERAL (SELECT (obj.value ->> 'product_id') AS product_id,
                      (obj.value -> 'price' -> 'net' ->> 'amount')::INT AS item_net_unit_price_amount
                  FROM jsonb_array_elements(ci.items_added) obj(value)) items_added
             JOIN cte_product pmap
                  ON pmap.product_uuid = CASE items_added.product_id WHEN 'fake' THEN '00000000-0000-0000-0000-000000000000' ELSE items_added.product_id END::UUID
             LEFT JOIN public.shop_extraction_cb_plan_id_map cbmap
                       ON pmap.cb_plan_id = cbmap.cb_plan_id
    WHERE cc.contact ->> 'email' NOT LIKE '%vimcar.com'
--                       AND cc.outbound_id = 'K10711749'  -- monthly billing cycles (good for testing)
--                       AND cc.outbound_id = 'K65597398' -- annual billing cycles (good for testing)
--                         AND cc.outbound_id = 'K45828570' -- annual billing cycles, 3-years contract (good for testing)
--                         AND cc.outbound_id = 'K67710870' -- there is no cancellation date for contract V74285730, must be populated in SQL
--                         and cc.outbound_id = 'K91452842' -- there is a discount 10% ot the contract level but coupon is missing, dummy must be created
--                             AND cc.outbound_id = '72006772' -- the calculated contract term end date should be 17 Nov'21
--       AND cc.outbound_id = 'K78533247' -- customer has 1 product only and had a price increase in January 2022
      AND cc.outbound_id = 'K78533247' -- customer has multiple products, 2 of them will get a price increase on 16 April'22
    GROUP BY ci.contract_outbound_id, pmap.cb_plan_id, cbmap.cb_plan_id_map, cbmap.cb_plan_id_map_nbr, item_net_unit_price_amount, pmap.cb_addon_id, pmap.cb_addon_price_net, pmap.monthly_payment
)
   , cte_inv_ctr_prod AS (
    SELECT i.shop_contract_id
         , i.plan_id
         , i.cb_plan_id_map
         , i.cb_plan_id_map_nbr
         , i.unit_price_net
         , i.cb_addon_id
         , i.cb_addon_price_net
         , i.monthly_payment
         , coalesce(c.item_quantity,0) AS item_quantity
    FROM cte_invoice_product i
             LEFT JOIN cte_contract_product c
                       ON i.shop_contract_id = c.shop_contract_id
                           AND i.plan_id = c.plan_id
                           AND i.unit_price_net = c.unit_price_net
    UNION
    SELECT * FROM cte_contract_product
)
   , unique_subscriptions AS (
    SELECT
        shop_contract_id
         , shop_contract_id ||'_'|| cb_plan_id_map_nbr AS unique_subscription_id
         , plan_id
         , true = ANY(ARRAY_AGG(monthly_payment)) AS monthly_payment
         , MAX(CASE
                   WHEN cb_addon_id IS NULL
                       THEN unit_price_net
                   ELSE 0
        END) AS plan_unit_price_net
         , cb_addon_id AS addon_id
         , cb_addon_price_net AS addon_unit_price_net
         , SUM(item_quantity) AS item_quantity
    FROM cte_inv_ctr_prod
    GROUP BY shop_contract_id, shop_contract_id, cb_plan_id_map_nbr, plan_id, cb_addon_id, cb_addon_price_net)
-- SELECT * FROM unique_subscriptions WHERE shop_contract_id = 'V67879223';
   , subscriptions_1 AS (
    SELECT
        s.shop_contract_id
         , s.unique_subscription_id  AS "subscription[id]"
         , s.plan_id                 AS "subscription[plan_id]"
         , CASE
               WHEN s.addon_id IS NOT NULL
                   THEN 1
               ELSE item_quantity
        END AS "subscription[plan_quantity]"
         , plan_unit_price_net  AS "subscription[plan_unit_price]"
---
         , lower(regexp_replace(contact ->> 'email', '[\t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g')) AS "customer[email]"
         , cu.outbound_id AS shop_customer_id
         , NULL AS "subscription[setup_fee]"
         , CASE
               WHEN s.addon_id IS NOT NULL THEN 'cancelled'  -- subscription for addons should be migrated as 'canceled'
               WHEN s.plan_id IN ('logbook_b2c-from_rent2buy-eur', 'logbook_b2c-one_time_buy-eur') THEN 'cancelled'  -- lifetime subs should be migrated as 'canceled'
               WHEN c.status = 'active' THEN 'active'
               WHEN c.status = 'canceled' THEN 'non_renewing'
               WHEN c.status = 'terminated' THEN 'cancelled'
        END AS "subscription[status]"
         , NULL AS "subscription[start_date]" -- only for future subscriptions (leave it blank)
         , NULL AS "subscription[trial_start]"
         , NULL AS "subscription[trial_end]"
         , c.started AT TIME ZONE 'Europe/Berlin' AS "tmp_subscription[started_at]"
         , c.provisioning_period_start AT TIME ZONE 'Europe/Berlin' AS "tmp_subscription[current_term_start]"
         , c.provisioning_period_end AT TIME ZONE 'Europe/Berlin' AS "tmp_subscription[current_term_end]"
         , GREATEST(c.provisioning_period_end AT TIME ZONE 'Europe/Berlin',
                    CASE
                        WHEN s.plan_id ILIKE '%3-years%'
                            OR s.plan_id ILIKE '%3_years%'
                            THEN c.started + INTERVAL '3 years'
                        WHEN monthly_payment = FALSE
                            OR c.status = 'terminated'
                            OR (c.status = 'canceled' AND c.provisioning_period_end + INTERVAL '3 months' < current_date)
                            THEN c.provisioning_period_end
                        ELSE c.started + ((extract(year from age(date_trunc('month',current_date), date_trunc('month',c.started) + INTERVAL '1 day'))
                            + extract(month from age(date_trunc('month',current_date), date_trunc('month',c.started) + INTERVAL '1 day'))::INT / 12) + 1)  * INTERVAL '1 year'
                        END AT TIME ZONE 'Europe/Berlin') AS "tmp_contract_term_end"
         , c.canceled AT TIME ZONE 'Europe/Berlin' AS "subscription[cancelled_at]"
         , c.cancelation_reason AS cancel_reason_code
         , NULL AS "subscription[pause_date]"
         , NULL AS "subscription[resume_date]"
         , CASE
               WHEN plan_id IN ('logbook_b2c-from_rent2buy-eur', 'logbook_b2c-one_time_buy-eur') THEN TRUE
               ELSE FALSE
        END cf_lifetime_licence
         , CASE
               WHEN plan_id LIKE '%month%'
                   THEN 12
               ELSE 1
        END AS "subscription[contract_term_billing_cycle_on_renewal]"  -- works also for 3 years contracts: https://www.notion.so/vimcar/Migration-Review-Learnings-Decisions-To-Dos-f32758b58cb54d3e8a026b7821d0632f
         , NULL AS "total_amount_raised" -- CHECK!
         , CASE
               WHEN rp.payment_method IN ('invoice', 'paypal')
                   OR rp.payment_method IS NULL
                   THEN 'off'
               ELSE 'on'
        END AS "subscription[auto_collection]"  --on when recurring payment method, off when payment by wire transfer
         , NULL AS "subscription[po_number]"
         , CASE
               WHEN coupon_code IS NOT NULL
                   THEN coupon_code
               WHEN round(CASE WHEN discount ->> 'type' = 'percentage' THEN discount ->> 'value' END::NUMERIC,4) >= 1
                   THEN concat_ws('', 'dummy_pct_1_', split_part((round((discount ->> 'value')::numeric, 4))::TEXT,'.', 2))
               WHEN round(CASE WHEN discount ->> 'type' = 'percentage' THEN discount ->> 'value' END::NUMERIC,4) > 0
                   THEN concat_ws('', 'dummy_pct_0_', split_part((round((discount ->> 'value')::numeric, 4))::TEXT,'.', 2))
               WHEN round(CASE WHEN discount ->> 'type' = 'amount' THEN discount -> 'value' ->> 'amount' END::numeric) > 0
                   THEN concat_ws('', 'dummy_gross_eur_', discount -> 'value' ->> 'amount' )
        END AS "coupon_ids[0]"
         , NULL AS "coupon_ids[1]"
         , NULL AS "subscription[payment_source_id]"
         , NULL AS "subscription[invoice_notes]"
         , NULL AS "subscription[meta_data]"
         , coalesce(btrim(regexp_replace(shipping_contact ->> 'first_name', '[\t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g')),
                    btrim(regexp_replace(contact ->> 'first_name', '[\t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g'))) AS "shipping_address[first_name]"
         , coalesce(btrim(regexp_replace(shipping_contact ->> 'last_name', '[\t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g')),
                    btrim(regexp_replace(contact ->> 'last_name', '[\t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g'))) AS "shipping_address[last_name]"
         , coalesce(btrim(lower(regexp_replace(shipping_contact ->> 'email', '[\t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g'))),
                    btrim(lower(regexp_replace(contact ->> 'email', '[ \t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g')))) AS "shipping_address[email]"
         , coalesce(btrim(regexp_replace(shipping_contact ->> 'company_name', '[\t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g')),
                    btrim(regexp_replace(contact ->> 'company_name', '[\t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g'))) AS "shipping_address[company]"
         , btrim(COALESCE(
            CASE
                WHEN coalesce(shipping_contact ->> 'phone_number',contact ->> 'phone_number') !~ '[0-9]' THEN NULL
                ELSE substring(coalesce(shipping_contact ->> 'phone_number',contact ->> 'phone_number') from '#"[0-9[:space:]+.\-]*#"' for '#')
                END,
            CASE
                WHEN coalesce(shipping_contact ->> 'mobile_phone_number',contact ->> 'mobile_phone_number') !~ '[0-9]' THEN NULL
                ELSE substring(coalesce(shipping_contact ->> 'mobile_phone_number',contact ->> 'mobile_phone_number') from '#"[0-9[:space:]+.\-]*#"' for '#')
                END, NULL))::TEXT AS "shipping_address[phone]"
         , btrim(nullif(regexp_replace(coalesce(shipping_contact -> 'address' ->> 'street_address',contact -> 'address' ->> 'street_address'), '[\t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g'), 'n/a')) AS shipping_address_raw
         , btrim(nullif(regexp_replace(coalesce(shipping_contact -> 'address' ->> 'locality',contact -> 'address' ->> 'locality'), '[\t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g'), 'n/a')) AS "shipping_address[city]"
         , NULL AS "shipping_address[state_code]"
         , NULL AS "shipping_address[state]"
         , to_char(
            btrim(CASE
                      WHEN coalesce(shipping_contact -> 'address' ->> 'postal_code',contact -> 'address' ->> 'postal_code')  !~ '[0-9]' THEN NULL
                      WHEN length(coalesce(shipping_contact -> 'address' ->> 'postal_code',contact -> 'address' ->> 'postal_code')) > 10 THEN NULL
                      ELSE regexp_replace(coalesce(shipping_contact -> 'address' ->> 'postal_code',contact -> 'address' ->> 'postal_code'),'[\t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g')
                END)::INT,'fm00000') AS "shipping_address[zip]"
         , btrim(nullif(regexp_replace(coalesce(shipping_contact -> 'address' ->> 'country',contact -> 'address' ->> 'country'), '[\t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g'), 'n/a')) AS "shipping_address[country]"
         , 'not_validated' AS "shipping_address[validation_status]"
         , s.addon_id AS "addons[id][0]"
         , s.item_quantity AS "addons[quantity][0]"
         , s.addon_unit_price_net AS "addons[unit_price][0]"
         , c.modified AT TIME ZONE 'Europe/Berlin' AS contract_modified_ts
    FROM unique_subscriptions s
             JOIN public.contract c
                  ON s.shop_contract_id = c.outbound_id
             JOIN public.customer cu
                  ON cu.id = c.customer_id
             LEFT JOIN (SELECT
                            domain
                             , provider
                             , payment_method
                        FROM (SELECT domain
                                   , row_number() OVER (PARTITION BY domain ORDER BY authorized DESC) AS row_nbr
                                   , payment_method ->> 'type' AS payment_method
                                   , provider
                              FROM public.payment
                              WHERE status = 'authorised'
                                AND refund_id IS NULL) recent_payments
                        WHERE row_nbr = 1) rp
                       ON rp.domain = cu.domain
    WHERE coalesce(c.cancelation_reason,'dummy') <> 'ghost contract'
)
   , subscriptions_2 AS (
    SELECT s.*
         , CASE
               WHEN ip.min_provisioning_start IS NULL
                   THEN s."tmp_subscription[current_term_end]"
               ELSE least(s."tmp_subscription[current_term_start]", ip.min_provisioning_start)
        END AS "subscription[started_at]"
         , CASE
               WHEN ip.min_provisioning_start IS NULL
                   THEN NULL
               ELSE ip.max_provisioning_start
--          ELSE "tmp_subscription[current_term_start]"
        END AS "subscription[current_term_start]"
         , CASE
               WHEN ip.min_provisioning_start IS NULL
                   THEN NULL
               ELSE ip.max_provisioning_end
--            ELSE "tmp_subscription[current_term_end]"
        END AS "subscription[current_term_end]"
         , CASE
               WHEN ip.min_provisioning_start IS NULL
                   THEN NULL
               ELSE ip.max_provisioning_end
--            ELSE "tmp_contract_term_end"
        END AS "contract term end"
--     , s."subscription[plan_id]"
--      , s."subscription[plan_quantity]"
         , ip.min_provisioning_start
         , ip.max_provisioning_start
         , ip.max_provisioning_end
    FROM subscriptions_1 s
             LEFT JOIN cte_invoice_product ip
                       ON ip.shop_contract_id = s.shop_contract_id
                           AND ip.plan_id = s."subscription[plan_id]"
)
-- SELECT * FROM subscriptions_2 ;
   , subscriptions_3 AS (
    SELECT
        "customer[email]"
         , shop_customer_id
         , "subscription[id]"
         , shop_contract_id AS "subscription[cf_vertragsnummer]"
         , "subscription[plan_id]"
         , CASE
               WHEN "subscription[plan_quantity]" > 0
                   THEN "subscription[plan_quantity]"
               ELSE NULL
        END AS "subscription[plan_quantity]"
         , "subscription[plan_unit_price]"
         , "subscription[setup_fee]"
         , CASE
               WHEN min_provisioning_start IS NULL -- meaning an invoice hasn't been issued yet
                   THEN 'future'
               WHEN "contract term end" <= current_date
                   THEN 'cancelled'
               WHEN "subscription[plan_quantity]" = 0
                   AND "subscription[status]" = 'active'
                   THEN 'non_renewing'
               ELSE "subscription[status]"
        END AS "subscription[status]"
         , "subscription[start_date]"
         , "subscription[trial_start]"
         , "subscription[trial_end]"
         , to_char("subscription[started_at]", 'YYYY-MM-DD HH24:MI:SS') AS "subscription[started_at]"
         , to_char("subscription[current_term_start]", 'YYYY-MM-DD HH24:MI:SS') AS "subscription[current_term_start]"
         , to_char("subscription[current_term_end]", 'YYYY-MM-DD HH24:MI:SS') AS "subscription[current_term_end]"
         , to_char(CASE
                       WHEN "subscription[status]" = 'non_renewing'
                           THEN NULL -- added on 2022-03-16 as per CB advise: https://vimcar.slack.com/archives/C02NJSXE57Z/p1647363276161959
                       WHEN "subscription[cancelled_at]" IS NOT NULL
                           THEN GREATEST("subscription[cancelled_at]", coalesce("subscription[current_term_start]","subscription[started_at]") + interval '1 day')
                       WHEN "subscription[status]" <> 'active'
                           OR ("subscription[plan_quantity]" = 0 AND "subscription[status]" = 'active')
                           THEN
                           CASE
                               WHEN contract_modified_ts BETWEEN "subscription[started_at]" AND "contract term end"
                                   THEN contract_modified_ts
                               ELSE coalesce(contract_modified_ts, "subscription[current_term_start]","subscription[started_at]") + interval '1 day'
                               END
                       END, 'YYYY-MM-DD HH24:MI:SS') AS "subscription[cancelled_at]"
         , cancel_reason_code
         , to_char("contract term end", 'YYYY-MM-DD HH24:MI:SS') AS "contract term end"
         , "subscription[pause_date]"
         , "subscription[resume_date]"
         , cf_lifetime_licence

         , CASE
               WHEN min_provisioning_start IS NULL -- invoice hasn't been issued yet
                   AND "subscription[plan_id]" ILIKE '%month%'                   -- for monthly payments
                   THEN 12
               WHEN min_provisioning_start IS NULL -- invoice hasn't been issued yet
                   THEN 1
               WHEN "contract term end" < now() AT TIME ZONE 'Europe/Berlin'  -- for non-active contracts
                   OR "subscription[plan_quantity]" = 0                                    -- for plans which ended already
                   OR "subscription[status]" = 'cancelled'
                   THEN 0
               WHEN "subscription[plan_id]" ILIKE '%month%'                   -- for monthly payments
                   THEN extract(year from age(date_trunc('month', "contract term end"), date_trunc('month', current_date))) * 12
                   + extract(month from age(date_trunc('month',"contract term end"), date_trunc('month', current_date)))

               ELSE extract(year from age(date_trunc('year', "contract term end"), date_trunc('year', current_date)))
        END::INT AS billing_cycles  -- CHECK! remaining billing cycles

         , total_amount_raised
         , "subscription[auto_collection]"
         , "subscription[po_number]"
         , "coupon_ids[0]" AS coupon_code_temp
         , "coupon_ids[1]"
         , "subscription[payment_source_id]"
         , "subscription[invoice_notes]"
         , "subscription[meta_data]"
         , "shipping_address[first_name]"
         , "shipping_address[last_name]"
         , "shipping_address[email]"
         , "shipping_address[company]"
         , "shipping_address[phone]"
         , CASE
               WHEN length(substring(shipping_address_raw from '^.*?(?=[0-9]|$)')) = 0
                   THEN shipping_address_raw
               ELSE substring(shipping_address_raw from '^.*?(?=[0-9]|$)')
        END  AS "shipping_address[line1]" -- street
         , CASE
               WHEN length(substring(shipping_address_raw from '^.*?(?=[0-9]|$)')) = 0
                   THEN NULL
               ELSE substring(shipping_address_raw from '[0-9].*$')
        END  AS "shipping_address[line2]" -- house number
         , NULL AS "shipping_address[line3]"
         , "shipping_address[city]"
         , "shipping_address[state_code]"
         , "shipping_address[state]"
         , "shipping_address[zip]"
         , "shipping_address[country]"
         , "shipping_address[validation_status]"
         , CASE
               WHEN "subscription[plan_quantity]" > 0
                   AND "subscription[status]" = 'active'
                   THEN "subscription[contract_term_billing_cycle_on_renewal]"
               ELSE 0
        END AS "subscription[contract_term_billing_cycle_on_renewal]"
         -- , CASE
         --       WHEN "subscription[plan_quantity]" > 0
         --           AND "subscription[status]" = 'active'
         --           THEN TRUE
         --	END AS "create_current_term_invoice"  -- OLD LOGIC until 2022-03-23
         , FALSE AS "create_current_term_invoice"
         , NULL AS "contract_term[id]"  -- CHECK!
         , NULL AS "contract_term[created_at]"  -- CHECK!
         , NULL AS "contract_term[contract_start]"  -- CHECK!
         , NULL AS "contract_term[billing_cycle]"  -- Rumeen will do
         , NULL AS "contract_term[total_amount_raised]"  -- CHECK!
         , NULL AS "contract_term[action_at_term_end]"  -- CHECK!
         , NULL AS "contract_term[cancellation_cutoff_period]"  -- CHECK!
         , NULL AS "transaction[amount]"  -- CHECK!
         , NULL AS "transaction[payment_method]"  -- CHECK!
         , NULL AS "transaction[reference_number]"  -- CHECK!
         , NULL AS "transaction[date]"  -- CHECK!
         , NULL AS "subscription[cf_cancelled_devices]"  -- CHECK
         , NULL AS "subscription[cf_total_sent_devices_for_subscription]"  -- CHECK
         , NULL AS "subscription[cf_total_received_devices_for_subscription]"  -- CHECK
         , NULL AS "subscription[cf_currently_held_devices_for_subscription]"  -- CHECK
         , NULL AS "subscription[cf_to_be_returned_devices_for_subscription]"  -- CHECK
    FROM subscriptions_2
-- WHERE shop_contract_id = 'V67879223'
)
SELECT p.product_old_price, s.*
FROM subscriptions_3 s
    LEFT JOIN (SELECT DISTINCT cb_plan_id, greatest(product_old_price) AS product_old_price FROM cte_product) p
        ON s."subscription[plan_id]" = p.cb_plan_id
WHERE "subscription[plan_id]" NOT LIKE 'hardware%'
;




SELECT * FROM subscriptions_3 WHERE "subscription[plan_id]" NOT LIKE 'hardware%'
-- As agreed with Anne and CB on 20220110 we stop sending hardware plans for migration, therefore the 2 lines below are obsolete
--UNION ALL
--SELECT DISTINCT * FROM subscriptions_2 WHERE "subscription[plan_id]" LIKE 'hardware%'
;

SELECT customer_id, count(*) FROM customer_user GROUP BY 1 ORDER BY 2 DESC;
SELECT DISTINCT ON (customer_id)
            customer_id
            , split_part(urn,':', 3) AS foxbox_domain_name
FROM customer_user
WHERE urn NOT LIKE 'urn:vimcar:com.vimcar.clients%'
AND customer_id = '875a1cc0-d9df-411d-9e7d-8c885844021f'
;
SELECT * FROM customer WHERE id = '906602bd-2c26-446d-b2b5-ddba7035d247';


