SELECT * FROM shop_extraction_order_invoice WHERE contract_outbound_id = 'V10155631';
SELECT * FROM mapping.umd_product_map WHERE product_uuid = '0a2205e9-0f1d-4597-ae49-ae445d356858';
SELECT * FROM shop_extraction_cb_plan_id_map WHERE cb_plan_id = 'fleet-pro-logbook-upgrade_monthly-eur'

-- sub_contracts SOLUTION FOR DATES 2022-07-18
WITH discounts_in_eur_amount AS (
    SELECT DISTINCT ON (contract_outbound_id)
        contract_outbound_id
                                            , CASE
                                                  WHEN round((i.discount -> 'value' ->> 'amount')::NUMERIC
                                                                 / ((i.discount -> 'value' ->> 'amount')::NUMERIC + (total_price -> 'gross' ->> 'amount')::NUMERIC),4) < 1
                                                      THEN concat_ws('', 'dummy_pct_0_', split_part(
                                                          round((i.discount -> 'value' ->> 'amount')::NUMERIC
                                                                    / ((i.discount -> 'value' ->> 'amount')::NUMERIC + (total_price -> 'gross' ->> 'amount')::NUMERIC),4)::TEXT
                                                      ,'.', 2))
                                                  WHEN round((i.discount -> 'value' ->> 'amount')::NUMERIC
                                                                 / ((i.discount -> 'value' ->> 'amount')::NUMERIC + (total_price -> 'gross' ->> 'amount')::NUMERIC),4) >= 1
                                                      THEN concat_ws('', 'dummy_pct_1_', split_part(
                                                          round((i.discount -> 'value' ->> 'amount')::NUMERIC
                                                                    / ((i.discount -> 'value' ->> 'amount')::NUMERIC + (total_price -> 'gross' ->> 'amount')::NUMERIC),4)::TEXT
                                                      ,'.', 2))
        END AS discount_amount_translated_to_percentage
    FROM shop_extraction_order_invoice i
    WHERE i.discount ->> 'type' = 'amount'
      AND (i.discount -> 'value' ->> 'amount')::NUMERIC >0
-- AND contract_outbound_id = 'V47424542' -- problematic contract, discount 282,89 € needs to be split into 3 subs
    ORDER BY contract_outbound_id, created DESC
)
   , cte_contract_product AS (
    SELECT
        cco.outbound_id::VARCHAR(9) AS shop_contract_id
         , pmap.product_uuid
         , pmap.cb_plan_id_unified_map AS plan_id
         , cbmap.cb_plan_id_map
         , cbmap.cb_plan_id_map_nbr
         , pmap.cb_addon_id
         , MAX(pmap.cb_addon_price_net) AS cb_addon_price_net
         , pmap.monthly_payment
         , MAX(item_net_unit_price_amount) AS unit_price_net
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
             JOIN mapping.umd_product_map pmap
                  ON pmap.product_uuid = CASE items.product_id WHEN 'fake' THEN '00000000-0000-0000-0000-000000000000' ELSE items.product_id END::UUID
             LEFT JOIN public.shop_extraction_cb_plan_id_map cbmap
                       ON pmap.cb_plan_id_unified_map = cbmap.cb_plan_id
    WHERE cc.contact ->> 'email' NOT LIKE '%vimcar.com'
--       AND cc.outbound_id = 'K51696968'
    GROUP BY cco.outbound_id::VARCHAR(9), pmap.cb_plan_id_unified_map, cbmap.cb_plan_id_map, cbmap.cb_plan_id_map_nbr, pmap.cb_addon_id, pmap.monthly_payment, pmap.product_uuid
)
-- SELECT * FROM cte_contract_product;
   , cte_invoice_product AS (
    SELECT
        ci.contract_outbound_id::VARCHAR(9) AS shop_contract_id
         , pmap.cb_plan_id_unified_map AS plan_id
         , cbmap.cb_plan_id_map
         , cbmap.cb_plan_id_map_nbr
         , pmap.cb_addon_id
         , MAX(pmap.cb_addon_price_net) AS cb_addon_price_net
         , pmap.monthly_payment
         , MAX(item_net_unit_price_amount) AS unit_price_net
         , MIN(ci.provisioning_start AT TIME ZONE 'Europe/Berlin')::DATE AS min_provisioning_start
         , MAX(ci.provisioning_start AT TIME ZONE 'Europe/Berlin')::DATE AS max_provisioning_start
         , MAX(ci.provisioning_end AT TIME ZONE 'Europe/Berlin')::DATE AS max_provisioning_end
    FROM public.shop_extraction_order_invoice ci
             JOIN public.customer cc
                  ON cc.id = ci.customer_id
--              JOIN public.shop_extraction_current_batch b  -- join batch table
--                   ON cc.outbound_id = b.customer_id
--                       AND coalesce(b.comment,'dummy') NOT IN ('Excluded from migration: Ghost customer or internal Vimcar email address.')
--              JOIN (SELECT DISTINCT customer_id
--                    FROM public.shop_extraction_batch
--                    WHERE coalesce(comment,'dummy') <> 'Not sent for migration - multiple customers with the same email address.') b2  -- excluding the problematic customer from the batch
--                   ON b2.customer_id = cc.outbound_id
       , LATERAL (SELECT (obj.value ->> 'product_id') AS product_id,
                      (obj.value -> 'price' -> 'net' ->> 'amount')::INT AS item_net_unit_price_amount
                  FROM jsonb_array_elements(ci.items_added) obj(value)) items_added
             JOIN mapping.umd_product_map pmap
                  ON pmap.product_uuid = CASE items_added.product_id WHEN 'fake' THEN '00000000-0000-0000-0000-000000000000' ELSE items_added.product_id END::UUID
             LEFT JOIN public.shop_extraction_cb_plan_id_map cbmap
                       ON pmap.cb_plan_id_unified_map = cbmap.cb_plan_id
    WHERE cc.contact ->> 'email' NOT LIKE '%vimcar.com'
--                       AND cc.outbound_id = 'K10711749'  -- monthly billing cycles (good for testing)
--                       AND cc.outbound_id = 'K65597398' -- annual billing cycles (good for testing)
--                         AND cc.outbound_id = 'K45828570' -- annual billing cycles, 3-years contract (good for testing)
--                         AND cc.outbound_id = 'K67710870' -- there is no cancellation date for contract V74285730, must be populated in SQL
--                         and cc.outbound_id = 'K91452842' -- there is a discount 10% ot the contract level but coupon is missing, dummy must be created
--                             AND cc.outbound_id = '72006772' -- the calculated contract term end date should be 17 Nov'21
--       AND cc.outbound_id = 'K78533247' -- customer has 1 product only and had a price increase in January 2022
--       and cc.outbound_id = 'K91544902' -- this one has 2 the same products invoices, therefore must have item_quantity = 2 (and not 1 as it was before 2022-05-05
--       AND cc.outbound_id = 'K91789534' -- customer has multiple products, 2 of them will get a price increase on 16 April'22
--     AND cc.outbound_id = 'K48177636' -- 3y contract, monthly billing, started in Oct'2019
--     AND cc.outbound_id = '79325532'-- 1y contract, yearly billing, started in 2017, PRICE INCREASE!
--       AND cc.outbound_id = 'K17650370'-- 1y contract, yearly billing, started in 2021, no price increase, PRORATED
--     AND cc.outbound_id = 'K24405027' -- 1y, monthly billing Geo, no price increase, started in Jan'2020
--       AND cc.outbound_id = 'K17109555' -- 3y contract, monthly billing, started in Oct'2021, multiple products, not prorated
--     AND cc.outbound_id = 'K41608077' -- prorated, multiple items (SLACK https://vimcar.slack.com/archives/C03FZHZ16MP/p1655296704949729)
--       AND cc.outbound_id = 'K52069358' -- 3y subscription, started in Sep'2019, good for checking what happens after 3 years
--       AND cc.outbound_id = 'K67416935' -- customer cancelled the 3y subscription within 100d, good for contract end checks
--     AND cc.outbound_id = 'K29745758' -- simple logbook customer, upsell +1 license in Apr'22 (now he has 2)
--       AND cc.outbound_id = 'K77776393'  -- simple logbook customer, 2 items, PRICE INCREASE in Apr'22
--       AND cc.outbound_id = 'K48062619' -- good for price increase investigation (logbook 1 and 3 years)
--       AND cc.outbound_id = 'K86580169' -- good for price increase investigation (Pro)
--       AND cc.outbound_id = 'K51696968' -- good for price increase
    GROUP BY ci.contract_outbound_id, pmap.cb_plan_id_unified_map, cbmap.cb_plan_id_map, cbmap.cb_plan_id_map_nbr, pmap.cb_addon_id, pmap.monthly_payment
)
--    SELECT * FROM cte_invoice_product;   ----- TESTING
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
             LEFT JOIN (SELECT shop_contract_id, plan_id, cb_plan_id_map, cb_plan_id_map_nbr, unit_price_net, cb_addon_id, cb_addon_price_net, monthly_payment
                             , SUM(item_quantity) AS item_quantity
                        FROM cte_contract_product
                        GROUP BY shop_contract_id, plan_id, cb_plan_id_map, cb_plan_id_map_nbr, unit_price_net, cb_addon_id, cb_addon_price_net, monthly_payment) c
                       ON i.shop_contract_id = c.shop_contract_id
                           AND i.plan_id = c.plan_id
                           AND i.unit_price_net = c.unit_price_net
    UNION
    SELECT shop_contract_id
         , plan_id
         , cb_plan_id_map
         , cb_plan_id_map_nbr
         , unit_price_net
         , cb_addon_id
         , cb_addon_price_net
         , monthly_payment
         , SUM(item_quantity) AS item_quantity
    FROM cte_contract_product
    GROUP BY shop_contract_id, plan_id, cb_plan_id_map, cb_plan_id_map_nbr, unit_price_net, cb_addon_id, cb_addon_price_net, monthly_payment
)
--    SELECT * FROM cte_inv_ctr_prod; where shop_contract_id = 'V48006979';
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
-- SELECT * FROM unique_subscriptions;
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
         , CASE
               WHEN s.addon_id IS NOT NULL THEN 'cancelled'  -- subscription for addons should be migrated as 'canceled'
               WHEN s.plan_id IN ('logbook_b2c-from_rent2buy-eur', 'logbook_b2c-one_time_buy-eur') THEN 'cancelled'  -- lifetime subs should be migrated as 'canceled'
               WHEN c.status = 'active' THEN 'active'
               WHEN c.status = 'canceled' THEN 'non_renewing'
               WHEN c.status = 'terminated' THEN 'cancelled'
        END AS "subscription[status]"
         , (c.created AT TIME ZONE 'Europe/Berlin')::DATE AS "contract_term[created_at]"
         , (c.started AT TIME ZONE 'Europe/Berlin')::DATE AS "tmp_subscription[started_at]"
         , (c.provisioning_period_start AT TIME ZONE 'Europe/Berlin')::DATE AS "tmp_subscription[current_term_start]"
         , (c.provisioning_period_end AT TIME ZONE 'Europe/Berlin')::DATE AS "tmp_subscription[current_term_end]"
         , ip.min_provisioning_start
         , ip.max_provisioning_start
         , ip.max_provisioning_end
--          , GREATEST(c.provisioning_period_end AT TIME ZONE 'Europe/Berlin',
--                     CASE
--                         WHEN s.plan_id ILIKE '%3_years%'
--                             THEN c.started + INTERVAL '3 years'
--                         WHEN s.monthly_payment = FALSE
--                             OR c.status = 'terminated'
--                             OR (c.status = 'canceled' AND c.provisioning_period_end + INTERVAL '3 months' < current_date)
--                             THEN c.provisioning_period_end
--                         ELSE c.started + ((extract(year from age(date_trunc('month',current_date), date_trunc('month',c.started) + INTERVAL '1 day'))
--                             + extract(month from age(date_trunc('month',current_date), date_trunc('month',c.started) + INTERVAL '1 day'))::INT / 12) + 1)  * INTERVAL '1 year'
--                         END AT TIME ZONE 'Europe/Berlin')::DATE AS "tmp_contract_term_end"
         , (c.canceled AT TIME ZONE 'Europe/Berlin')::DATE AS "subscription[cancelled_at]"
         , c.cancelation_reason AS cancel_reason_code
         , CASE
               WHEN s.plan_id IN ('logbook_b2c-from_rent2buy-eur', 'logbook_b2c-one_time_buy-eur') THEN TRUE
               ELSE FALSE
        END cf_lifetime_licence
         , CASE
               WHEN s.plan_id LIKE '%month%'
                   THEN 12
               ELSE 1
        END AS "subscription[contract_term_billing_cycle_on_renewal]"  -- works also for 3 years contracts: https://www.notion.so/vimcar/Migration-Review-Learnings-Decisions-To-Dos-f32758b58cb54d3e8a026b7821d0632f
         , CASE
               WHEN coupon_code IS NOT NULL
                   THEN coupon_code
               WHEN round(CASE WHEN discount ->> 'type' = 'percentage' THEN discount ->> 'value' END::NUMERIC,4) >= 1
                   THEN concat_ws('', 'dummy_pct_1_', split_part((round((discount ->> 'value')::numeric, 4))::TEXT,'.', 2))
               WHEN round(CASE WHEN discount ->> 'type' = 'percentage' THEN discount ->> 'value' END::NUMERIC,4) > 0
                   THEN concat_ws('', 'dummy_pct_0_', split_part((round((discount ->> 'value')::numeric, 4))::TEXT,'.', 2))
               WHEN d.discount_amount_translated_to_percentage IS NOT NULL
                   THEN d.discount_amount_translated_to_percentage
        --                WHEN round(CASE WHEN discount ->> 'type' = 'amount' THEN discount -> 'value' ->> 'amount' END::numeric) > 0
--                    THEN concat_ws('', 'dummy_gross_eur_', discount -> 'value' ->> 'amount' )
        END AS "coupon_ids[0]"
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
                WHEN btrim(coalesce(shipping_contact ->> 'phone_number',contact ->> 'phone_number')) !~ '[0-9]' THEN NULL
                ELSE substring(btrim(coalesce(shipping_contact ->> 'phone_number',contact ->> 'phone_number')) from '#"[0-9[:space:]+.\-]*#"' for '#')
                END,
            CASE
                WHEN btrim(coalesce(shipping_contact ->> 'mobile_phone_number',contact ->> 'mobile_phone_number')) !~ '[0-9]' THEN NULL
                ELSE substring(btrim(coalesce(shipping_contact ->> 'mobile_phone_number',contact ->> 'mobile_phone_number')) from '#"[0-9[:space:]+.\-]*#"' for '#')
                END, NULL))::TEXT AS "shipping_address[phone]"
         , nullif(regexp_replace(btrim(coalesce(shipping_contact -> 'address' ->> 'street_address',contact -> 'address' ->> 'street_address')), '[\t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g'), 'n/a') AS shipping_address_raw
         , nullif(regexp_replace(btrim(coalesce(shipping_contact -> 'address' ->> 'locality',contact -> 'address' ->> 'locality')), '[\t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g'), 'n/a') AS "shipping_address[city]"
         , to_char(
            btrim(CASE
                      WHEN btrim(coalesce(shipping_contact -> 'address' ->> 'postal_code',contact -> 'address' ->> 'postal_code')) !~ '^[0-9.]+$' THEN NULL
                      WHEN length(btrim(coalesce(shipping_contact -> 'address' ->> 'postal_code',contact -> 'address' ->> 'postal_code'))) > 10 THEN NULL
                      ELSE regexp_replace(btrim(coalesce(shipping_contact -> 'address' ->> 'postal_code',contact -> 'address' ->> 'postal_code')),'[\t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g')
                END)::INT,'fm00000') AS "shipping_address[zip]"
         , nullif(regexp_replace(btrim(coalesce(shipping_contact -> 'address' ->> 'country',contact -> 'address' ->> 'country')), '[\t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g'), 'n/a') AS "shipping_address[country]"
         , 'not_validated' AS "shipping_address[validation_status]"
         , s.addon_id AS "addons[id][0]"
         , s.item_quantity AS "addons[quantity][0]"
         , s.addon_unit_price_net AS "addons[unit_price][0]"
         , (c.modified AT TIME ZONE 'Europe/Berlin')::DATE AS contract_modified_ts
    FROM unique_subscriptions s
             JOIN public.contract c
                  ON s.shop_contract_id = c.outbound_id
             JOIN public.customer cu
                  ON cu.id = c.customer_id
             LEFT JOIN discounts_in_eur_amount d
                       ON d.contract_outbound_id = s.shop_contract_id
             LEFT JOIN cte_invoice_product ip
                       ON ip.shop_contract_id = s.shop_contract_id
                           AND ip.plan_id = s.plan_id
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
--    SELECT * FROM subscriptions_1 ; WHERE shop_contract_id IN ('V60796615','V39310061') ;
   , subscriptions_dates_precalculation AS (
    SELECT s.*
         , CASE
               WHEN "subscription[plan_id]" ILIKE '%3_years%'
                   THEN TRUE
               ELSE FALSE
        END AS is_3y_plan
         , CASE
               WHEN "subscription[plan_id]" ILIKE '%month%'
                   THEN TRUE
               ELSE FALSE
        END AS is_monthly_billing_plan
         , CASE
               WHEN "subscription[plan_id]" = 'addons-plan'
                   THEN s."tmp_subscription[current_term_start]"
               WHEN ip.min_provisioning_start IS NULL
                   THEN s."tmp_subscription[current_term_end]"
               ELSE least(s."tmp_subscription[current_term_start]", ip.min_provisioning_start)
        END AS "subscription[started_at]"
         , CASE
               WHEN "subscription[plan_id]" = 'addons-plan'
                   THEN s."tmp_subscription[current_term_start]"
               WHEN ip.min_provisioning_start IS NULL
                   THEN NULL
               ELSE ip.max_provisioning_start
--          ELSE "tmp_subscription[current_term_start]"
        END AS "subscription[current_term_start]"
         , CASE
               WHEN "subscription[plan_id]" = 'addons-plan'
                   THEN s."tmp_subscription[current_term_start]"
               WHEN ip.min_provisioning_start IS NULL
                   THEN NULL
               ELSE ip.max_provisioning_end
--            ELSE "tmp_subscription[current_term_end]"
        END AS "subscription[current_term_end]"
         , CASE
               WHEN "subscription[plan_id]" = 'addons-plan'
                   THEN s."tmp_subscription[current_term_start]"
               WHEN ip.min_provisioning_start IS NULL
                   THEN NULL
               ELSE ip.max_provisioning_end
--            ELSE "tmp_contract_term_end"
        END AS "temp contract term end"
--     , s."subscription[plan_id]"
--      , s."subscription[plan_quantity]"
--          , ip.min_provisioning_start
--          , ip.max_provisioning_start
--          , ip.max_provisioning_end
    FROM subscriptions_1 s
             LEFT JOIN cte_invoice_product ip
                       ON ip.shop_contract_id = s.shop_contract_id
                           AND ip.plan_id = s."subscription[plan_id]"
)
-- SELECT * FROM subscriptions_dates_precalculation;
   , subscriptions_dates_precalculation_2 AS (
    SELECT
        s2.*
         , GREATEST("subscription[current_term_end]",
                    CASE
                        WHEN is_3y_plan = TRUE
                            AND is_monthly_billing_plan = FALSE
                            THEN make_date(EXTRACT (YEAR FROM min_3y_prod_start_date)::INT
                                     , EXTRACT (MONTH FROM "temp contract term end")::INT
                                     , EXTRACT (DAY FROM "temp contract term end")::INT) + INTERVAL '3 years'
                        WHEN is_3y_plan = TRUE
                            AND is_monthly_billing_plan = TRUE
                            THEN min_3y_prod_start_date + INTERVAL '3 years'
                        WHEN is_monthly_billing_plan = FALSE
                            OR "subscription[status]" = 'terminated'
                            OR ("subscription[status]" = 'canceled' AND "subscription[current_term_end]" + INTERVAL '3 months' < current_date)
                            THEN "subscription[current_term_end]"
                        ELSE "subscription[started_at]" + ((extract(year from age(date_trunc('month',current_date), date_trunc('month',"subscription[started_at]") + INTERVAL '1 day'))
                            + extract(month from age(date_trunc('month',current_date), date_trunc('month',"subscription[started_at]") + INTERVAL '1 day'))::INT / 12) + 1)  * INTERVAL '1 year'
                        END) AS "contract term end"
    FROM subscriptions_dates_precalculation s2
             LEFT JOIN (SELECT shop_contract_id AS contract_id_from_shop, MIN("subscription[started_at]") AS min_3y_prod_start_date
                        FROM subscriptions_dates_precalculation
                        WHERE "subscription[plan_id]" ILIKE '%3_years%'
                        GROUP BY shop_contract_id) three_year
                       ON three_year.contract_id_from_shop = s2.shop_contract_id
)
-- SELECT * FROM subscriptions_dates_precalculation_2; WHERE shop_contract_id IN ('V60796615','V39310061') ;
   , subscriptions_2 AS (
    SELECT
        "customer[email]"
         , shop_customer_id
         , "subscription[id]"
         , shop_contract_id AS "subscription[cf_vertragsnummer]"
         , "subscription[plan_id]"
         , CASE
               WHEN "subscription[plan_quantity]" > 0
                   THEN "subscription[plan_quantity]"
        END AS "subscription[plan_quantity]"
         , "subscription[plan_unit_price]"
         -- STATUS MAPPING: https://www.notion.so/vimcar/Migration-Review-Learnings-Decisions-To-Dos-f32758b58cb54d3e8a026b7821d0632f
         , CASE
               WHEN "subscription[current_term_end]" + INTERVAL '14 days' < current_date
                   THEN 'cancelled'
               WHEN "subscription[status]" = 'active'
                   AND "subscription[plan_quantity]"  < 1
                   AND "contract term end" < current_date
                   THEN 'cancelled'
               WHEN "subscription[status]" = 'active'
                   AND "subscription[plan_quantity]" < 1
                   THEN 'cancelled'
               WHEN "subscription[started_at]" > current_date
                   THEN 'future'
               WHEN "subscription[status]" <> 'active'
                   AND "subscription[current_term_end]" = "contract term end"   -- meaning there are NO billing cycles left
                   AND  current_date < "contract term end"                      -- contract end date is in the future
                   THEN 'non_renewing'
               WHEN "subscription[status]" <> 'active'
                   AND "contract term end" <= current_date
                   THEN 'cancelled'
               WHEN "subscription[status]" = 'active'
                   AND current_date <= "contract term end"                      -- contract end date is today or in the future
                   THEN 'active'
               ELSE "subscription[status]"
        END AS "subscription[status]"
         , "subscription[cancelled_at]"
         , contract_modified_ts
         , "contract_term[created_at]"
         , "subscription[started_at]"
         , "subscription[current_term_start]"
         , "subscription[current_term_end]"
         , "contract term end"
         , cancel_reason_code
         , cf_lifetime_licence
--          , "subscription[auto_collection]"
         , "coupon_ids[0]" AS coupon_code_temp
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
         , "shipping_address[city]"
         , "shipping_address[zip]"
         , "shipping_address[country]"
         , "shipping_address[validation_status]"
         , "subscription[contract_term_billing_cycle_on_renewal]"
         , is_monthly_billing_plan
         , is_3y_plan
         , CASE
               WHEN is_3y_plan = TRUE
                   AND is_monthly_billing_plan = TRUE
                   THEN 36
               WHEN is_3y_plan = TRUE
                   AND is_monthly_billing_plan = FALSE
                   THEN 3
               WHEN is_3y_plan = FALSE
                   AND is_monthly_billing_plan = TRUE
                   THEN 12
               WHEN is_3y_plan = FALSE
                   AND is_monthly_billing_plan = FALSE
                   THEN 1
        END AS default_billing_cycles
    FROM subscriptions_dates_precalculation_2
)
-- SELECT * FROM subscriptions_2 ;
   , subscriptions_3 AS (SELECT "customer[email]"
                              , shop_customer_id
                              , "subscription[id]"
                              , "subscription[cf_vertragsnummer]"
                              , "subscription[plan_id]"
                              , "subscription[plan_quantity]"
                              , "subscription[plan_unit_price]"
                              , "subscription[status]"
                              , CASE WHEN "subscription[status]" = 'future' THEN "subscription[started_at]" END AS "subscription[start_date]"
                              , CASE WHEN "subscription[status]" <> 'future' THEN "subscription[started_at]" END AS "subscription[started_at]"
                              , CASE WHEN "subscription[status]" IN ('active', 'non_renewing') THEN "subscription[current_term_start]" END AS "subscription[current_term_start]"
                              , CASE WHEN "subscription[status]" IN ('active', 'non_renewing') THEN "subscription[current_term_end]" END AS "subscription[current_term_end]"
                              , CASE
                                    WHEN "subscription[status]" != 'cancelled'
                                        THEN NULL -- added on 2022-03-16 as per CB advise: https://vimcar.slack.com/archives/C02NJSXE57Z/p1647363276161959
                                    WHEN "subscription[cancelled_at]" IS NOT NULL
                                        AND "subscription[cancelled_at]" > coalesce("subscription[current_term_start]","subscription[started_at]")
                                        THEN "subscription[cancelled_at]"
                                    WHEN "subscription[cancelled_at]" IS NOT NULL
                                        THEN coalesce("subscription[current_term_start]","subscription[started_at]" + interval '1 day')
                                    WHEN "subscription[status]" <> 'active'
                                        AND "contract term end" <= current_date
                                        THEN "contract term end" - INTERVAL '1 day'
                                    WHEN "subscription[status]" <> 'active'
                                        OR ("subscription[plan_quantity]" = 0 AND "subscription[status]" = 'active')
                                        THEN coalesce("subscription[current_term_start]","subscription[started_at]" + interval '1 day')
        END AS "subscription[cancelled_at]"
                              , CASE WHEN "subscription[status]" = 'cancelled' THEN cancel_reason_code END AS cancel_reason_code
                              , CASE
                                    WHEN "subscription[status]" = 'cancelled'
                                        THEN "subscription[current_term_end]"
                                    WHEN "subscription[status]" <> 'future'
                                        THEN "contract term end"
        END AS "contract term end"
                              , CASE
                                    WHEN "subscription[status]" = 'future'
                                        THEN "subscription[started_at]"
                                    WHEN is_3y_plan = FALSE
                                        THEN "contract term end" - INTERVAL '1 year' -- logic for PRORATED ??
                                    WHEN is_3y_plan = TRUE
                                        THEN "contract term end" - INTERVAL '3 years' -- logic for PRORATED ??
        END AS "contract_term[contract_start]"
                              , cf_lifetime_licence
--                               , "subscription[auto_collection]"
                              , coupon_code_temp
                              , "shipping_address[first_name]"
                              , "shipping_address[last_name]"
                              , "shipping_address[email]"
                              , "shipping_address[company]"
                              , "shipping_address[phone]"
                              , "shipping_address[line1]"
                              , "shipping_address[line2]"
                              , "shipping_address[city]"
                              , "shipping_address[zip]"
                              , "shipping_address[country]"
                              , "shipping_address[validation_status]"
                              , "subscription[contract_term_billing_cycle_on_renewal]"
                              , "contract_term[created_at]"
                              , is_monthly_billing_plan
                              , is_3y_plan
                              , default_billing_cycles
                         FROM subscriptions_2
)
-- SELECT * FROM subscriptions_3; WHERE "subscription[cf_vertragsnummer]"  IN ('V60796615','V39310061') ;
   , subscriptions_4 AS (
    SELECT
        "customer[email]"
         , shop_customer_id
         , "subscription[id]"
         , "subscription[cf_vertragsnummer]"
         , "subscription[plan_id]"
         , "subscription[plan_quantity]"
         , "subscription[plan_unit_price]"
         , "subscription[status]"
         , "subscription[start_date]"
         , "subscription[started_at]"
         , "subscription[current_term_start]" AS "subscription[current_term_start_shop]"
         , CASE
               WHEN is_monthly_billing_plan = TRUE
                   THEN "subscription[current_term_end]" - INTERVAL '1 month'
               ELSE "subscription[current_term_end]" - INTERVAL '1 year'
        END AS "subscription[current_term_start]"
         , "subscription[current_term_end]"
         , "contract term end"
         , "contract_term[contract_start]"
         , "contract_term[created_at]"
         , CASE
               WHEN "subscription[cancelled_at]" > "subscription[started_at]"
                   AND "subscription[cancelled_at]" <= "contract term end"
                   THEN "subscription[cancelled_at]"
               WHEN "subscription[cancelled_at]" <= "subscription[started_at]"
                   THEN "subscription[started_at]" + INTERVAL '1 minute'
               WHEN "subscription[cancelled_at]" > "contract term end"
                   THEN "contract term end"
        END AS "subscription[cancelled_at]"
         , CASE
               WHEN is_3y_plan = TRUE
                   AND is_monthly_billing_plan = FALSE
                   AND "subscription[started_at]" + interval '3 years' <= current_date
                   THEN 1
               WHEN is_3y_plan = TRUE
                   AND is_monthly_billing_plan = TRUE
                   AND "subscription[started_at]" + interval '3 years' <= current_date
                   THEN 12
               ELSE default_billing_cycles
        END AS "contract_term[billing_cycle]" -- billing cycles for this plan
         , "subscription[contract_term_billing_cycle_on_renewal]"
         , coupon_code_temp
         , cancel_reason_code
         , cf_lifetime_licence
         , "shipping_address[first_name]"
         , "shipping_address[last_name]"
         , "shipping_address[email]"
         , "shipping_address[company]"
         , "shipping_address[phone]"
         , "shipping_address[line1]"
         , "shipping_address[line2]"
         , "shipping_address[city]"
         , "shipping_address[zip]"
         , "shipping_address[country]"
         , "shipping_address[validation_status]"
         , default_billing_cycles
         , is_3y_plan
         , is_monthly_billing_plan
    FROM subscriptions_3
)
--    SELECT * from subscriptions_4;
   , subscriptions_5 AS (SELECT "customer[email]"
                              , shop_customer_id
                              , "subscription[id]"
                              , "subscription[cf_vertragsnummer]"
                              , "subscription[plan_id]"
                              , "subscription[plan_quantity]"
                              , "subscription[plan_unit_price]"
                              , "subscription[status]"
                              , "subscription[start_date]"
                              , "subscription[started_at]"
--                               , least("subscription[started_at]","contract_term[contract_start]","subscription[current_term_start]") AS "subscription[started_at]"
                              , "subscription[current_term_start_shop]"
                              , "subscription[current_term_start]" AS "subscription[current_term_start]"
                              , "subscription[current_term_end]" AS "subscription[current_term_end]"
                              , "contract term end"
--                               , CASE
--                                     WHEN is_monthly_billing_plan = FALSE
--                                         THEN "contract term end" - (default_billing_cycles * INTERVAL '1 year')
--                                     WHEN is_monthly_billing_plan = TRUE
--                                         THEN "contract term end" - (default_billing_cycles * INTERVAL '1 month')
--                                 END::DATE AS "contract_term[contract_start]"
                              , "contract_term[contract_start]"
--                               , least("subscription[started_at]","contract_term[created_at]","subscription[current_term_start]") AS "contract_term[created_at]"
                              , "contract_term[created_at]"
                              , "subscription[cancelled_at]"
                              , "contract_term[billing_cycle]"
                              , "subscription[contract_term_billing_cycle_on_renewal]"
                              , default_billing_cycles
                              , is_3y_plan
                              , is_monthly_billing_plan
                         FROM subscriptions_4
)
   , subscriptions_6 AS (SELECT
                             "customer[email]"
                              , shop_customer_id
                              , "subscription[id]"
                              , "subscription[cf_vertragsnummer]"
                              , "subscription[plan_id]"
                              , "subscription[plan_quantity]"
                              , "subscription[plan_unit_price]"
                              , CASE
                                    WHEN "subscription[status]" = 'non_renewing'
                                        -- check if billing cycles >1
                                        AND (CASE
                                            -- 3y which completed the first 36 months:
                                                 WHEN is_3y_plan = TRUE
                                                     AND is_monthly_billing_plan = TRUE
                                                     AND "subscription[started_at]" + interval '3 years' <= current_date
                                                     THEN 12
                                            -- 1y and 3y, yearly billing:
                                                 WHEN is_monthly_billing_plan = FALSE
                                                     THEN GREATEST(1, EXTRACT(year FROM age("contract term end","subscription[current_term_start]"::DATE)))
                                            -- 1y and 3y, monthly billing:
                                                 WHEN is_monthly_billing_plan = TRUE
                                                     THEN GREATEST(1, EXTRACT(year FROM age("contract term end","subscription[current_term_start]"::DATE))*12
                                                     + EXTRACT(month FROM age("contract term end","subscription[current_term_start]"::DATE)))
                                            END::INT) >1
                                        THEN 'active'
                                    ELSE "subscription[status]"
        END AS "subscription[status]"
                              , "subscription[start_date]"
                              , "subscription[started_at]"
                              , "subscription[current_term_start_shop]"
                              , "subscription[current_term_start]"
                              , "subscription[current_term_end]"
                              , "contract term end"
                              , "contract_term[contract_start]" --2022-06-23, contract start date cannot not be lesser than the Subscription start date
                              , "contract_term[created_at]"
                              , "subscription[cancelled_at]"
                              , CASE
                                    WHEN "subscription[status]" = 'cancelled'
                                        THEN NULL
                                    WHEN "subscription[status]" = 'future'
                                        THEN default_billing_cycles
        -- 3y which completed the first 36 months:
                                    WHEN is_3y_plan = TRUE
                                        AND is_monthly_billing_plan = TRUE
                                        AND "subscription[started_at]" + interval '3 years' <= current_date
                                        THEN 12
        -- 1y and 3y, yearly billing:
                                    WHEN is_monthly_billing_plan = FALSE
                                        THEN GREATEST(1, EXTRACT(year FROM age("contract term end","subscription[current_term_start]"::DATE)))
        -- 1y and 3y, monthly billing:
                                    WHEN is_monthly_billing_plan = TRUE
                                        THEN GREATEST(1, EXTRACT(year FROM age("contract term end","subscription[current_term_start]"::DATE))*12
                                        + EXTRACT(month FROM age("contract term end","subscription[current_term_start]"::DATE)))
        END::INT AS billing_cycles -- remaining billing cycles
                              , "contract_term[billing_cycle]"
                              , "subscription[contract_term_billing_cycle_on_renewal]"
                              , default_billing_cycles
                              , is_monthly_billing_plan
                         FROM subscriptions_5
)
   , subscriptions_7 AS (
    SELECT "customer[email]"
         , shop_customer_id
         , "subscription[id]"
         , "subscription[cf_vertragsnummer]"
         , "subscription[plan_id]"
         , "subscription[plan_quantity]"
         , "subscription[plan_unit_price]"
         , "subscription[status]"
         , "subscription[start_date]"
         , "subscription[started_at]"
         , "subscription[current_term_start_shop]"
         , "subscription[current_term_start]"
         , "subscription[current_term_end]"
         , CASE WHEN "subscription[status]" = 'active' THEN "contract term end" END AS "contract term end"
--          , "contract_term[contract_start]"
         , CASE
               WHEN "subscription[status]" <> 'active'
                   THEN NULL
               WHEN is_monthly_billing_plan = FALSE
                   THEN "subscription[current_term_start]" - ((default_billing_cycles - billing_cycles) * interval '1 year')
               WHEN is_monthly_billing_plan = TRUE
                   THEN "subscription[current_term_start]" - ((default_billing_cycles - billing_cycles) * interval '1 month')
        END::DATE                                                                                                           AS "contract_term[contract_start]"
         , CASE WHEN "subscription[status]" = 'active' THEN "contract_term[created_at]" END                                     AS "contract_term[created_at]"
         , "subscription[cancelled_at]"
         , CASE WHEN "subscription[status]" = 'active' THEN billing_cycles END                                                  AS billing_cycles
         , CASE WHEN "subscription[status]" = 'active' THEN "contract_term[billing_cycle]" END                                  AS "contract_term[billing_cycle]"
         , CASE
               WHEN "subscription[status]" IN ('active', 'future')
                   THEN "subscription[contract_term_billing_cycle_on_renewal]"
        END AS "subscription[contract_term_billing_cycle_on_renewal]"
         , default_billing_cycles
         , is_monthly_billing_plan
    FROM subscriptions_6
)
-- SELECT * from subscriptions_7 ;
   , subscriptions_8 AS (
    SELECT s."customer[email]"
         , s.shop_customer_id
         , s."subscription[id]"
         , s."subscription[cf_vertragsnummer]"
         , s."subscription[plan_id]"
         , s."subscription[plan_quantity]"
         , s."subscription[plan_unit_price]"
         , s."subscription[status]"
         , CASE WHEN s."subscription[status]" = 'future' THEN least(s."subscription[start_date]", s."subscription[started_at]", s."contract_term[contract_start]") END AS "subscription[start_date]"
         , CASE WHEN s."subscription[status]" <> 'future' THEN least(s."subscription[started_at]", s."contract_term[contract_start]") END AS "subscription[started_at]"
         , s."subscription[current_term_start_shop]"
         , CASE
               WHEN s."subscription[status]" = 'active'
                   AND s."subscription[current_term_end]" <= current_date
                   THEN
                   CASE
                       WHEN s.is_monthly_billing_plan = TRUE
                           THEN s."subscription[current_term_start]" + INTERVAL '1 month'
                       WHEN s.is_monthly_billing_plan = FALSE
                           THEN s."subscription[current_term_start]" + INTERVAL '1 year'
                       END
               ELSE s."subscription[current_term_start]"
        END::DATE AS "subscription[current_term_start] TEST"
         , s."subscription[current_term_start]"
         , s."subscription[current_term_end]"
         , CASE
               WHEN s."subscription[status]" = 'active'
                   AND s."subscription[current_term_end]" <= current_date
                   THEN TRUE
               ELSE FALSE
        END pushed_by_one_billing_cycle
         , s."contract term end"
         , s."contract_term[contract_start]"
         , CASE WHEN s."subscription[status]" = 'active' THEN least(s."contract_term[created_at]", s."subscription[started_at]", s."contract_term[contract_start]") END AS "contract_term[created_at]"
         , s."subscription[cancelled_at]"
         , s.billing_cycles
         , s."contract_term[billing_cycle]"
         , s."subscription[contract_term_billing_cycle_on_renewal]"
         , s.default_billing_cycles
         , s4.coupon_code_temp
         , s4.cancel_reason_code
         , s4.cf_lifetime_licence
         , s4."shipping_address[first_name]"
         , s4."shipping_address[last_name]"
         , s4."shipping_address[email]"
         , s4."shipping_address[company]"
         , s4."shipping_address[phone]"
         , s4."shipping_address[line1]"
         , s4."shipping_address[line2]"
         , s4."shipping_address[city]"
         , s4."shipping_address[zip]"
         , s4."shipping_address[country]"
         , s4."shipping_address[validation_status]"
    FROM subscriptions_7 s
             JOIN subscriptions_4 s4
                  ON s."subscription[id]" = s4."subscription[id]"
)
SELECT
    CASE
        WHEN "customer[email]" = 'johannes.horsters@dvag-de'
            THEN 'johannes.horsters@dvag.de'
        ELSE split_part("customer[email]",',',1)
        END AS "customer[email]"
     , shop_customer_id
     , "subscription[id]"
     , "subscription[cf_vertragsnummer]"
     , "subscription[plan_id]"
     , "subscription[plan_quantity]"
     , "subscription[plan_unit_price]"
     , "subscription[status]"

     , to_char("subscription[start_date]",'YYYY-MM-DD HH24:MI:SS') AS "subscription[start_date]"
     , to_char("subscription[started_at]",'YYYY-MM-DD HH24:MI:SS') AS "subscription[started_at]"
     , to_char("subscription[current_term_start_shop]",'YYYY-MM-DD HH24:MI:SS') AS "subscription[current_term_start_shop]"
     , to_char("subscription[current_term_start]",'YYYY-MM-DD HH24:MI:SS') AS "subscription[current_term_start]"
--      , "subscription[current_term_start] TEST"
     , to_char("subscription[current_term_end]",'YYYY-MM-DD HH24:MI:SS') AS "subscription[current_term_end]"
     , to_char("contract term end",'YYYY-MM-DD HH24:MI:SS') AS "contract term end"
     , to_char("contract_term[contract_start]",'YYYY-MM-DD HH24:MI:SS') AS "contract_term[contract_start]" --2022-06-23, contract start date cannot not be lesser than the Subscription start date
     , to_char("contract_term[created_at]",'YYYY-MM-DD HH24:MI:SS') AS "contract_term[created_at]"
     , to_char("subscription[cancelled_at]",'YYYY-MM-DD HH24:MI:SS') AS "subscription[cancelled_at]"
     , pushed_by_one_billing_cycle
     , billing_cycles
--      , CASE
--          WHEN pushed_by_one_billing_cycle = TRUE
--              AND billing_cycles >1
--                 THEN billing_cycles - 1
--         ELSE billing_cycles
--         END AS billing_cycles
     , "contract_term[billing_cycle]"
     , "subscription[contract_term_billing_cycle_on_renewal]"
     , coupon_code_temp
     , cancel_reason_code
     , cf_lifetime_licence
     , FALSE AS "create_current_term_invoice"
     , "shipping_address[first_name]"
     , "shipping_address[last_name]"
     , CASE
           WHEN "shipping_address[email]" = 'johannes.horsters@dvag-de'
               THEN 'johannes.horsters@dvag.de'
           ELSE split_part("shipping_address[email]",',',1)
    END AS "shipping_address[email]"
     , "shipping_address[company]"
     , "shipping_address[phone]"
     , "shipping_address[line1]"
     , "shipping_address[line2]"
     , NULL AS "shipping_address[line3]"
     , "shipping_address[city]"
     , NULL AS "shipping_address[state_code]"
     , NULL AS "shipping_address[state]"
     , "shipping_address[zip]"
     , "shipping_address[country]"
     , "shipping_address[validation_status]"
     , default_billing_cycles
     , FALSE AS "create_current_term_invoice"
     , NULL AS total_amount_raised
     , NULL AS "subscription[setup_fee]"
     , NULL AS "subscription[trial_start]"
     , NULL AS "subscription[trial_end]"
     , NULL AS "subscription[pause_date]"
     , NULL AS "subscription[resume_date]"
     , NULL AS "subscription[po_number]"
     , NULL AS "coupon_ids[1]"
     , NULL AS "subscription[payment_source_id]"
     , NULL AS "subscription[invoice_notes]"
     , NULL AS "subscription[meta_data]"
     , NULL AS "contract_term[id]"
     , NULL AS "contract_term[total_amount_raised]"
     , NULL AS "contract_term[action_at_term_end]"
     , 28   AS "contract_term[cancellation_cutoff_period]"
     , NULL AS "transaction[amount]"
     , NULL AS "transaction[payment_method]"
     , NULL AS "transaction[reference_number]"
     , NULL AS "transaction[date]"
     , NULL AS "subscription[cf_cancelled_devices]"
     , NULL AS "subscription[cf_total_sent_devices_for_subscription]"
     , NULL AS "subscription[cf_total_received_devices_for_subscription]"
     , NULL AS "subscription[cf_currently_held_devices_for_subscription]"
     , NULL AS "subscription[cf_to_be_returned_devices_for_subscription]"
FROM subscriptions_8
WHERE "subscription[plan_id]" NOT LIKE 'hardware%'
--and shop_customer_id = 'K41608077'
-- As agreed with Anne and CB on 20220110 we stop sending hardware plans for migration, therefore the 2 lines below are obsolete
--UNION ALL
--SELECT DISTINCT * FROM subscriptions_6 WHERE "subscription[plan_id]" LIKE 'hardware%'
;
