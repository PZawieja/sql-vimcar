WITH unique_subscriptions AS (
    SELECT
        shop_contract_id
         , shop_contract_id ||'_'|| cb_plan_id_map AS unique_subscription_id
         , plan_id
         , MAX(CASE
                   WHEN cb_addon_id IS NULL
                       THEN unit_price_net
                   ELSE 0
        END) AS plan_unit_price_net
         , cb_addon_id AS addon_id
         , cb_addon_price_net AS addon_unit_price_net
         , SUM(item_quantity) AS item_quantity
    FROM (
             SELECT
                 inv.shop_contract_id
                  , inv.plan_id
                  , inv.cb_plan_id_map
                  , inv.unit_price_net
                  , coalesce(ctr.item_quantity, 0) AS item_quantity
                  , inv.cb_addon_id
                  , inv.cb_addon_price_net
             FROM (
                      SELECT DISTINCT
                          ci.contract_outbound_id::VARCHAR(9) AS shop_contract_id
                                    , coalesce(pmap.cb_plan_id, CASE WHEN items_added.product_id = 'fake' THEN '00000000-0000-0000-0000-000000000000' ELSE items_added.product_id END) AS plan_id
                                    , cbmap.cb_plan_id_map
                                    , item_net_unit_price_amount AS unit_price_net
                                    , pmap.cb_addon_id
                                    , pmap.cb_addon_price_net
                      FROM public.shop_extraction_order_invoice ci
                               JOIN public.customer cc
                                    ON cc.id = ci.customer_id
                               JOIN public.shop_extraction_current_batch b  -- join batch table
                                    ON cc.outbound_id = b.customer_id
                         , LATERAL (SELECT (obj.value ->> 'product_id') AS product_id,
                                        (obj.value -> 'price' -> 'net' ->> 'amount')::INT AS item_net_unit_price_amount
                                    FROM jsonb_array_elements(ci.items_added) obj(value)) items_added
                               JOIN mapping.umd_product_map pmap
                                    ON pmap.product_uuid = CASE items_added.product_id WHEN 'fake' THEN '00000000-0000-0000-0000-000000000000' ELSE items_added.product_id END::UUID
                               LEFT JOIN public.shop_extraction_cb_plan_id_map cbmap
                                         ON pmap.cb_plan_id = cbmap.cb_plan_id
                      WHERE cc.contact ->> 'email' NOT LIKE '%vimcar.com'
--                       AND cc.outbound_id = 'K10711749'  -- monthly billing cycles (good for testing)
--                       AND cc.outbound_id = 'K65597398' -- annual billing cycles (good for testing)
--                         AND cc.outbound_id = 'K45828570' -- annual billing cycles, 3-years contract (good for testing)
                 AND cc.outbound_id = 'K24422326'
                  ) inv
                      LEFT JOIN (
                 SELECT
                     cco.outbound_id::VARCHAR(9) AS shop_contract_id
                      , coalesce(pmap.cb_plan_id, CASE WHEN items.product_id = 'fake' THEN '00000000-0000-0000-0000-000000000000' ELSE items.product_id END) AS plan_id
                      , item_net_unit_price_amount AS unit_price_net
                      , SUM(item_quantity) AS item_quantity
                 FROM public.contract cco
                          JOIN public.customer cc
                               ON cc.id = cco.customer_id
                          JOIN public.shop_extraction_current_batch b  -- join batch table
                               ON cc.outbound_id = b.customer_id
                    , LATERAL (SELECT (obj.value ->> 'product_id') AS product_id,
                                   (obj.value -> 'price' -> 'net' ->> 'amount')::INT AS item_net_unit_price_amount,
                                   (obj.value ->> 'quantity')::SMALLINT AS item_quantity
                               FROM jsonb_array_elements(cco.items) obj(value)) items
                          JOIN mapping.umd_product_map pmap
                               ON pmap.product_uuid = CASE items.product_id WHEN 'fake' THEN '00000000-0000-0000-0000-000000000000' ELSE items.product_id END::UUID
                 WHERE cc.contact ->> 'email' NOT LIKE '%vimcar.com'
                 GROUP BY cco.outbound_id, pmap.cb_plan_id, items.product_id, item_net_unit_price_amount
             ) ctr
                                ON ctr.shop_contract_id = inv.shop_contract_id
                                    AND ctr.plan_id = inv.plan_id
                                    AND ctr.unit_price_net = inv.unit_price_net
         ) all_plans_ever
    GROUP BY shop_contract_id, plan_id, cb_plan_id_map, cb_addon_id, cb_addon_price_net
)
   , subscriptions_1 AS (
    SELECT
        s.shop_contract_id
         , s.unique_subscription_id  AS "subscription[id]"
         , s.plan_id                 AS "subscription[plan_id]"
         , CASE
               WHEN s.addon_id IS NOT NULL
                   THEN 1
               ELSE item_quantity
        END                      AS "subscription[plan_quantity]"
         , plan_unit_price_net       AS "subscription[plan_unit_price]"
---
         , lower(regexp_replace(contact ->> 'email', '[ \t\n\r]*', '', 'g')) AS "customer[email]"
         , cu.outbound_id AS shop_customer_id
         , NULL AS "subscription[setup_fee]"
         , CASE
               WHEN s.addon_id IS NOT NULL   THEN 'cancelled'
               WHEN c.status = 'active'     THEN 'active'
               WHEN c.status = 'canceled'   THEN 'non_renewing'
               WHEN c.status = 'terminated' THEN 'cancelled'
        END AS "subscription[status]"
         , NULL AS "subscription[start_date]" -- only for future subscriptions (leave it blank)
         , NULL AS "subscription[trial_start]"
         , NULL AS "subscription[trial_end]"
         , c.started AT TIME ZONE 'Europe/Berlin' AS "subscription[started_at]"
         , c.provisioning_period_start AT TIME ZONE 'Europe/Berlin' AS "subscription[current_term_start]"
         , c.provisioning_period_end AT TIME ZONE 'Europe/Berlin' AS "subscription[current_term_end]"
         , CASE
               WHEN s.plan_id ILIKE '%3-years%'
                   OR s.plan_id ILIKE '%3_years%'
                   THEN c.started + INTERVAL '3 years'
               WHEN c.status = 'terminated'
                   OR (c.status = 'canceled' AND c.provisioning_period_end + INTERVAL '3 months' < current_date)
                   THEN c.provisioning_period_end
               ELSE c.started + ((extract(year from age(date_trunc('month',current_date), date_trunc('month',c.started) + INTERVAL '1 day'))
                   + extract(month from age(date_trunc('month',current_date), date_trunc('month',c.started) + INTERVAL '1 day'))::INT / 12) + 1)  * INTERVAL '1 year'
               END AT TIME ZONE 'Europe/Berlin' AS "contract term end"
         , c.canceled AT TIME ZONE 'Europe/Berlin' AS "subscription[cancelled_at]"
         , NULL AS "subscription[pause_date]"
         , NULL AS "subscription[resume_date]"
         , CASE
               WHEN plan_id IN ('hardware-eur', 'logbook_b2c-one_time_buy-eur', 'hardware-one_time_buy-eur') THEN TRUE
               ELSE FALSE
        END cf_lifetime_licence
         , CASE
               WHEN plan_id LIKE '%month%'
                   THEN 12
               ELSE 1
        END AS "subscription[contract_term_billing_cycle_on_renewal]"  -- CHECK!
         , NULL AS "total_amount_raised" -- CHECK!
         , CASE
               WHEN rp.payment_method IN ('invoice', 'paypal')
                   OR rp.payment_method IS NULL
                   THEN 'off'
               ELSE 'on'
        END AS "subscription[auto_collection]"  --on when recurring payment method, off when payment by wire transfer
         , NULL AS "subscription[po_number]"
         , CASE
               WHEN contract_with_min_one_coupon_ever.contract_outbound_id IS NULL
                   THEN NULL
               WHEN coupon_code IS NOT NULL
                   THEN coupon_code
               WHEN round(CASE WHEN discount ->> 'type' = 'percentage' THEN discount ->> 'value' END::NUMERIC,4) >= 1
                   THEN concat_ws('', 'dummy_pct_1_', split_part((round((discount ->> 'value')::numeric, 4))::TEXT,'.', 2))
               WHEN round(CASE WHEN discount ->> 'type' = 'percentage' THEN discount ->> 'value' END::NUMERIC,4) > 0
                   THEN concat_ws('', 'dummy_pct_0_', split_part((round((discount ->> 'value')::numeric, 4))::TEXT,'.', 2))
        END AS "coupon_ids[0]"
         , NULL AS "coupon_ids[1]"
         , NULL AS "subscription[payment_source_id]"
         , NULL AS "subscription[invoice_notes]"
         , NULL AS "subscription[meta_data]"
         , btrim(regexp_replace(shipping_contact ->> 'first_name', '[\t\n\r]*', '', 'g')) AS "shipping_address[first_name]"
         , btrim(regexp_replace(shipping_contact ->> 'last_name', '[\t\n\r]*', '', 'g')) AS "shipping_address[last_name]"
         , btrim(lower(regexp_replace(shipping_contact ->> 'email', '[ \t\n\r]*', '', 'g'))) AS "shipping_address[email]"
         , btrim(regexp_replace(shipping_contact ->> 'company_name', '[\t\n\r]*', '', 'g')) AS "shipping_address[company]"
         , btrim(COALESCE(
            CASE
                WHEN shipping_contact ->> 'phone_number' !~ '[0-9]' THEN NULL
                ELSE substring(shipping_contact ->> 'phone_number' from '#"[0-9[:space:]+.\-]*#"' for '#')
                END,
            CASE
                WHEN shipping_contact ->> 'mobile_phone_number' !~ '[0-9]' THEN NULL
                ELSE substring(shipping_contact ->> 'mobile_phone_number' from '#"[0-9[:space:]+.\-]*#"' for '#')
                END, NULL))::TEXT AS "shipping_address[phone]"
         , btrim(nullif(regexp_replace(shipping_contact -> 'address' ->> 'street_address', '[\t\n\r]*', '', 'g'), 'n/a')) AS "shipping_address[line1]"
         , NULL AS "shipping_address[line2]"
         , NULL AS "shipping_address[line3]"
         , btrim(nullif(regexp_replace(shipping_contact -> 'address' ->> 'locality', '[\t\n\r]*', '', 'g'), 'n/a')) AS "shipping_address[city]"
         , NULL AS "shipping_address[state_code]"
         , NULL AS "shipping_address[state]"
         , btrim(CASE
                     WHEN shipping_contact -> 'address' ->> 'postal_code'  !~ '[0-9]' THEN NULL
                     WHEN length(shipping_contact -> 'address' ->> 'postal_code') > 16 THEN NULL
                     ELSE shipping_contact -> 'address' ->> 'postal_code'
        END)::TEXT AS "shipping_address[zip]"
         , btrim(nullif(regexp_replace(shipping_contact -> 'address' ->> 'country', '[\t\n\r]*', '', 'g'), 'n/a')) AS "shipping_address[country]"
         , 'not_validated' AS "shipping_address[validation_status]"
         , s.addon_id AS "addons[id][0]"
         , s.item_quantity AS "addons[quantity][0]"
         , s.addon_unit_price_net AS "addons[unit_price][0]"
    FROM unique_subscriptions s
             JOIN public.contract c
                  ON s.shop_contract_id = c.outbound_id
             JOIN public.customer cu
                  ON cu.id = c.customer_id
             LEFT JOIN (SELECT outbound_id AS contract_outbound_id
                             , count(coupon_code) AS cnt_coupon_records
                        FROM public.shop_extraction_order_invoice
                        GROUP BY outbound_id
                        HAVING count(coupon_code)>0) contract_with_min_one_coupon_ever
                       ON contract_with_min_one_coupon_ever.contract_outbound_id = s.shop_contract_id
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
    WHERE NOT EXISTS (SELECT 1
                      FROM public.shop_extraction_order_invoice i
                               LEFT JOIN public.payment p
                                         ON p.invoice_id = i.id
                      WHERE c.id = i.contract_id
                        AND ((i.creator = 'urn:vimcar::service/new-user-checkout' AND i.modifier = 'urn:vimcar::service/new-user-checkout')
                          OR (i.creator = 'urn:vimcar::service/new-user-checkout' AND i.modifier LIKE 'urn:vimcar:com.vimcar:user%' AND p.status <> 'authorised'))
        )
)
   , subscriptions_2 AS (
    SELECT
        "customer[email]"
         , shop_customer_id
         , "subscription[id]"
         , split_part("subscription[id]", '_',1)::TEXT AS "subscription[cf_vertragsnummer]"
         , "subscription[plan_id]"
         , CASE
               WHEN "subscription[plan_quantity]" > 0
                   THEN "subscription[plan_quantity]"
               ELSE NULL
        END AS "subscription[plan_quantity]"
         , "subscription[plan_unit_price]"
         , "subscription[setup_fee]"
         , CASE
               WHEN "subscription[plan_quantity]" = 0
                   AND "subscription[status]" = 'active'
                   THEN 'non-renewing'
               ELSE "subscription[status]"
        END AS "subscription[status]"
         , "subscription[start_date]"
         , "subscription[trial_start]"
         , "subscription[trial_end]"
         , LEFT(("subscription[started_at]")::TEXT, 19) AS "subscription[started_at]"
         , LEFT(("subscription[current_term_start]")::TEXT, 19) AS "subscription[current_term_start]"
         , LEFT(("subscription[current_term_end]")::TEXT, 19) AS "subscription[current_term_end]"
         , LEFT("contract term end"::TEXT, 19) AS "contract term end"
         , LEFT(CASE
                    WHEN "subscription[cancelled_at]" IS NOT NULL
                        THEN "subscription[cancelled_at]"
                    WHEN "subscription[status]" <> 'active'
                        OR ("subscription[plan_quantity]" = 0 AND "subscription[status]" = 'active')
                        THEN coalesce("subscription[current_term_start]","subscription[started_at]") + interval '1 day'
                    END::TEXT, 19) AS "subscription[cancelled_at]"
         , "subscription[pause_date]"
         , "subscription[resume_date]"
         , cf_lifetime_licence
         , CASE
               WHEN "subscription[current_term_end]" < now() AT TIME ZONE 'Europe/Berlin'  -- for non-active contracts
                   OR "subscription[plan_quantity]" = 0                                    -- for plans which ended already
                   THEN 1

               WHEN "subscription[plan_id]" ILIKE '%month%'                   -- for monthly payments
                   THEN extract(year from age(date_trunc('month', "contract term end"), date_trunc('month', current_date))) * 12
                   + extract(month from age(date_trunc('month',"contract term end"), date_trunc('month', current_date)))

               ELSE extract(year from age(date_trunc('year', "contract term end"), date_trunc('year', current_date)))
        END::INT AS billing_cycles  -- CHECK! remaining billing cycles

         , CASE
               WHEN "subscription[plan_quantity]" > 0
                   AND "subscription[status]" = 'active'
                   THEN "subscription[contract_term_billing_cycle_on_renewal]"
               ELSE 0
        END AS "subscription[contract_term_billing_cycle_on_renewal]"
         , total_amount_raised
         , "subscription[auto_collection]"
         , "subscription[po_number]"
         , "coupon_ids[0]"
         , "coupon_ids[1]"
         , "subscription[payment_source_id]"
         , "subscription[invoice_notes]"
         , "subscription[meta_data]"
         , "shipping_address[first_name]"
         , "shipping_address[last_name]"
         , "shipping_address[email]"
         , "shipping_address[company]"
         , "shipping_address[phone]"
         , "shipping_address[line1]"
         , "shipping_address[line2]"
         , "shipping_address[line3]"
         , "shipping_address[city]"
         , "shipping_address[state_code]"
         , "shipping_address[state]"
         , "shipping_address[zip]"
         , "shipping_address[country]"
         , "shipping_address[validation_status]"
    FROM subscriptions_1
)
SELECT * FROM subscriptions_2 WHERE "subscription[plan_id]" NOT LIKE 'hardware%'
UNION ALL
SELECT DISTINCT * FROM subscriptions_2 WHERE "subscription[plan_id]" LIKE 'hardware%'
;