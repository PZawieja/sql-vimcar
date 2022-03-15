WITH customers_raw AS
         (SELECT
--  outbound_id AS customer_id, -- FRESHSALES Freshsales contact id - match via contact email
              btrim(left(regexp_replace(contact ->> 'first_name', '[\t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g'), 64)) AS "customer[first_name]"
               , btrim(left(regexp_replace(contact ->> 'last_name', '[\t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g'), 64)) AS "customer[last_name]"
               , COALESCE(
                     CASE
                         WHEN contact ->> 'phone_number' !~ '[0-9]' THEN NULL
                         ELSE substring(contact ->> 'phone_number' from '#"[0-9[:space:]+.\-]*#"' for '#')
                         END,
                     CASE
                         WHEN contact ->> 'mobile_phone_number' !~ '[0-9]' THEN NULL
                         ELSE substring(contact ->> 'mobile_phone_number' from '#"[0-9[:space:]+.\-]*#"' for '#')
                         END, NULL)::TEXT AS "customer[phone]"
               , btrim(regexp_replace(contact ->> 'company_name', '[\t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g')) AS "customer[company]"
               , btrim(lower(regexp_replace(contact ->> 'email', '[ \t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g'))) AS "customer[email]"
               , CASE
                     WHEN contact ->> 'gender' = 'unknown'
                         THEN 'else'
                     ELSE contact ->> 'gender'
                 END AS "cf_gender_of_the_customer"
               , outbound_id::TEXT AS "cf_kundennummer"
               , lower(trim(TRAILING '.' FROM cc.domain))::TEXT AS "cf_shop_domain"
               , CASE
                     WHEN rp.payment_method = 'invoice'
                         THEN 'bank_transfer'
                     WHEN rp.payment_method = 'paypal'
                         THEN NULL
                     ELSE rp.payment_method
                 END AS "payment_method[type]" --payment method (card, sepa, bank transfer, paypal
               ,
              CASE
                  WHEN rp.payment_method = 'invoice'
                      THEN 'bank_transfer'
                  WHEN rp.payment_method IN ('sepa', 'card')
                      THEN 'adyen'
                  WHEN rp.payment_method = 'paypal'
                      THEN NULL
                  END AS "payment_method[gateway_account_id]"  -- payment provider
               ,
              CASE
                  WHEN rp.provider = 'adyen'
                      AND rp.payment_method IN ('sepa', 'card') -- Adyen, see logic for the field "payment_method[gateway_account_id]" above
                      THEN adyen_shopper_refenece
                  END AS "payment_method[reference_id]"  -- payment profile ID -> Adyen, example B-123567890
               ,
              CASE WHEN rp.payment_method IN ('invoice','paypal')
                  OR rp.payment_method IS NULL
                       THEN 'off'
                   ELSE 'on'
                  END AS "customer[auto_collection]"  --on when recurring payment method, off when payment by wire transfer
               , 'taxable' AS "customer[taxability]"
               , 'EUR' AS "customer[preferred_currency_code]"
               , 0 AS "customer[net_term_days]"
               , CASE
                     WHEN rp.payment_method = 'sepa'
                         THEN TRUE
                     ELSE FALSE
                 END AS allow_direct_debit
               , btrim(regexp_replace(coalesce(billing_contact ->> 'first_name',
                                               contact ->> 'first_name', i.invoice_billing_contact ->> 'first_name'), '[\t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g')) AS "billing_address[first_name]"
               , btrim(regexp_replace(coalesce(billing_contact ->> 'last_name',
                                               contact ->> 'last_name', i.invoice_billing_contact ->> 'last_name'), '[\t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g')) AS "billing_address[last_name]"
               , btrim(regexp_replace(coalesce(billing_contact ->> 'email',
                                               contact ->> 'email', i.invoice_billing_contact ->> 'email'), '[\t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g')) AS "billing_address[email]"
               , btrim(regexp_replace(coalesce(billing_contact ->> 'company_name',
                                               contact ->> 'company_name', i.invoice_billing_contact ->> 'company_name'), '[\t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g')) AS "billing_address[company]"
               , COALESCE(
                     CASE
                         WHEN coalesce(billing_contact ->> 'phone_number',contact ->> 'phone_number',i.invoice_billing_contact ->> 'phone_number' ) !~ '[0-9]' THEN NULL
                         ELSE substring(coalesce(billing_contact ->> 'phone_number',contact ->> 'phone_number', i.invoice_billing_contact ->> 'phone_number') from '#"[0-9[:space:]+.\-]*#"' for '#')
                         END,
                     CASE
                         WHEN coalesce(billing_contact ->> 'mobile_phone_number',contact ->> 'mobile_phone_number',i.invoice_billing_contact ->> 'mobile_phone_number') !~ '[0-9]' THEN NULL
                         ELSE substring(coalesce(billing_contact ->> 'mobile_phone_number',contact ->> 'mobile_phone_number', i.invoice_billing_contact ->> 'mobile_phone_number') from '#"[0-9[:space:]+.\-]*#"' for '#')
                         END) AS "billing_address[phone]"
               , btrim(nullif(regexp_replace(coalesce(billing_contact -> 'address' ->> 'street_address',
                                                      contact -> 'address' ->> 'street_address', i.invoice_billing_contact -> 'address' ->> 'street_address'), '[\t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g'), 'n/a')) AS billing_address_raw
               , btrim(nullif(regexp_replace(coalesce(billing_contact -> 'address' ->> 'locality',
                                                      contact -> 'address' ->> 'locality', i.invoice_billing_contact -> 'address' ->> 'locality'), '[\t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g'), 'n/a')) AS "billing_address[city]"
               , to_char(btrim(
                                CASE
                                   WHEN coalesce(billing_contact -> 'address' ->> 'postal_code',contact -> 'address' ->> 'postal_code', i.invoice_billing_contact -> 'address' ->> 'postal_code') !~ '[0-9]' THEN NULL
                                   WHEN length(coalesce(billing_contact -> 'address' ->> 'postal_code',contact -> 'address' ->> 'postal_code', i.invoice_billing_contact -> 'address' ->> 'postal_code')) > 16 THEN NULL
                                   ELSE regexp_replace(coalesce(billing_contact -> 'address' ->> 'postal_code',contact -> 'address' ->> 'postal_code', i.invoice_billing_contact -> 'address' ->> 'postal_code'),'[\t\n\r\s\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g')
                                END)::INT,'fm00000') AS "billing_address[zip]"  -- zip codes shorter than 5 digits should have leading 0s (example: K82787217)
                 , btrim(nullif(regexp_replace(coalesce(billing_contact -> 'address' ->> 'country',
                                                      contact -> 'address' ->> 'country', i.invoice_billing_contact -> 'address' ->> 'country'), '[\t\n\r]*', '', 'g'), 'n/a')) AS "billing_address[country]"
               , cf_annual_calendar_billing_date
          FROM public.customer cc
                   JOIN public.shop_extraction_current_batch b  -- join batch table
                        ON cc.outbound_id = b.customer_id
              --         JOIN (SELECT DISTINCT customer_id
--               FROM public.shop_extraction_batch
--               WHERE coalesce(comment,'dummy') = 'Not sent for migration - multiple customers with the same email address.') b2  -- exclude problematic customers from the batch
--              ON b2.customer_id = b.customer_id
                   LEFT JOIN (SELECT DISTINCT ON (oi.customer_id)
                                  oi.customer_id
                                                                , oi.billing_contact AS invoice_billing_contact
                                                                , oi.shipping_contact AS invoice_shipping_contact
                              FROM shop_extraction_order_invoice oi
                                       JOIN customer c
                                            ON c.id = oi.customer_id
                                       JOIN shop_extraction_current_batch secb
                                            ON c.outbound_id = secb.customer_id
                              ORDER BY oi.customer_id, oi.created_at DESC) i
                             ON i.customer_id = cc.id

                   LEFT JOIN (SELECT
                                  domain
                                   , provider
                                   , payment_method
                                   , adyen_shopper_refenece
                              FROM (SELECT p.domain
                                         , row_number() OVER (PARTITION BY p.domain ORDER BY p.authorized DESC) AS row_nbr
                                         , p.payment_method ->> 'type' AS payment_method
                                         , p.provider
--                                          , an.payload -> 'notificationItems' -> 0 -> 'NotificationRequestItem' -> 'additionalData'  ->> 'shopperReference'||'/'|| p.psp_reference_id AS adyen_shopper_refenece -- OLD, before 20220110
                                         , an.payload -> 'notificationItems' -> 0 -> 'NotificationRequestItem' -> 'additionalData'  ->> 'shopperReference' AS adyen_shopper_refenece
                                    FROM public.payment p
                                             LEFT JOIN public.adyen_notification an
                                                       ON p.psp_reference_id = an.payload -> 'notificationItems' -> 0 -> 'NotificationRequestItem' ->> 'pspReference'
                                    WHERE status = 'authorised') recent_payments
                              WHERE row_nbr = 1) rp
                             ON rp.domain = cc.domain

                   LEFT JOIN (SELECT customer_id AS customer_uuid
                                   , to_char(MIN(started AT TIME ZONE 'Europe/Berlin')::DATE, 'YYYY-MM-DD HH24:MI:SS') AS cf_annual_calendar_billing_date
                              FROM contract
                              GROUP BY customer_id) ctr
                             ON ctr.customer_uuid = cc.id
          WHERE cc.contact ->> 'email' NOT LIKE '%vimcar.com'
            AND coalesce(b.comment,'dummy') NOT IN ('Excluded from migration: Ghost customer or internal Vimcar email address.')
--     AND outbound_id IN ('K15710634')
         )
-- SELECT * FROM customers_raw;
SELECT
    "customer[first_name]"
     , "customer[last_name]"
     , "customer[phone]"
--      , replace("customer[company]",',','.') AS "customer[company]" -- uncomment
     , "customer[company]" -- delete
     , split_part("customer[email]",',',1) AS "customer[email]"
     , cf_gender_of_the_customer
     , cf_kundennummer
     , cf_shop_domain
     , "payment_method[type]"
     , "payment_method[gateway_account_id]"
     , "payment_method[reference_id]"
     , "customer[auto_collection]"
     , "customer[taxability]"
     , NULL AS "customer[vat_number]"
     , "customer[preferred_currency_code]"
     , "customer[net_term_days]"
     , allow_direct_debit
     , 'de' AS "locale"
     , NULL AS "customer[meta_data]"
     , NULL AS "customer[consolidated_invoicing]"
     , NULL AS "customer[invoice_notes]"
     , "billing_address[first_name]"
     , "billing_address[last_name]"
     , split_part("billing_address[email]",',',1) AS "billing_address[email]"
     , "billing_address[company]"
     , "billing_address[phone]"
     , CASE
           WHEN length(substring(billing_address_raw from '^.*?(?=[0-9]|$)')) = 0
               THEN billing_address_raw
           ELSE substring(billing_address_raw from '^.*?(?=[0-9]|$)')
    END  AS "billing_address[line1]" -- street
     , CASE
           WHEN length(substring(billing_address_raw from '^.*?(?=[0-9]|$)')) = 0
               THEN NULL
           ELSE substring(billing_address_raw from '[0-9].*$')
    END  AS "billing_address[line2]" -- house number
     , NULL AS "billing_address[line3]" -- original address (street and number)
     , "billing_address[city]"
     , NULL AS "billing_address[state_code]"
     , NULL AS "billing_address[state]"
     , "billing_address[zip]"
     , "billing_address[country]"
     , 'not_validated' AS "billing_address[validation_status]"
     , NULL AS "customer[registered_for_gst]"
     , NULL AS "customer[entity_code]"
     , NULL AS "customer[exempt_number]"
    , cf_annual_calendar_billing_date -- this should be uncommented for the ACTIVE customers!!
FROM customers_raw
-- WHERE cf_kundennummer IN ('K80460642','K28546759','K68785204','K24606900','69840691','K34971666','K28292707','K83098180') -- examples for address parsing
-- WHERE "billing_address[line1]" IS NULL
;


WITH cte_contract_product AS (
    SELECT
        cco.outbound_id::VARCHAR(9) AS shop_contract_id
         , coalesce(pmap.cb_plan_id, CASE WHEN items.product_id = 'fake' THEN '00000000-0000-0000-0000-000000000000' ELSE items.product_id END) AS plan_id
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
             JOIN public.shop_extraction_current_batch b  -- join batch table
                  ON cc.outbound_id = b.customer_id
                      AND coalesce(b.comment,'dummy') NOT IN ('Excluded from migration: Ghost customer or internal Vimcar email address.')
       , LATERAL (SELECT (obj.value ->> 'product_id') AS product_id,
                      (obj.value -> 'price' -> 'net' ->> 'amount')::INT AS item_net_unit_price_amount,
                      (obj.value ->> 'quantity')::SMALLINT AS item_quantity
                  FROM jsonb_array_elements(cco.items) obj(value)) items
             JOIN mapping.umd_product_map pmap
                  ON pmap.product_uuid = CASE items.product_id WHEN 'fake' THEN '00000000-0000-0000-0000-000000000000' ELSE items.product_id END::UUID
             LEFT JOIN public.shop_extraction_cb_plan_id_map cbmap
                       ON pmap.cb_plan_id = cbmap.cb_plan_id
    WHERE cc.contact ->> 'email' NOT LIKE '%vimcar.com'
--       and cc.outbound_id = 'K30846175'
    GROUP BY cco.outbound_id, pmap.cb_plan_id, cbmap.cb_plan_id_map, cbmap.cb_plan_id_map_nbr, items.product_id, item_net_unit_price_amount
           , pmap.cb_addon_id, pmap.cb_addon_price_net, pmap.monthly_payment
)
   , cte_invoice_product AS (
    SELECT DISTINCT
        ci.contract_outbound_id::VARCHAR(9) AS shop_contract_id
                  , coalesce(pmap.cb_plan_id, CASE WHEN items_added.product_id = 'fake' THEN '00000000-0000-0000-0000-000000000000' ELSE items_added.product_id END) AS plan_id
                  , cbmap.cb_plan_id_map
                  , cbmap.cb_plan_id_map_nbr
                  , item_net_unit_price_amount AS unit_price_net
                  , pmap.cb_addon_id
                  , pmap.cb_addon_price_net
                  , pmap.monthly_payment
    FROM public.shop_extraction_order_invoice ci
             JOIN public.customer cc
                  ON cc.id = ci.customer_id
             JOIN public.shop_extraction_current_batch b  -- join batch table
                  ON cc.outbound_id = b.customer_id
                      AND coalesce(b.comment,'dummy') NOT IN ('Excluded from migration: Ghost customer or internal Vimcar email address.')
--                               JOIN (SELECT DISTINCT customer_id
--                                     FROM public.shop_extraction_batch
--                                     WHERE coalesce(comment,'dummy') <> 'Not sent for migration - multiple customers with the same email address.') b2  -- excluding the problematic customer from the batch
--                                    ON b2.customer_id = cc.outbound_id
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
--                         AND cc.outbound_id = 'K67710870' -- there is no cancellation date for contract V74285730, must be populated in SQL
--                         and cc.outbound_id = 'K91452842' -- there is a discount 10% ot the contract level but coupon is missing, dummy must be created
--                             AND cc.outbound_id = '72006772' -- the calculated contract term end date should be 17 Nov'21
--       and cc.outbound_id = 'K30846175'
)
   , cte_inv_ctr_prod AS (
    SELECT i.*, coalesce(c.item_quantity,0) AS item_quantity
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
         , c.started AT TIME ZONE 'Europe/Berlin' AS "subscription[started_at]"
         , c.provisioning_period_start AT TIME ZONE 'Europe/Berlin' AS "subscription[current_term_start]"
         , c.provisioning_period_end AT TIME ZONE 'Europe/Berlin' AS "subscription[current_term_end]"
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
                        END AT TIME ZONE 'Europe/Berlin') AS "contract term end"
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
                      WHEN length(coalesce(shipping_contact -> 'address' ->> 'postal_code',contact -> 'address' ->> 'postal_code')) > 16 THEN NULL
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
--    SELECT * FROM subscriptions_1;
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
               ELSE NULL
        END AS "subscription[plan_quantity]"
         , "subscription[plan_unit_price]"
         , "subscription[setup_fee]"
         , CASE
               WHEN "subscription[plan_quantity]" = 0
                   AND "subscription[status]" = 'active'
                   THEN 'non_renewing'
               WHEN "subscription[status]" = 'non_renewing'
                   AND "contract term end" <= current_date
                   THEN 'cancelled'
               ELSE "subscription[status]"
        END AS "subscription[status]"
         , "subscription[start_date]"
         , "subscription[trial_start]"
         , "subscription[trial_end]"
         , to_char("subscription[started_at]", 'YYYY-MM-DD HH24:MI:SS') AS "subscription[started_at]"
         , to_char("subscription[current_term_start]", 'YYYY-MM-DD HH24:MI:SS') AS "subscription[current_term_start]"
         , to_char("subscription[current_term_end]", 'YYYY-MM-DD HH24:MI:SS') AS "subscription[current_term_end]"
         , to_char(CASE
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
--          , CASE
--                WHEN "subscription[current_term_end]" < now() AT TIME ZONE 'Europe/Berlin'  -- for non-active contracts
--                    OR "subscription[plan_quantity]" = 0                                    -- for plans which ended already
--                    OR "subscription[contract_term_billing_cycle_on_renewal]" = 1           -- for annual payments
--                    THEN 1
--                WHEN "subscription[contract_term_billing_cycle_on_renewal]" = 12            -- for monthly payments
--                    THEN 12 -
--                         (extract(year from age(date_trunc('month', "contract term end"), date_trunc('month', current_date))) * 12
--                             + extract(month from age(date_trunc('month',"contract term end"), date_trunc('month', current_date))))
--         END AS billing_cycles  -- CHECK! remaining billing cycles (OLD logic)

         , CASE
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
         , CASE
               WHEN "subscription[plan_quantity]" > 0
                   AND "subscription[status]" = 'active'
                   THEN TRUE
        END AS "create_current_term_invoice"  -- CHECK!
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
    FROM subscriptions_1
-- WHERE shop_contract_id = 'V48519719'
)
SELECT * FROM subscriptions_2 WHERE "subscription[plan_id]" NOT LIKE 'hardware%'
-- As agreed with Anne and CB on 20220110 we stop sending hardware plans for migration, therefore the 2 lines below are obsolete
--UNION ALL
--SELECT DISTINCT * FROM subscriptions_2 WHERE "subscription[plan_id]" LIKE 'hardware%'
;




WITH cte_contract_product AS (
    SELECT
        cco.outbound_id::VARCHAR(9) AS shop_contract_id
         , coalesce(pmap.cb_plan_id, CASE WHEN items.product_id = 'fake' THEN '00000000-0000-0000-0000-000000000000' ELSE items.product_id END) AS plan_id
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
             JOIN public.shop_extraction_current_batch b  -- join batch table
                  ON cc.outbound_id = b.customer_id
                      AND coalesce(b.comment,'dummy') NOT IN ('Excluded from migration: Ghost customer or internal Vimcar email address.')
       , LATERAL (SELECT (obj.value ->> 'product_id') AS product_id,
                      (obj.value -> 'price' -> 'net' ->> 'amount')::INT AS item_net_unit_price_amount,
                      (obj.value ->> 'quantity')::SMALLINT AS item_quantity
                  FROM jsonb_array_elements(cco.items) obj(value)) items
             JOIN mapping.umd_product_map pmap
                  ON pmap.product_uuid = CASE items.product_id WHEN 'fake' THEN '00000000-0000-0000-0000-000000000000' ELSE items.product_id END::UUID
             LEFT JOIN public.shop_extraction_cb_plan_id_map cbmap
                       ON pmap.cb_plan_id = cbmap.cb_plan_id
    WHERE cc.contact ->> 'email' NOT LIKE '%vimcar.com'
--      and cc.outbound_id = 'K30846175'
    GROUP BY cco.outbound_id, pmap.cb_plan_id, cbmap.cb_plan_id_map, cbmap.cb_plan_id_map_nbr, items.product_id, item_net_unit_price_amount
           , pmap.cb_addon_id, pmap.cb_addon_price_net, pmap.monthly_payment
)
   , cte_invoice_product AS (
    SELECT DISTINCT
        ci.contract_outbound_id::VARCHAR(9) AS shop_contract_id
                  , coalesce(pmap.cb_plan_id, CASE WHEN items_added.product_id = 'fake' THEN '00000000-0000-0000-0000-000000000000' ELSE items_added.product_id END) AS plan_id
                  , cbmap.cb_plan_id_map
                  , cbmap.cb_plan_id_map_nbr
                  , item_net_unit_price_amount AS unit_price_net
                  , pmap.cb_addon_id
                  , pmap.cb_addon_price_net
                  , pmap.monthly_payment
    FROM public.shop_extraction_order_invoice ci
             JOIN public.customer cc
                  ON cc.id = ci.customer_id
             JOIN public.shop_extraction_current_batch b  -- join batch table
                  ON cc.outbound_id = b.customer_id
                      AND coalesce(b.comment,'dummy') NOT IN ('Excluded from migration: Ghost customer or internal Vimcar email address.')
--                               JOIN (SELECT DISTINCT customer_id
--                                     FROM public.shop_extraction_batch
--                                     WHERE coalesce(comment,'dummy') <> 'Not sent for migration - multiple customers with the same email address.') b2  -- excluding the problematic customer from the batch
--                                    ON b2.customer_id = cc.outbound_id
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
--                         AND cc.outbound_id = 'K67710870' -- there is no cancellation date for contract V74285730, must be populated in SQL
--                         and cc.outbound_id = 'K91452842' -- there is a discount 10% ot the contract level but coupon is missing, dummy must be created
--                             AND cc.outbound_id = '72006772' -- the calculated contract term end date should be 17 Nov'21
--       and cc.outbound_id = 'K30846175'
)
   , cte_inv_ctr_prod AS (
    SELECT i.*, coalesce(c.item_quantity,0) AS item_quantity
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
         , lower(regexp_replace(contact ->> 'email', '[\t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g')) AS "customer[email]"
         , cu.outbound_id AS shop_customer_id
         , NULL AS "subscription[setup_fee]"
         , CASE
               WHEN s.addon_id IS NOT NULL THEN 'cancelled'
               WHEN s.plan_id IN ('logbook_b2c-from_rent2buy-eur', 'logbook_b2c-one_time_buy-eur') THEN 'cancelled'
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
                        END AT TIME ZONE 'Europe/Berlin') AS "contract term end"
         , c.canceled AT TIME ZONE 'Europe/Berlin' AS "subscription[cancelled_at]"
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
         , btrim(regexp_replace(coalesce(shipping_contact ->> 'first_name',contact ->> 'first_name'), '[\t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g')) AS "shipping_address[first_name]"
         , btrim(regexp_replace(coalesce(shipping_contact ->> 'last_name',contact ->> 'last_name'), '[\t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g')) AS "shipping_address[last_name]"
         , btrim(lower(regexp_replace(coalesce(shipping_contact ->> 'email',contact ->> 'email'), '[\t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g'))) AS "shipping_address[email]"
         , btrim(regexp_replace(coalesce(shipping_contact ->> 'company_name',contact ->> 'company_name'), '[\t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g')) AS "shipping_address[company]"
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
                      ELSE regexp_replace(coalesce(shipping_contact -> 'address' ->> 'postal_code',contact -> 'address' ->> 'postal_code'), '[\t\n\r\u00a0\u180e\u2007\u200b-\u200f\u202f\u2060\ufeff]*', '', 'g')
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
    SELECT
        "customer[email]"
         , shop_customer_id
         , "subscription[id]" -- old approach until Batch C
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
               WHEN "subscription[plan_quantity]" = 0
                   AND "subscription[status]" = 'active'
                   THEN 'non_renewing'
               WHEN "subscription[status]" = 'non_renewing'
                   AND "contract term end" <= current_date
                   THEN 'cancelled'
               ELSE "subscription[status]"
        END AS "subscription[status]"
         , "subscription[start_date]"
         , "subscription[trial_start]"
         , "subscription[trial_end]"
         , to_char(("subscription[started_at]"), 'YYYY-MM-DD HH24:MI:SS') AS "subscription[started_at]"
         , to_char(("subscription[current_term_start]"), 'YYYY-MM-DD HH24:MI:SS') AS "subscription[current_term_start]"
         , to_char(("subscription[current_term_end]"), 'YYYY-MM-DD HH24:MI:SS') AS "subscription[current_term_end]"
         , to_char("contract term end", 'YYYY-MM-DD HH24:MI:SS') AS "contract term end"
         , to_char(CASE
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
         , "subscription[pause_date]"
         , "subscription[resume_date]"
         , cf_lifetime_licence

         , CASE
               WHEN "contract term end" < now() AT TIME ZONE 'Europe/Berlin'  			  -- for non-active contracts
                   OR "subscription[plan_quantity]" = 0                                    -- for plans which ended already
                   OR "subscription[status]" = 'cancelled'
                   THEN 0

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
    FROM subscriptions_1
--WHERE shop_customer_id = 'K30846175'
)
SELECT * FROM subscriptions_2 WHERE "subscription[plan_id]" NOT LIKE 'hardware%'
-- As agreed with Anne and CB on 20220110 we stop sending hardware plans for migration, therefore the 2 lines below are obsolete
--UNION ALL
--SELECT DISTINCT * FROM subscriptions_2 WHERE "subscription[plan_id]" LIKE 'hardware%'
;
