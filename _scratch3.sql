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
                     WHEN rp.payment_method = 'card'
                         THEN 'card'
                     WHEN rp.payment_method = 'sepa'
                         THEN 'direct_debit'
                 END AS "payment_method[type]"
               ,
              CASE
                  WHEN rp.provider = 'adyen'
                      AND rp.payment_method ='sepa'
                      THEN 'gw_AzqNJTSewKCj11Lo6' -- static, agreed with Anne on 2022-03-23 (Slack https://vimcar.slack.com/archives/DUMQU3V2P/p1648042373784799)
--                    THEN adyen_gateway_id -- obsolete since 2022-03-23
                  END AS "payment_method[gateway_account_id]"
               ,
              CASE
                  WHEN rp.provider = 'adyen'
                      AND rp.payment_method = 'sepa'
                      THEN concat_ws('/', adyen_shopper_reference, adyen_recurring_reference)
                  END AS "payment_method[reference_id]"
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
                                     WHEN length(coalesce(billing_contact -> 'address' ->> 'postal_code',contact -> 'address' ->> 'postal_code', i.invoice_billing_contact -> 'address' ->> 'postal_code')) > 10 THEN NULL
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
                                   , adyen_shopper_reference
                                   , adyen_recurring_reference
--                                    , adyen_gateway_id
                              FROM (SELECT p.domain
                                         , row_number() OVER (PARTITION BY p.domain ORDER BY p.authorized DESC) AS row_nbr
                                         , p.payment_method ->> 'type' AS payment_method
                                         , p.provider
--                                          , an.payload -> 'notificationItems' -> 0 -> 'NotificationRequestItem' -> 'additionalData'  ->> 'shopperReference'||'/'|| ard.recurring_reference AS adyen_shopper_referece -- OLD, before 20220110
                                         , an.payload -> 'notificationItems' -> 0 -> 'NotificationRequestItem' -> 'additionalData'  ->> 'shopperReference' AS adyen_shopper_reference
                                         , ard.recurring_reference AS adyen_recurring_reference
--                                          , an.payload -> 'notificationItems' -> 0 -> 'NotificationRequestItem' ->> 'merchantAccountCode' AS adyen_gateway_id
                                    FROM public.payment p
                                             LEFT JOIN adyen_recurring_detail ard
                                                       ON ard.domain = p.domain
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
-- WHERE cf_kundennummer = 'K17774689' -- customer from batch E, good for checking the Adyen details
-- WHERE cf_kundennummer IN ('K80460642','K28546759','K68785204','K24606900','69840691','K34971666','K28292707','K83098180') -- examples for address parsing
-- WHERE "billing_address[line1]" IS NULL
;

SELECT
    domain
     , provider
     , payment_method
     , adyen_shopper_reference
     , adyen_recurring_reference
     , status
     , authorized
--                                    , adyen_gateway_id
FROM (SELECT p.domain
           , row_number() OVER (PARTITION BY p.domain ORDER BY p.authorized DESC) AS row_nbr
           , p.payment_method ->> 'type' AS payment_method
           , p.provider
--                                          , an.payload -> 'notificationItems' -> 0 -> 'NotificationRequestItem' -> 'additionalData'  ->> 'shopperReference'||'/'|| ard.recurring_reference AS adyen_shopper_referece -- OLD, before 20220110
           , an.payload -> 'notificationItems' -> 0 -> 'NotificationRequestItem' -> 'additionalData'  ->> 'shopperReference' AS adyen_shopper_reference
           , ard.recurring_reference AS adyen_recurring_reference
--                                          , an.payload -> 'notificationItems' -> 0 -> 'NotificationRequestItem' ->> 'merchantAccountCode' AS adyen_gateway_id
            , status
          , p.authorized
      FROM public.payment p
               LEFT JOIN adyen_recurring_detail ard
                         ON ard.domain = p.domain
               LEFT JOIN public.adyen_notification an
                         ON p.psp_reference_id = an.payload -> 'notificationItems' -> 0 -> 'NotificationRequestItem' ->> 'pspReference'
      WHERE p.domain = 'com.vimcar.de.k11640939.'

--     AND        status = 'authorised'
    ) recent_payments

SELECT
    domain
     , provider
     , payment_method
     , adyen_recurring_reference
     , coalesce(adyen_shopper_reference, any_adyen_shopper_reference) AS adyen_shopper_reference
--                                    , adyen_gateway_id
FROM (SELECT p.domain
           , row_number() OVER (PARTITION BY p.domain ORDER BY p.authorized DESC) AS row_nbr
           , p.payment_method ->> 'type' AS payment_method
           , p.provider
           , an.payload -> 'notificationItems' -> 0 -> 'NotificationRequestItem' -> 'additionalData'  ->> 'shopperReference' AS adyen_shopper_reference
           , MAX(an.payload -> 'notificationItems' -> 0 -> 'NotificationRequestItem' -> 'additionalData'  ->> 'shopperReference') OVER (PARTITION BY p.domain) AS any_adyen_shopper_reference -- example: domain = 'com.vimcar.de.k11640939.'
           , ard.recurring_reference AS adyen_recurring_reference
--                                          , an.payload -> 'notificationItems' -> 0 -> 'NotificationRequestItem' ->> 'merchantAccountCode' AS adyen_gateway_id
      FROM public.payment p
               LEFT JOIN adyen_recurring_detail ard
                         ON ard.domain = p.domain
               LEFT JOIN public.adyen_notification an
                         ON p.psp_reference_id = an.payload -> 'notificationItems' -> 0 -> 'NotificationRequestItem' ->> 'pspReference'
      WHERE status = 'authorised'
    ) recent_payments
WHERE row_nbr = 1;


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
                     WHEN rp.payment_method = 'card'
                         THEN 'card'
                     WHEN rp.payment_method = 'sepa'
                         THEN 'direct_debit'
                 END AS "payment_method[type]"
               ,
              CASE
                  WHEN rp.provider = 'adyen'
                      AND rp.payment_method ='sepa'
                      THEN 'gw_AzqNJTSewKCj11Lo6' -- static, agreed with Anne on 2022-03-23 (Slack https://vimcar.slack.com/archives/DUMQU3V2P/p1648042373784799)
--                    THEN adyen_gateway_id -- obsolete since 2022-03-23
                  END AS "payment_method[gateway_account_id]"
               ,
              CASE
                  WHEN rp.provider = 'adyen'
                      AND rp.payment_method = 'sepa'
                      THEN concat_ws('/', adyen_shopper_reference, adyen_recurring_reference)
                  END AS "payment_method[reference_id]"
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
                                     WHEN length(coalesce(billing_contact -> 'address' ->> 'postal_code',contact -> 'address' ->> 'postal_code', i.invoice_billing_contact -> 'address' ->> 'postal_code')) > 10 THEN NULL
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
                                   , adyen_recurring_reference
                                   , coalesce(adyen_shopper_reference, any_adyen_shopper_reference) AS adyen_shopper_reference
--                                    , adyen_gateway_id
                              FROM (SELECT p.domain
                                         , row_number() OVER (PARTITION BY p.domain ORDER BY p.authorized DESC) AS row_nbr
                                         , p.payment_method ->> 'type' AS payment_method
                                         , p.provider
                                         , an.payload -> 'notificationItems' -> 0 -> 'NotificationRequestItem' -> 'additionalData'  ->> 'shopperReference' AS adyen_shopper_reference
                                         , MAX(an.payload -> 'notificationItems' -> 0 -> 'NotificationRequestItem' -> 'additionalData'  ->> 'shopperReference') OVER (PARTITION BY p.domain) AS any_adyen_shopper_reference -- example: domain = 'com.vimcar.de.k11640939.'
                                         , ard.recurring_reference AS adyen_recurring_reference
--                                          , an.payload -> 'notificationItems' -> 0 -> 'NotificationRequestItem' ->> 'merchantAccountCode' AS adyen_gateway_id
                                    FROM public.payment p
                                             LEFT JOIN adyen_recurring_detail ard
                                                       ON ard.domain = p.domain
                                             LEFT JOIN public.adyen_notification an
                                                       ON p.psp_reference_id = an.payload -> 'notificationItems' -> 0 -> 'NotificationRequestItem' ->> 'pspReference'
                                    WHERE status = 'authorised'
                                   ) recent_payments
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
-- WHERE cf_kundennummer = 'K17774689' -- customer from batch E, good for checking the Adyen details
-- WHERE cf_kundennummer IN ('K80460642','K28546759','K68785204','K24606900','69840691','K34971666','K28292707','K83098180') -- examples for address parsing
-- WHERE "billing_address[line1]" IS NULL
;