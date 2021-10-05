WITH cte_tax_advisors_cp AS (
SELECT DISTINCT
    o.domain_name
    , lower(o.coupon_code)  AS stb_coupon
    ,'ta_coupon_used'       AS tax_advisor_flag_base
FROM dwh_main.dim_order o
     LEFT JOIN dwh_main.umd_mandator_coupon_code umcc ON lower(o.coupon_code) = lower(umcc.coupon_code) AND coupon_receiver_email IS NOT NULL
     LEFT JOIN (SELECT DISTINCT coupon_code FROM dwh_main.dim_coupon
                WHERE lower(coupon_code) LIKE '%\_cc\_%'
                    OR coupon_category IN (
                                            'BVBC',
                                            'Call Center StB',
                                            'DATEV Aussendienst',
                                            'DATEV Mandant',
                                            'DATEV StB',
                                            'datev_v50',
                                            'datevauto',
                                            'dstrauto',
                                            'DSTV Mandant',
                                            'DSTV StB',
                                            'dvag10',
                                            'dvag15',
                                            'Ecovis Mandant',
                                            'Ecovis StB',
                                            'ETL Mandant',
                                            'ETL StB',
                                            'HSP Mandant',
                                            'hsp_1hd1',
                                            'hsp_2bt1',
                                            'hsp_ddlx',
                                            'hsp_ds7i',
                                            'hsp_eusk',
                                            'hsp_g7q6',
                                            'hsp_partner_1863',
                                            'hsp_partner_2632',
                                            'hsp_vchf',
                                            'hsp_wjm9',
                                            'Lohnkonzept Mandant',
                                            'Lohnkonzept StB',
                                            'Lohnsteuer Mandant',
                                            'mandant',
                                            'stb_fleet',
                                            'steuerberater',
                                            'StPro',
                                            'WST Mandant',
                                            'WST StB'
                    )) cp ON lower(cp.coupon_code) = lower(o.coupon_code)
WHERE CASE WHEN umcc.coupon_code IS NOT NULL OR cp.coupon_code IS NOT NULL THEN TRUE END = TRUE
),
cte_tax_advisors AS (SELECT c.domain_name, NULL AS stb_coupon, 'word_in_company_name' AS tax_advisor_flag_base
                     FROM dwh_main.dim_customer c
                     WHERE (c.customer_contact_company_name LIKE '%StB%' OR
                            c.customer_contact_company_name LIKE '%Steuer%' OR
                            c.customer_contact_company_name LIKE '%Kanzlei%' OR
                            c.customer_contact_company_name LIKE '%Treuhand%' OR
                            c.customer_contact_company_name LIKE '%Wirtschaftsprüfung%' OR
                            c.customer_contact_company_name LIKE '%ETL%' OR
                            c.customer_contact_company_name LIKE '%HSP%' OR
                            c.customer_contact_company_name LIKE '%Ecovis%' OR
                            c.customer_contact_company_name LIKE '%& Partner%' OR
                            c.customer_contact_company_name LIKE '%und Partner%' OR
                            c.customer_contact_company_name LIKE '%+Partner%' OR
                            c.customer_contact_company_name LIKE '%Berata%' OR
                            c.customer_contact_company_name LIKE '%Tax %' OR
                            c.customer_contact_company_name LIKE '%Tax-%')
                       AND NOT EXISTS(SELECT domain_name
                                      FROM cte_tax_advisors_cp
                                      WHERE cte_tax_advisors_cp.domain_name = c.domain_name)
                     UNION
                     SELECT domain_name, stb_coupon, tax_advisor_flag_base
                     FROM cte_tax_advisors_cp
),
cte_revenue_raw AS (
    SELECT
     ctr.contract_id
     , ctr.contract_uuid
     , upm.product_name
     , upm.product_uuid
     , o.domain_name
     , o.order_uuid
     , o.order_id
     , o.order_created_ts::DATE                                     AS order_dt
     , o.order_discount_amount                                      AS discount_amount
     , coalesce(i.discount_percentage, o.order_discount_percentage) AS discount_percentage
     , lower(o.coupon_code)                                         AS order_coupon_code
     , o.order_has_full_refund
     , foi.order_item_total_price_net_amt
     , coalesce(refund_net_amount,0)                                AS refund_net_amount
     , upm.monthly_payment
--      , i.invoice_id
FROM dwh_main.fact_order_item foi
        JOIN dwh_main.dim_order o ON o.order_key = foi.order_key
        JOIN (SELECT DISTINCT domain_name FROM cte_tax_advisors) ta ON ta.domain_name = o.domain_name
        JOIN dwh_main.umd_product_map upm ON foi.order_item_product_key = upm.product_key
        JOIN dwh_main.dim_contract ctr ON ctr.contract_uuid = o.contract_uuid
        LEFT JOIN dwh_main.dim_invoice i ON i.order_uuid = o.order_uuid
        LEFT JOIN (SELECT invoice_id, SUM(refund_net_amount) AS refund_net_amount
                   FROM dwh_main.dim_refund
                   GROUP BY invoice_id) refund ON refund.invoice_id = i.invoice_id
WHERE o.order_was_draft = FALSE
    AND foi.order_item_total_price_net_amt > 0
    AND NOT (upm.is_recurring = FALSE AND upm.is_subsequent_recurring = FALSE)
--     AND ctr.contract_id = 'V53991486'
-- AND ctr.contract_id in ('V63270040')
--     AND o.contract_uuid = '45df84f6-06d3-41a1-9e20-ab163321d0c3'
),
cte_revenue_product_lvl AS (
SELECT
    cte_revenue_raw.*
    , coalesce(round(CASE WHEN monthly_payment = FALSE THEN
       (CASE
            WHEN order_has_full_refund = TRUE
                THEN 0
            WHEN discount_percentage >0
                THEN
                order_item_total_price_net_amt
                        * (1-discount_percentage)
                        - (coalesce(refund_net_amount,0) * ((order_item_total_price_net_amt / sum(order_item_total_price_net_amt) OVER (PARTITION BY order_uuid)::numeric)::numeric))
            WHEN discount_amount >0
                THEN order_item_total_price_net_amt
                        - (round((discount_amount  *  (order_item_total_price_net_amt / sum(order_item_total_price_net_amt) OVER (PARTITION BY order_uuid)::numeric) /1.19)))
                        - (coalesce(refund_net_amount,0) * ((order_item_total_price_net_amt / sum(order_item_total_price_net_amt) OVER (PARTITION BY order_uuid)::numeric)::numeric))
            ELSE order_item_total_price_net_amt
                        - (coalesce(refund_net_amount,0) * ((order_item_total_price_net_amt / sum(order_item_total_price_net_amt) OVER (PARTITION BY order_uuid)::numeric)::numeric))
        END) /12
     ELSE
       (CASE
            WHEN order_has_full_refund = TRUE
                THEN 0
            WHEN discount_percentage >0
                THEN
                order_item_total_price_net_amt
                        * (1-discount_percentage)
                        - (coalesce(refund_net_amount,0) * ((order_item_total_price_net_amt / sum(order_item_total_price_net_amt) OVER (PARTITION BY order_uuid)::numeric)::numeric))
            WHEN discount_amount >0
                THEN order_item_total_price_net_amt
                        - (round((discount_amount  *  (order_item_total_price_net_amt / sum(order_item_total_price_net_amt) OVER (PARTITION BY order_uuid)::numeric) /1.19)))
                        - (coalesce(refund_net_amount,0) * ((order_item_total_price_net_amt / sum(order_item_total_price_net_amt) OVER (PARTITION BY order_uuid)::numeric)::numeric))
            ELSE order_item_total_price_net_amt
                        - (coalesce(refund_net_amount,0) * ((order_item_total_price_net_amt / sum(order_item_total_price_net_amt) OVER (PARTITION BY order_uuid)::numeric)::numeric))
        END)
     END /100,2),0) AS mrr_product_lvl
FROM cte_revenue_raw
),
cte_revenue_order_lvl AS (
SELECT
    contract_uuid
    , contract_id
    , domain_name           AS order_domain
    , order_uuid
    , order_id
    , order_dt
    , order_coupon_code
    , SUM(mrr_product_lvl)  AS mrr_order_lvl
FROM cte_revenue_product_lvl
GROUP BY contract_uuid, contract_id, domain_name, order_uuid, order_id, order_dt, order_coupon_code
)
SELECT
  rev.order_domain
, ta.tax_advisor_flag_base
, c.customer_contact_company_name
, c.customer_contact_country
, c.customer_contact_region
, c.customer_contact_postal_code
-- , c.customer_contact_street_name
, rev.contract_uuid
, rev.contract_id
, rev.order_coupon_code AS coupon_code
, CASE WHEN rev.order_coupon_code = umcc.ta_coupon_code THEN TRUE ELSE FALSE END                        AS coupon_code_mandator
, CASE WHEN rev.order_coupon_code LIKE '%\_cc\_%' THEN TRUE ELSE FALSE END                              AS coupon_code_call_center
, CASE WHEN ta_cp.stb_coupon IS NOT NULL THEN TRUE ELSE FALSE END                                       AS coupon_code_stb_category
, CASE WHEN sum(CASE WHEN ta_cp.stb_coupon IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY rev.order_domain) > 0
    OR tax_advisor_flag_base = 'ta_coupon_used'  THEN TRUE ELSE FALSE END AS domain_ever_used_stb_coupon
, rev.order_dt
, rev.order_uuid
, rev.order_id
, rev.mrr_order_lvl                                                                                     AS mrr_order
, SUM(rev.mrr_order_lvl) OVER (PARTITION BY rev.order_domain) AS mrr_domain
, CASE WHEN rev.order_coupon_code LIKE '%\_cc\_%' THEN 'Avedo' ELSE umcc.coupon_receiver_name END       AS coupon_receiver_name
, CASE WHEN rev.order_coupon_code LIKE '%\_cc\_%' THEN 'Avedo' ELSE umcc.coupon_receiver_email END      AS coupon_receiver_email
, CASE WHEN rev.order_coupon_code LIKE '%\_cc\_%' THEN 'DE' ELSE ta_geo.ta_country END                  AS ta_country
, CASE WHEN rev.order_coupon_code LIKE '%\_cc\_%' THEN 'München' ELSE ta_geo.ta_region END              AS ta_region
, CASE WHEN rev.order_coupon_code LIKE '%\_cc\_%' THEN '81379' ELSE ta_geo.ta_postal_code END           AS ta_postal_code
FROM cte_revenue_order_lvl rev
    JOIN dwh_main.dim_customer c ON c.domain_name = rev.order_domain
    JOIN (SELECT DISTINCT domain_name, tax_advisor_flag_base FROM cte_tax_advisors) ta
        ON ta.domain_name = rev.order_domain
    LEFT JOIN (SELECT DISTINCT stb_coupon FROM cte_tax_advisors) ta_cp
        ON ta_cp.stb_coupon = rev.order_coupon_code
    LEFT JOIN (SELECT DISTINCT lower(coupon_code) AS ta_coupon_code, lower(trim(coupon_receiver_email)) AS coupon_receiver_email, upper(coupon_receiver_name) AS coupon_receiver_name
               FROM dwh_main.umd_mandator_coupon_code) umcc
        ON umcc.ta_coupon_code = rev.order_coupon_code
    LEFT JOIN (SELECT DISTINCT umcc2.coupon_receiver_email
                             , first_value(c2.customer_contact_country) OVER (PARTITION BY umcc2.coupon_receiver_email)     AS ta_country
                             , first_value(c2.customer_contact_region) OVER (PARTITION BY umcc2.coupon_receiver_email)      AS ta_region
                             , first_value(c2.customer_contact_postal_code) OVER (PARTITION BY umcc2.coupon_receiver_email) AS ta_postal_code
                            FROM (SELECT DISTINCT lower(trim(coupon_receiver_email)) AS coupon_receiver_email
                              FROM dwh_main.umd_mandator_coupon_code
                              WHERE coupon_receiver_email IS NOT NULL) umcc2
                              JOIN dwh_main.dim_customer c2 ON lower(trim(c2.customer_contact_email)) = umcc2.coupon_receiver_email
                                                                  AND c2.customer_contact_country IS NOT NULL
                                                                  AND c2.customer_contact_postal_code IS NOT NULL) ta_geo
        ON ta_geo.coupon_receiver_email = umcc.coupon_receiver_email
-- WHERE rev.order_domain = 'com.vimcar.de.12657774'
WHERE rev.order_domain ='com.vimcar.de.k22370589'
-- WHERE umcc.coupon_receiver_email = 'felix.mueller@nlg-steuer.de'
-- ORDER BY   rev.order_domain, rev.order_dt


;
------------------------------------------------------------------------------------------------------------------
WITH cte_tax_advisors_cp AS (
SELECT DISTINCT
    o.domain_name
    , lower(o.coupon_code)  AS stb_coupon
    ,'ta_coupon_used'       AS tax_advisor_flag_base
FROM dwh_main.dim_order o
     LEFT JOIN dwh_main.umd_mandator_coupon_code umcc ON lower(o.coupon_code) = lower(umcc.coupon_code) AND coupon_receiver_email IS NOT NULL
     LEFT JOIN (SELECT DISTINCT coupon_code FROM dwh_main.dim_coupon
                WHERE lower(coupon_code) LIKE '%\_cc\_%'
                    OR coupon_category IN (
                                            'BVBC',
                                            'Call Center StB',
                                            'DATEV Aussendienst',
                                            'DATEV Mandant',
                                            'DATEV StB',
                                            'datev_v50',
                                            'datevauto',
                                            'dstrauto',
                                            'DSTV Mandant',
                                            'DSTV StB',
                                            'dvag10',
                                            'dvag15',
                                            'Ecovis Mandant',
                                            'Ecovis StB',
                                            'ETL Mandant',
                                            'ETL StB',
                                            'HSP Mandant',
                                            'hsp_1hd1',
                                            'hsp_2bt1',
                                            'hsp_ddlx',
                                            'hsp_ds7i',
                                            'hsp_eusk',
                                            'hsp_g7q6',
                                            'hsp_partner_1863',
                                            'hsp_partner_2632',
                                            'hsp_vchf',
                                            'hsp_wjm9',
                                            'Lohnkonzept Mandant',
                                            'Lohnkonzept StB',
                                            'Lohnsteuer Mandant',
                                            'mandant',
                                            'stb_fleet',
                                            'steuerberater',
                                            'StPro',
                                            'WST Mandant',
                                            'WST StB'
                    )) cp ON lower(cp.coupon_code) = lower(o.coupon_code)
WHERE CASE WHEN umcc.coupon_code IS NOT NULL OR cp.coupon_code IS NOT NULL THEN TRUE END = TRUE
)
SELECT * FROM cte_tax_advisors_cp
WHERE domain_name ='com.vimcar.de.33847728'
;
SELECT * FROM dwh_main.dim_order o
LEFT join dwh_main.dim_invoice i ON i.order_uuid = o.order_uuid
WHERE o.domain_name ='com.vimcar.de.45201769' AND order_id = '45080024';

    SELECT
     ctr.contract_id
     , ctr.contract_uuid
     , upm.product_name
     , upm.product_uuid
     , o.domain_name
     , o.order_uuid
     , o.order_id
     , o.order_created_ts::DATE                                     AS order_dt
     , o.order_discount_amount                                      AS discount_amount
     , coalesce(i.discount_percentage, o.order_discount_percentage) AS discount_percentage
     , lower(o.coupon_code)                                         AS order_coupon_code
     , o.order_has_full_refund
     , foi.order_item_total_price_net_amt
     , coalesce(refund_net_amount,0)                                AS refund_net_amount
     , upm.monthly_payment
--      , i.invoice_id
FROM dwh_main.fact_order_item foi
        JOIN dwh_main.dim_order o ON o.order_key = foi.order_key
        JOIN dwh_main.umd_product_map upm ON foi.order_item_product_key = upm.product_key
        JOIN dwh_main.dim_contract ctr ON ctr.contract_uuid = o.contract_uuid
        LEFT JOIN dwh_main.dim_invoice i ON i.order_uuid = o.order_uuid
        LEFT JOIN (SELECT invoice_id, SUM(refund_net_amount) AS refund_net_amount
                   FROM dwh_main.dim_refund
                   GROUP BY invoice_id) refund ON refund.invoice_id = i.invoice_id
WHERE o.domain_name = 'com.vimcar.de.58528997'
-- AND o.order_was_draft = FALSE
--     AND foi.order_item_total_price_net_amt > 0
--     AND NOT (upm.is_recurring = FALSE AND upm.is_subsequent_recurring = FALSE)
;
SELECT coupon_code  FROM dwh_main.umd_mandator_coupon_code
-- WHERE coupon_code ILIKE '%etl_man_7935%'



WITH cte_tax_advisors_cp AS (
SELECT DISTINCT
    o.domain_name AS domain_name_ta
    ,'ta_coupon_used'       AS tax_advisor_flag_base
FROM dwh_main.dim_order o
     LEFT JOIN dwh_main.umd_mandator_coupon_code umcc ON lower(o.coupon_code) = lower(umcc.coupon_code) AND coupon_receiver_email IS NOT NULL
     LEFT JOIN (SELECT DISTINCT coupon_code FROM dwh_main.dim_coupon
                WHERE lower(coupon_code) LIKE '%\_cc\_%'
                    OR coupon_category IN (
                                            'BVBC',
                                            'Call Center StB',
                                            'DATEV Aussendienst',
                                            'DATEV Mandant',
                                            'DATEV StB',
                                            'datev_v50',
                                            'datevauto',
                                            'dstrauto',
                                            'DSTV Mandant',
                                            'DSTV StB',
                                            'dvag10',
                                            'dvag15',
                                            'Ecovis Mandant',
                                            'Ecovis StB',
                                            'ETL Mandant',
                                            'ETL StB',
                                            'HSP Mandant',
                                            'hsp_1hd1',
                                            'hsp_2bt1',
                                            'hsp_ddlx',
                                            'hsp_ds7i',
                                            'hsp_eusk',
                                            'hsp_g7q6',
                                            'hsp_partner_1863',
                                            'hsp_partner_2632',
                                            'hsp_vchf',
                                            'hsp_wjm9',
                                            'Lohnkonzept Mandant',
                                            'Lohnkonzept StB',
                                            'Lohnsteuer Mandant',
                                            'mandant',
                                            'stb_fleet',
                                            'steuerberater',
                                            'StPro',
                                            'WST Mandant',
                                            'WST StB'
                    )) cp ON lower(cp.coupon_code) = lower(o.coupon_code)
WHERE CASE WHEN umcc.coupon_code IS NOT NULL OR cp.coupon_code IS NOT NULL THEN TRUE END = TRUE
)
SELECT c.domain_name AS domain_name_ta, 'word_in_company_name' AS tax_advisor_flag_base
                     FROM dwh_main.dim_customer c
                     WHERE (c.customer_contact_company_name LIKE '%StB%' OR
                            c.customer_contact_company_name LIKE '%Steuer%' OR
                            c.customer_contact_company_name LIKE '%Kanzlei%' OR
                            c.customer_contact_company_name LIKE '%Treuhand%' OR
                            c.customer_contact_company_name LIKE '%Wirtschaftsprüfung%' OR
                            c.customer_contact_company_name LIKE '%ETL%' OR
                            c.customer_contact_company_name LIKE '%HSP%' OR
                            c.customer_contact_company_name LIKE '%Ecovis%' OR
                            c.customer_contact_company_name LIKE '%& Partner%' OR
                            c.customer_contact_company_name LIKE '%und Partner%' OR
                            c.customer_contact_company_name LIKE '%+Partner%' OR
                            c.customer_contact_company_name LIKE '%Berata%' OR
                            c.customer_contact_company_name LIKE '%Tax %' OR
                            c.customer_contact_company_name LIKE '%Tax-%')
                       AND NOT EXISTS(SELECT domain_name_ta
                                      FROM cte_tax_advisors_cp
                                      WHERE cte_tax_advisors_cp.domain_name_ta = c.domain_name )
                     UNION
                     SELECT domain_name_ta, tax_advisor_flag_base
                     FROM cte_tax_advisors_cp

WITH cte AS (
SELECT DISTINCT lower(coupon_code) AS coupon_code FROM dwh_main.dim_coupon
                WHERE lower(coupon_code) LIKE '%\_cc\_%'
                    OR coupon_category IN (
                                            'BVBC',
                                            'Call Center StB',
                                            'DATEV Aussendienst',
                                            'DATEV Mandant',
                                            'DATEV StB',
                                            'datev_v50',
                                            'datevauto',
                                            'dstrauto',
                                            'DSTV Mandant',
                                            'DSTV StB',
                                            'dvag10',
                                            'dvag15',
                                            'Ecovis Mandant',
                                            'Ecovis StB',
                                            'ETL Mandant',
                                            'ETL StB',
                                            'HSP Mandant',
                                            'hsp_1hd1',
                                            'hsp_2bt1',
                                            'hsp_ddlx',
                                            'hsp_ds7i',
                                            'hsp_eusk',
                                            'hsp_g7q6',
                                            'hsp_partner_1863',
                                            'hsp_partner_2632',
                                            'hsp_vchf',
                                            'hsp_wjm9',
                                            'Lohnkonzept Mandant',
                                            'Lohnkonzept StB',
                                            'Lohnsteuer Mandant',
                                            'mandant',
                                            'stb_fleet',
                                            'steuerberater',
                                            'StPro',
                                            'WST Mandant',
                                            'WST StB')
)
SELECT count(*), count(DISTINCT coupon_code) FROM cte;


WITH cte AS (
SELECT DISTINCT lower(coupon_code) AS coupon_code FROM dwh_main.dim_coupon
                WHERE lower(coupon_code) LIKE '%\_cc\_%'
                    OR coupon_category IN (
                                            'BVBC',
                                            'Call Center StB',
                                            'DATEV Aussendienst',
                                            'DATEV Mandant',
                                            'DATEV StB',
                                            'datev_v50',
                                            'datevauto',
                                            'dstrauto',
                                            'DSTV Mandant',
                                            'DSTV StB',
                                            'dvag10',
                                            'dvag15',
                                            'Ecovis Mandant',
                                            'Ecovis StB',
                                            'ETL Mandant',
                                            'ETL StB',
                                            'HSP Mandant',
                                            'hsp_1hd1',
                                            'hsp_2bt1',
                                            'hsp_ddlx',
                                            'hsp_ds7i',
                                            'hsp_eusk',
                                            'hsp_g7q6',
                                            'hsp_partner_1863',
                                            'hsp_partner_2632',
                                            'hsp_vchf',
                                            'hsp_wjm9',
                                            'Lohnkonzept Mandant',
                                            'Lohnkonzept StB',
                                            'Lohnsteuer Mandant',
                                            'mandant',
                                            'stb_fleet',
                                            'steuerberater',
                                            'StPro',
                                            'WST Mandant',
                                            'WST StB')
UNION
SELECT lower(coupon_code) AS coupon_code FROM dwh_main.umd_mandator_coupon_code
)
SELECT count(*), count(DISTINCT coupon_code) FROM cte;

SELECT lower(coupon_code) FROM dwh_main.umd_mandator_coupon_code
