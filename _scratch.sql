-- R35364007 multiple payments, the most  recent refused but the 1st one wws authorised
SELECT NULL AS test
FROM dwh_main.dim_cb_payment
WHERE coalesce(NULL, 'testt') NOT IN ('test')
LIMIT 1



WITH cte_payment AS (SELECT invoice_uuid,
                         payment_status,
                                 rank() over (PARTITION BY invoice_uuid ORDER BY payment_status) AS rank,  -- authorised payments first!
                         payment_modified_ts
                     FROM dwh_main.dim_payment
                     WHERE payment_status <> 'initial'),

-- SHOP Abgeltung:
     cte_abgeltung AS (
         SELECT DISTINCT invoice_key--, product_name, invoice_line_item_type
         FROM dwh_main.fact_invoice_line fil
                  JOIN dwh_main.dim_product p
                       ON fil.item_product_key = p.product_key
         WHERE fil.invoice_line_item_type = 'added' AND (p.product_name LIKE '%Abgeltung%' OR p.product_name LIKE '%Ersatz%')),

-- CB Abgeltung:
     cte_abgeltung2 AS (SELECT DISTINCT cb_inv_id--, product_name, cb_inv_line_item_amount
                        FROM dwh_main.fact_cb_invoice_line_item fil
                                 JOIN dwh_main.dim_cb_invoice i ON i.cb_inv_key = fil.cb_inv_key
                                 JOIN dwh_main.dim_combined_product cp
                                      ON cp.entity_id = fil.cb_inv_line_item_entity_id
                                          AND cp.entity_type = fil.cb_inv_line_item_entity_type
                        WHERE lower(cp.product_name) LIKE '%abgeltung%'
                           OR lower(cp.product_name) LIKE '%compensation%'
                           OR lower(cp.product_name) LIKE '%ersatz%'),

--Chargeback:
     cte_chargeback AS (SELECT DISTINCT invoice_id, transaction_type
                        FROM data_mart_mtool.mtool_adyen_outbound_chargeback
                        WHERE transaction_type = 'Chargeback'),

--Pipedrive:
     cte_pipedrive AS (SELECT DISTINCT ON (invoice_id) invoice_id, pd_deal_id, pd_deal_status
                       FROM dwh_main.dim_pd_deal
                       ORDER BY invoice_id, pd_deal_created_ts ASC),

-- Chargebee payments: there can be multiple payments for an invoice, example: cb_inv_id = '2021008094'
     cte_cb_payment AS (SELECT DISTINCT ON (pim.cb_inv_id) pim.cb_inv_id, pa.cb_payment_status
                        FROM dwh_main.dim_cb_payment_invoice_map pim
                                 JOIN dwh_main.dim_cb_payment pa ON pa.cb_payment_id = pim.cb_payment_id
                        ORDER BY pim.cb_inv_id, pa.cb_payment_ts DESC)
, cte_cashflow_report AS (
-- SELECT i.invoice_id, --CASE WHEN ad.invoice_id IS NOT NULL AND payment_status = 'authorised' AND pd.invoice_id IS NOT NULL THEN pd.pd_deal_id::VARCHAR ELSE i.invoice_id END AS invoice_id,
--     i.invoice_billing_country,
--     p.payment_status,
--     CASE WHEN i.invoice_billing_country = 'DE' THEN 'EUR' ELSE 'GBP' END AS currency,
--     (i.invoice_created_ts AT TIME ZONE 'Europe/Berlin')::DATE AS invoice_created_dt,
--     o.invoice_status,
--     pd.pd_deal_status,
--     ad.transaction_type,
--     CASE WHEN cmb.customer_is_fleet = TRUE THEN 'B2B'
--          ELSE 'B2C' END AS is_fleet,
--     CASE WHEN a.invoice_key IS NOT NULL THEN 'Abgeltungsrechnung' ELSE 'Lizenzrechnung' END AS product_licence,
--         i.invoice_total_price_gross_amount/100.0 AS invoice_total_price_gross_amount,
--         r.refund_gross_amount/100.0 AS refund_gross_amount,
--         (i.invoice_total_price_gross_amount - COALESCE(r.refund_gross_amount,0))/100.0 AS total_amount
-- FROM dwh_main.dim_invoice i
--          LEFT JOIN cte_payment p
--                    ON i.invoice_uuid = p.invoice_uuid
--          JOIN dwh_main.dim_order o
--               ON i.order_uuid = o.order_uuid
--          JOIN dwh_main.dim_customer cust
--               ON i.customer_uuid = cust.customer_uuid
--          LEFT JOIN dwh_main.dim_combined_customer cmb
--                    ON cust.customer_id = cmb.customer_id_shop
--          LEFT JOIN (SELECT invoice_id, sum(refund_gross_amount) AS refund_gross_amount
--                     FROM dwh_main.dim_refund
--                     GROUP BY invoice_id) r
--                    ON i.invoice_id = r.invoice_id
--          LEFT JOIN cte_abgeltung a
--                    ON i.invoice_key = a.invoice_key
--          LEFT JOIN cte_chargeback ad
--                    ON i.invoice_id = ad.invoice_id
--          LEFT JOIN cte_pipedrive pd
--                    ON i.invoice_id = pd.invoice_id
-- WHERE (i.invoice_created_ts AT TIME ZONE 'Europe/Berlin')::DATE >= '2020-01-01' --current_timestamp - INTERVAL '1 year'
--   AND (rank = 1 OR p.invoice_uuid IS NULL) --to include invoices without payment
--   AND i.invoice_billing_email NOT LIKE '%@vimcar.com'
--   AND ((substr(i.invoice_creator_urn, 23, 4) = 'user' OR i.invoice_creator_urn = 'urn:vimcar:com.vimcar:service/contract-renewal') OR ((substr(i.invoice_creator_urn, 23, 4) <> 'user' OR i.invoice_creator_urn <> 'urn:vimcar:com.vimcar:service/contract-renewal') AND payment_status NOT IN ('initial', 'refused', 'error'))) --Excluding Web Checkout
--   AND o.order_has_full_refund = FALSE
-- --   AND i.invoice_id = 'R35364007'
--
-- UNION ALL

SELECT inv.cb_inv_id
    /*CASE
        WHEN cbp.cb_payment_status = 'authorised'
                 AND pd.invoice_id IS NOT NULL
            THEN pd.pd_deal_id::VARCHAR
        ELSE inv.cb_inv_id
        END AS invoice_id*/
     , inv.cb_inv_billing_address_country
     , cbp.cb_payment_status
     , inv.cb_inv_currency_code
     , (inv.cb_inv_created_ts  AT TIME ZONE 'Europe/Berlin')::DATE AS invoice_created_dt
     , inv.cb_inv_status
     , pd.pd_deal_status
     , ad.transaction_type
     ,  CASE WHEN cust.customer_is_fleet = TRUE THEN 'B2B'
             ELSE 'B2C' END AS is_fleet
     ,  CASE WHEN ab.cb_inv_id IS NOT NULL THEN 'Abgeltungsrechnung' ELSE 'Lizenzrechnung' END AS product_licence
     , inv.cb_inv_total/100.0 AS invoice_total_price_gross_amount
     , cn.refund_gross_amount/100.0 AS refund_gross_amount
     , (inv.cb_inv_total - COALESCE(cn.refund_gross_amount,0))/100.0 AS total_amount
--, c.cb_cust_id
, ev.emp_generated_inv
, cb_payment_status
FROM dwh_main.dim_cb_invoice inv
         JOIN dwh_main.dim_cb_customer c
              ON c.cb_cust_id = inv.cb_inv_customer_id
         LEFT JOIN cte_cb_payment cbp
                   ON cbp.cb_inv_id = inv.cb_inv_id
         LEFT JOIN dwh_main.dim_combined_customer cust
                   ON inv.cb_inv_customer_id = cust.customer_id
         LEFT JOIN (SELECT cb_cn_reference_invoice_id, sum(cb_cn_total) AS refund_gross_amount
                    FROM dwh_main.dim_cb_credit_note
                    WHERE cb_cn_voided_ts IS NULL
                    GROUP BY cb_cn_reference_invoice_id) cn
                   ON cn.cb_cn_reference_invoice_id = inv.cb_inv_id
         LEFT JOIN (SELECT cb_event_type_id
                         , TRUE AS emp_generated_inv
                    FROM dwh_main.dim_cb_event
                    WHERE cb_event_type = 'invoice_generated') ev
                   ON ev.cb_event_type_id = inv.cb_inv_id
         LEFT JOIN cte_abgeltung2 ab
                   ON inv.cb_inv_id = ab.cb_inv_id
         LEFT JOIN cte_chargeback ad
                   ON inv.cb_inv_id = ad.invoice_id
         LEFT JOIN cte_pipedrive pd
                   ON inv.cb_inv_id = pd.invoice_id
WHERE 1=1
  AND (inv.cb_inv_created_ts AT TIME ZONE 'Europe/Berlin')::DATE >= '2020-01-01' --current_timestamp - INTERVAL '1 year'
  AND c.cb_cust_email NOT LIKE '%vimcar.com'
-- only include employees generated invoices:
  AND (ev.emp_generated_inv IS TRUE OR (emp_generated_inv IS NULL AND coalesce(cb_payment_status,'dummy') <> 'failure'))  -- in CB we have 3 statuses: success,in_progress,failure
-- exclude full refunds:
  AND ((inv.cb_inv_total - cn.refund_gross_amount) > 0 OR refund_gross_amount IS NULL)
)
SELECT * FROM cte_cashflow_report WHERE cb_inv_id = '2021005189' ;
SELECT ci.*
FROM dwh_main.dim_combined_invoice_line ci
    LEFT JOIN cte_cashflow_report cfr
        ON cfr.invoice_id = ci.invoice_id
WHERE ci.invoice_created_dt >= '2021-01-01'
AND cfr.invoice_id IS NULL -- to see the invoices from COMBINED INVOICE TABLE which are missing in CASHFLOW REPORT
AND ci.full_refund_flag = FALSE
AND ci.product_group <> 'Hardware'
AND currency_code = 'EUR'
AND ci.invoice_id NOT LIKE 'o-%'
AND ci.invoice_status <> 'voided'
AND ci.invoice_id = '2021005189'
;

SELECT cb_cn_voided_ts FROM dwh_main.dim_cb_credit_note WHERE cb_cn_reference_invoice_id = '2022-005400'
