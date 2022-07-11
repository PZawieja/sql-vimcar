WITH cte_payment AS (SELECT invoice_uuid,           --Only most recent payment
                         payment_status,
                                 rank() over (PARTITION BY invoice_uuid ORDER BY payment_status ASC) AS rank,
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
                        ORDER BY pim.cb_inv_id, pa.cb_payment_ts DESC),
     cte_cashflow_report AS (
         SELECT i.invoice_id, --CASE WHEN ad.invoice_id IS NOT NULL AND payment_status = 'authorised' AND pd.invoice_id IS NOT NULL THEN pd.pd_deal_id::VARCHAR ELSE i.invoice_id END AS invoice_id,
             i.invoice_billing_country,
             p.payment_status,
             CASE WHEN i.invoice_billing_country = 'DE' THEN 'EUR' ELSE 'GBP' END AS currency,
             (i.invoice_created_ts AT TIME ZONE 'Europe/Berlin')::DATE AS invoice_created_dt,
             o.invoice_status,
             pd.pd_deal_status,
             ad.transaction_type,
             CASE WHEN cmb.customer_is_fleet = TRUE THEN 'B2B'
                  ELSE 'B2C' END AS is_fleet,
             CASE WHEN a.invoice_key IS NOT NULL THEN 'Abgeltungsrechnung' ELSE 'Lizenzrechnung' END AS product_licence,
                 i.invoice_total_price_gross_amount/100.0 AS invoice_total_price_gross_amount,
                 r.refund_gross_amount/100.0 AS refund_gross_amount,
                 (i.invoice_total_price_gross_amount - COALESCE(r.refund_gross_amount,0))/100.0 AS total_amount
         FROM dwh_main.dim_invoice i
                  LEFT JOIN cte_payment p
                            ON i.invoice_uuid = p.invoice_uuid
                  JOIN dwh_main.dim_order o
                       ON i.order_uuid = o.order_uuid
                  JOIN dwh_main.dim_customer cust
                       ON i.customer_uuid = cust.customer_uuid
                  LEFT JOIN dwh_main.dim_combined_customer cmb
                            ON cust.customer_id = cmb.customer_id_shop
                  LEFT JOIN (SELECT invoice_id, sum(refund_gross_amount) AS refund_gross_amount
                             FROM dwh_main.dim_refund
                             GROUP BY invoice_id) r
                            ON i.invoice_id = r.invoice_id
                  LEFT JOIN cte_abgeltung a
                            ON i.invoice_key = a.invoice_key
                  LEFT JOIN cte_chargeback ad
                            ON i.invoice_id = ad.invoice_id
                  LEFT JOIN cte_pipedrive pd
                            ON i.invoice_id = pd.invoice_id
         WHERE 1=1
           AND (i.invoice_created_ts AT TIME ZONE 'Europe/Berlin')::DATE >= '2020-01-01' --current_timestamp - INTERVAL '1 year'
           --AND i.invoice_id = 'R93154798'
           AND (rank = 1 OR p.invoice_uuid IS NULL) --to include invoices without payment
           AND i.invoice_billing_email NOT LIKE '%@vimcar.com'
           AND ((substr(i.invoice_creator_urn, 23, 4) = 'user' OR i.invoice_creator_urn = 'urn:vimcar:com.vimcar:service/contract-renewal') OR ((substr(i.invoice_creator_urn, 23, 4) <> 'user' OR i.invoice_creator_urn <> 'urn:vimcar:com.vimcar:service/contract-renewal') AND payment_status NOT IN ('initial', 'refused', 'error'))) --Excluding Web Checkout
           AND o.order_has_full_refund = FALSE
     )
SELECT i.invoice_id
     , (i.invoice_created_ts AT TIME ZONE 'Europe/Berlin')::DATE AS invoice_created_dt
     , i.invoice_total_price_net_amount / 100.0 AS invoice_net_value
     , i.invoice_creator_urn
     , i.invoice_billing_email
     , p.payment_status
     , i.contract_id
     , CASE WHEN ctr.contract_id_with_one_invoice IS NOT NULL THEN TRUE ELSE FALSE END AS contract_has_one_invoice
FROM dwh_main.dim_invoice i
         LEFT JOIN cte_cashflow_report cf
                   ON cf.invoice_id = i.invoice_id
         LEFT JOIN cte_payment p
                   ON i.invoice_uuid = p.invoice_uuid
         JOIN dwh_main.dim_order o
              ON i.order_uuid = o.order_uuid
         JOIN dwh_main.dim_customer cust
              ON i.customer_uuid = cust.customer_uuid
         LEFT JOIN dwh_main.dim_combined_customer cmb
                   ON cust.customer_id = cmb.customer_id_shop
         LEFT JOIN (SELECT invoice_id, sum(refund_gross_amount) AS refund_gross_amount
                    FROM dwh_main.dim_refund
                    GROUP BY invoice_id) r
                   ON i.invoice_id = r.invoice_id
         LEFT JOIN cte_abgeltung a
                   ON i.invoice_key = a.invoice_key
         LEFT JOIN cte_chargeback ad
                   ON i.invoice_id = ad.invoice_id
         LEFT JOIN cte_pipedrive pd
                   ON i.invoice_id = pd.invoice_id
         LEFT JOIN (SELECT contract_id AS contract_id_with_one_invoice
                    FROM dwh_main.dim_invoice
                    GROUP BY contract_id
                    HAVING count(*) = 1) ctr
                   ON ctr.contract_id_with_one_invoice = i.contract_id
WHERE 1=1
  AND cf.invoice_id IS NULL
  AND (i.invoice_created_ts AT TIME ZONE 'Europe/Berlin')::DATE >= '2020-01-01' --current_timestamp - INTERVAL '1 year'
  AND o.order_has_full_refund = FALSE
  AND i.invoice_id = 'R10172429'
-- AND p.payment_status NOT IN ('initial', 'refused', 'error')
ORDER BY invoice_total_price_net_amount DESC
;

SELECT sum(mrr_eur) FROM dwh_main.dim_combined_invoice_line WHERE invoice_id LIKE 'o-%' AND invoice_created_dt >= '2020-01-01' AND mrr_eur > 0