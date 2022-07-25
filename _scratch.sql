WITH cte_dstv_customers AS (SELECT customer_id
                            FROM dwh_main.dim_combined_invoice_line
                            WHERE LOWER(coupon_code) LIKE '%dstv%')
   , cte_dstv_customer_invoices AS (
    SELECT
        i.customer_id
         , c.customer_id_chargebee
         , c.customer_id_shop
         , CASE
               WHEN c.customer_id_chargebee IS NOT NULL
                   THEN 'CB'
               ELSE 'SH'
        END current_customer_system
         , i.invoice_id
         , i.source_system AS invoice_source_system
         , i.invoice_provisioning_start_dt
         , i.invoice_created_dt
         , i.invoice_status
         , i.currency_code
         , i.entity_id
         , i.product_name
--      , coalesce(i.coupon_code, s.subscription_coupon_1) AS coupon_code
         , i.coupon_code
         , i.full_refund_flag
         , mst.min_provisioning_start_dt
         , extract( year from age(invoice_provisioning_start_dt, min_provisioning_start_dt)) AS renewal_year
         , CASE
               WHEN extract( month from age(invoice_provisioning_start_dt, min_provisioning_start_dt)) = 0
                   THEN TRUE
               ELSE FALSE
        END AS is_renewal_month
         , MAX(cp.product_price_net) AS product_price_net
         , SUM(i.item_quantity) AS item_quantity
--      , round(cp.product_price_net * 1.19 / 100, 2) AS product_price_gross
    FROM dwh_main.dim_combined_invoice_line i
             JOIN dwh_main.dim_combined_subscription s
                  ON i.subscription_id = s.subscription_id
             JOIN dwh_main.dim_combined_customer c
                  ON c.customer_id = i.customer_id
             JOIN dwh_main.dim_combined_product cp
                  ON cp.entity_id = i.entity_id
                      AND cp.entity_type = i.entity_type
             JOIN (SELECT customer_id, MIN(invoice_provisioning_start_dt) AS min_provisioning_start_dt
                   FROM dwh_main.dim_combined_invoice_line
                   WHERE voided_ts IS NULL
                   GROUP BY customer_id) mst
                  ON i.customer_id = mst.customer_id
    WHERE 1=1
      AND i.voided_ts IS NULL
      AND i.product_terms LIKE '%yearly payment' -- AND c.customer_id = 'K76606887' -- monthly payment, should be included
      AND i.product_group <> 'Hardware'
      AND exists(SELECT 1 FROM cte_dstv_customers dstv WHERE dstv.customer_id = i.customer_id)
--     AND i.customer_id = 'K69893354' -- customer migrated to CB
--     AND i.customer_id = 'K74495058' -- has both admin and logbook
--         AND I.customer_id = '30003291532'
-- AND c.customer_id = 'K76606887' -- monthly payment
    GROUP BY i.customer_id, i.invoice_id, i.source_system, i.invoice_provisioning_start_dt, i.invoice_created_dt
           , i.invoice_status, i.currency_code, i.entity_id, i.coupon_code, s.subscription_coupon_1
           , i.full_refund_flag, i.product_name, mst.min_provisioning_start_dt, c.customer_id_chargebee, c.customer_id_shop
)
-- SELECT * FROM cte_dstv_customer_invoices;
   , cte_refund AS (
    SELECT cb_cn_reference_invoice_id AS refund_invoice_id
         , sum(cb_cn_total) AS refund_gross_amount
    FROM dwh_main.dim_cb_credit_note
    WHERE cb_cn_voided_ts IS NULL
    GROUP BY cb_cn_reference_invoice_id
    UNION ALL
    SELECT invoice_id AS refund_invoice_id
         , sum(refund_gross_amount) AS refund_gross_amount
    FROM dwh_main.dim_refund
    GROUP BY invoice_id
)
   , cte_invoice_cb_sh AS (
    SELECT cb_inv_id AS invoice_id
         , cb_inv_total AS invoice_total_price_gross_amount
    FROM dwh_main.dim_cb_invoice
    UNION ALL
    SELECT invoice_id
         , invoice_total_price_gross_amount
    FROM dwh_main.dim_invoice
)
   , cte_final AS (
    SELECT i.*
         , r.refund_gross_amount
         , i2.invoice_total_price_gross_amount
    FROM cte_dstv_customer_invoices i
             JOIN cte_invoice_cb_sh i2      -- LEFT JOIN possible as before 2018 there were orders without invoices, Magento migration
                  ON i.invoice_id = i2.invoice_id
             LEFT JOIN cte_refund r
                       ON r.refund_invoice_id = i.invoice_id
)
SELECT * FROM cte_final