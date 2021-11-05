WITH geo_customers AS (
    SELECT DISTINCT i.customer_id
    FROM dwh_main.dim_combined_invoice_line i
    WHERE 1=1
      AND i.invoice_status <> 'voided'
      AND i.invoice_provisioning_start_dt <= current_date
      AND i.invoice_provisioning_end_dt > current_date
      AND i.product_group = 'Geo'
    )
SELECT
c.customer_id
, map.map_customer_domain_name
, map.map_fb_main_domain_name
, map.map_fb_main_domain_name
FROM geo_customers gc
    JOIN dwh_main.dim_combined_customer c
        ON c.customer_id = gc.customer_id
    JOIN dwh_main.map_customer_to_foxbox_domain map
         ON c.customer_id = map.map_unified_customer_id
WHERE map.record_nbr_per_unified_customer_id = 1
;





SELECT
    i.customer_id
    , c.min_subscription_start_dt
    , sum(i.mrr_eur) AS mrr_eur
    , sum(i.refund_mrr_eur) AS refund_mrr_eur
    , sum(i.item_quantity) AS item_quantity
    , round(sum(i.item_quantity) * (1-(sum(i.refund_mrr_eur) / sum(nullif(i.mrr_eur,0)))), 1) AS item_quantity
FROM dwh_main.dim_combined_invoice_line i
    JOIN dwh_main.dim_combined_customer c
        ON c.customer_id = i.customer_id
WHERE 1=1
  AND i.invoice_status <> 'voided'
  AND i.full_refund_flag = false
  AND i.product_group = 'Geo'
  AND c.min_subscription_start_dt = i.invoice_provisioning_start_dt
GROUP BY i.customer_id, c.min_subscription_start_dt
;

