

SELECT
    c.customer_id
     , c.customer_is_fleet
     , c.customer_country
     , c.customer_status
     , coalesce(r.first_invoice_dt, r.first_invoice_dt_all_invoices) AS first_subscription_start_dt
     , c.max_subscription_cancelled_dt
     , coalesce(c.max_subscription_cancelled_dt, current_date) AS reference_dt
--      , array_agg(r.product_terms) OVER (PARTITION BY c.customer_id) AS product_terms
     , true = ANY(ARRAY_AGG(CASE WHEN r.product_terms LIKE '%36 %' THEN TRUE END))          AS subscription_length_36_mths
     , true = ANY(ARRAY_AGG(CASE WHEN r.product_terms LIKE '%12 %' THEN TRUE END))          AS subscription_length_12_mths
     , true = ANY(ARRAY_AGG(CASE WHEN r.product_terms LIKE '%1 %' THEN TRUE END))           AS subscription_length_1_mths
     , true = ANY(ARRAY_AGG(CASE WHEN r.product_terms LIKE '% yearly%' THEN TRUE END))      AS billing_every_12_mths
     , true = ANY(ARRAY_AGG(CASE WHEN r.product_terms LIKE '% bi-yearly%' THEN TRUE END))   AS billing_every_6_mths
     , true = ANY(ARRAY_AGG(CASE WHEN r.product_terms LIKE '% quarterly%' THEN TRUE END))   AS billing_every_3_mths
     , true = ANY(ARRAY_AGG(CASE WHEN r.product_terms LIKE '% monthly%' THEN TRUE END))     AS billing_every_1_mths
     , SUM(mrr_with_credit_note) AS mrr -- local currency
     , SUM(mrr_with_credit_note) FILTER ( WHERE r.product_group = 'Logbook' ) AS mrr_logbook
     , SUM(mrr_with_credit_note) FILTER ( WHERE r.product_group = 'Admin' ) AS mrr_admin
     , SUM(mrr_with_credit_note) FILTER ( WHERE r.product_group = 'Geo' ) AS mrr_geo
     , SUM(mrr_with_credit_note) FILTER ( WHERE r.product_group = 'Pro' ) AS mrr_pro
     , SUM(mrr_with_credit_note) FILTER ( WHERE r.product_group = 'Other' ) AS mrr_fsk
FROM data_mart_internal_reporting.tmp_revenue_reporting_sh_cb r
         JOIN dwh_main.dim_combined_customer c
              ON c.customer_id = r.customer_id
WHERE r.the_most_recent_customer_mrr IS NOT NULL
  AND c.customer_is_fleet = True
GROUP BY c.customer_id, c.customer_is_fleet, c.customer_country , c.customer_status, r.product_terms,
         r.first_invoice_dt, r.first_invoice_dt_all_invoices, c.max_subscription_cancelled_dt;
