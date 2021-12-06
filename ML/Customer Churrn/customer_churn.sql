WITH customers_with_revenue AS (
    SELECT
        c.customer_id
         , c.customer_is_fleet
         , c.customer_country
         , c.customer_status
         , c.min_subscription_start_dt AS first_subscription_start_dt
         , c.max_subscription_cancelled_dt
         , coalesce(c.max_subscription_cancelled_dt, current_date) AS reference_dt
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_length_and_payment LIKE '%36 %' THEN TRUE END))          AS subscription_length_36_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_length_and_payment LIKE '%12 %' THEN TRUE END))          AS subscription_length_12_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_length_and_payment LIKE '%1 %' THEN TRUE END))           AS subscription_length_1_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_length_and_payment LIKE '% yearly%' THEN TRUE END))      AS billing_every_12_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_length_and_payment LIKE '% bi-yearly%' THEN TRUE END))   AS billing_every_6_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_length_and_payment LIKE '% quarterly%' THEN TRUE END))   AS billing_every_3_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_length_and_payment LIKE '% monthly%' THEN TRUE END))     AS billing_every_1_mths
         , SUM(mrr_eur) AS mrr
         , SUM(mrr_eur) FILTER ( WHERE r.product_group = 'Logbook' ) AS mrr_logbook
         , SUM(mrr_eur) FILTER ( WHERE r.product_group = 'Admin' ) AS mrr_admin
         , SUM(mrr_eur) FILTER ( WHERE r.product_group = 'Geo' ) AS mrr_geo
         , SUM(mrr_eur) FILTER ( WHERE r.product_group = 'Pro' ) AS mrr_pro
         , SUM(mrr_eur) FILTER ( WHERE r.product_group = 'Other' ) AS mrr_other
    FROM data_mart_internal_reporting.ir_m_investor_report r
             JOIN (SELECT customer_id, MAX(reporting_month) AS last_mth_with_value
                   FROM data_mart_internal_reporting.ir_m_investor_report
                   WHERE mrr_eur > 0
                   GROUP BY customer_id) r2
                  ON r.customer_id = r2.customer_id
                      AND r.reporting_month = r2.last_mth_with_value
             JOIN dwh_main.dim_combined_customer c
                  ON c.customer_id = r.customer_id
    WHERE 1=1
      AND c.customer_is_fleet = True
--       AND c.customer_id = '10052946'
    GROUP BY c.customer_id, c.customer_is_fleet, c.customer_country, c.customer_status, c.min_subscription_start_dt, c.max_subscription_cancelled_dt, coalesce(c.max_subscription_cancelled_dt, current_date)
)
   , customers_without_revenue AS (
    SELECT
        c.customer_id
         , c.customer_is_fleet
         , c.customer_country
         , c.customer_status
         , r.first_invoice_dt_all_invoices AS first_subscription_start_dt
         , c.max_subscription_cancelled_dt
         , coalesce(c.max_subscription_cancelled_dt, current_date) AS reference_dt
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_terms LIKE '%36 %' THEN TRUE END))          AS subscription_length_36_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_terms LIKE '%12 %' THEN TRUE END))          AS subscription_length_12_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_terms LIKE '%1 %' THEN TRUE END))           AS subscription_length_1_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_terms LIKE '% yearly%' THEN TRUE END))      AS billing_every_12_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_terms LIKE '% bi-yearly%' THEN TRUE END))   AS billing_every_6_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_terms LIKE '% quarterly%' THEN TRUE END))   AS billing_every_3_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_terms LIKE '% monthly%' THEN TRUE END))     AS billing_every_1_mths
         , round(SUM(mrr_without_credit_note / exchange_rate), 2) AS mrr
         , round(SUM(mrr_without_credit_note / exchange_rate) FILTER ( WHERE r.product_group = 'Logbook' ), 2) AS mrr_logbook
         , round(SUM(mrr_without_credit_note / exchange_rate) FILTER ( WHERE r.product_group = 'Admin' ), 2) AS mrr_admin
         , round(SUM(mrr_without_credit_note / exchange_rate) FILTER ( WHERE r.product_group = 'Geo' ), 2) AS mrr_geo
         , round(SUM(mrr_without_credit_note / exchange_rate) FILTER ( WHERE r.product_group = 'Pro' ), 2) AS mrr_pro
         , round(SUM(mrr_without_credit_note / exchange_rate) FILTER ( WHERE r.product_group = 'Other' ), 2) AS mrr_other
    FROM data_mart_internal_reporting.tmp_revenue_reporting_sh_cb r
             JOIN (SELECT customer_id, MAX(reporting_month) AS last_mth_with_value
                   FROM data_mart_internal_reporting.tmp_revenue_reporting_sh_cb
                   WHERE mrr_without_credit_note > 0
                   GROUP BY customer_id) r2
                  ON r.customer_id = r2.customer_id
                      AND r.reporting_month = r2.last_mth_with_value
             JOIN dwh_main.dim_combined_customer c
                  ON c.customer_id = r.customer_id
    WHERE 1=1
      AND c.customer_is_fleet = True
    GROUP BY c.customer_id, c.customer_is_fleet, c.customer_country, c.customer_status, r.first_invoice_dt_all_invoices, c.max_subscription_cancelled_dt, coalesce(c.max_subscription_cancelled_dt, current_date)
)
   , customers_all AS (SELECT FALSE AS is_tester, *
                       FROM customers_with_revenue r
                       UNION ALL
                       SELECT TRUE AS is_tester, *
                       FROM customers_without_revenue nr
                       WHERE NOT EXISTS(SELECT 1 FROM customers_with_revenue r WHERE r.customer_id = nr.customer_id))
SELECT * FROM customers_all
;



