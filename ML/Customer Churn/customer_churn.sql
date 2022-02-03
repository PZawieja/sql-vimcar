
WITH customers_with_revenue AS (
    SELECT
        c.customer_id
         , c.customer_is_fleet
         , c.customer_country
         , c.customer_status
         , CASE WHEN c.customer_id_chargebee IS NOT NULL THEN 'CB' ELSE 'SH' END AS current_system
         , c.min_subscription_start_dt AS first_subscription_start_dt
         , CASE
             WHEN c.customer_status IN ('cancelled', 'non_renewing')
                 THEN c.max_subscription_cancelled_dt
            END AS customer_cancelled_dt
--          , coalesce(c.max_subscription_cancelled_dt, current_date) AS reference_dt
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_length_and_payment LIKE '%36 %' THEN TRUE END))          AS subscription_length_36_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_length_and_payment LIKE '%12 %' THEN TRUE END))          AS subscription_length_12_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_length_and_payment LIKE '%1 %' THEN TRUE END))           AS subscription_length_1_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_length_and_payment LIKE '% yearly%' THEN TRUE END))      AS billing_every_12_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_length_and_payment LIKE '% bi-yearly%' THEN TRUE END))   AS billing_every_6_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_length_and_payment LIKE '% quarterly%' THEN TRUE END))   AS billing_every_3_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_length_and_payment LIKE '% monthly%' THEN TRUE END))     AS billing_every_1_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.invoice_discount_pct >0 THEN TRUE END))                          AS has_discount
         , SUM(mrr_eur) AS mrr
         , SUM(mrr_eur) FILTER ( WHERE r.product_group = 'Logbook' ) AS mrr_logbook
         , SUM(mrr_eur) FILTER ( WHERE r.product_group = 'Admin' ) AS mrr_admin
         , SUM(mrr_eur) FILTER ( WHERE r.product_group = 'Geo' ) AS mrr_geo
         , SUM(mrr_eur) FILTER ( WHERE r.product_group = 'Pro' ) AS mrr_pro
         , SUM(mrr_eur) FILTER ( WHERE r.product_group = 'Other' ) AS mrr_other
    FROM data_mart_internal_reporting.ir_m_sh_cb_investor_report r
             JOIN (SELECT customer_id
                        , MAX(reporting_month) AS last_mth_with_value
                   FROM data_mart_internal_reporting.ir_m_sh_cb_investor_report
                   WHERE mrr_eur > 0
                     AND flag_future = FALSE
                   GROUP BY customer_id) r2
                  ON r.customer_id = r2.customer_id
                      AND r.reporting_month = r2.last_mth_with_value
             JOIN dwh_main.dim_combined_customer c
                  ON c.customer_id = r.customer_id
    WHERE 1=1
--       AND c.customer_is_fleet = True
--       AND c.customer_id = '10052946'
--         AND c.customer_id = '10923834'
    GROUP BY c.customer_id, c.customer_is_fleet, c.customer_country, c.customer_status, c.customer_id_chargebee,
             c.min_subscription_start_dt, c.max_subscription_cancelled_dt, coalesce(c.max_subscription_cancelled_dt, current_date)
)
   , customers_without_revenue AS (
    SELECT
        c.customer_id
         , c.customer_is_fleet
         , c.customer_country
         , c.customer_status
         , CASE WHEN c.customer_id_chargebee IS NOT NULL THEN 'CB' ELSE 'SH' END AS current_system
         , c.min_subscription_start_dt AS first_subscription_start_dt
         , CASE
               WHEN c.customer_status IN ('cancelled', 'non_renewing')
                   THEN c.max_subscription_cancelled_dt
            END AS customer_cancelled_dt
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_terms LIKE '%36 %' THEN TRUE END))          AS subscription_length_36_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_terms LIKE '%12 %' THEN TRUE END))          AS subscription_length_12_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_terms LIKE '%1 %' THEN TRUE END))           AS subscription_length_1_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_terms LIKE '% yearly%' THEN TRUE END))      AS billing_every_12_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_terms LIKE '% bi-yearly%' THEN TRUE END))   AS billing_every_6_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_terms LIKE '% quarterly%' THEN TRUE END))   AS billing_every_3_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.product_terms LIKE '% monthly%' THEN TRUE END))     AS billing_every_1_mths
         , true = ANY(ARRAY_AGG(CASE WHEN r.invoice_discount_pct >0 THEN TRUE END))             AS has_discount
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
        AND c.customer_status IN ('non_renewing', 'cancelled')
--       AND c.customer_is_fleet = True
--       AND c.customer_id = '10923834'
    GROUP BY c.customer_id, c.customer_is_fleet, c.customer_country, c.customer_status, c.customer_id_chargebee,
             c.min_subscription_start_dt, c.max_subscription_cancelled_dt, coalesce(c.max_subscription_cancelled_dt, current_date)
)
   , customers_all AS (SELECT TRUE AS had_revenue_ever, *
                       FROM customers_with_revenue r
                       UNION ALL
                       SELECT FALSE AS had_revenue_ever, *
                       FROM customers_without_revenue nr
                       WHERE NOT EXISTS(SELECT 1 FROM customers_with_revenue r WHERE r.customer_id = nr.customer_id))
SELECT
CASE WHEN customer_cancelled_dt IS NULL THEN FALSE ELSE TRUE END AS churn_flag
, CASE WHEN (customer_cancelled_dt - first_subscription_start_dt) <= 100 THEN TRUE ELSE FALSE END AS churn_100d_flag
, coalesce(customer_cancelled_dt, current_date) - first_subscription_start_dt AS tenure_days
, *
FROM customers_all
WHERE date_trunc('month', first_subscription_start_dt) <> coalesce(date_trunc('month', customer_cancelled_dt), '2100-01-01') -- exclude customers who started and cancelled within the same calendar month
--AND first_subscription_start_dt <= current_date
;

