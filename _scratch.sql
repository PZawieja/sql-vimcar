-- Micheal Hein, customer segmentation report:

WITH customers AS (
    SELECT
        customer_id
         , source_system
         , CASE WHEN customer_country = 'GB' THEN 'UK' ELSE 'DACH' END AS customer_country
         , MIN(invoice_provisioning_start_dt) OVER (PARTITION BY customer_id) AS first_order_date
         , MAX(reporting_month) FILTER ( WHERE churn_mth = TRUE ) OVER (PARTITION BY customer_id) AS churn_mth
         , customer_status
         , CASE WHEN product_group NOT IN ('Admin', 'Logbook', 'Geo', 'Pro') THEN 'Other' ELSE product_group END AS product
         , CASE WHEN mrr_eur_new >0 THEN TRUE END AS flag_first_mth
         , CASE WHEN reporting_month = MAX(reporting_month) OVER (PARTITION BY customer_id) THEN TRUE END AS flag_current_mth
         , reporting_month
         , mrr_eur
         , licenses
         , CASE WHEN SUM(mrr_eur_expansion + mrr_eur_contraction) OVER (PARTITION BY customer_id, reporting_month ) > 0 THEN TRUE END AS flag_expansion
         , mrr_eur_expansion + mrr_eur_contraction AS mrr_fluctuation
    FROM data_mart_internal_reporting.ir_m_investor_report
    WHERE is_fleet_customer = TRUE
      AND flag_future = false
--       AND customer_status IN ('active', 'non_renewing')
--     AND customer_id = 'K36527423' -- 506.35 expansion is correct
--     and customer_id = '18941291'  -- 374.35 is correct
      AND customer_id = 'K16187900'
)
SELECT
    source_system
     , customer_id
     , customer_country
     , first_order_date
     , churn_mth
     , customer_status
     , product
-- MRR initial
     , coalesce(sum(mrr_eur) FILTER ( WHERE product = 'Admin' AND flag_first_mth = TRUE),0) AS admin_mrr_initial
     , coalesce(sum(mrr_eur) FILTER ( WHERE product = 'Logbook' AND flag_first_mth = TRUE),0) AS logbook_mrr_initial
     , coalesce(sum(mrr_eur) FILTER ( WHERE product = 'Geo' AND flag_first_mth = TRUE),0) AS geo_mrr_initial
     , coalesce(sum(mrr_eur) FILTER ( WHERE product = 'Pro' AND flag_first_mth = TRUE),0) AS pro_mrr_initial
     , coalesce(sum(mrr_eur) FILTER ( WHERE product = 'Other' AND flag_first_mth = TRUE),0) AS other_mrr_initial
     , coalesce(sum(mrr_eur) FILTER ( WHERE flag_first_mth = TRUE),0) AS total_mrr_initial
-- MRR current
     , coalesce(sum(mrr_eur) FILTER ( WHERE product = 'Admin' AND flag_current_mth = TRUE),0) AS admin_mrr_current
     , coalesce(sum(mrr_eur) FILTER ( WHERE product = 'Logbook' AND flag_current_mth = TRUE),0) AS logbook_mrr_current
     , coalesce(sum(mrr_eur) FILTER ( WHERE product = 'Geo' AND flag_current_mth = TRUE),0) AS geo_mrr_current
     , coalesce(sum(mrr_eur) FILTER ( WHERE product = 'Pro' AND flag_current_mth = TRUE),0) AS pro_mrr_current
     , coalesce(sum(mrr_eur) FILTER ( WHERE product = 'Other' AND flag_current_mth = TRUE),0) AS other_mrr_current
     , coalesce(sum(mrr_eur) FILTER ( WHERE flag_current_mth = TRUE),0) AS total_mrr_current
-- licenses initial
     , coalesce(sum(licenses) FILTER ( WHERE product = 'Admin' AND flag_first_mth = TRUE),0) AS admin_items_initial
     , coalesce(sum(licenses) FILTER ( WHERE product = 'Logbook' AND flag_first_mth = TRUE),0) AS logbook_items_initial
     , coalesce(sum(licenses) FILTER ( WHERE product = 'Geo' AND flag_first_mth = TRUE),0) AS geo_items_initial
     , coalesce(sum(licenses) FILTER ( WHERE product = 'Pro' AND flag_first_mth = TRUE),0) AS pro_items_initial
     , coalesce(sum(licenses) FILTER ( WHERE product = 'Other' AND flag_first_mth = TRUE),0) AS other_items_initial
     , coalesce(sum(licenses) FILTER ( WHERE flag_first_mth = TRUE),0) AS total_items_initial
-- licenses current
     , coalesce(sum(licenses) FILTER ( WHERE product = 'Admin' AND flag_current_mth = TRUE),0) AS admin_items_current
     , coalesce(sum(licenses) FILTER ( WHERE product = 'Logbook' AND flag_current_mth = TRUE),0) AS logbook_items_current
     , coalesce(sum(licenses) FILTER ( WHERE product = 'Geo' AND flag_current_mth = TRUE),0) AS geo_items_current
     , coalesce(sum(licenses) FILTER ( WHERE product = 'Pro' AND flag_current_mth = TRUE),0) AS pro_items_current
     , coalesce(sum(licenses) FILTER ( WHERE product = 'Other' AND flag_current_mth = TRUE),0) AS other_items_current
     , coalesce(sum(licenses) FILTER ( WHERE flag_current_mth = TRUE),0) AS total_items_current

     , coalesce(sum(mrr_fluctuation) FILTER ( WHERE flag_expansion = TRUE ),0)  AS total_mrr_expansion

FROM customers
WHERE flag_first_mth = TRUE
   OR flag_current_mth = TRUE
   OR flag_expansion = TRUE
GROUP BY source_system, customer_id, customer_country, first_order_date, churn_mth, customer_status, product
;

-------------------------------------------------------------------------------------



-- 1 SQL: customers
WITH customers AS (
    SELECT
        customer_id
         , source_system
         , CASE WHEN customer_country = 'GB' THEN 'UK' ELSE 'DACH' END AS customer_country
         , MIN(invoice_provisioning_start_dt) OVER (PARTITION BY r.customer_id) AS first_order_date
         , customer_status
         , CASE WHEN product_group NOT IN ('Admin', 'Logbook', 'Geo', 'Pro') THEN 'Other' ELSE product_group END AS product
         , CASE WHEN mrr_eur_new >0 THEN TRUE END AS flag_first_mth
         , CASE WHEN reporting_month = MAX(reporting_month) OVER (PARTITION BY customer_id) THEN TRUE END AS flag_current_mth
         , CASE WHEN reporting_month = MAX(reporting_month) FILTER ( WHERE mrr_eur >0 ) OVER (PARTITION BY customer_id) THEN TRUE END AS flag_last_mth_with_value
         , cm.churn_mth
         , mrr_eur
         , CASE WHEN SUM(mrr_eur_expansion + mrr_eur_contraction) OVER (PARTITION BY customer_id, reporting_month ) > 0 THEN TRUE END AS flag_expansion
         , mrr_eur_expansion + mrr_eur_contraction AS mrr_fluctuation
         , licenses
    FROM data_mart_internal_reporting.test_ir_m_investor_report r
        LEFT JOIN (SELECT customer_id AS churn_customer_id
                        , MAX(reporting_month) AS churn_mth
                    FROM data_mart_internal_reporting.test_ir_m_investor_report WHERE churn_mth = true GROUP BY customer_id) cm
            ON cm.churn_customer_id = r.customer_id
    WHERE is_fleet_customer = TRUE
      AND flag_future = false
--     AND customer_id = 'K36527423' -- 506.35 expansion is correct
--       and customer_id = '18941291'  -- 374.35 is correct
--       AND customer_id = 'K16187900'
)
SELECT
    source_system
     , customer_id
     , customer_country
     , customer_status
     , churn_mth
     , coalesce(sum(mrr_eur) FILTER ( WHERE flag_first_mth = TRUE ),0)  AS mrr_eur_initial
     , coalesce(sum(mrr_eur) FILTER ( WHERE flag_current_mth = TRUE ),0)  AS mrr_eur_current
     , coalesce(sum(mrr_eur) FILTER ( WHERE flag_last_mth_with_value = TRUE ),0)  AS mrr_eur_most_recent_value
     , coalesce(sum(mrr_fluctuation) FILTER ( WHERE flag_expansion = TRUE ),0)  AS tenure_mrr_expansion
FROM customers
WHERE flag_first_mth = TRUE
   OR flag_current_mth = TRUE
   OR flag_expansion = TRUE
   OR flag_last_mth_with_value = TRUE
GROUP BY source_system, customer_id, customer_country, first_order_date, churn_mth, customer_status, churn_mth
;


-- 2 SQL: MRR initial and current
WITH customers AS (
    SELECT
        customer_id
         , source_system
         , CASE WHEN customer_country = 'GB' THEN 'UK' ELSE 'DACH' END AS customer_country
         , MIN(invoice_provisioning_start_dt) OVER (PARTITION BY r.customer_id) AS first_order_date
         , customer_status
         , CASE WHEN product_group NOT IN ('Admin', 'Logbook', 'Geo', 'Pro') THEN 'Other' ELSE product_group END AS product
         , CASE WHEN mrr_eur_new >0 THEN TRUE END AS flag_first_mth
         , CASE WHEN reporting_month = MAX(reporting_month) OVER (PARTITION BY customer_id) THEN TRUE END AS flag_current_mth
         , CASE WHEN reporting_month = MAX(reporting_month) FILTER ( WHERE mrr_eur >0 ) OVER (PARTITION BY customer_id) THEN TRUE END AS flag_last_mth_with_value
         , cm.churn_mth
         , mrr_eur
         , CASE WHEN SUM(mrr_eur_expansion + mrr_eur_contraction) OVER (PARTITION BY customer_id, reporting_month ) > 0 THEN TRUE END AS flag_expansion
         , mrr_eur_expansion + mrr_eur_contraction AS mrr_fluctuation
         , licenses
    FROM data_mart_internal_reporting.test_ir_m_investor_report r
             LEFT JOIN (SELECT customer_id AS churn_customer_id
                             , MAX(reporting_month) AS churn_mth
                        FROM data_mart_internal_reporting.test_ir_m_investor_report WHERE churn_mth = true GROUP BY customer_id) cm
                       ON cm.churn_customer_id = r.customer_id
    WHERE is_fleet_customer = TRUE
      AND flag_future = false
      AND subscription_id = 'V13028084'
--     AND customer_id = 'K36527423' -- 506.35 expansion is correct
--       and customer_id = '18941291'  -- 374.35 is correct
--       AND customer_id = 'K16187900'
)
SELECT
    source_system
     , customer_id
     , customer_country
     , customer_status
     , churn_mth
     , product
     , coalesce(sum(mrr_eur) FILTER ( WHERE flag_first_mth = TRUE ),0)  AS mrr_eur_initial
     , coalesce(sum(mrr_eur) FILTER ( WHERE flag_current_mth = TRUE ),0)  AS mrr_eur_current
     , coalesce(sum(mrr_eur) FILTER ( WHERE flag_last_mth_with_value = TRUE ),0)  AS mrr_eur_most_recent_value
     , coalesce(sum(mrr_fluctuation) FILTER ( WHERE flag_expansion = TRUE ),0) AS lifetime_mrr_eur_expansion
FROM customers
WHERE flag_first_mth = TRUE
   OR flag_current_mth = TRUE
   OR flag_expansion = TRUE
   OR flag_last_mth_with_value = TRUE
GROUP BY source_system, customer_id, customer_country, first_order_date, churn_mth, customer_status, churn_mth, product
;

-- Micheal Hein, customer segmentation report, TENURE SQL
WITH customers AS (SELECT customer_id
                        , CASE
                              WHEN SUM(mrr_eur_expansion + mrr_eur_contraction)
                                   OVER (PARTITION BY customer_id, reporting_month ) > 0 THEN TRUE END AS flag_expansion
                        , mrr_eur_expansion + mrr_eur_contraction AS                                      mrr_fluctuation
                        , licenses
                        , EXTRACT(YEAR FROM AGE(reporting_month, first_invoice_mth)) * 12 +
                          EXTRACT(MONTH FROM AGE(reporting_month, first_invoice_mth)) AS                  tenure_mths
                        , EXTRACT(YEAR FROM AGE(reporting_month, first_invoice_mth)) AS                   tenure_years
                        , CASE
                              WHEN ((EXTRACT(YEAR FROM AGE(reporting_month, first_invoice_mth)) * 12 +
                                     EXTRACT(MONTH FROM AGE(reporting_month, first_invoice_mth))) /
                                    NULLIF(EXTRACT(YEAR FROM AGE(reporting_month, first_invoice_mth)), 0)) = 12
                                  THEN TRUE
                              ELSE FALSE END AS                                                           flag_tenure_year_start
                   FROM data_mart_internal_reporting.test_ir_m_investor_report
                   WHERE is_fleet_customer = TRUE
                     AND flag_future = FALSE
--                      AND customer_id = 'K36527423' -- 506.35 expansion is correct
--       and customer_id = '18941291'  -- 374.35 is correct
    AND subscription_id = 'V13028084'
)
SELECT customer_id
     , tenure_years
     , COALESCE(SUM(mrr_fluctuation) FILTER ( WHERE flag_expansion = TRUE ), 0) AS tenure_mrr_eur_expansion
FROM customers
GROUP BY customer_id, tenure_years
;


