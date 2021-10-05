-- mapping
SELECT DISTINCT
        customer_id AS map_customer_id
         , CASE WHEN product_group NOT IN ('Admin', 'Logbook', 'Geo', 'Pro') THEN 'Other' ELSE product_group END AS map_product
    FROM data_mart_internal_reporting.test_ir_m_investor_report r
    WHERE is_fleet_customer = TRUE
      AND flag_future = false
;
-- customers main sql:
WITH customers AS (
    SELECT
        customer_id
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
--      AND subscription_id = 'V13028084'
--     AND customer_id = 'K36527423' -- 506.35 expansion is correct
--       and customer_id = '18941291'  -- 374.35 is correct
--       AND customer_id = 'K16187900'
)
SELECT
    customer_id
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
GROUP BY customer_id, customer_country, first_order_date, churn_mth, customer_status, churn_mth, product
;

-- tenure:
WITH customers AS (SELECT customer_id
                        , CASE WHEN product_group NOT IN ('Admin', 'Logbook', 'Geo', 'Pro') THEN 'Other' ELSE product_group END AS product
                        , CASE
                              WHEN SUM(mrr_eur_expansion + mrr_eur_contraction)
                                   OVER (PARTITION BY customer_id, reporting_month, product_group ) > 0 THEN TRUE END AS flag_expansion
                        , mrr_eur_expansion + mrr_eur_contraction AS mrr_fluctuation
                        , licenses
                        , EXTRACT(YEAR FROM AGE(reporting_month, first_invoice_mth)) * 12 +
                          EXTRACT(MONTH FROM AGE(reporting_month, first_invoice_mth)) + 1 AS                  tenure_mths
                        , EXTRACT(YEAR FROM AGE(reporting_month, first_invoice_mth)) + 1 AS                   tenure_years
                        , CASE
                              WHEN ((EXTRACT(YEAR FROM AGE(reporting_month, first_invoice_mth)) * 12 +
                                     EXTRACT(MONTH FROM AGE(reporting_month, first_invoice_mth))) /
                                    NULLIF(EXTRACT(YEAR FROM AGE(reporting_month, first_invoice_mth)), 0)) = 12
                                  THEN TRUE
                              ELSE FALSE END AS                                                           flag_tenure_year_start
                   FROM data_mart_internal_reporting.test_ir_m_investor_report
                   WHERE is_fleet_customer = TRUE
                     AND flag_future = FALSE
--                          AND customer_id = 'K36527423' -- 506.35 expansion is correct
--       and customer_id = '18941291'  -- 374.35 is correct
--    AND subscription_id = 'V13028084'
--     AND customer_id = '11436777' -- multiple products
)
SELECT c1.customer_id
     , c1.product
     , c1.tenure_mths
     , COALESCE(SUM(c1.mrr_fluctuation) FILTER ( WHERE c1.flag_expansion = TRUE ), 0) AS tenure_mths_mrr_eur_expansion
     , c1.tenure_years
     , c2.tenure_years_mrr_eur_expansion
FROM customers c1
         JOIN (SELECT customer_id
                    , tenure_years
                    , product
                    , COALESCE(SUM(mrr_fluctuation) FILTER ( WHERE flag_expansion = TRUE ), 0) AS tenure_years_mrr_eur_expansion
               FROM customers
               GROUP BY customer_id, product, tenure_years) c2
              ON c2.customer_id = c1.customer_id
                  AND c2.product = c1.product
                  AND c2.tenure_years = c1.tenure_years
GROUP BY c1.customer_id, c1.product, c1.tenure_mths, c1.tenure_years, c1.flag_expansion, c2,tenure_years_mrr_eur_expansion
ORDER BY c1.customer_id, c1.tenure_mths, c1.product
;

-- tenure without product split:
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
--    AND subscription_id = 'V13028084'
)
SELECT customer_id
     , tenure_years
     , COALESCE(SUM(mrr_fluctuation) FILTER ( WHERE flag_expansion = TRUE ) OVER (PARTITION BY customer_id, tenure_years), 0) AS tenure_years_mrr_eur_expansion
     , tenure_mths
     , COALESCE(SUM(mrr_fluctuation) FILTER ( WHERE flag_expansion = TRUE ), 0) AS tenure_mths_mrr_eur_expansion
FROM customers
GROUP BY customer_id, tenure_years, tenure_mths, mrr_fluctuation, flag_expansion
