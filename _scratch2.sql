K77940214;

WITH customers AS (
    SELECT
        r.customer_id
         , CASE WHEN c.customer_id_chargebee IS NOT NULL THEN 'CB' ELSE 'SH' END source_system
         , c.customer_id_shop
         , CASE WHEN r.customer_country = 'GB' THEN 'UK' ELSE 'DACH' END AS customer_country
         , c.customer_contact_company_name
         , MIN(r.invoice_provisioning_start_dt) OVER (PARTITION BY r.customer_id) AS first_order_date
         , r.customer_status
         , CASE WHEN r.customer_status IN ('cancelled','non_renewing') THEN max_subscription_cancelled_dt END AS cancellation_dt
         , CASE WHEN p.product_group NOT IN ('Logbook B2C', 'Admin', 'Logbook B2B', 'Geo', 'Pro') THEN 'Other' ELSE p.product_group END AS product
         , CASE
               WHEN p.product_group NOT IN ('Logbook B2C', 'Admin', 'Logbook B2B', 'Geo', 'Pro')
                   THEN 'Other'
               WHEN p.product_group LIKE 'Logbook%'
                   THEN 'Logbook'
               ELSE p.product_group
        END AS product_journey
         , CASE WHEN p.product_group IN ('Admin', 'Logbook B2B', 'Geo', 'Pro') THEN TRUE ELSE FALSE END AS is_b2b_product
         , CASE WHEN reporting_month = MIN(reporting_month) OVER (PARTITION BY r.customer_id) THEN TRUE END AS flag_first_mth
         , CASE WHEN reporting_month = date_trunc('month', current_date)::DATE THEN TRUE END AS flag_current_mth
         , CASE WHEN reporting_month = MAX(reporting_month) FILTER ( WHERE mrr_eur >0) OVER (PARTITION BY r.customer_id) THEN TRUE END AS flag_last_mth_with_value
         , cm.churn_mth
         , mrr_eur
         , CASE WHEN SUM(mrr_eur_expansion + mrr_eur_contraction) OVER (PARTITION BY r.customer_id, reporting_month ) > 0 THEN TRUE END AS flag_expansion
         , CASE WHEN SUM(mrr_eur_expansion + mrr_eur_contraction) OVER (PARTITION BY r.customer_id, reporting_month ) < 0 THEN TRUE END AS flag_contraction
         , mrr_eur_expansion + mrr_eur_contraction AS mrr_fluctuation
         , licenses
         , MIN(r.reporting_month) FILTER ( WHERE flg_expansion_crossselling = true ) OVER (PARTITION BY r.customer_id) AS first_flg_expansion_crossselling
    FROM data_mart_internal_reporting.ir_m_sh_cb_investor_report r
             JOIN dwh_main.dim_combined_customer c
                  ON c.customer_id = r.customer_id
             JOIN dwh_main.dim_combined_product p
                  ON p.entity_id = r.plan_id
             LEFT JOIN (SELECT customer_id AS churn_customer_id
                             , MAX(reporting_month) AS churn_mth
                        FROM data_mart_internal_reporting.ir_m_sh_cb_investor_report
                        WHERE churn_mth = true
                        GROUP BY customer_id) cm
                       ON cm.churn_customer_id = r.customer_id
    WHERE is_fleet_customer = TRUE
      AND flag_future = false
--      AND subscription_id = 'V13028084'
--      AND c.customer_id = 'K36527423' -- 506.35 expansion is correct
--       and c.customer_id = '18941291'  -- 374.35 is correct
--        AND r.customer_id = '30003253017'
)
   , customers_2 AS (
    SELECT
        c.customer_id
         , source_system
         , c.customer_country
         , customer_contact_company_name
         , first_order_date
         , date_trunc('month',first_order_date)::DATE AS first_order_mth
         , first_flg_expansion_crossselling
--          , age(first_flg_expansion_crossselling, date_trunc('month',first_order_date)::DATE)
         , extract(year from age(first_flg_expansion_crossselling, date_trunc('month',first_order_date)::DATE)) * 12
        + extract(month from age(first_flg_expansion_crossselling, date_trunc('month',first_order_date)::DATE)) AS first_corssselling_after_n_months
         , c.customer_status
         , cancellation_dt
         , churn_mth
         , segment_mrr_initial
         , segment_mrr_current
         , segment_mrr_most_recent
         , string_agg(DISTINCT CASE WHEN flag_first_mth = TRUE THEN product END, ','
                      order by CASE WHEN flag_first_mth = TRUE THEN product END) AS products_initial
         , string_agg(DISTINCT CASE WHEN flag_last_mth_with_value = TRUE THEN product END, ','
                      order by CASE WHEN flag_last_mth_with_value = TRUE THEN product END) AS products_most_recently

         , string_agg(DISTINCT CASE WHEN flag_first_mth = TRUE THEN product_journey END, ','
                      order by CASE WHEN flag_first_mth = TRUE THEN product_journey END) AS products_journey_initial
         , string_agg(DISTINCT CASE WHEN flag_last_mth_with_value = TRUE THEN product_journey END, ','
                      order by CASE WHEN flag_last_mth_with_value = TRUE THEN product_journey END) AS products_journey_most_recently
--      , product
-- MRR overall:
         , coalesce(sum(mrr_eur) FILTER ( WHERE flag_first_mth = TRUE ),0)  AS mrr_eur_initial
         , coalesce(sum(mrr_eur) FILTER ( WHERE flag_current_mth = TRUE ),0) AS mrr_eur_current
         , coalesce(sum(mrr_eur) FILTER ( WHERE flag_last_mth_with_value = TRUE ),0) AS mrr_eur_most_recent_value
         , coalesce(sum(mrr_fluctuation) FILTER ( WHERE flag_expansion = TRUE ),0) AS lifetime_mrr_eur_expansion
         , coalesce(sum(mrr_fluctuation) FILTER ( WHERE flag_contraction = TRUE ),0) AS lifetime_mrr_eur_contraction
-- Licenses overall:
         , coalesce(sum(licenses) FILTER ( WHERE flag_first_mth = TRUE ),0) AS licenses_initial
         , coalesce(sum(licenses) FILTER ( WHERE flag_current_mth = TRUE ),0) AS licenses_current
         , coalesce(sum(licenses) FILTER ( WHERE flag_last_mth_with_value = TRUE ),0) AS licenses_most_recent_value
         , coalesce(sum(licenses) FILTER ( WHERE flag_first_mth = TRUE AND is_b2b_product = TRUE),0) AS licenses_initial_b2b
         , coalesce(sum(licenses) FILTER ( WHERE flag_current_mth = TRUE AND is_b2b_product = TRUE),0) AS licenses_current_b2b
         , coalesce(sum(licenses) FILTER ( WHERE flag_last_mth_with_value = TRUE AND is_b2b_product = TRUE),0) AS licenses_most_recent_value_b2b
         , coalesce(fs.fs_vehicles_overall, 0) AS fs_vehicles_overall
         , coalesce(cfg.domain_configuration_product, 'unknown') AS foxbox_configuration_product
------ Admin:
         , coalesce(sum(mrr_eur) FILTER ( WHERE flag_first_mth = TRUE AND product = 'Admin'),0)  AS mrr_eur_initial_admin
         , coalesce(sum(mrr_eur) FILTER ( WHERE flag_current_mth = TRUE AND product = 'Admin'),0) AS mrr_eur_current_admin
         , coalesce(sum(mrr_eur) FILTER ( WHERE flag_last_mth_with_value = TRUE AND product = 'Admin'),0) AS mrr_eur_most_recent_value_admin
         , coalesce(sum(licenses) FILTER ( WHERE flag_first_mth = TRUE AND product = 'Admin' ),0) AS licenses_initial_admin
         , coalesce(sum(licenses) FILTER ( WHERE flag_current_mth = TRUE AND product = 'Admin'),0) AS licenses_current_admin
         , coalesce(sum(licenses) FILTER ( WHERE flag_last_mth_with_value = TRUE AND product = 'Admin'),0) AS licenses_most_recent_value_admin
------ Logbook:
         , coalesce(sum(mrr_eur) FILTER ( WHERE flag_first_mth = TRUE AND product LIKE 'Logbook%'),0)  AS mrr_eur_initial_logbook
         , coalesce(sum(mrr_eur) FILTER ( WHERE flag_current_mth = TRUE AND product LIKE 'Logbook%'),0) AS mrr_eur_current_logbook
         , coalesce(sum(mrr_eur) FILTER ( WHERE flag_last_mth_with_value = TRUE AND product LIKE 'Logbook%'),0) AS mrr_eur_most_recent_value_logbook
         , coalesce(sum(licenses) FILTER ( WHERE flag_first_mth = TRUE AND product LIKE 'Logbook%' ),0) AS licenses_initial_logbook
         , coalesce(sum(licenses) FILTER ( WHERE flag_current_mth = TRUE AND product LIKE 'Logbook%'),0) AS licenses_current_logbook
         , coalesce(sum(licenses) FILTER ( WHERE flag_last_mth_with_value = TRUE AND product LIKE 'Logbook%'),0) AS licenses_most_recent_value_logbook
------ Geo:
         , coalesce(sum(mrr_eur) FILTER ( WHERE flag_first_mth = TRUE AND product = 'Geo'),0)  AS mrr_eur_initial_geo
         , coalesce(sum(mrr_eur) FILTER ( WHERE flag_current_mth = TRUE AND product = 'Geo'),0) AS mrr_eur_current_geo
         , coalesce(sum(mrr_eur) FILTER ( WHERE flag_last_mth_with_value = TRUE AND product = 'Geo'),0) AS mrr_eur_most_recent_value_geo
         , coalesce(sum(licenses) FILTER ( WHERE flag_first_mth = TRUE AND product = 'Geo' ),0) AS licenses_initial_geo
         , coalesce(sum(licenses) FILTER ( WHERE flag_current_mth = TRUE AND product = 'Geo'),0) AS licenses_current_geo
         , coalesce(sum(licenses) FILTER ( WHERE flag_last_mth_with_value = TRUE AND product = 'Geo'),0) AS licenses_most_recent_value_geo
------ Pro:
         , coalesce(sum(mrr_eur) FILTER ( WHERE flag_first_mth = TRUE AND product = 'Pro'),0)  AS mrr_eur_initial_pro
         , coalesce(sum(mrr_eur) FILTER ( WHERE flag_current_mth = TRUE AND product = 'Pro'),0) AS mrr_eur_current_pro
         , coalesce(sum(mrr_eur) FILTER ( WHERE flag_last_mth_with_value = TRUE AND product = 'Pro'),0) AS mrr_eur_most_recent_value_pro
         , coalesce(sum(licenses) FILTER ( WHERE flag_first_mth = TRUE AND product = 'Pro' ),0) AS licenses_initial_pro
         , coalesce(sum(licenses) FILTER ( WHERE flag_current_mth = TRUE AND product = 'Pro'),0) AS licenses_current_pro
         , coalesce(sum(licenses) FILTER ( WHERE flag_last_mth_with_value = TRUE AND product = 'Pro'),0) AS licenses_most_recent_value_pro
------ Other:
         , coalesce(sum(mrr_eur) FILTER ( WHERE flag_first_mth = TRUE AND product = 'Other'),0)  AS mrr_eur_initial_other
         , coalesce(sum(mrr_eur) FILTER ( WHERE flag_current_mth = TRUE AND product = 'Other'),0) AS mrr_eur_current_other
         , coalesce(sum(mrr_eur) FILTER ( WHERE flag_last_mth_with_value = TRUE AND product = 'Other'),0) AS mrr_eur_most_recent_value_other
         , coalesce(sum(licenses) FILTER ( WHERE flag_first_mth = TRUE AND product = 'Other' ),0) AS licenses_initial_other
         , coalesce(sum(licenses) FILTER ( WHERE flag_current_mth = TRUE AND product = 'Other'),0) AS licenses_current_other
         , coalesce(sum(licenses) FILTER ( WHERE flag_last_mth_with_value = TRUE AND product = 'Other'),0) AS licenses_most_recent_value_other
    FROM customers c
             JOIN (SELECT customer_id
                        , CASE
                              WHEN sum(mrr_eur) FILTER ( WHERE flag_first_mth = TRUE ) > 1000 THEN 'XL (1000+)'
                              WHEN sum(mrr_eur) FILTER ( WHERE flag_first_mth = TRUE ) > 500 THEN 'L (500-1000)'
                              WHEN sum(mrr_eur) FILTER ( WHERE flag_first_mth = TRUE ) > 200 THEN 'M (200-500)'
                              WHEN sum(mrr_eur) FILTER ( WHERE flag_first_mth = TRUE ) > 50 THEN 'S (50-200)'
                              WHEN sum(mrr_eur) FILTER ( WHERE flag_first_mth = TRUE ) >= 0 THEN 'XS (0-50)'
                              ELSE 'Other'
            END AS segment_mrr_initial
                        , CASE
                              WHEN sum(mrr_eur) FILTER ( WHERE flag_current_mth = TRUE ) > 1000 THEN 'XL (1000+)'
                              WHEN sum(mrr_eur) FILTER ( WHERE flag_current_mth = TRUE ) > 500 THEN 'L (500-1000)'
                              WHEN sum(mrr_eur) FILTER ( WHERE flag_current_mth = TRUE ) > 200 THEN 'M (200-500)'
                              WHEN sum(mrr_eur) FILTER ( WHERE flag_current_mth = TRUE ) > 50 THEN 'S (50-200)'
                              WHEN sum(mrr_eur) FILTER ( WHERE flag_current_mth = TRUE ) >= 0 THEN 'XS (0-50)'
                              ELSE 'Other (churned already)'
            END AS segment_mrr_current
                        , CASE
                              WHEN sum(mrr_eur) FILTER ( WHERE flag_last_mth_with_value = TRUE ) > 1000 THEN 'XL (1000+)'
                              WHEN sum(mrr_eur) FILTER ( WHERE flag_last_mth_with_value = TRUE ) > 500 THEN 'L (500-1000)'
                              WHEN sum(mrr_eur) FILTER ( WHERE flag_last_mth_with_value = TRUE ) > 200 THEN 'M (200-500)'
                              WHEN sum(mrr_eur) FILTER ( WHERE flag_last_mth_with_value = TRUE ) > 50 THEN 'S (50-200)'
                              WHEN sum(mrr_eur) FILTER ( WHERE flag_last_mth_with_value = TRUE ) >= 0 THEN 'XS (0-50)'
                              ELSE 'Other'
            END AS segment_mrr_most_recent
                   FROM customers
                   GROUP BY customer_id
    ) t1
                  ON t1.customer_id = c.customer_id
             LEFT JOIN (SELECT fs_account_contact_id, SUM(fs_account_cf_vehicles_overall) AS fs_vehicles_overall
                        FROM dwh_main.dim_fs_account
                        WHERE fs_account_cf_vehicles_overall >0
                        GROUP BY fs_account_contact_id) fs
                       ON fs.fs_account_contact_id::varchar = c.customer_id
             LEFT JOIN (SELECT
                            m.map_unified_customer_id
                             , string_agg(DISTINCT d.domain_configuration_template_match, ',') AS domain_configuration_product
                        FROM dwh_main.map_customer_to_foxbox_domain m
                                 JOIN dwh_main.dim_v_dom_domain d
                                      ON d.domain_name = m.map_fb_domain_name
                        WHERE record_nbr_per_unified_customer_id = 1
                          AND map_fb_main_domain_name IS NOT NULL
                        GROUP BY m.map_unified_customer_id) cfg
                       ON cfg.map_unified_customer_id = c.customer_id

    GROUP BY c.customer_id, source_system, cancellation_dt, customer_country, customer_contact_company_name, first_order_date, first_flg_expansion_crossselling, churn_mth, customer_status, churn_mth,
             segment_mrr_initial, segment_mrr_current, segment_mrr_most_recent, fs.fs_vehicles_overall, cfg.domain_configuration_product--, product;
)
SELECT * FROM customers_2;



WITH customers AS (SELECT customer_id
                        , product_group
                        , CASE
                              WHEN SUM(mrr_eur_expansion + mrr_eur_contraction)
                                   OVER (PARTITION BY customer_id, reporting_month, product_group ) > 0 THEN TRUE END AS flag_expansion
                        , flg_expansion_crossselling
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
                   FROM data_mart_internal_reporting.ir_m_sh_cb_investor_report
                   WHERE is_fleet_customer = TRUE
                     AND flag_future = FALSE
                     AND customer_id = 'K77940214' -- 506.35 expansion is correct
--       and customer_id = '18941291'  -- 374.35 is correct
--    AND subscription_id = 'V13028084'
--     AND customer_id = '11436777' -- multiple products
)
   , cte_tenure AS (
    SELECT c1.customer_id
--      , c1.product_group
         , c1.tenure_mths
         , COALESCE(SUM(c1.mrr_fluctuation) FILTER ( WHERE c1.flag_expansion = TRUE ), 0) AS tenure_mths_mrr_eur_expansion
         , c1.tenure_years
         , SUM(c2.tenure_years_mrr_eur_expansion) AS tenure_years_mrr_eur_expansion
    FROM customers c1
             JOIN (SELECT customer_id
                        , tenure_years
                        , product_group
                        , COALESCE(SUM(mrr_fluctuation) FILTER ( WHERE flag_expansion = TRUE ), 0) AS tenure_years_mrr_eur_expansion
                   FROM customers
                   GROUP BY customer_id, product_group, tenure_years) c2
                  ON c2.customer_id = c1.customer_id
                      AND c2.product_group = c1.product_group
                      AND c2.tenure_years = c1.tenure_years
    GROUP BY c1.customer_id, c1.tenure_mths, c1.tenure_years, c1.flag_expansion
)
-- SELECT * FROM cte_tenure;
SELECT customer_id
     , 0 AS tenure_mths
     , SUM(mrr_eur_new) AS tenure_mths_mrr_eur_expansion
     , 0 AS tenure_years
     , SUM(mrr_eur_new) AS tenure_years_mrr_eur_expansion
FROM data_mart_internal_reporting.ir_m_sh_cb_investor_report r
WHERE EXISTS (SELECT 1 FROM cte_tenure t WHERE t.customer_id = r.customer_id )
GROUP BY customer_id
UNION ALL
SELECT customer_id
     , tenure_mths
     , tenure_mths_mrr_eur_expansion
     , tenure_years
     , tenure_years_mrr_eur_expansion
FROM cte_tenure
WHERE tenure_mths >1
ORDER BY tenure_mths;

-- Felix
WITH cte AS (SELECT i.customer_id
                  , date_trunc('month', c.min_subscription_start_dt)::DATE AS first_subscription_start_month
                  , SUM(mrr_eur - refund_mrr_eur)              AS mrr_eur
                  , STRING_AGG(DISTINCT i.product_group, ' - ') FILTER ( WHERE i.product_group NOT IN ('Other', 'Hardware')) AS products
             FROM dwh_main.dim_combined_invoice_line i
                      JOIN dwh_main.dim_combined_subscription s ON s.subscription_id = i.subscription_id
                      JOIN dwh_main.dim_combined_customer c ON c.customer_id = i.customer_id
                      JOIN dwh_main.dim_combined_product p
                           ON i.entity_id = p.entity_id AND i.entity_type = p.entity_type
             WHERE i.invoice_status <> 'voided'
               AND i.invoice_provisioning_start_dt <= CURRENT_DATE
               AND i.invoice_provisioning_end_dt > CURRENT_DATE
               AND i.full_refund_flag = FALSE
               AND c.min_subscription_start_dt >= '2021-01-01'
             GROUP BY i.customer_id, c.min_subscription_start_dt
             HAVING SUM(mrr_eur - refund_mrr_eur) >= 200)
SELECT
    *
     , CASE
           WHEN products = 'Admin'
               THEN 'Admin only'
           WHEN products = 'Pro'
               THEN 'Pro only'
           WHEN products LIKE 'Admin%'
               OR products LIKE '%Pro%'
               THEN 'Admin equipped'
           WHEN products NOT LIKE 'Admin%'
               AND products NOT LIKE '%Pro%'
               THEN 'Not Admin equipped'
    END test
FROM cte

;