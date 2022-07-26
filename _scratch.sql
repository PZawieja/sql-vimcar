WITH freshsales AS (SELECT i.customer_id
                         , string_agg(DISTINCT fs_deal_sales_account_id::TEXT, '; ') AS freshsales_account_id
                    FROM dwh_main.dim_fs_deal d
                             JOIN dwh_main.dim_combined_invoice_line i
                                  ON d.fs_deal_cf_invoice_number = i.invoice_id
                    WHERE fs_deal_cf_invoice_number IS NOT NULL
                    GROUP BY i.customer_id)
   , customers AS (
    SELECT
        r.customer_id
         , CASE WHEN c.customer_id_chargebee IS NOT NULL THEN 'CB' ELSE 'SH' END source_system
         , CASE WHEN r.customer_country = 'GB' THEN 'UK' ELSE 'DACH' END AS customer_country
         , c.customer_contact_company_name
         , f.freshsales_account_id
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
    FROM data_mart_internal_reporting.ir_m_sh_cb_investor_report r
             JOIN dwh_main.dim_combined_customer c
                  ON c.customer_id = r.customer_id
             JOIN dwh_main.dim_combined_product p
                  ON p.entity_id = r.plan_id
             LEFT JOIN freshsales f
                       ON f.customer_id = r.customer_id
             LEFT JOIN (SELECT customer_id AS churn_customer_id
                             , MAX(reporting_month) AS churn_mth
                        FROM data_mart_internal_reporting.ir_m_sh_cb_investor_report
                        WHERE churn_mth = true
                        GROUP BY customer_id) cm
                       ON cm.churn_customer_id = r.customer_id
    WHERE r.is_fleet_customer = TRUE
      AND r.flag_future = false
      AND p.entity_type = 'plan'
--      AND subscription_id = 'V13028084'
   AND c.customer_id = 'K36527423' -- 506.35 expansion is correct
--       and c.customer_id = '18941291'  -- 374.35 is correct
--       AND r.customer_id = '18941291'
)
   , customers_2 AS (
    SELECT
        c.customer_id
         , source_system
         , c.customer_country
         , customer_contact_company_name
         , freshsales_account_id
         , first_order_date
         , date_trunc('month',first_order_date)::DATE AS first_order_mth
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

    GROUP BY c.customer_id, source_system, cancellation_dt, customer_country, customer_contact_company_name, freshsales_account_id, first_order_date, churn_mth, customer_status, churn_mth,
             segment_mrr_initial, segment_mrr_current, segment_mrr_most_recent, fs.fs_vehicles_overall, cfg.domain_configuration_product--, product;
)
SELECT * FROM customers_2