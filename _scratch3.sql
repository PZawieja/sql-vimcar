SELECT * FROM data_mart_internal_reporting.ir_sh_cb_customer WHERE customer_id = '26113805';

SELECT * FROM data_mart_internal_reporting.ir_sh_cb_invoice_line WHERE customer_id = 'K58126111';
SELECT * FROM data_mart_internal_reporting.ir_sh_cb_subscription WHERE customer_id = 'K58126111';

SELECT * FROM data_mart_internal_reporting.ir_sh_cb_invoice_line WHERE plan_id = '1db52694-baee-45fb-902f-8de7293621fe';
SELECT * FROM dwh_main.dim_cb_customer WHERE cb_cust_cf_kundennummer is NOT null;
SELECT * FROM data_mart_internal_reporting.test_ir_m_investor_report WHERE customer_id in ('94963619','169ldVSkbP3oXCYnH');
SELECT * FROM data_mart_internal_reporting.ir_sh_cb_customer WHERE customer_id in ('94963619','169ldVSkbP3oXCYnH');
SELECT * FROM data_mart_internal_reporting.ir_sh_cb_subscription WHERE customer_id in ('94963619','169ldVSkbP3oXCYnH');
SELECT * FROM data_mart_internal_reporting.ir_sh_cb_invoice_line WHERE customer_id in ('94963619','169ldVSkbP3oXCYnH');
SELECT * FROM data_mart_internal_reporting.ir_sh_cb_customer WHERE customer_id = 'K76310569';
SELECT * FROM dwh_main.map_customer_to_foxbox_domain WHERE map_customer_id IN ('169ldVSkbP3oXCYnH','94963619')

SELECT m1.product_name, m2.product_name
FROM dwh_main.umd_product_map m1
left JOIN (SELECT product_name FROM dwh_main.umd_product_map WHERE product_name LIKE '%3_jährige Laufzeit%') m2
    ON m1.product_name = m2.product_name
WHERE m1.product_name LIKE '%3_jährige %'





WITH cte_domains AS (
    SELECT d.domain_name
         , COALESCE(mcpd.map_fb_domain_name,d.domain_name) AS map_principal_domain
         , COALESCE(mcpd.map_fb_main_domain_name,d.main_domain_name) AS map_principal_main_domain
         , d.valid_from_ts::DATE AS valid_from_ts
         , d.valid_to_ts::DATE AS valid_to_ts
         , dt.date_key
         , c.customer_status
         , c.customer_is_fleet
         , c.min_subscription_start_dt
         , domain_contract_status
         , domain_contract_type
         , domain_user_type
         , first_usage_date_key
         , domain_is_active
         , domain_active_car_amt
         , domain_uses_booking_ft
         , domain_uses_contract_ft
         , domain_uses_cost_ft
         , domain_uses_cust_property_ft
         , domain_uses_group_ft
         , domain_uses_documents_ft
         , domain_uses_private_mode_ft
         , domain_uses_vehicle_search_ft
         , domain_uses_address_vis_delay_ft
         , domain_uses_logbook_vis_delay_ft
         , domain_uses_logbook_ft
         , domain_uses_vehicle_localisation_ft
         , domain_uses_lapid_plus_ft
         , domain_uses_manual_dl_check_ft
         , domain_uses_route_documentation_usage_ft
         , domain_uses_route_documentation_ft
         , domain_uses_geo_time_fencing_ft
         , domain_uses_geo_time_fencing_activated_ft
         , domain_uses_pin_ft
         , domain_uses_events_route_documentation_usage_ft
         , domain_uses_events_vehicle_localisation_usage_ft
         , domain_uses_task_ft
    FROM dwh_main.dim_domain_scd2 d
             JOIN dwh_main.dim_date dt
                  ON dt.date_key >= d.valid_from_ts::DATE
                      AND dt.date_key <= d.valid_to_ts::DATE
             LEFT JOIN dwh_main.map_customer_to_foxbox_domain mcpd
                       ON mcpd.map_customer_domain_name = d.domain_name
                           AND record_nbr_per_customer_id = 1
             LEFT JOIN data_mart_internal_reporting.ir_sh_cb_customer c
                       ON c.customer_id = mcpd.map_unified_customer_id -- (CB or Shop customer ID, depending on if already migrated or not)
    WHERE TRUE
      AND dt.date_key BETWEEN current_date::DATE - 400 AND current_date - 1
      AND NOT EXISTS (SELECT domain_name
                      FROM dwh_main.dim_domain_scd2 t
                      WHERE is_last_version = TRUE
                        AND domain_is_test_domain = TRUE
                        AND t.domain_name = d.domain_name
                      GROUP BY domain_name)
      AND COALESCE(mcpd.map_fb_main_domain_name,d.main_domain_name) != 'com.vimcar'
-- sample problematic customer, domains mismatch: FOXBOX: 'com.vimcar.foodspring-gmbh' | SHOP: 'com.vimcar.de.k96844926'
--     AND d.domain_name IN ('com.vimcar.de.k96844926', 'com.vimcar.foodspring-gmbh')
--       AND d.domain_name = 'com.vimcar.gb.a19b21c13db0492e81ed392f85d6510c'
        AND domain_name = 'com.vimcar.26113805'
)
SELECT
    c1.map_principal_main_domain AS main_domain_name
     , c1.date_key AS reporting_date
     , CASE WHEN mds.match_to_contract IS NULL THEN FALSE ELSE mds.match_to_contract END AS match_to_contract
     , MIN(coalesce(c1.min_subscription_start_dt, '2099-12-31')) AS contract_started_dt
     , MIN(coalesce(c2.first_usage_date_key, c1.first_usage_date_key)) AS first_usage_dt
     , MIN(c1.valid_from_ts) AS valid_from_dt
     , MAX(c1.valid_to_ts) AS valid_to_dt
     , MIN(coalesce(c1.domain_contract_status, c1.domain_contract_status)) AS domain_contract_status
     , CASE WHEN mds.match_to_contract IS NOT NULL THEN 'Customer' ELSE 'Tester' END AS domain_contract_type
--     , MIN(c1.domain_user_type) AS domain_user_type -- definition Product
     , CASE WHEN true = ANY(ARRAY_AGG(c1.customer_is_fleet)) = TRUE THEN 'Fleet' ELSE 'Single' END AS domain_user_type -- definition BI and Management
     --  , CASE WHEN SUM(CASE WHEN c1.domain_is_active = TRUE THEN 1 ELSE 0 END)>0 THEN TRUE ELSE FALSE END AS domain_is_active
     , true = ANY(ARRAY_AGG(c1.domain_is_active)) AS domain_is_active
     , MAX(coalesce(c2.domain_active_car_amt, c1.domain_active_car_amt))  AS domain_active_car_amt
     , true = ANY(ARRAY_AGG(coalesce(c2.domain_uses_booking_ft, c1.domain_uses_booking_ft))) AS domain_uses_booking_ft
     , true = ANY(ARRAY_AGG(coalesce(c2.domain_uses_contract_ft, c1.domain_uses_contract_ft))) AS domain_uses_contract_ft
     , true = ANY(ARRAY_AGG(coalesce(c2.domain_uses_cost_ft, c1.domain_uses_cost_ft))) AS domain_uses_cost_ft
     , true = ANY(ARRAY_AGG(coalesce(c2.domain_uses_cust_property_ft, c1.domain_uses_cust_property_ft))) AS domain_uses_cust_property_ft
     , true = ANY(ARRAY_AGG(coalesce(c2.domain_uses_group_ft, c1.domain_uses_group_ft))) AS domain_uses_group_ft
     , true = ANY(ARRAY_AGG(coalesce(c2.domain_uses_documents_ft, c1.domain_uses_documents_ft))) AS domain_uses_documents_ft
     , true = ANY(ARRAY_AGG(coalesce(c2.domain_uses_private_mode_ft, c1.domain_uses_private_mode_ft))) AS domain_uses_private_mode_ft
     , true = ANY(ARRAY_AGG(coalesce(c2.domain_uses_vehicle_search_ft, c1.domain_uses_vehicle_search_ft))) AS domain_uses_vehicle_search_ft
     , true = ANY(ARRAY_AGG(coalesce(c2.domain_uses_address_vis_delay_ft, c1.domain_uses_address_vis_delay_ft))) AS domain_uses_address_vis_delay_ft
     , true = ANY(ARRAY_AGG(coalesce(c2.domain_uses_logbook_vis_delay_ft, c1.domain_uses_logbook_vis_delay_ft))) AS domain_uses_logbook_vis_delay_ft
     , true = ANY(ARRAY_AGG(coalesce(c2.domain_uses_logbook_ft, c1.domain_uses_logbook_ft))) AS domain_uses_logbook_ft
     , true = ANY(ARRAY_AGG(coalesce(c2.domain_uses_vehicle_localisation_ft, c1.domain_uses_vehicle_localisation_ft))) AS domain_uses_vehicle_localisation_ft
     , true = ANY(ARRAY_AGG(coalesce(c2.domain_uses_lapid_plus_ft, c1.domain_uses_lapid_plus_ft))) AS domain_uses_lapid_plus_ft
     , true = ANY(ARRAY_AGG(coalesce(c2.domain_uses_manual_dl_check_ft, c1.domain_uses_manual_dl_check_ft))) AS domain_uses_manual_dl_check_ft
     , true = ANY(ARRAY_AGG(coalesce(c2.domain_uses_route_documentation_usage_ft, c1.domain_uses_route_documentation_usage_ft))) AS domain_uses_route_documentation_usage_ft
     , true = ANY(ARRAY_AGG(coalesce(c2.domain_uses_route_documentation_ft, c1.domain_uses_route_documentation_ft))) AS domain_uses_route_documentation_ft
     , true = ANY(ARRAY_AGG(coalesce(c2.domain_uses_geo_time_fencing_ft, c1.domain_uses_geo_time_fencing_ft))) AS domain_uses_geo_time_fencing_ft
     , true = ANY(ARRAY_AGG(coalesce(c2.domain_uses_geo_time_fencing_activated_ft, c1.domain_uses_geo_time_fencing_activated_ft))) AS domain_uses_geo_time_fencing_activated_ft
     , true = ANY(ARRAY_AGG(coalesce(c2.domain_uses_pin_ft, c1.domain_uses_pin_ft))) AS domain_uses_pin_ft
     , true = ANY(ARRAY_AGG(coalesce(c2.domain_uses_events_route_documentation_usage_ft, c1.domain_uses_events_route_documentation_usage_ft))) AS domain_uses_events_route_documentation_usage_ft
     , true = ANY(ARRAY_AGG(coalesce(c2.domain_uses_events_vehicle_localisation_usage_ft, c1.domain_uses_events_vehicle_localisation_usage_ft))) AS domain_uses_events_vehicle_localisation_usage_ft
     , true = ANY(ARRAY_AGG(coalesce(c2.domain_uses_task_ft, c1.domain_uses_task_ft))) AS domain_uses_task_ft
     -- product fields:
     , true = ANY(ARRAY_AGG(mds.md_has_logbook_non_fleet)) AS md_has_logbook_non_fleet
     , true = ANY(ARRAY_AGG(mds.md_has_fleet_logbook)) AS md_has_fleet_logbook
     , true = ANY(ARRAY_AGG(mds.md_has_fleet_admin)) AS md_has_fleet_admin
     , true = ANY(ARRAY_AGG(mds.md_has_fleet_geo)) AS md_has_fleet_geo
     , true = ANY(ARRAY_AGG(mds.md_has_fleet_pro)) AS md_has_fleet_pro
FROM cte_domains c1
         LEFT JOIN cte_domains c2
                   ON c1.map_principal_domain = c2.domain_name
                       AND c1.date_key = c2.date_key
         LEFT JOIN dwh_main.dim_main_domain_stat_scd2 mds
                   ON c1.map_principal_main_domain = mds.main_domain_foxbox
                       AND c1.date_key >= mds.valid_from_ts::DATE
                       AND c1.date_key < mds.valid_to_ts::DATE
GROUP BY c1.map_principal_main_domain, c1.date_key, mds.match_to_contract
ORDER BY c1.map_principal_main_domain, c1.date_key
;

SELECT 'Shop' as source_system, date_key, product_group
FROM dwh_main.dim_date
         FULL OUTER JOIN (SELECT DISTINCT
                              CASE WHEN product_group LIKE 'Logbook%' THEN 'Logbook' ELSE product_group END AS product_group
                          FROM data_mart_internal_reporting.ir_sh_cb_invoice_line upm) prod ON 1 = 1
WHERE date_key BETWEEN '2019-01-01' AND current_date

UNION ALL

SELECT 'Chargebee' as source_system, date_key, product_group
FROM dwh_main.dim_date
         FULL OUTER JOIN (SELECT DISTINCT
                              CASE WHEN product_group LIKE 'Logbook%' THEN 'Logbook' ELSE product_group END AS product_group
                          FROM data_mart_internal_reporting.ir_sh_cb_invoice_line upm) prod ON 1 = 1
WHERE date_key BETWEEN '2019-01-01' AND current_date;

SELECT product_group FROM data_mart_internal_reporting.ir_sh_cb_invoice_line GROUP BY 1;
SELECT cb_subscr_id ,cb_subscr_cancelled_ts, cb_subscr_current_term_end_ts FROM dwh_main.dim_cb_subscription WHERE cb_subscr_cancelled_ts is not null;
SELECT cb_event_type FROM dwh_main.dim_cb_event GROUP BY 1