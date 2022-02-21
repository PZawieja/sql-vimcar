SELECT * FROM dwh_main.dim_main_domain_stat_scd2 WHERE main_domain_foxbox IN ('com.vimcar.de.k96844926', 'com.vimcar.foodspring-gmbh');
SELECT * FROM dwh_main.dim_combined_invoice_line WHERE subscription_id = 'AzyXbWSsKjQif3isa';
SELECT * FROM dwh_main.dim_combined_subscription WHERE subscription_id = 'AzyXbWSsKjQif3isa';
;


WITH cte_domains_in_scope AS (
    SELECT domain_name
         , greatest(dd.has_logbook_b2c, dd.has_fleet_logbook, dd.has_logbook_light)         AS domain_has_logbook
         , greatest(dd.has_fleet_admin, dd.has_fleet_admin_uk, dd.has_fleet_admin_light)    AS domain_has_admin
         , greatest(dd.has_fleet_geo, dd.has_fleet_geo_light)                               AS domain_has_geo
         , greatest(dd.has_fleet_pro, dd.has_fleet_pro_uk, dd.has_fleet_pro_light)          AS domain_has_pro
         , dd.has_logbook_b2c                                                               AS domain_has_logbook_b2c
         , dd.has_fleet_logbook                                                             AS domain_has_fleet_logbook
         , dd.has_fleet_share                                                               AS domain_has_fleet_share
         , dd.has_damage_management                                                         AS domain_has_damage_management
         , dd.has_driver_behaviour                                                          AS domain_has_driver_behaviour
         , greatest(dd.has_fleet_pro, dd.has_fleet_pro_uk, dd.has_fleet_pro_light
        , dd.has_fleet_admin, dd.has_fleet_admin_uk, dd.has_fleet_admin_light
        , dd.has_fleet_geo, dd.has_fleet_geo_light, dd.has_fleet_logbook
        , dd.has_damage_management, dd.has_driver_behaviour, dd.has_fleet_share)        AS domain_has_fleet_product
    FROM dwh_main.dim_v_dom_domain dd
    WHERE dd.domain_is_test = false
      AND dd.domain_name IN ('com.vimcar.de.k96844926', 'com.vimcar.foodspring-gmbh')
)
   , cte_activity_fleet_event AS (
    SELECT p.domain_name, MAX(a.amp_event_ts AT TIME ZONE 'Europe/Berlin')::DATE AS last_activity_dt_cet
    FROM dwh_main.dim_amp_event_fleet a
             JOIN dwh_main.dim_principal p ON p.principal_uuid = a.principal_uuid::UUID
             JOIN cte_domains_in_scope d ON p.domain_name = d.domain_name
    WHERE a.amp_event_ts >= CURRENT_DATE - ${nbr_days}
    GROUP BY p.domain_name)

   , cte_activity_trip_logbook AS (SELECT t.domain_name, MAX(trip_start_date_cet) AS last_activity_dt_cet
                                   FROM dwh_main.dim_trip t
                                            JOIN cte_domains_in_scope d ON t.domain_name = d.domain_name
                                   WHERE trip_start_date_cet >= CURRENT_DATE -  ${nbr_days}
                                   GROUP BY t.domain_name)
   , cte_activity_trip_geo AS (SELECT t.domain_name, MAX(gl_trip_start_date_cet) AS last_activity_dt_cet
                               FROM dwh_main.dim_v_gl_trip t
                                        JOIN cte_domains_in_scope d ON t.domain_name = d.domain_name
                               WHERE gl_trip_start_date_cet >= CURRENT_DATE - ${nbr_days}
                               GROUP BY t.domain_name)
   , cte_activity_session AS (SELECT p.domain_name
                                   , MAX(s.v_id_user_session_last_access_ts AT TIME ZONE 'Europe/Berlin')::DATE AS last_activity_dt_cet
                              FROM dwh_main.dim_v_id_user_session s
                                       JOIN dwh_main.dim_principal p ON s.v_id_user_id = p.principal_uuid
                                       JOIN cte_domains_in_scope d ON p.domain_name = d.domain_name
                              WHERE s.v_id_user_session_last_access_ts >= CURRENT_DATE -  ${nbr_days}
                              GROUP BY p.domain_name)
;

---------------------------------------------------------------------------
WITH cte_domains_in_scope AS (
    SELECT domain_name
         , greatest(dd.has_logbook_b2c, dd.has_fleet_logbook, dd.has_logbook_light)         AS domain_has_logbook
         , greatest(dd.has_fleet_admin, dd.has_fleet_admin_uk, dd.has_fleet_admin_light)    AS domain_has_admin
         , greatest(dd.has_fleet_geo, dd.has_fleet_geo_light)                               AS domain_has_geo
         , greatest(dd.has_fleet_pro, dd.has_fleet_pro_uk, dd.has_fleet_pro_light)          AS domain_has_pro
         , dd.has_logbook_b2c                                                               AS domain_has_logbook_b2c
         , dd.has_fleet_logbook                                                             AS domain_has_fleet_logbook
         , dd.has_fleet_share                                                               AS domain_has_fleet_share
         , dd.has_damage_management                                                         AS domain_has_damage_management
         , dd.has_driver_behaviour                                                          AS domain_has_driver_behaviour
         , greatest(dd.has_fleet_pro, dd.has_fleet_pro_uk, dd.has_fleet_pro_light
        , dd.has_fleet_admin, dd.has_fleet_admin_uk, dd.has_fleet_admin_light
        , dd.has_fleet_geo, dd.has_fleet_geo_light, dd.has_fleet_logbook
        , dd.has_damage_management, dd.has_driver_behaviour, dd.has_fleet_share)        AS domain_has_fleet_product
    FROM dwh_main.dim_v_dom_domain dd
    WHERE dd.domain_is_test = false
    AND dd.domain_name = 'com.vimcar.de.k30846175'
--       AND dd.domain_name IN ('com.vimcar.de.k96844926', 'com.vimcar.foodspring-gmbh')
)
   , cte_calc AS (
    SELECT d.domain_name
         , COALESCE(mcpd.map_fb_domain_name,d.domain_name) AS map_principal_domain
         , COALESCE(mcpd.map_fb_main_domain_name,d.main_domain_name) AS map_principal_main_domain
         , d.valid_from_ts::DATE AS valid_from_ts
         , d.valid_to_ts::DATE AS valid_to_ts
         , dt.date_key
         , CASE WHEN c.customer_status IS NOT NULL THEN TRUE ELSE FALSE END AS match_to_subscription
         , coalesce(CASE WHEN c.customer_status = 'cancelled' THEN 'cancelled' ELSE 'active' END, 'unknown') AS customer_status
         , d.domain_is_active
         , c.customer_is_fleet
         , c.min_subscription_start_dt
         , first_usage_date_key
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
         , dd.domain_has_logbook
         , dd.domain_has_admin
         , dd.domain_has_geo
         , dd.domain_has_pro
         , dd.domain_has_logbook_b2c
         , dd.domain_has_fleet_logbook
         , dd.domain_has_fleet_share
         , dd.domain_has_damage_management
         , dd.domain_has_driver_behaviour
         , dd.domain_has_fleet_product
    FROM dwh_main.dim_domain_scd2 d
             JOIN cte_domains_in_scope dd
                  ON dd.domain_name = d.domain_name
             JOIN dwh_main.dim_date dt
                  ON dt.date_key >= d.valid_from_ts::DATE
                      AND dt.date_key <= d.valid_to_ts::DATE
             LEFT JOIN dwh_main.map_customer_to_foxbox_domain mcpd
                       ON mcpd.map_customer_domain_name = d.domain_name
                           AND record_nbr_per_customer_id = 1
             LEFT JOIN dwh_main.dim_combined_customer c
                       ON c.customer_id = mcpd.map_unified_customer_id -- (CB or Shop customer ID, depending on if already migrated or not)
    WHERE dt.date_key BETWEEN '2021-12-01' AND current_date - 1
      AND COALESCE(mcpd.map_fb_main_domain_name,d.main_domain_name) != 'com.vimcar'
-- sample problematic customer, domains mismatch: FOXBOX: 'com.vimcar.foodspring-gmbh' | SHOP: 'com.vimcar.de.k96844926'
--         AND d.domain_name IN ('com.vimcar.de.k96844926', 'com.vimcar.foodspring-gmbh')
--       AND d.domain_name = 'com.vimcar.gb.a19b21c13db0492e81ed392f85d6510c'
)
SELECT
    c1.domain_name
     , c1.date_key AS reporting_date
     , CASE WHEN true = ANY(ARRAY_AGG(greatest(c2.domain_has_fleet_product, c1.domain_has_fleet_product))) = TRUE THEN 'B2B' ELSE 'B2C' END AS domain_customer_type -- based on the config template
     , true = ANY(ARRAY_AGG(greatest(c2.domain_is_active, c1.domain_is_active))) AS domain_is_active
------- Shop & Chargebee:
     , CASE WHEN true = ANY(ARRAY_AGG(greatest(c2.match_to_subscription, c1.match_to_subscription))) = TRUE THEN TRUE ELSE FALSE END    AS match_to_subscription
     , CASE WHEN true = ANY(ARRAY_AGG(greatest(c2.customer_is_fleet, c1.customer_is_fleet))) = TRUE THEN 'B2B' ELSE 'B2C' END           AS subscription_customer_type -- definition BI and Management
     , MIN(coalesce(c2.customer_status, c1.customer_status))                                                                            AS subscription_customer_status
     , MIN(least(c2.min_subscription_start_dt, c1.min_subscription_start_dt))                                                           AS min_subscription_start_dt
     , MIN(least(c2.first_usage_date_key, c1.first_usage_date_key))                                                                     AS first_usage_dt
     , MIN(c1.valid_from_ts) AS valid_from_dt
     , MAX(c1.valid_to_ts) AS valid_to_dt
     , MAX(coalesce(c2.domain_active_car_amt, c1.domain_active_car_amt))  AS domain_active_car_amt
     , true = ANY(ARRAY_AGG(greatest(c2.domain_uses_booking_ft, c1.domain_uses_booking_ft))) AS domain_uses_booking_ft
     , true = ANY(ARRAY_AGG(greatest(c2.domain_uses_contract_ft, c1.domain_uses_contract_ft))) AS domain_uses_contract_ft
     , true = ANY(ARRAY_AGG(greatest(c2.domain_uses_cost_ft, c1.domain_uses_cost_ft))) AS domain_uses_cost_ft
     , true = ANY(ARRAY_AGG(greatest(c2.domain_uses_cust_property_ft, c1.domain_uses_cust_property_ft))) AS domain_uses_cust_property_ft
     , true = ANY(ARRAY_AGG(greatest(c2.domain_uses_group_ft, c1.domain_uses_group_ft))) AS domain_uses_group_ft
     , true = ANY(ARRAY_AGG(greatest(c2.domain_uses_documents_ft, c1.domain_uses_documents_ft))) AS domain_uses_documents_ft
     , true = ANY(ARRAY_AGG(greatest(c2.domain_uses_private_mode_ft, c1.domain_uses_private_mode_ft))) AS domain_uses_private_mode_ft
     , true = ANY(ARRAY_AGG(greatest(c2.domain_uses_vehicle_search_ft, c1.domain_uses_vehicle_search_ft))) AS domain_uses_vehicle_search_ft
     , true = ANY(ARRAY_AGG(greatest(c2.domain_uses_address_vis_delay_ft, c1.domain_uses_address_vis_delay_ft))) AS domain_uses_address_vis_delay_ft
     , true = ANY(ARRAY_AGG(greatest(c2.domain_uses_logbook_vis_delay_ft, c1.domain_uses_logbook_vis_delay_ft))) AS domain_uses_logbook_vis_delay_ft
     , true = ANY(ARRAY_AGG(greatest(c2.domain_uses_logbook_ft, c1.domain_uses_logbook_ft))) AS domain_uses_logbook_ft
     , true = ANY(ARRAY_AGG(greatest(c2.domain_uses_vehicle_localisation_ft, c1.domain_uses_vehicle_localisation_ft))) AS domain_uses_vehicle_localisation_ft
     , true = ANY(ARRAY_AGG(greatest(c2.domain_uses_lapid_plus_ft, c1.domain_uses_lapid_plus_ft))) AS domain_uses_lapid_plus_ft
     , true = ANY(ARRAY_AGG(greatest(c2.domain_uses_manual_dl_check_ft, c1.domain_uses_manual_dl_check_ft))) AS domain_uses_manual_dl_check_ft
     , true = ANY(ARRAY_AGG(greatest(c2.domain_uses_route_documentation_usage_ft, c1.domain_uses_route_documentation_usage_ft))) AS domain_uses_route_documentation_usage_ft
     , true = ANY(ARRAY_AGG(greatest(c2.domain_uses_route_documentation_ft, c1.domain_uses_route_documentation_ft))) AS domain_uses_route_documentation_ft
     , true = ANY(ARRAY_AGG(greatest(c2.domain_uses_geo_time_fencing_ft, c1.domain_uses_geo_time_fencing_ft))) AS domain_uses_geo_time_fencing_ft
     , true = ANY(ARRAY_AGG(greatest(c2.domain_uses_geo_time_fencing_activated_ft, c1.domain_uses_geo_time_fencing_activated_ft))) AS domain_uses_geo_time_fencing_activated_ft
     , true = ANY(ARRAY_AGG(greatest(c2.domain_uses_pin_ft, c1.domain_uses_pin_ft))) AS domain_uses_pin_ft
     , true = ANY(ARRAY_AGG(greatest(c2.domain_uses_events_route_documentation_usage_ft, c1.domain_uses_events_route_documentation_usage_ft))) AS domain_uses_events_route_documentation_usage_ft
     , true = ANY(ARRAY_AGG(greatest(c2.domain_uses_events_vehicle_localisation_usage_ft, c1.domain_uses_events_vehicle_localisation_usage_ft))) AS domain_uses_events_vehicle_localisation_usage_ft
     , true = ANY(ARRAY_AGG(greatest(c2.domain_uses_task_ft, c1.domain_uses_task_ft))) AS domain_uses_task_ft
     -- product fields:
     , true = ANY(ARRAY_AGG(greatest(c2.domain_has_logbook, c1.domain_has_logbook))) AS domain_has_logbook
     , true = ANY(ARRAY_AGG(greatest(c2.domain_has_admin, c1.domain_has_admin))) AS domain_has_admin
     , true = ANY(ARRAY_AGG(greatest(c2.domain_has_geo, c1.domain_has_geo))) AS domain_has_geo
     , true = ANY(ARRAY_AGG(greatest(c2.domain_has_pro, c1.domain_has_pro))) AS domain_has_pro
     , true = ANY(ARRAY_AGG(greatest(c2.domain_has_fleet_share, c1.domain_has_fleet_share))) AS domain_has_fleet_share
     , true = ANY(ARRAY_AGG(greatest(c2.domain_has_damage_management, c1.domain_has_damage_management))) AS domain_has_damage_management
     , true = ANY(ARRAY_AGG(greatest(c2.domain_has_driver_behaviour, c1.domain_has_driver_behaviour))) AS domain_has_driver_behaviour
FROM cte_calc c1
         LEFT JOIN cte_calc c2
                   ON c1.map_principal_domain = c2.domain_name
                       AND c1.date_key = c2.date_key
GROUP BY c1.domain_name, c1.date_key, c1.customer_status
;







-- Amplitude:
SELECT p.domain_name
     , MAX(a.amp_event_ts AT TIME ZONE 'Europe/Berlin')::DATE AS last_activity_dt_cet
FROM dwh_main.dim_amp_event_fleet a
         JOIN dwh_main.dim_principal p
              ON p.principal_uuid= a.principal_uuid::UUID
WHERE a.amp_event_ts >= CURRENT_DATE - 14

UNION ALL
-- trips:
SELECT domain_name, trip_start_date_cet AS last_activity_dt_cet
FROM dwh_main.dim_trip
WHERE trip_start_date_cet >= CURRENT_DATE - 14

UNION ALL
-- trips geo loc.:
SELECT domain_name, gl_trip_start_date_cet  AS last_activity_dt_cet
FROM dwh_main.dim_v_gl_trip
WHERE gl_trip_start_date_cet >= CURRENT_DATE - 14

UNION ALL
-- sessions:
SELECT p.domain_name, (s.v_id_user_session_last_access_ts AT TIME ZONE 'Europe/Berlin')::DATE AS last_activity_dt_cet
FROM dwh_main.dim_v_id_user_session s
         JOIN dwh_main.dim_principal p
              ON s.v_id_user_id = p.principal_uuid
WHERE s.v_id_user_session_last_access_ts >= CURRENT_DATE - 14


