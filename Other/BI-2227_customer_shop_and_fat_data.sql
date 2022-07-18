
WITH cte_revenue AS (
    SELECT i.customer_id
         , coalesce(sum(mrr_eur-refund_mrr_eur),0) AS current_mrr_eur
         , coalesce(sum(mrr_eur-refund_mrr_eur) FILTER ( WHERE product_group LIKE 'Logbook%'),0) AS current_mrr_eur_logbook
         , coalesce(sum(mrr_eur-refund_mrr_eur) FILTER ( WHERE product_group = 'Admin'),0) AS current_mrr_eur_admin
         , coalesce(sum(mrr_eur-refund_mrr_eur) FILTER ( WHERE product_group = 'Geo'),0) AS current_mrr_eur_geo
         , coalesce(sum(mrr_eur-refund_mrr_eur) FILTER ( WHERE product_group = 'Pro'),0) AS current_mrr_eur_pro
         , coalesce(sum(mrr_eur-refund_mrr_eur) FILTER ( WHERE product_group NOT IN ('Logbook B2C','Logbook B2B','Admin','Geo','Pro')),0) AS current_mrr_eur_other
         , coalesce(sum(item_quantity-refund_item_quantity) FILTER ( WHERE product_group LIKE 'Logbook%'),0) AS current_items_logbook
         , coalesce(sum(item_quantity-refund_item_quantity) FILTER ( WHERE product_group = 'Admin'),0) AS current_items_admin
         , coalesce(sum(item_quantity-refund_item_quantity) FILTER ( WHERE product_group = 'Geo'),0) AS current_items_geo
         , coalesce(sum(item_quantity-refund_item_quantity) FILTER ( WHERE product_group = 'Pro'),0) AS current_items_pro
         , coalesce(sum(item_quantity-refund_item_quantity) FILTER ( WHERE product_group IN ('Logbook B2C','Logbook B2B','Admin','Geo','Pro')),0) AS current_items
         , coalesce(sum(item_quantity-refund_item_quantity) FILTER ( WHERE product_group NOT IN ('Logbook B2C','Logbook B2B','Admin','Geo','Pro')),0) AS current_items_other
    FROM dwh_main.dim_combined_invoice_line i
             JOIN dwh_main.dim_combined_subscription s
                  ON s.subscription_id = i.subscription_id
             JOIN dwh_main.dim_combined_customer c
                  ON c.customer_id = i.customer_id
    WHERE invoice_status <> 'voided'
      AND invoice_provisioning_start_dt <= current_date
      AND invoice_provisioning_end_dt > current_date
      AND product_group NOT IN ('Hardware', 'Trial')
      AND c.customer_is_fleet = true
--       AND c.customer_id = 'K28704588'
    GROUP BY i.customer_id
)
   , cte_active_cars AS (
    SELECT domain_name
         , sum(1) AS cnt_active_cars
    FROM dwh_main.dim_car
    WHERE car_end_logbook_ts IS NULL GROUP BY domain_name
)
   , cte_tracking AS (
    SELECT s.domain_name
         , SUM(1) FILTER (WHERE s.gl_vehicle_setting_has_tracking) AS cnt_cars_with_tracking -- Geo-Funktionen aktivieren
         , SUM(1) FILTER (WHERE s.gl_vehicle_setting_has_dynamic_tracking) AS cnt_cars_with_dynamic_tracking -- Fahrer/in ermoglichen Geo-Funktionen zu aktivieren
    FROM dwh_main.dim_gl_vehicle_setting s
             JOIN dwh_main.dim_v_fb_vehicle v
                  ON v.fb_vehicle_uuid = split_part(gl_vehicle_urn, '/', 2)::UUID
    WHERE greatest(gl_vehicle_setting_has_tracking, gl_vehicle_setting_has_dynamic_tracking) = true
      AND v.fb_vehicle_end_logbook_ts IS NULL
    GROUP BY s.domain_name
)
   , cte_inventory_booking AS (
    SELECT i.domain AS domain_name
         , SUM(1) cnt_cars_with_booking_enabled
    FROM dwh_main.dim_v_cal_inventory i
             JOIN dwh_main.dim_v_fb_vehicle v
                  ON v.fb_vehicle_uuid = split_part(i.inventory_resource_urn, '/', 2)::UUID
    WHERE i.inventory_is_bookable = true
      AND i.inventory_archived_ts IS NULL
      AND v.fb_vehicle_end_logbook_ts IS NULL
    GROUP BY i.domain_name
)
   , cte_foxbox AS (SELECT cf.*
                         , d.has_logbook_b2c
                         , d.has_fleet_logbook
                         , d.has_fleet_pro
                         , d.has_fleet_admin
                         , d.has_fleet_geo
                         , coalesce(ac.cnt_active_cars, 0) AS cnt_active_cars
                         , coalesce(t.cnt_cars_with_tracking, 0) AS cnt_cars_with_tracking
                         , coalesce(t.cnt_cars_with_dynamic_tracking, 0) AS cnt_cars_with_dynamic_tracking
                         , coalesce(ib.cnt_cars_with_booking_enabled, 0) AS cnt_cars_with_booking_enabled
                    FROM dwh_main.dim_v_dom_configuration cf
                             JOIN dwh_main.dim_v_dom_domain d
                                  ON d.domain_name = cf.domain_name
                             LEFT JOIN cte_active_cars ac
                                    ON ac.domain_name = cf.domain_name
                             LEFT JOIN cte_tracking t
                                    ON t.domain_name = cf.domain_name
                             LEFT JOIN cte_inventory_booking ib
                                       ON ib.domain_name = cf.domain_name
                    WHERE d.domain_is_test = false)
SELECT r.customer_id
     , CASE WHEN c.customer_id_chargebee IS NULL THEN 'Shop' ELSE 'Chargebee' END AS subscription_system
     , c.customer_contact_company_name
     , c.customer_contact_postal_code
     , STRING_AGG(DISTINCT m.map_fb_domain_name, ',') AS foxbox_domain
--      , STRING_AGG(DISTINCT m.map_fb_domain_name ||' ('||mapping_method ||')', ',') AS foxbox_domain
     , NULL AS "Domain configuration"
     , coalesce(true = ANY(ARRAY_AGG(f.has_fleet_admin)), FALSE) AS config_admin
     , coalesce(true = ANY(ARRAY_AGG(f.has_fleet_geo)), FALSE) AS config_geo
     , coalesce(true = ANY(ARRAY_AGG(f.has_fleet_pro)), FALSE) AS config_pro
     , coalesce(true = ANY(ARRAY_AGG(f.has_fleet_logbook)), FALSE) AS config_logbook_b2b
     , coalesce(true = ANY(ARRAY_AGG(f.has_logbook_b2c)), FALSE) AS config_logbook_b2c
     , coalesce(true = ANY(ARRAY_AGG(greatest(f.has_fleet_logbook, f.has_logbook_b2c))), FALSE) AS config_logbook
     , NULL AS "Domain features settings"
     , coalesce(true = ANY(ARRAY_AGG(f.address_visibility_delay)), FALSE) AS address_visibility_delay
     , coalesce(true = ANY(ARRAY_AGG(f.logbook_for_pool_visible)), FALSE) AS logbook_for_pool_visible
     , coalesce(true = ANY(ARRAY_AGG(f.logbook_for_personal_visible)), FALSE) AS logbook_for_personal_visible
     , coalesce(true = ANY(ARRAY_AGG(f.trip_statistics)), FALSE) AS trip_statistics
     , coalesce(true = ANY(ARRAY_AGG(f.private_mode_include_fleet_managers)), FALSE) AS private_mode_include_fleet_managers
     , coalesce(true = ANY(ARRAY_AGG(f.private_trip_locations)), FALSE) AS private_trip_locations
     , coalesce(true = ANY(ARRAY_AGG(f.show_car_type)), FALSE) AS show_car_type
     , coalesce(true = ANY(ARRAY_AGG(f.tax_pdf_timestamps)), FALSE) AS tax_pdf_timestamps
     , coalesce(true = ANY(ARRAY_AGG(f.tax_pdf_drivers)), FALSE) AS tax_pdf_drivers
     , coalesce(true = ANY(ARRAY_AGG(f.csv_exports_trips)), FALSE) AS csv_exports_trips
     , coalesce(true = ANY(ARRAY_AGG(f.general_pdf_trips)), FALSE) AS general_pdf_trips
     , coalesce(true = ANY(ARRAY_AGG(f.contact_required)), FALSE) AS contact_required
     , coalesce(true = ANY(ARRAY_AGG(f.dongle_information_required)), FALSE) AS dongle_information_required
     , coalesce(true = ANY(ARRAY_AGG(f.pregenerated_car_activation)), FALSE) AS pregenerated_car_activation
     , coalesce(true = ANY(ARRAY_AGG(f.cost_import)), FALSE) AS cost_import
     , coalesce(true = ANY(ARRAY_AGG(f.costs)), FALSE) AS costs
     , coalesce(true = ANY(ARRAY_AGG(f.cost_rules)), FALSE) AS cost_rules
     , coalesce(true = ANY(ARRAY_AGG(f.costs_fleet)), FALSE) AS costs_fleet
     , coalesce(true = ANY(ARRAY_AGG(f.contracts)), FALSE) AS contracts
     , coalesce(true = ANY(ARRAY_AGG(f.live_location)), FALSE) AS live_location
     , coalesce(true = ANY(ARRAY_AGG(f.route_tracking)), FALSE) AS route_tracking
     , coalesce(true = ANY(ARRAY_AGG(f.fencing)), FALSE) AS fencing
     , coalesce(true = ANY(ARRAY_AGG(f.fencing_personal)), FALSE) AS fencing_personal
     , coalesce(true = ANY(ARRAY_AGG(f.logbook)), FALSE) AS logbook
     , coalesce(true = ANY(ARRAY_AGG(f.tasks)), FALSE) AS tasks
     , coalesce(true = ANY(ARRAY_AGG(f.fuelcard_pins)), FALSE) AS fuelcard_pins
     , coalesce(true = ANY(ARRAY_AGG(f.booking)), FALSE) AS booking
     , coalesce(true = ANY(ARRAY_AGG(f.files)), FALSE) AS files
     , coalesce(true = ANY(ARRAY_AGG(f.custom_fields)), FALSE) AS custom_fields
     , coalesce(true = ANY(ARRAY_AGG(f.vin_decoding)), FALSE) AS vin_decoding
     , coalesce(true = ANY(ARRAY_AGG(f.driver_license_check_lapid_manual)), FALSE) AS driver_license_check_lapid_manual
     , coalesce(true = ANY(ARRAY_AGG(f.driver_license_check_lapid_advanced)), FALSE) AS driver_license_check_lapid_advanced
     , coalesce(true = ANY(ARRAY_AGG(f.travel_expenses)), FALSE) AS travel_expenses
     , coalesce(true = ANY(ARRAY_AGG(f.cost_centers)), FALSE) AS cost_centers
     , coalesce(true = ANY(ARRAY_AGG(f.odometer_records)), FALSE) AS odometer_records
     , coalesce(true = ANY(ARRAY_AGG(f.key_management_witte_box)), FALSE) AS key_management_witte_box
     , coalesce(true = ANY(ARRAY_AGG(f.default_driver)), FALSE) AS default_driver
     , coalesce(true = ANY(ARRAY_AGG(f.tires)), FALSE) AS tires
     , coalesce(true = ANY(ARRAY_AGG(f.administrators)), FALSE) AS administrators
     , coalesce(true = ANY(ARRAY_AGG(f.privacy_settings)), FALSE) AS privacy_settings
     , coalesce(true = ANY(ARRAY_AGG(f.driver_instructions_lapid)), FALSE) AS driver_instructions_lapid
     , coalesce(true = ANY(ARRAY_AGG(f.groups)), FALSE) AS groups
     , coalesce(true = ANY(ARRAY_AGG(f.co2_reporting)), FALSE) AS co2_reporting
     , coalesce(true = ANY(ARRAY_AGG(f.damage_management)), FALSE) AS damage_management
     , coalesce(true = ANY(ARRAY_AGG(f.driver_behaviour)), FALSE) AS driver_behaviour
     , coalesce(true = ANY(ARRAY_AGG(f.entity_groups)), FALSE) AS entity_groups
     , NULL AS "MRR"
     , r.current_mrr_eur AS "Current MRR"
     , r.current_mrr_eur_logbook AS "MRR Logbook"
     , r.current_mrr_eur_admin AS "MRR Admin"
     , r.current_mrr_eur_geo AS "MRR Geo"
     , r.current_mrr_eur_pro AS "MRR Pro"
     , r.current_mrr_eur_other AS "MRR Other"
     , NULL AS "Cars vs licenses"
     , r.current_items AS "Current items"
     , r.current_items_logbook AS "Items Logbook"
     , r.current_items_admin AS "Items Admin"
     , r.current_items_geo AS "Items Geo"
     , r.current_items_pro AS "Items Pro"
     , r.current_items_other AS "Items Other"
     , sum(f.cnt_active_cars) AS cnt_active_cars
     , sum(f.cnt_cars_with_tracking) AS cnt_cars_with_tracking
--      , sum(f.cnt_cars_with_dynamic_tracking) AS cnt_cars_with_dynamic_tracking
     , sum(f.cnt_cars_with_booking_enabled) AS cnt_cars_with_booking_enabled
     , CASE
           WHEN STRING_AGG(DISTINCT m.map_fb_domain_name, ',') IS NULL
               THEN 'No link to Foxbox'
           WHEN sum(f.cnt_active_cars) > current_items
               THEN 'cars > licenses'
           WHEN sum(f.cnt_active_cars) <= current_items
               THEN 'cars <= licenses'
    END AS cars_vs_items_check
     , CASE
           WHEN STRING_AGG(DISTINCT m.map_fb_domain_name, ',') IS NULL
               THEN 'No link to Foxbox'
           WHEN sum(f.cnt_cars_with_tracking) > (current_items_geo + current_items_pro)
               THEN 'cars > licenses'
           WHEN sum(f.cnt_cars_with_tracking) <= (current_items_geo + current_items_pro)
               THEN 'cars <= licenses'
    END AS cars_with_tracking_vs_items_check
--      , CASE
--            WHEN STRING_AGG(DISTINCT m.map_fb_domain_name, ',') IS NULL
--                THEN 'No link to Foxbox'
--            WHEN sum(f.cnt_cars_with_dynamic_tracking) > (current_items_geo + current_items_pro)
--                THEN 'cars > licenses'
--            WHEN sum(f.cnt_cars_with_dynamic_tracking) <= (current_items_geo + current_items_pro)
--                THEN 'cars <= licenses'
--     END AS cars_with_dynamic_tracking_vs_items_check
     , CASE
           WHEN STRING_AGG(DISTINCT m.map_fb_domain_name, ',') IS NULL
               THEN 'No link to Foxbox'
           WHEN sum(f.cnt_cars_with_booking_enabled) > current_items
               THEN 'cars > licenses'
           WHEN sum(f.cnt_cars_with_booking_enabled) <= current_items
               THEN 'cars <= licenses'
    END AS cars_with_booking_enabled_vs_items_check
FROM cte_revenue r
         JOIN dwh_main.dim_combined_customer c
            ON c.customer_id = r.customer_id
         LEFT JOIN dwh_main.map_customer_to_foxbox_domain m
                   ON r.customer_id = m.map_unified_customer_id
         LEFT JOIN cte_foxbox f
                   ON m.map_fb_domain_name = f.domain_name
                       AND m.record_nbr_per_unified_customer_id = 1
WHERE r.current_mrr_eur > 0
    AND customer_country <> 'GB'
-- AND customer_id IN ('K89964693','K33258999')
GROUP BY current_mrr_eur, r.customer_id, current_mrr_eur_logbook, current_mrr_eur_admin, current_mrr_eur_geo,
         current_mrr_eur_pro, current_mrr_eur_other, r.current_items_logbook, current_items_admin, current_items_geo,
         current_items_pro, current_items, current_items_other, c.customer_id_chargebee, c.customer_contact_company_name,
         c.customer_contact_postal_code
;
