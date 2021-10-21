WITH domain_conf AS (
    SELECT domain_name
         , row_to_json((SELECT d FROM (SELECT address_visibility_delay
                                            , logbook_visibility_delay_minutes
                                            , logbook_for_pool_visible
                                            , logbook_for_personal_visible
                                            , trip_statistics
                                            , private_mode_include_fleet_managers
                                            , private_trip_locations
                                            , show_car_type
                                            , tax_pdf_timestamps
                                            , tax_pdf_drivers
                                            , csv_exports_trips
                                            , general_pdf_trips
                                            , contact_required
                                            , dongle_information_required
                                            , pregenerated_car_activation
                                            , costs
                                            , cost_rules
                                            , costs_fleet
                                            , contracts
                                            , live_location
                                            , route_tracking
                                            , fencing
                                            , fencing_personal
                                            , logbook
                                            , task_delegation
                                            , tasks
                                            , fuelcard_pins
                                            , booking
                                            , files
                                            , custom_fields
                                            , vin_decoding
                                            , driver_license_check_lapid_manual
                                            , driver_license_check_lapid_advanced
                                            , travel_expenses
                                            , cost_centers
                                            , odometer_records
                                            , key_management_witte_box
                                            , default_driver
                                            , tires
                                            , administrators
                                            , privacy_settings
                                            , driver_instructions_lapid
                                            , groups
                                            , co2_reporting
                                            , damage_management
                                            , username_prefix
                                            , custom_usernames
                                            , email_placeholder_domain
                                            , email_required ) d)) AS configuration

    FROM dwh_main.dim_v_dom_configuration
-- WHERE domain_name = 'com.vimcar.de.k59294822' --https://operations.vimcar.com/dashboard/contract/V96462436 has Mietmodell and all functions activated
-- WHERE domain_name = 'com.vimcar.de.k22438069'  - has logbook b2c in Shop
-- WHERE domain_name = 'com.vimcar.de.10426869'  -- has logbook b2c and Geo in Shop
    WHERE domain_name = 'com.vimcar.26113805'  -- has logbook b2c and Geo in Shop
)
   , configuration_template AS (SELECT domain_configuration_template_name
                                     , json_strip_nulls(ROW_TO_JSON((SELECT d
                                                                     FROM (SELECT trip_statistics
                                                                                , tax_pdf_drivers
                                                                                , csv_exports_trips
                                                                                , costs
                                                                                , cost_rules
                                                                                , costs_fleet
                                                                                , contracts
                                                                                , live_location
                                                                                , route_tracking
                                                                                , fencing
                                                                                , logbook
                                                                                , task_delegation
                                                                                , tasks
                                                                                , fuelcard_pins
                                                                                , booking
                                                                                , files
                                                                                , custom_fields
                                                                                , vin_decoding
                                                                                , driver_license_check_lapid_manual
                                                                                , cost_centers
                                                                                , odometer_records
                                                                                , default_driver
                                                                                , administrators
                                                                                , privacy_settings
                                                                                , driver_instructions_lapid
                                                                                , groups
                                                                                , co2_reporting) d))) AS configuration
                                FROM dwh_main.dim_v_dom_configuration_template
)
   , cte_matching AS (
    SELECT domain_name
         , dc.configuration
         , ct.configuration
         , ct.domain_configuration_template_name
    FROM domain_conf dc
             JOIN configuration_template ct
                  ON ct.configuration::jsonb <@ dc.configuration::jsonb = TRUE
)
SELECT
    domain_name
     , true = ANY(ARRAY_AGG(CASE WHEN domain_configuration_template_name = 'B2C Logbook' THEN TRUE END)) AS has_logbook_b2c
     , true = ANY(ARRAY_AGG(CASE WHEN domain_configuration_template_name = 'Fleet Logbook' THEN TRUE END)) AS has_fleet_logbook
     , true = ANY(ARRAY_AGG(CASE WHEN domain_configuration_template_name = 'Fleet Admin' THEN TRUE END)) AS has_fleet_admin
     , true = ANY(ARRAY_AGG(CASE WHEN domain_configuration_template_name = 'Fleet Geo' THEN TRUE END)) AS has_fleet_geo
     , true = ANY(ARRAY_AGG(CASE WHEN domain_configuration_template_name = 'Fleet Pro' THEN TRUE END)) AS has_fleet_pro
     , true = ANY(ARRAY_AGG(CASE WHEN domain_configuration_template_name = 'Fleet Admin UK' THEN TRUE END)) AS has_fleet_admin_uk
     , true = ANY(ARRAY_AGG(CASE WHEN domain_configuration_template_name = 'Fleet Pro UK' THEN TRUE END)) AS has_fleet_pro_uk
FROM cte_matching
GROUP BY domain_name;
