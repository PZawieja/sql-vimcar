
WITH domain_conf AS (
    SELECT
        domain_uuid
         , ROW_TO_JSON((SELECT d
                        FROM (SELECT address_visibility_delay
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
                                   --  , task_delegation
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
                                   , entity_groups
                                   , username_prefix
                                   , custom_usernames
                                   , email_placeholder_domain
                                   , driver_behaviour
                                   , entity_groups
                                   , email_required) d)) AS domain_configuration
    FROM dwh_main.dim_v_dom_configuration
-- WHERE domain_name = 'com.vimcar.de.k59294822' --https://operations.vimcar.com/dashboard/contract/V96462436 has Mietmodell and all functions activated
-- WHERE domain_name = 'com.vimcar.de.k22438069'  - has logbook b2c in Shop
-- WHERE domain_name = 'com.vimcar.de.10426869' -- has logbook b2c and Geo in Shopp
    WHERE domain_name IN ('com.vimcar.de.k71685685','com.vimcar.diakonie-bayreuth')
)
   , configuration_template AS (
    SELECT
        domain_configuration_template_name
         , JSON_STRIP_NULLS(ROW_TO_JSON((SELECT d
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
                                                    -- , task_delegation
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
                                                    , co2_reporting
                                                    , key_management_witte_box
                                                    , damage_management
                                                    , driver_behaviour
                                                    , entity_groups
                                                    , entity_groups) d))) AS template_configuration
    FROM dwh_main.dim_v_dom_configuration_template)
   , cte_matching AS (
    SELECT
        domain_uuid
         , dc.domain_configuration
         , ct.template_configuration
         , ct.domain_configuration_template_name
    FROM domain_conf dc
             JOIN configuration_template ct
                  ON ct.template_configuration::jsonb <@ dc.domain_configuration::jsonb = TRUE
    WHERE ct.template_configuration::text <> '{}'::text -- ignore empty templates
)
SELECT
    domain_uuid
     , true = ANY(ARRAY_AGG(CASE WHEN domain_configuration_template_name = 'B2C Logbook' THEN TRUE END)) AS has_logbook_b2c
     , true = ANY(ARRAY_AGG(CASE WHEN domain_configuration_template_name = 'Fleet Logbook' THEN TRUE END)) AS has_fleet_logbook
     , true = ANY(ARRAY_AGG(CASE WHEN domain_configuration_template_name = 'Fleet Admin' THEN TRUE END)) AS has_fleet_admin
     , true = ANY(ARRAY_AGG(CASE WHEN domain_configuration_template_name = 'Fleet Geo' THEN TRUE END)) AS has_fleet_geo
     , true = ANY(ARRAY_AGG(CASE WHEN domain_configuration_template_name = 'Fleet Pro' THEN TRUE END)) AS has_fleet_pro
     , true = ANY(ARRAY_AGG(CASE WHEN domain_configuration_template_name = 'Fleet Admin UK' THEN TRUE END)) AS has_fleet_admin_uk
     , true = ANY(ARRAY_AGG(CASE WHEN domain_configuration_template_name = 'Fleet Pro UK' THEN TRUE END)) AS has_fleet_pro_uk
     , true = ANY(ARRAY_AGG(CASE WHEN domain_configuration_template_name = 'Fleet Share' THEN TRUE END)) AS has_fleet_share
     , true = ANY(ARRAY_AGG(CASE WHEN domain_configuration_template_name = 'Damage Management' THEN TRUE END)) AS has_damage_management
     , true = ANY(ARRAY_AGG(CASE WHEN domain_configuration_template_name = 'Driver Behaviour' THEN TRUE END)) AS has_driver_behaviour
     , true = ANY(ARRAY_AGG(CASE WHEN domain_configuration_template_name = 'Fleet Pro light' THEN TRUE END)) AS has_fleet_pro_light
     , true = ANY(ARRAY_AGG(CASE WHEN domain_configuration_template_name = 'Fleet Geo light' THEN TRUE END)) AS has_fleet_geo_light
     , true = ANY(ARRAY_AGG(CASE WHEN domain_configuration_template_name = 'Fleet Admin light' THEN TRUE END)) AS has_fleet_admin_light
     , true = ANY(ARRAY_AGG(CASE WHEN domain_configuration_template_name = 'Logbook light' THEN TRUE END)) AS has_logbook_light
     , true = ANY(ARRAY_AGG(CASE WHEN domain_configuration_template_name = 'Fleet Geo Live' THEN TRUE END)) AS has_fleet_geo_live
     , string_agg(domain_configuration_template_name, ',' order by domain_configuration_template_name) AS domain_configuration_template_match
FROM cte_matching
GROUP BY domain_uuid
;