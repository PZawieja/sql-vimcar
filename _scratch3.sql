





WITH conf AS (
SELECT
    c.configuration_id AS domain_configuration_uuid
     , c.domain_id AS domain_uuid
     , trim(trailing '.' FROM d.domain) AS domain_name
     -- configuration_data_privacy
     , address_visibility_delay
     , logbook_visibility_delay_minutes::smallint
     , logbook_for_pool_visible
     , logbook_for_personal_visible
     , trip_statistics
     , private_mode_include_fleet_managers
     , private_trip_locations
     , show_car_type
     -- configuration_export
     , tax_pdf_timestamps
     , tax_pdf_drivers
     , csv_exports_trips
     , general_pdf_trips
     -- configuration_logbook
     , contact_required
     -- configuration_process
     , dongle_information_required
     , pregenerated_car_activation
     -- configuration_product
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
--      , configuration_user
     , username_prefix
     , custom_usernames
     , email_placeholder_domain
     , email_required
     , c.creator AS dom_config_creator
     , c.created AS dom_config_created_ts
     , c.modifier AS dom_config_modifier
     , c.modified dom_config_modified_ts
FROM public.domain d
         JOIN public.configuration c
              ON c.domain_id = d.domain_id
         JOIN public.configuration_data_privacy cdp
              ON cdp.configuration_id = c.configuration_id
         JOIN public.configuration_export ce
              ON ce.configuration_id = c.configuration_id
         JOIN public.configuration_logbook cl
              ON cl.configuration_id = c.configuration_id
         JOIN public.configuration_process cp
              ON cp.configuration_id = c.configuration_id
         JOIN public.configuration_product cpr
              ON cpr.configuration_id = c.configuration_id
         JOIN public.configuration_user cu
              ON cu.configuration_id = c.configuration_id
    WHERE c.configuration_id = '347a3256-8aab-4f82-bc1c-ab0784ef3599'
)
SELECT
row_to_json((SELECT d FROM (SELECT address_visibility_delay
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
                                 , email_required ) d))
, *
FROM conf
;
