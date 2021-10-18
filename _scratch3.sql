SELECT * FROM public.contract WHERE outbound_id = '97839195';  65a0bfd5-d789-4a7e-aad2-9787d1f1048e
SELECT * FROM public.invoice WHERE outbound_id = 'R86651702'; 78c84eea-8752-45e8-b9cb-848392af1117
;
SELECT * FROM shop_extraction_cb_plan_id_map;
SELECT * FROM ext_sources.chargebee_addons
;
SELECT
*
FROM public.domain_configuration
WHERE TRUE
AND configuration -> 'services' ->> 'costs' = 'true'
  AND configuration -> 'services' ->> 'costs_fleet' = 'true'
  AND configuration -> 'services' ->> 'contracts' = 'true'
  AND configuration -> 'services' ->> 'default_locations' = 'true'
  AND configuration -> 'services' ->> 'route_tracking' = 'true'
  AND configuration -> 'services' ->> 'route_tracking' = 'true'
  AND configuration -> 'services' ->> 'route_tracking' = 'true'
  AND configuration -> 'services' ->> 'route_tracking' = 'true'
  AND configuration -> 'services' ->> 'route_tracking' = 'true'
  AND configuration -> 'services' ->> 'route_tracking' = 'true'
  AND configuration -> 'services' ->> 'route_tracking' = 'true'
  AND configuration -> 'services' ->> 'route_tracking' = 'true'
  AND configuration -> 'services' ->> 'route_tracking' = 'true'
  AND configuration -> 'services' ->> 'route_tracking' = 'true'
  AND configuration -> 'services' ->> 'route_tracking' = 'true'
  AND configuration -> 'services' ->> 'route_tracking' = 'true'
  AND configuration -> 'services' ->> 'route_tracking' = 'true'
;
