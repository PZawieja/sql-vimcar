SELECT cb_plan_id
FROM dwh_main.dim_cb_plan
WHERE cb_plan_id IN ('fleet-pro-logbook-upgrade_yearly-eur-3-years','fleet-pro-logbook-upgrade_yearly-eur-3_years')
-- AND cb_plan_id LIKE '%3_years%'
-- AND cb_plan_id LIKE '%3-years%'
  AND cb_plan_id LIKE '%3\_years%'


















-- AND cb_plan_id LIKE '%3_years%' -- 'fleet-pro-logbook-upgrade_yearly-eur-3-years','fleet-pro-logbook-upgrade_yearly-eur-3_years'
-- AND cb_plan_id LIKE '%3-years%' -- 'fleet-pro-logbook-upgrade_yearly-eur-3-years'
--   AND cb_plan_id LIKE '%3\_years%' -- backslash to escape underscore, 'fleet-pro-logbook-upgrade_yearly-eur-3_years'



         SELECT map_customer_id,
                map_unified_customer_id,
             COUNT(car_id) AS number_of_cars
         FROM dwh_main.map_customer_to_foxbox_domain mcf
                  JOIN dwh_main.dim_car dc
                       ON mcf.map_fb_domain_name = dc.domain_name
         WHERE map_unified_customer_id = '169ldVSkbP3oXCYnH'
         AND  mcf.record_nbr_per_unified_customer_id = 1 --IMPORTANT!
         GROUP BY map_customer_id,map_unified_customer_id
;
SELECT * FROM dwh_main.map_customer_to_foxbox_domain WHERE map_unified_customer_id = '169ldVSkbP3oXCYnH';
SELECT * FROM data_mart_internal_reporting.ir_sh_cb_customer WHERE customer_id IN ('169ldVSkbP3oXCYnH','94963619');

SELECT cb_cust_cf_kundennummer FROM dwh_main.dim_cb_customer WHERE cb_cust_cf_kundennummer is not null;