-- termiante SQL:
SELECT datname, pid, usename, query_start
, (DATE_PART('day', now() - query_start) * 24 + DATE_PART('hour', now() - query_start)) * 60 + DATE_PART('minute', now() - query_start) AS query_time_minutes
, wait_event_type, wait_event, state, query, backend_type
FROM pg_stat_activity
WHERE query not like 'SET application%' AND query not like 'SHOW TRANSACTION%'
AND state = 'active'
-- AND usename = 'tableau'
ORDER BY usename, query;

SELECT pg_cancel_backend(19654);

-- test  domains:
AND mcpd.map_principal_domain NOT IN ('com.vimcar.demoaccounts.demofleet', 'com.vimcar.demoaccounts' , 'com.vimcar.test.backend', 'com.vimcar', 'com.vimcar.clients')
AND mcpd.map_principal_domain NOT LIKE '%test%'
;

SELECT * FROM dwh_main.dim_main_domain_stat_scd2 WHERE updated_at::DATE = '2021-08-14'
;
SELECT * FROM dwh_main.dim_main_domain_stat_scd2 WHERE md_has_driver_ident  = true
;
SELECT * FROM dwh_main.dim_main_domain_stat_scd2 WHERE main_domain_foxbox = 'com.vimcar.de.k55402707' ORDER BY version_nr
;


-- time zone format
o.order_provisioning_start_ts AT TIME ZONE 'Europe/Berlin'
;
-- job_history (STAGING!!)
SELECT id, job_name, status, start_timestamp, end_timestamp FROM stg.dwh_job.job_history WHERE job_name = 'vimcar_batch_dwh' ORDER BY id DESC;

;
-- how many Foxbox domains can be linked to Shop:
WITH cte_mapping AS (
SELECT DISTINCT
    map_principal_main_domain
FROM dwh_main.map_customer_principal_domain mcpd
WHERE mcpd.mapped_principal_domains = 1
    AND mcpd.map_principal_domain NOT IN ('com.vimcar.clients', 'com.vimcar')
)
, foxbox_domains AS (
    SELECT DISTINCT main_domain_name AS fb_main_domain
    FROM dwh_main.dim_principal p
    WHERE domain_name NOT IN ('com.vimcar','com.vimcar.cients')
)
,  fb_shop AS (
    SELECT fb_main_domain, map_principal_main_domain
    FROM foxbox_domains LEFT JOIN cte_mapping ON cte_mapping.map_principal_main_domain = foxbox_domains.fb_main_domain
)
SELECT count(*) AS main_domain_foxbox
, count(map_principal_main_domain) AS match_to_shop
, round(count(map_principal_main_domain) / count(*)::numeric, 2) AS pct_matched
FROM fb_shop
;

-- Amount of active cars now:
WITH cars_all AS (SELECT count(DISTINCT car_id) AS cars
                  FROM dwh_main.dim_car dc
                           JOIN dwh_main.dim_principal dp ON dp.principal_id = dc.car_owner_id
                  WHERE car_end_logbook_ts IS NULL
                    AND dc.domain_name NOT IN
                        ('com.vimcar.demoaccounts.demofleet', 'com.vimcar.demoaccounts', 'com.vimcar.test.backend')
                    AND dp.email NOT LIKE '%quentman%'
                    AND NOT EXISTS(SELECT map_principal_domain
                                   FROM (SELECT map_principal_domain
                                         FROM dwh_main.map_customer_principal_domain mcpd
                                                  JOIN dwh_main.dim_contract ctr ON ctr.domain_name = mcpd.map_customer_domain
                                         WHERE mcpd.mapped_principal_domains = 1
                                           AND mcpd.map_principal_domain NOT IN ('com.vimcar.clients', 'com.vimcar')
                                         GROUP BY 1
                                         HAVING MIN(ctr.contract_status) = 'terminated') s1
                                   WHERE s1.map_principal_domain = dc.domain_name))
, cars_testers AS (SELECT count(*) AS dongles_tester
                    FROM dwh_main.umd_dongle_shipping
                    WHERE date_key >= current_date - 31
                    AND dongle_shipping_type IN ('Test Fleet', 'Inside Sales', 'Steuerberater'))
SELECT cars_all.cars - cars_testers.dongles_tester AS cars
FROM cars_all
    JOIN cars_testers ON 1=1
;

-- How many active Shop customers we have?
SELECT md_is_fleet
     ,count(main_domain_foxbox) as cnt_customers
    , sum(active_cars_amt) AS cars
FROM dwh_main.dim_main_domain_stat_scd2
WHERE is_last_version = TRUE
AND md_status IN ('active')
GROUP BY 1
;

-- cars per customer, drive/engine type
SELECT
    c.customer_contact_company_name
--     , c.domain_name
--     , mcpd.map_principal_domain
    , car.car_id
    , car.car_brand_name
    , car.car_model_name
    , car.vin
    , map_cars.drive
    , CASE WHEN car.car_end_logbook_ts IS NULL THEN TRUE ELSE FALSE END AS car_active
FROM dwh_main.dim_customer c
LEFT JOIN dwh_main.map_customer_principal_domain mcpd
    ON c.domain_name = mcpd.map_customer_domain
LEFT JOIN dwh_main.dim_car car
    ON car.domain_name = mcpd.map_principal_domain
LEFT JOIN dwh_main.tmp_car_models map_cars
    ON UPPER(map_cars.car_brand_name) = UPPER(car.car_brand_name)
        AND map_cars.car_model_name = car.car_model_name
-- LEFT JOIN dwh_main.tmp_car_models tcm
--                   ON cr.car_model_name = tcm.car_model_name
WHERE mcpd.mapped_principal_domains = 1
    AND mcpd.map_principal_domain NOT IN ('com.vimcar.clients', 'com.vimcar')
    AND c.customer_contact_company_name IN ('care mobile Pflegedienste GmbH')
;

-- user roles:
SELECT
    principal_uuid
    , email
    , domain_name AS domain_principal
    , principal_first_name ||' '||principal_last_name AS user_name
    , principal_created_ts::DATE AS user_created_dt
    , principal_is_rolemanager AS user_is_rolemanager
    , principal_is_carmanager AS user_is_carmanager
    , principal_is_usermanager AS user_is_usermanager
    , CASE WHEN principal_is_carmanager=TRUE THEN 'carmanager' END AS role_carmanager
    , CASE WHEN principal_is_subscriber=TRUE THEN 'subscriber' END AS role_subscriber
    , CASE WHEN principal_is_admin=TRUE THEN 'admin' END AS role_admin
    , CASE WHEN principal_is_customer=TRUE THEN 'customer' END AS role_customer
    , CASE WHEN principal_is_operations=TRUE THEN 'operations' END AS role_operations
    , CASE WHEN principal_is_configurator=TRUE THEN 'configurator' END AS role_configurator
    , CASE WHEN principal_is_contentmanager=TRUE THEN 'contentmanager' END AS role_contentmanager
    , CASE WHEN principal_is_customerservice=TRUE THEN 'customerservice' END AS role_customerservice
    , CASE WHEN principal_is_domainmanager=TRUE THEN 'domainmanager' END AS role_domainmanager
    , CASE WHEN principal_is_rolemanager=TRUE THEN 'rolemanager' END AS role_rolemanager
    , CASE WHEN principal_is_userrecordreader=TRUE THEN 'userrecordreader' END AS role_userrecordreader
    , CASE WHEN principal_is_contentreader=TRUE THEN 'contentreader' END AS role_contentreader
    , CASE WHEN principal_is_paymentmanager=TRUE THEN 'paymentmanager' END AS role_paymentmanager
    , CASE WHEN principal_is_roleadmin=TRUE THEN 'roleadmin' END AS role_roleadmin
    , CASE WHEN principal_is_dataprovider=TRUE THEN 'dataprovider' END AS role_dataprovider
    , CASE WHEN principal_is_domainconfigurator=TRUE THEN 'domainconfigurator' END AS role_domainconfigurator
    , CASE WHEN principal_is_userreader=TRUE THEN 'userreader' END AS role_userreader
    , CASE WHEN principal_is_lockedaccount=TRUE THEN 'lockedaccount' END AS role_lockedaccount
    , CASE WHEN principal_is_usermanager=TRUE THEN 'usermanager' END AS role_usermanager
    , CASE WHEN principal_is_inviter=TRUE THEN 'inviter' END AS role_inviter
    , CASE WHEN principal_is_bookingmanager=TRUE THEN 'bookingmanager' END AS role_bookingmanager
    , CASE WHEN principal_is_locationreader=TRUE THEN 'locationreader' END AS role_locationreader
    , CASE WHEN principal_is_costmanager=TRUE THEN 'costmanager' END AS role_costmanager
    , CASE WHEN principal_is_logbookmanager=TRUE THEN 'logbookmanager' END AS role_logbookmanager
FROM dwh_main.dim_principal
WHERE principal_deleted_ts IS NULL
    AND principal_is_lockedaccount = FALSE
;

-- fleet domain based on global roles + subscription to all cars:
SELECT
    p.domain_name AS domain_principal
FROM dwh_main.dim_principal p
    LEFT JOIN dwh_main.dim_subscription s
           ON s.owner_id = p.principal_id
          AND s.domain_name = p.domain_name
          AND s.opcode <> 'D'
          AND s.car_id = 0
WHERE principal_deleted_ts IS NULL
    AND principal_is_lockedaccount = FALSE
    AND (principal_is_carmanager=TRUE
        OR principal_is_bookingmanager=TRUE
        OR principal_is_contentmanager=TRUE
        OR principal_is_contentreader=TRUE
        OR principal_is_costmanager=TRUE
        OR principal_is_domainmanager=TRUE
        OR principal_is_inviter=TRUE
        OR principal_is_locationreader=TRUE
        OR principal_is_rolemanager=TRUE
        OR principal_is_subscriber=TRUE
        OR principal_is_usermanager=TRUE
        OR principal_is_userrecordreader=TRUE
        OR principal_is_logbookmanager=TRUE
        OR s.owner_id IS NOT NULL
    )
;

-- CB Chargebee mrr and arr at the invoice level:
SELECT i.cb_inv_id, mrr_eur
FROM dwh_main.fact_cb_invoice_line_item fil
             JOIN dwh_main.dim_cb_invoice i
                  ON i.cb_inv_key = fil.cb_inv_key
             JOIN dwh_main.dim_cb_plan p
                  ON p.cb_plan_id = fil.cb_inv_line_item_entity_id
            JOIN dwh_main.dim_cb_subscription s
                 ON s.cb_subscr_key = fil.cb_subscr_key
            JOIN dwh_main.dim_cb_customer c
                 ON s.cb_subscr_customer_id = c.cb_cust_id
LEFT JOIN (
                        SELECT invoice_id, plan_id, SUM(mrr_eur) AS mrr_eur
                        FROM ( SELECT
                                   invoice_id
                                    , reporting_month
                                    , plan_id
                                    , mrr_eur
                                    , row_number() OVER (PARTITION BY invoice_id, plan_id ORDER BY reporting_month, plan_id) AS invoice_occurence
                               FROM data_mart_internal_reporting.ir_m_investor_report
                               WHERE source_system = 'CB'
                             ) sub
                        WHERE invoice_occurence = 1
                        GROUP BY invoice_id, plan_id
                    ) ir
                   ON i.cb_inv_id = ir.invoice_id
                    AND p.cb_plan_id = ir.plan_id
WHERE cb_cust_email NOT LIKE '%@vimcar.com'
  AND cb_cust_email NOT LIKE '%@internal.vimcar.com'
;


-- DISTINCT ON AND CUSTOM SORT (array_position)
SELECT DISTINCT ON (external_id) external_id, email, org_customer_id, org_status
FROM data_mart_internal_reporting.ir_mkt_braze_report
WHERE external_id IN ('06f8257a-7b3d-435b-a801-e0665a6e841c')
ORDER BY external_id, array_position(ARRAY ['cancelled','active','future','non_renewing'], org_status::text), type NULLS LAST ;

-- current MRR from combined invoice line table:
SELECT i.customer_id
     , c.customer_status
     , c.customer_is_fleet
     , c.max_subscription_cancelled_dt
     , i.product_group
     , CASE
    -- only invoices started in the past or today, the ones ending today are EXCLUDED
           WHEN invoice_provisioning_start_dt <= CURRENT_DATE AND
                invoice_provisioning_end_dt > CURRENT_DATE
               THEN mrr_eur - refund_mrr_eur
    END AS mrr_eur_after_refund
     , CASE
           WHEN invoice_provisioning_start_dt <= CURRENT_DATE AND
                invoice_provisioning_end_dt > CURRENT_DATE
               THEN item_quantity
    END AS items
FROM dwh_main.dim_combined_invoice_line i
         JOIN dwh_main.dim_combined_subscription s
              ON i.subscription_id = s.subscription_id
         JOIN dwh_main.dim_combined_customer c
              ON c.customer_id = s.customer_id
         JOIN dwh_main.dim_combined_product cp
              ON cp.entity_id = i.entity_id
                  AND cp.entity_type = i.entity_type
WHERE i.invoice_status <> 'voided'
--   AND i.product_group <> 'Hardware'
--   AND i.full_refund_flag = FALSE
  AND ((invoice_provisioning_start_dt <= CURRENT_DATE AND invoice_provisioning_end_dt > CURRENT_DATE)
           OR invoice_provisioning_start_dt < CURRENT_DATE AND invoice_provisioning_end_dt >= CURRENT_DATE) -- condition to retrieve also the invoices ending today
;

------------------------------------------------------------------------------------------------------------
-- PRODUCT AND FEATURES:
-- Main tables: dim_v_dom_configuration  /  dim_v_dom_setting
SELECT
    c.*
FROM dwh_main.dim_v_dom_configuration c
         JOIN dwh_main.dim_v_dom_domain d
              ON c.domain_name = d.domain_name
WHERE d.domain_is_test = FALSE
  AND d.domain_name NOT IN (
      'com.vimcar.clients'
    , 'com.vimcar.clients.vimcar'
    , 'com.vimcar')
-- AND route_tracking = TRUE -- route documentation / route tracking

------------------------------------------------------------------------------------------------------------
-- Variable in sql:
SELECT DISTINCT
    doi.car_id
              , doi.odometer_inquiry_ts AS odo_inq_ts_valid_from
              , coalesce(LEAD(doi.odometer_inquiry_ts, 1) OVER (ORDER BY doi.odometer_inquiry_ts), now()) AS odo_inq_ts_valid_until
              , doi.total_odometer_value_km
FROM c_tmp.dm_odometerinquiry doi
WHERE doi.car_id = ${car_id}
  AND doi.total_odometer_value_km IS NOT NULL
  AND doi.odometer_inquiry_is_valid IS TRUE;

------------------------------------------------------------------------------------------------------------
-- format to_char
to_char("subscription[started_at]", 'YYYY-MM-DD HH24:MI:SS') ;


------------------------------------------------------------------------------------------------------------
-- combined product table:
SELECT
    c.cb_plan_id AS plan_id
     , c.cb_plan_name AS product_name
     , c.cb_plan_product_group AS product_group
     , CASE WHEN cb_plan_id IN ('logbook_b2c-from_rent2buy-eur', 'logbook_b2c-one_time_buy-eur') THEN TRUE ELSE FALSE END AS product_is_one_time_buy
     , CASE
           WHEN cb_plan_id IN ('logbook_b2c-from_rent2buy-eur', 'logbook_b2c-one_time_buy-eur')
               OR c.cb_plan_product_group = 'Hardware'
               THEN FALSE
           ELSE TRUE
    END product_is_recurring
     , c.cb_plan_billing_frequency AS product_billing_frequency
     , c.cb_plan_duration_months AS product_duration_mths
     , c.cb_plan_price AS product_price_net
     , c.cb_plan_currency_code AS product_currency_code
FROM dwh_main.dim_cb_plan c

UNION ALL

SELECT
    p.product_uuid::varchar(32) AS plan_id
     , p.product_name AS product_name
     , upm.product_family AS product_group
     , CASE WHEN p.product_uuid IN ('1db52694-baee-45fb-902f-8de7293621fe', '974e92e4-fed9-499e-8b7e-582c986f8a6b') THEN TRUE ELSE FALSE END AS product_is_one_time_buy
     , CASE
           WHEN (upm.is_recurring IS TRUE OR upm.is_subsequent_recurring IS TRUE)
               AND p.product_uuid NOT IN ('1db52694-baee-45fb-902f-8de7293621fe', '974e92e4-fed9-499e-8b7e-582c986f8a6b')
               THEN TRUE
           ELSE FALSE
    END product_is_recurring
     , CASE WHEN upm.monthly_payment IS TRUE THEN 'monthly' ELSE 'yearly' END AS product_billing_frequency
     , CASE WHEN p.product_name LIKE '%3-jährig%' THEN 36 ELSE 12 END AS product_duration_mths
     , p.product_price_net AS product_price_net
     , 'EUR' AS product_currency_code
FROM dwh_main.dim_product p
         LEFT JOIN dwh_main.umd_product_map upm
                   ON upm.product_uuid = p.product_uuid
;

-- update UK invoices not in CB:
UPDATE dwh_main.umd_uk_invoices_not_imported_to_chargebee
SET invoice_provisioning_end_ts = '2021-10-03 22:00:00.000000 +00:00'
WHERE invoice_id = '200000782';
;

--- current paying customers:
SELECT i.*
FROM dwh_main.dim_combined_invoice_line i
         JOIN dwh_main.dim_combined_subscription s
              ON s.subscription_id = i.subscription_id
         JOIN dwh_main.dim_combined_customer c
              ON c.customer_id = i.customer_id
         JOIN dwh_main.dim_combined_product p
              ON i.entity_id = p.entity_id
                  AND i.entity_type = p.entity_type
WHERE i.invoice_status <> 'voided'
  AND i.invoice_provisioning_start_dt <= current_date
  AND i.invoice_provisioning_end_dt > current_date;


-- How to join configuration template to customer:
SELECT
    m.map_unified_customer_id
     , string_agg(DISTINCT d.domain_configuration_template_match, ',') AS foxbox_domain_configuration_product
FROM dwh_main.map_customer_to_foxbox_domain m
         JOIN dwh_main.dim_v_dom_domain d
              ON d.domain_name = m.map_fb_domain_name
WHERE record_nbr_per_unified_customer_id = 1
  AND map_fb_main_domain_name IS NOT NULL
GROUP BY m.map_unified_customer_id;


-- Braze array position:
/*
Users linked to multiple customer domains create multiple records.
In order to make the external_id unique wit is agreed with CRM to aggregate the data in the way
that first active customer ID is selected but thee product licenses are aggregated from the other customer ID too.
Example: user 06f8257a-7b3d-435b-a801-e0665a6e841c
   */
WITH braze AS (
    SELECT
        b.*
         , CASE
               WHEN b.type IS NOT NULL
                   THEN b.type
               WHEN fm.main_domain_name IS NOT NULL
                   THEN 'B2B'
               ELSE 'B2C'
        END customer_type_foxbox
         , CASE
               WHEN t.main_domain_name IS NOT NULL
                   OR ft.main_domain_name IS NOT NULL
                   THEN 'active'
               ELSE 'inactive'
        END customer_status_foxbox
    FROM data_mart_internal_reporting.ir_mkt_braze_report b
             JOIN dwh_main.dim_principal p
                  ON b.external_id = p.principal_uuid
             LEFT JOIN (SELECT DISTINCT main_domain_name FROM dwh_main.dim_principal WHERE principal_is_def_fleetmanager = TRUE) fm
                       ON fm.main_domain_name = p.main_domain_name
             LEFT JOIN (SELECT d.main_domain_name
                        FROM dwh_main.dim_car c
                                 JOIN dwh_main.dim_domain d
                                      ON d.domain_name = c.domain_name
                        WHERE c.car_last_trip_start_ts > current_date - INTERVAL '30 days') t
                       ON t.main_domain_name = p.main_domain_name
             LEFT JOIN (SELECT main_domain_name
                        FROM data_mart_internal_reporting.ir_p_features_usage
                        WHERE reporting_date = CURRENT_DATE - 1
                          AND (domain_uses_cust_property_ft = TRUE
                            OR domain_uses_cost_ft = TRUE
                            OR domain_uses_documents_ft = TRUE)
                        GROUP BY main_domain_name) ft
                       ON ft.main_domain_name = p.main_domain_name
-- WHERE external_id IN ('06f8257a-7b3d-435b-a801-e0665a6e841c' , '0000774c-8d9c-427c-8a04-fa6211c7f0d8')
)
SELECT DISTINCT ON (external_id)
    external_id
                               , email
                               , first_name
                               , last_name
                               , gender
                               , app_lang
                               , user_status
                               , user_role
                               , org_name
                               , org_domain
                               , coalesce(type, customer_type_foxbox) AS type
                               , CASE
                                     WHEN org_territory IS NULL
                                         AND org_domain LIKE 'com.vimcar.gb.%'
                                         THEN 'UK'
                                     WHEN org_territory IS NULL
                                         THEN 'DACH'
                                     ELSE org_territory
                                END AS org_territory
                               , LEFT(user_created_date::TEXT, 19) AS user_created_date
                               , org_freshsales_id
                               , org_customer_id
                               , org_subscription_management_tool
                               , org_csm
                               , coalesce(org_status, customer_status_foxbox) AS org_status
                               , LEFT(org_start_date::TEXT, 10) AS org_start_date
                               , LEFT(org_end_date::TEXT, 10) AS org_end_date
                               , coalesce(sum(org_licenses_logbook) FILTER (WHERE org_status IN ('active','non_renewing')) OVER (PARTITION BY external_id),0) AS org_licenses_logbook
                               , coalesce(sum(org_licenses_geo) FILTER (WHERE org_status IN ('active','non_renewing')) OVER (PARTITION BY external_id),0) AS org_licenses_geo
                               , coalesce(sum(org_licenses_admin) FILTER (WHERE org_status IN ('active','non_renewing')) OVER (PARTITION BY external_id),0) AS org_licenses_admin
                               , coalesce(sum(org_licenses_pro) FILTER (WHERE org_status IN ('active','non_renewing')) OVER (PARTITION BY external_id),0) AS org_licenses_pro
                               , coalesce(sum(org_licenses_share) FILTER (WHERE org_status IN ('active','non_renewing')) OVER (PARTITION BY external_id),0) AS org_licenses_share
                               , coalesce(sum(org_licenses_driverident) FILTER (WHERE org_status IN ('active','non_renewing')) OVER (PARTITION BY external_id),0) AS org_licenses_driverident
                               , coalesce(sum(org_licenses_geoapi) FILTER (WHERE org_status IN ('active','non_renewing')) OVER (PARTITION BY external_id),0) AS org_licenses_geoapi
                               , coalesce(sum(org_licenses_damagemanagement) FILTER (WHERE org_status IN ('active','non_renewing')) OVER (PARTITION BY external_id),0) AS org_licenses_damagemanagement
--                                , coalesce(sum(org_licenses_fsk) FILTER (WHERE org_status IN ('active','non_renewing')) OVER (PARTITION BY external_id),0) AS org_licenses_fsk
                               , coalesce(sum(org_licenses_lapid + org_licenses_fsk) FILTER (WHERE org_status IN ('active','non_renewing')) OVER (PARTITION BY external_id),0) AS org_licenses_lapid
                               , coalesce(sum(org_licenses_fuelcard) FILTER (WHERE org_status IN ('active','non_renewing')) OVER (PARTITION BY external_id),0) AS org_licenses_fuelcard

                               , CASE WHEN sum(org_licenses_logbook) FILTER (WHERE org_status IN ('active','non_renewing')) OVER (PARTITION BY external_id) >0 THEN TRUE ELSE FALSE END AS org_has_logbook
                               , CASE WHEN sum(org_licenses_geo) FILTER (WHERE org_status IN ('active','non_renewing')) OVER (PARTITION BY external_id) >0 THEN TRUE ELSE FALSE END AS org_has_geo
                               , CASE WHEN sum(org_licenses_admin) FILTER (WHERE org_status IN ('active','non_renewing')) OVER (PARTITION BY external_id) >0 THEN TRUE ELSE FALSE END AS org_has_admin
                               , CASE WHEN sum(org_licenses_pro) FILTER (WHERE org_status IN ('active','non_renewing')) OVER (PARTITION BY external_id) >0 THEN TRUE ELSE FALSE END AS org_has_pro
                               , CASE WHEN sum(org_licenses_share) FILTER (WHERE org_status IN ('active','non_renewing')) OVER (PARTITION BY external_id) >0 THEN TRUE ELSE FALSE END AS org_has_share
                               , CASE WHEN sum(org_licenses_driverident) FILTER (WHERE org_status IN ('active','non_renewing')) OVER (PARTITION BY external_id) >0 THEN TRUE ELSE FALSE END AS org_has_driverident
                               , CASE WHEN sum(org_licenses_geoapi) FILTER (WHERE org_status IN ('active','non_renewing')) OVER (PARTITION BY external_id) >0 THEN TRUE ELSE FALSE END AS org_has_geoapi
                               , CASE WHEN sum(org_licenses_damagemanagement) FILTER (WHERE org_status IN ('active','non_renewing')) OVER (PARTITION BY external_id) >0 THEN TRUE ELSE FALSE END AS org_has_damagemanagement
--                                , CASE WHEN sum(org_licenses_fsk) FILTER (WHERE org_status IN ('active','non_renewing')) OVER (PARTITION BY external_id) >0 THEN TRUE ELSE FALSE END AS org_has_fsk
                               , CASE WHEN sum(org_licenses_lapid + org_licenses_fsk) FILTER (WHERE org_status IN ('active','non_renewing')) OVER (PARTITION BY external_id) >0 THEN TRUE ELSE FALSE END AS org_has_lapid
                               , CASE WHEN sum(org_licenses_fuelcard) FILTER (WHERE org_status IN ('active','non_renewing')) OVER (PARTITION BY external_id) >0 THEN TRUE ELSE FALSE END AS org_has_fuelcard
                               , org_cars_pool
                               , org_cars_private
                               , user_freshworks_id
                               , user_nps
                               , user_group_dvag
                               , user_group_taxadvisor
                               , user_group_taxadvisorclient
FROM braze
ORDER BY external_id, array_position(array['active','future','non_renewing','cancelled', 'inactive'], org_status::text), type NULLS LAST
;

-- customer source_system (ORIGIN!)
CASE WHEN c.customer_id_shop IS NOT NULL THEN 'SH' ELSE 'CB' END AS customer_origin_system;

-- array_agg how to use:
CASE WHEN array_agg(i.product_group) @> '{"Geo"}' THEN TRUE ELSE FALSE END AS has_geo ;
-- calculate time difference in seconds:
extract(EPOCH FROM event_end_ts::timestamp - event_start_ts::timestamp) ;

SELECT * FROM dwh_main.dim_principal WHERE principal_uuid = '1eae1f7a-eafe-4a2c-9913-5bbc73eabc73'
----------------
-- Foxbox domain MRR and status, match to subscription:
WITH cte_users AS (
    SELECT p.domain_name
         , p.main_domain_name
         , SUM(1) FILTER ( WHERE principal_deleted_ts IS NULL AND principal_is_lockedaccount = FALSE) AS cnt_users
    FROM dwh_main.dim_principal p
             JOIN dwh_main.dim_v_dom_domain d
                  ON d.domain_name = p.domain_name
    WHERE d.domain_is_test = false
    GROUP BY p.domain_name, p.main_domain_name
)
   , cte_cars AS (
    SELECT c.domain_name
         , SUM(1) FILTER ( WHERE c.car_end_logbook_ts IS NULL ) AS cnt_cars
    FROM dwh_main.dim_car c
             JOIN cte_users u
                  ON c.domain_name = u.domain_name
    GROUP BY c.domain_name
)
   , cte_domain_status AS (
    SELECT domain_name
         , domain_customer_type
         , domain_is_active
         , match_to_subscription
         , subscription_customer_type
         , subscription_customer_status
    FROM data_mart_internal_reporting.ir_p_fb_domain_features_usage
    WHERE reporting_date = (SELECT MAX(reporting_date) FROM data_mart_internal_reporting.ir_p_fb_domain_features_usage)
)
   , cte_domains AS (SELECT u.domain_name
--                           , u.main_domain_name
                          , STRING_AGG(m.map_unified_customer_id, '; ')                                AS subscription_customer_id
                          , u.cnt_users
                          , car.cnt_cars
                          , SUM(c.customer_current_mrr_eur)                                           AS current_mrr_eur
                          , CASE WHEN SUM(c.customer_current_mrr_eur) >= 200 THEN TRUE ELSE FALSE END AS "is_m+_customer"
                     FROM cte_users u
                              LEFT JOIN cte_cars car
                                        ON car.domain_name = u.domain_name
                              LEFT JOIN dwh_main.map_customer_to_foxbox_domain m
                                        ON m.map_fb_domain_name = u.domain_name
                                            AND record_nbr_per_unified_customer_id = 1
                              LEFT JOIN dwh_main.dim_combined_customer c
                                        ON c.customer_id = m.map_unified_customer_id
                     GROUP BY u.domain_name, u.main_domain_name, car.cnt_cars, u.cnt_users
)
SELECT d.*
     , s.domain_customer_type
     , s.domain_is_active
     , s.match_to_subscription
     , s.subscription_customer_type
     , s.subscription_customer_status
     , p.principal_uuid
     , p.email AS principal_email
     , p.principal_first_name
     , p.principal_last_name
FROM cte_domains d
         JOIN dwh_main.dim_principal p
              ON d.domain_name = p.domain_name
         LEFT JOIN cte_domain_status s
                   ON d.domain_name = s.domain_name
WHERE 1=1
    AND p.principal_deleted_ts IS NULL
    AND p.principal_is_lockedaccount = FALSE
--      AND principal_deleted_ts IS NULL
-- AND s.domain_name is null
;

-- K55821225

WITH cte_connected_cars AS (
    SELECT domain_name
         , sum(1) AS cnt_cars
         , sum(1) FILTER ( WHERE car_imei IS NOT NULL) AS cnt_cars_with_dongle
    FROM dwh_main.dim_car
    GROUP BY domain_name
)
, cte_customers AS (
    SELECT i.customer_id
         , c.customer_contact_company_name
         , c.customer_status
         , m.map_fb_domain_name AS foxbox_domain_name
         , string_agg(DISTINCT product_group, ', ') AS products
         , bool_or(CASE WHEN product_group = 'Geo' THEN TRUE ELSE FALSE END) AS has_geo
         , coalesce(sum(mrr_eur-refund_mrr_eur),0) AS current_mrr_eur
         , coalesce(cc.cnt_cars, 0) AS cnt_cars
         , coalesce(cc.cnt_cars_with_dongle, 0) AS cnt_cars_with_dongle
    FROM dwh_main.dim_combined_invoice_line i
             JOIN dwh_main.dim_combined_customer c
                  ON c.customer_id = i.customer_id
             JOIN dwh_main.map_customer_to_foxbox_domain m
                       ON m.map_unified_customer_id = c.customer_id
                           AND m.record_nbr_per_unified_customer_id = 1
             LEFT JOIN cte_connected_cars cc -- connected cars
                    ON cc.domain_name = m.map_fb_domain_name
    WHERE invoice_status <> 'voided'
      AND product_group NOT IN ('Hardware', 'Other')
      AND ((invoice_provisioning_start_dt <= CURRENT_DATE AND invoice_provisioning_end_dt > CURRENT_DATE)
        OR invoice_provisioning_start_dt < CURRENT_DATE AND invoice_provisioning_end_dt >= CURRENT_DATE) -- condition to retrieve also the invoices ending today
    GROUP BY i.customer_id, c.customer_contact_company_name, c.customer_status, m.map_fb_domain_name,
             cc.domain_name, cc.cnt_cars, cc.cnt_cars_with_dongle
)
SELECT
    customer_id
     , customer_contact_company_name
     , customer_status
     , current_mrr_eur
     , CASE
        WHEN current_mrr_eur >= 200
            THEN TRUE
        ELSE FALSE
        END AS "is M+ customer"
     , foxbox_domain_name
     , products
     , has_geo
     , cnt_cars
     , cnt_cars_with_dongle
     , p.principal_uuid AS user_uuid
     , p.email
     , p.principal_first_name AS user_first_name
     , p.principal_last_name AS user_last_name
FROM cte_customers c
         LEFT JOIN dwh_main.dim_principal p
                   ON p.domain_name = c.foxbox_domain_name
                       AND principal_deleted_ts IS NULL
                       AND principal_is_lockedaccount IS FALSE
                       AND principal_is_locked IS FALSE
WHERE has_geo = TRUE
    AND foxbox_domain_name NOT LIKE 'com.vimcar.gb.%'
AND cnt_cars_with_dongle = 0
;

SELECT * FROM dwh_main.dim_v_fb_dataprovider_handle
SELECT *
FROM dwh_main.dim_car
WHERE domain_name = 'com.vimcar.de.k44928493'
