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

-- How to join domains mapping table:
SELECT map_principal_domain
FROM dwh_main.dim_contract ctr
JOIN dwh_main.map_customer_principal_domain mcpd ON ctr.domain_name = mcpd.map_customer_domain
WHERE mcpd.mapped_principal_domains = 1
AND mcpd.map_principal_domain NOT IN ('com.vimcar.demoaccounts.demofleet', 'com.vimcar.demoaccounts' , 'com.vimcar.test.backend', 'com.vimcar', 'com.vimcar.clients')
AND mcpd.map_principal_domain NOT LIKE '%test%'
GROUP BY 1
HAVING MIN(ctr.contract_status) != 'active'
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
FROM data_mart_internal_reporting.ir_sh_cb_invoice_line i
         JOIN data_mart_internal_reporting.ir_sh_cb_subscription s
              ON i.subscription_id = s.subscription_id
         JOIN data_mart_internal_reporting.ir_sh_cb_customer c
              ON c.customer_id = s.customer_id
WHERE i.invoice_status <> 'voided'
--   AND i.product_group <> 'Hardware'
--   AND i.full_refund_flag = FALSE
  AND ((invoice_provisioning_start_dt <= CURRENT_DATE AND invoice_provisioning_end_dt > CURRENT_DATE) OR invoice_provisioning_start_dt < CURRENT_DATE AND invoice_provisioning_end_dt >= CURRENT_DATE) -- condition to retrieve also the invoices ending today
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