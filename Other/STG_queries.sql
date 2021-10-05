SELECT domain_name_ios FROM (
SELECT
fp.domain_name AS domain_name_ios
, count(*) FILTER ( WHERE user_agent ILIKE '%ios%'
                     OR user_agent ILIKE '%iphone%'
                     OR user_agent ILIKE '%macintosh%') AS ios
, count(*) AS all_sessions
FROM stg.vimcar.foxbox_principal fp
JOIN stg.fdw_identity.user_session us ON fp.uuid = us.user_id
GROUP BY 1) s1
WHERE round(ios/all_sessions::DECIMAL,2) >= 0.5;


SELECT count(*),count(DISTINCT session_id) FROM stg.fdw_identity.user_session
SELECT * FROM stg.fdw_identity.user_session LIMIT 100;
WHERE user_agent ilike '%ios%' or user_agent ilike '%iphone%' or user_agent ilike '%Macintosh%'

SELECT min(sign_in_at) FROM stg.fdw_identity.user_session;

SELECT *
FROM stg.vimcar.customers_invoice_item_added;

SELECT count(domain_name_ios) FROM (
SELECT
fp.domain_name AS domain_name_ios
, count(*) FILTER ( WHERE user_agent ILIKE '%ios%'
                     OR user_agent ILIKE '%iphone%') AS ios
, count(*) AS all_sessions
FROM stg.vimcar.foxbox_principal fp
JOIN stg.fdw_identity.user_session us ON fp.uuid = us.user_id
GROUP BY 1) s1
WHERE round(ios/all_sessions::DECIMAL,2) >= 0.5;

SELECT * FROM stg.fdw_identity.user_session;
SELECT * FROM stg.vimcar.geolocation_fence;

SELECT * FROM stg.ext_sources.amplitude_events;
SELECT count(DISTINCT uuid), count(DISTINCT amplitude_id) FROM stg.ext_sources.amplitude_events;
;
WHERE insert_id = '0045fdde-5790-4b3c-b1b0-95d6e5e1f87c'
;
SELECT * FROM stg.ext_sources.zendesk_tickets;
SELECT principal_uuid FROM dw 'f2d66c25-790f-440a-b92f-957abf78601dR'::UUID;

SELECT
  user_id
, uuid
, amplitude_id
, user_creation_time
, paying
, user_properties
, country
, region
, city
, location_lat
, location_lng
, dma
, ip_address
, device_type
, device_id
, device_manufacturer
, device_brand
, device_model
, device_family
, device_carrier
, platform
, os_name
, os_version
, "language"
, groups
, "data"
, adid
, session_id
, sample_rate
, idfa
, amplitude_attribution_ids
, start_version
, version_name
FROM ext_sources.amplitude_events
--WHERE dwh_job_id <= ${DWH_JOB_ID}
ORDER BY insert_id

SELECT *
FROM ext_sources.amplitude_events e
JOIN (SELECT insert_id FROM ext_sources.amplitude_events
GROUP BY 1 HAVING count(insert_id) > 1) s1
ON s1.insert_id = e.insert_id

SELECT created_at, count(*)
FROM ext_sources.amplitude_events e
WHERE created_at>'2019-10-25 14:01:09.730549'
GROUP BY 1;

SELECT insert_id FROM ext_sources.amplitude_events
WHERE created_at>'2019-10-25 14:01:09.730549'
GROUP BY 1 HAVING count(insert_id) > 1;

SELECT * FROM ext_sources.amplitude_events;

SELECT session_id FROM ext_sources.amplitude_events
GROUP BY session_id
HAVING count(event_id)>1;

SELECT device_id  FROM ext_sources.amplitude_events WHERE length(device_id) > 36;
SELECT count(DISTINCT amplitude_id),count(DISTINCT uuid),count(*) FROM ext_sources.amplitude_events;
SELECT * FROM ext_sources.amplitude_events WHERE user_id IS NULL ;

SELECT
  app AS amp_app
, insert_id AS amp_insert_id
, user_id AS amp_user_id
, uuid AS amp_uuid
, amplitude_id AS amp_amplitude_id
, processed_time AS amp_processed_time
, client_event_time AS amp_client_event_time
, "schema" AS amp_schema
, event_time AS amp_event_time
, user_creation_time AS amp_user_creation_time
, paying AS amp_paying
, group_properties AS amp_group_properties
, user_properties AS amp_user_properties
, client_upload_time AS amp_client_upload_time
, server_upload_time AS amp_server_upload_time
, event_type AS amp_event_type
, event_id AS amp_event_id
, event_properties AS amp_event_properties
, is_attribution_event AS amp_is_attribution_event
, amplitude_event_type AS amp_amplitude_event_type
, library AS amp_library
, ip_address AS amp_ip_address
, location_lng AS amp_location_lng
, location_lat AS amp_location_lat
, city AS amp_city
, region AS amp_region
, country AS amp_country
, dma AS amp_dma
, device_type AS amp_device_type
, device_id AS amp_device_id
, device_manufacturer AS amp_device_manufacturer
, device_brand AS amp_device_brand
, device_model AS amp_device_model
, device_family AS amp_device_family
, device_carrier AS amp_device_carrier
, platform AS amp_platform
, os_name AS amp_os_name
, os_version AS amp_os_version
, "language" AS amp_language
, groups AS amp_groups
, "data" AS amp_data
, adid AS amp_adid
, session_id AS amp_session_id
, sample_rate AS amp_sample_rate
, idfa AS amp_idfa
, amplitude_attribution_ids AS amp_amplitude_attribution_ids
, start_version AS amp_start_version
, version_name AS amp_version_name
FROM ext_sources.amplitude_events
WHERE dwh_job_id < 99999999999999
ORDER BY insert_id
;
SELECT
      insert_id AS amp_event_insert_id
     , session_id AS amp_session_id
     , user_id AS amp_amplitude_user_id
     , user_creation_time AS amp_amplitude_user_creation_ts
     , uuid AS customer_uuid
     , amplitude_id AS amp_amplitude_id
     , paying AS amp_user_is_paying
     , user_properties AS amp_user_properties
     , event_id AS amp_event_id
     , event_type AS amp_event_type
     , amplitude_event_type AS amp_amplitude_event_type
     , event_time AS amp_event_ts
     , client_event_time AS amp_client_event_ts
     , client_upload_time AS amp_client_upload_ts
     , server_upload_time AS amp_server_upload_ts
     , processed_time AS amp_processed_ts
     , event_properties AS amp_event_properties
     , is_attribution_event AS amp_is_attribution_event
     , amplitude_attribution_ids AS amp_amplitude_attribution_ids
     , ip_address AS amp_event_ip_address
     , country AS amp_event_country
     , region AS amp_event_region
     , city AS amp_event_city
     , location_lng AS amp_event_location_lng
     , location_lat AS amp_event_location_lat
     , dma AS amp_event_dma
     , device_type AS amp_event_device_type
     , device_id AS amp_event_device_id
     , device_manufacturer AS amp_event_device_manufacturer
     , device_brand AS amp_event_device_brand
     , device_model AS amp_event_device_model
     , device_family AS amp_event_device_family
     , device_carrier AS amp_event_device_carrier
     , platform AS amp_event_platform
     , os_name AS amp_event_os_name
     , os_version AS amp_event_os_version
     , start_version AS amp_start_version
     , version_name AS amp_version_name
     , language AS amp_event_language
     , groups AS amp_event_groups
     , group_properties AS amp_event_group_properties
     , "data" AS amp_event_data
     , adid AS amp_event_adid
     , idfa AS amp_event_idfa
     , sample_rate AS amp_event_sample_rate
     , app AS amp_amplitude_app
     , "schema" AS amp_amplitude_schema
     , library AS amp_amplitude_library
FROM ext_sources.amplitude_events
;

SELECT
      insert_id AS amp_event_insert_id
     , session_id AS amp_session_id
     , user_id AS amp_amplitude_user_id
     , user_creation_time AS amp_amplitude_user_creation_ts
     , uuid AS customer_uuid
     , amplitude_id AS amp_amplitude_id
     , paying AS amp_user_is_paying
     , user_properties AS amp_user_properties
     , event_id AS amp_event_id
     , event_type AS amp_event_type
     , amplitude_event_type AS amp_amplitude_event_type
     , event_time AS amp_event_ts
     , client_event_time AS amp_client_event_ts
     , client_upload_time AS amp_client_upload_ts
     , server_upload_time AS amp_server_upload_ts
     , processed_time AS amp_processed_ts
     , event_properties AS amp_event_properties
     , is_attribution_event AS amp_is_attribution_event
     , amplitude_attribution_ids AS amp_amplitude_attribution_ids
     , ip_address AS amp_event_ip_address
     , country AS amp_event_country
     , region AS amp_event_region
     , city AS amp_event_city
     , location_lng AS amp_event_location_lng
     , location_lat AS amp_event_location_lat
     , dma AS amp_event_dma
     , device_type AS amp_event_device_type
     , device_id AS amp_event_device_id
     , device_manufacturer AS amp_event_device_manufacturer
     , device_brand AS amp_event_device_brand
     , device_model AS amp_event_device_model
     , device_family AS amp_event_device_family
     , device_carrier AS amp_event_device_carrier
     , platform AS amp_event_platform
     , os_name AS amp_event_os_name
     , os_version AS amp_event_os_version
     , start_version AS amp_start_version
     , version_name AS amp_version_name
     , language AS amp_event_language
     , groups AS amp_event_groups
     , group_properties AS amp_event_group_properties
     , "data" AS amp_event_data
     , adid AS amp_event_adid
     , idfa AS amp_event_idfa
     , sample_rate AS amp_event_sample_rate
     , app AS amp_amplitude_app
     , "schema" AS amp_amplitude_schema
     , library AS amp_amplitude_library
FROM ext_sources.amplitude_events
;

SELECT
      insert_id AS amp_event_insert_id
     , session_id AS amp_session_id
     , user_id AS amp_amplitude_user_id
     , user_creation_time AS amp_amplitude_user_creation_ts
     , uuid AS customer_uuid
     , amplitude_id AS amp_amplitude_id
     , paying AS amp_user_is_paying
     , user_properties AS amp_user_properties
     , event_id AS amp_event_id
     , event_type AS amp_event_type
     , amplitude_event_type AS amp_amplitude_event_type
     , event_time AS amp_event_ts
     , client_event_time AS amp_client_event_ts
     , client_upload_time AS amp_client_upload_ts
     , server_upload_time AS amp_server_upload_ts
     , processed_time AS amp_processed_ts
     , event_properties AS amp_event_properties
     , is_attribution_event AS amp_is_attribution_event
     , amplitude_attribution_ids AS amp_amplitude_attribution_ids
     , ip_address AS amp_event_ip_address
     , country AS amp_event_country
     , region AS amp_event_region
     , city AS amp_event_city
     , location_lng AS amp_event_location_lng
     , location_lat AS amp_event_location_lat
     , dma AS amp_event_dma
     , device_type AS amp_event_device_type
     , device_id AS amp_event_device_id
     , device_manufacturer AS amp_event_device_manufacturer
     , device_brand AS amp_event_device_brand
     , device_model AS amp_event_device_model
     , device_family AS amp_event_device_family
     , device_carrier AS amp_event_device_carrier
     , platform AS amp_event_platform
     , os_name AS amp_event_os_name
     , os_version AS amp_event_os_version
     , start_version AS amp_start_version
     , version_name AS amp_version_name
     , language AS amp_event_language
     , groups AS amp_event_groups
     , group_properties AS amp_event_group_properties
     , "data" AS amp_event_data
     , adid AS amp_event_adid
     , idfa AS amp_event_idfa
     , sample_rate AS amp_event_sample_rate
     , app AS amp_amplitude_app
     , "schema" AS amp_amplitude_schema
     , library AS amp_amplitude_library
FROM ext_sources.amplitude_events
WHERE dwh_job_id >= ${DWH_FROM_JOB_ID} -- remove "=" after testing!
AND dwh_job_id <= ${DWH_TO_JOB_ID}
ORDER BY insert_id;

SELECT * FROM ext_sources.amplitude_events WHERE insert_id = '000001db-59cc-4554-993d-5b86360ce143'


SELECT
domain_name
, TRUE AS domain_uses_group_ft
FROM (
WITH groups_cte AS (
SELECT DISTINCT
lower(fc.domain_name) AS domain_name
, count(fc.domain_name) OVER (PARTITION BY
CASE
WHEN lower(fc.domain_name) = 'com.vimcar' THEN 'com.vimcar'
WHEN lower(fc.domain_name) NOT LIKE 'com.vimcar.de.%' THEN 'com.vimcar.' || lower(split_part(fc.domain_name, '.', 3))
WHEN lower(fc.domain_name) LIKE 'com.vimcar.de.%' THEN 'com.vimcar.de.' || lower(split_part(fc.domain_name, '.', 4))
END) AS user_group_amt -- amount active users with groups under MAIN domain
FROM vimcar.foxbox_configuration fc
JOIN stg.vimcar.foxbox_principal fp
ON fc.owner_id = fp.id
WHERE fc.payload -> 'map' ->> 'fleetGroupIdentifier' IS NOT NULL
AND fc.payload -> 'map' ->> 'fleetGroupIdentifier' != '[]'
AND fp.active IS TRUE
AND fp.deleted IS NULL
)
SELECT
cte.domain_name
, cte.user_group_amt
, ss1.user_domain_amt
FROM groups_cte cte
JOIN (SELECT DISTINCT
lower(domain_name) AS domain_name
, count(*) OVER (PARTITION BY
CASE
WHEN lower(fp.domain_name) = 'com.vimcar' THEN 'com.vimcar'
WHEN lower(fp.domain_name) NOT LIKE 'com.vimcar.de.%' THEN 'com.vimcar.' || lower(split_part(fp.domain_name, '.', 3))
WHEN lower(fp.domain_name) LIKE 'com.vimcar.de.%' THEN 'com.vimcar.de.' || lower(split_part(fp.domain_name, '.', 4))
END ) AS user_domain_amt -- amount users under MAIN domain
FROM vimcar.foxbox_principal fp
WHERE fp.deleted IS NULL
AND fp.active IS TRUE) ss1
ON ss1.domain_name = cte.domain_name) s1
--WHERE round(user_group_amt / user_domain_amt ::DECIMAL, 2) >= 0.5;

SELECT * FROM vimcar.foxbox_configuration
SELECT * FROM stg.vimcar.foxbox_principal
SELECT * FROM stg.vimcar.foxbox_cars
SELECT * FROM stg.vimcar.foxbox_domain_configuration
SELECT * FROM stg.vimcar.geolocation_vehicle_settings

SELECT
      insert_id AS amp_event_insert_id
     , session_id AS amp_session_id
     , user_id AS amp_amplitude_user_id
     , user_creation_time AS amp_amplitude_user_creation_ts
     , uuid AS customer_uuid
     , amplitude_id AS amp_amplitude_id
     , paying AS amp_user_is_paying
     , user_properties AS amp_user_properties
     , event_id AS amp_event_id
     , event_type AS amp_event_type
     , amplitude_event_type AS amp_amplitude_event_type
     , event_time AS amp_event_ts
     , client_event_time AS amp_client_event_ts
     , client_upload_time AS amp_client_upload_ts
     , server_upload_time AS amp_server_upload_ts
     , processed_time AS amp_processed_ts
     , event_properties AS amp_event_properties
     , is_attribution_event AS amp_is_attribution_event
     , amplitude_attribution_ids AS amp_amplitude_attribution_ids
     , ip_address AS amp_event_ip_address
     , country AS amp_event_country
     , region AS amp_event_region
     , city AS amp_event_city
     , location_lng AS amp_event_location_lng
     , location_lat AS amp_event_location_lat
     , dma AS amp_event_dma
     , device_type AS amp_event_device_type
     , device_id AS amp_event_device_id
     , device_manufacturer AS amp_event_device_manufacturer
     , device_brand AS amp_event_device_brand
     , device_model AS amp_event_device_model
     , device_family AS amp_event_device_family
     , device_carrier AS amp_event_device_carrier
     , platform AS amp_event_platform
     , os_name AS amp_event_os_name
     , os_version AS amp_event_os_version
     , start_version AS amp_start_version
     , version_name AS amp_version_name
     , language AS amp_event_language
     , groups AS amp_event_groups
     , group_properties AS amp_event_group_properties
     , "data" AS amp_event_data
     , adid AS amp_event_adid
     , idfa AS amp_event_idfa
     , sample_rate AS amp_event_sample_rate
     , app AS amp_amplitude_app
     , "schema" AS amp_amplitude_schema
     , library AS amp_amplitude_library
FROM ext_sources.amplitude_events
WHERE insert_id = '000001db-59cc-4554-993d-5b86360ce143'
--WHERE dwh_job_id >= ${DWH_FROM_JOB_ID} -- remove "=" after testing!
--AND dwh_job_id <= ${DWH_TO_JOB_ID}
ORDER BY insert_id;


SELECT * FROM ext_sources.amplitude_events;

SELECT t.* FROM stg.vimcar.foxbox_trips t WHERE TRUE
AND id = 202976013

SELECT t.* FROM stg.vimcar.foxbox_trips t,

       LATERAL (SELECT obj.value ->> 'product_id' AS product_uuid,
                       (obj.value -> 'total_provisioned_price' -> 'net' ->> 'amount')::INT AS total_provisioned_price_net_amount,
                       s.product_name
                FROM jsonb_array_elements(c.items) obj(value)
                     JOIN (SELECT id,
                                  product_name,
                                  monthly_payment
                             FROM stg.vimcar.foxbox_cars c) s
                       ON s.product_uuid::TEXT = obj.value ->> 'product_id') contract_items
WHERE TRUE
AND id = 202976013
SELECT id, payload->>'licencePlate' AS car_licence_plate, payload FROM stg.vimcar.foxbox_cars

SELECT t.*, s.car_licence_plate FROM stg.vimcar.foxbox_trips t JOIN (SELECT id, payload->>'licencePlate' AS car_licence_plate FROM stg.vimcar.foxbox_cars) s ON s.id = t.car_id WHERE TRUE
AND t.id = 202976013
AND car_licence_plate = 'F-DB 3767'
AND t.start_timestamp::timestamp >= '2019-10-24 00:00:00'
AND t.start_timestamp <= '2019-10-24 23:59:59'

SELECT t.*, s.car_licence_plate FROM stg.vimcar.foxbox_trips t JOIN (SELECT id, payload->>'licencePlate' AS car_licence_plate FROM stg.vimcar.foxbox_cars) s ON s.id = t.car_id WHERE TRUE
 AND t.id = 202976013

SELECT t.*, s.car_licence_plate FROM stg.vimcar.foxbox_trips t JOIN (SELECT id, payload->>'licencePlate' AS car_licence_plate FROM stg.vimcar.foxbox_cars) s ON s.id = t.car_id WHERE TRUE AND t.id = 202976013 AND car_licence_plate = 'F-DB 3767'
SELECT tc.*, s.car_licence_plate FROM stg.vimcar.foxbox_trip_changes tc JOIN stg.vimcar.foxbox_trips t ON tc.trip_id = t.id JOIN (SELECT id, payload->>'licencePlate' AS car_licence_plate FROM stg.vimcar.foxbox_cars) s ON s.id = t.car_id WHERE TRUE AND t.id = 202976013 AND car_licence_plate = 'F-DB 3767'

SELECT t.*, s.car_licence_plate FROM stg.fdw_foxbox.trips t JOIN (SELECT id, payload->>'licencePlate' AS car_licence_plate FROM stg.vimcar.foxbox_cars) s ON s.id = CAST(t.payload->>'carId' AS INTEGER)
WHERE t.id = 202976013
LIMIT 100

SELECT DISTINCT
lower(p.domain_name) AS domain_name
, TRUE AS domain_uses_events_vehicle_localisation_usage_ft
FROM stg.ext_sources.amplitude_events amp
JOIN stg.vimcar.foxbox_principal p ON p.uuid = amp.user_id
WHERE event_time::DATE >= current_date - 30
AND amp.event_type IN (
'live locations - vehicle selected', 'live locations - map dragged', 'live locations - go to vehicle details',
'live locations - vehicle unselected', 'live locations - zoomed in on vehicle', 'live locations - address field focussed',
'live locations - route calculated', 'live locations - address field cleared','live locations - alternative route calculated'
    );



SELECT DISTINCT
lower(p.domain_name) AS domain_name
, TRUE AS domain_uses_events_route_documentation_usage_ft
FROM stg.ext_sources.amplitude_events amp
JOIN stg.vimcar.foxbox_principal p ON p.uuid = amp.user_id
WHERE event_time::DATE >= current_date - 30
AND amp.event_type IN ('route tracking - routes loaded');

-- com.vimcar.alltagshelfer-zuhause

SELECT c.outbound_id, r.created::DATE, r.reason, r.comment, p.name, sum(r.quantity)
FROM stg.vimcar.customers_return r
JOIN stg.vimcar.customers_contract c ON c.id = r.contract_id
LEFT JOIN stg.vimcar.customers_product p ON p.id::TEXT = r.product_id::TEXT
WHERE r.created::DATE BETWEEN '2019-09-01' AND '2019-09-30'
--AND c.outbound_id = 'V98577872'
GROUP BY 1,2,3,4,5
;
SELECT * FROM stg.vimcar.customers_contract;
SELECT * FROM stg.vimcar.customers_return;


SELECT c.outbound_id, r.created::DATE, r.reason,p.name, sum(r.quantity)
FROM stg.vimcar.customers_return r
JOIN stg.vimcar.customers_contract c ON c.id = r.contract_id
LEFT JOIN stg.vimcar.customers_product p ON p.id::TEXT = r.product_id::TEXT
WHERE r.created::DATE BETWEEN '2019-10-01' AND '2019-10-31'
AND c.outbound_id = '19654809'
GROUP BY 1,2,3,4
;

SELECT * FROM stg.vimcar.customers_order
WHERE id = '3b647ac9-6014-4b51-9dba-13a0a0c4a48c'
;

#,order_uuid
1,3b647ac9-6014-4b51-9dba-13a0a0c4a48c
2,3bb44c17-52f8-4454-9361-4bb5457ccaf4
3,4133d692-078c-40fa-97eb-b7a6bbdce567
4,4c0132bf-ee60-447c-b0e7-3232587294c0
5,5329f88f-63e7-4df4-8487-fac3286374be
6,5ce9091f-e897-4cbf-b541-9d4f4ecb2ec0
7,94ef8408-26cc-4510-abf2-32e205d8fa31
8,69a88f19-9657-4098-98fc-73ddd69da637
9,6ed93778-abf3-4a0d-bce8-dbb8d9088eae
10,833f1264-1cb6-4c97-8267-46bffb6808ef
11,977be94d-ca4e-404b-a5fe-b48d8319c6fb
12,a2a35e28-39c3-41cb-a27f-65fc6fe8d08c
13,a51290ac-209d-4d5c-90f6-df8d79a85cf1
14,ab74e30f-72b1-4a7e-b219-fc6f88440d65
15,c2e1b6f5-bcc2-4f36-8898-8bcfebc4c6f3
16,c4740b75-07b6-455f-af49-9e9b83102469
17,cc56d7ea-3719-431c-8159-be2ee82e67a5
18,d133a3e6-56d8-40e5-84ca-2014a151d0c0


SELECT DISTINCT
  co.id AS order_uuid
, co.outbound_id AS order_id
, lower(trim(trailing '.' from co.domain))::VARCHAR(128) AS domain_name
, co.customer_id AS customer_uuid
, co.contract_id AS contract_uuid
, co.foxbox_id AS principal_id
, co.magento_id::VARCHAR(16) AS magento_id
, ao."Magento-Status"::VARCHAR(16) AS magento_status
, co.status::VARCHAR(16) AS order_status
, (CASE
    WHEN LEFT(co.magento_id, 2) IN ('77', '78') THEN 'paid'
    WHEN ao."Magento-Status" IN ('complete', 'closed') THEN 'paid'
    WHEN ao."Magento-Status" IS NOT NULL AND co.magento_id IS NOT NULL THEN 'open'
    WHEN cp.status = 'authorised' THEN 'paid'
    WHEN ci.id IS NOT NULL THEN 'open'
    ELSE 'no_invoice'
  END)::VARCHAR(16) AS invoice_status
, CASE WHEN co.discount ->> 'type' = 'percentage' THEN (co.discount ->> 'value')::DECIMAL(3,2) END AS order_discount_percentage
, CASE WHEN co.discount ->> 'type' = 'amount' THEN (co.discount -> 'value' ->> 'amount')::INT END AS order_discount_amount
, (co.discount -> 'value' ->> 'currency')::CHAR(3) AS order_discount_currency_code
, (CASE WHEN ao."Bestellung Nr." = co.magento_id THEN ao."gutscheincode" ELSE co.coupon_code END)::VARCHAR(64) AS coupon_code
, (co.total_price -> 'net' ->> 'amount')::INT AS order_total_price_net_amt
, (co.total_price -> 'gross' ->> 'amount')::INT AS order_total_price_gross_amt
, (co.total_price ->> 'tax')::DECIMAL(3,2) AS order_total_price_tax
, (co.total_price -> 'net' ->> 'currency')::CHAR(3) AS order_price_currency_code
, (co.total_provisioned_price -> 'net' ->> 'amount')::INT AS order_total_provisioned_price_net_amt
, (co.total_provisioned_price -> 'gross' ->> 'amount')::INT AS order_total_provisioned_price_gross_amt
, co.provisioning_start AS order_provisioning_start_ts
, co.provisioning_end AS order_provisioning_end_ts
, (co.billing_contact ->> 'first_name')::VARCHAR(64) AS order_billing_first_name
, (co.billing_contact ->> 'last_name')::VARCHAR(64) AS order_billing_last_name
, (co.billing_contact -> 'address' ->> 'street_address')::VARCHAR(128) AS order_billing_street_address
, (co.billing_contact -> 'address' ->> 'postal_code')::VARCHAR(16) AS order_billing_postal_code
, (co.billing_contact -> 'address' ->> 'locality')::VARCHAR(64) AS order_billing_locality
, (co.billing_contact -> 'address' ->> 'region')::VARCHAR(64) AS order_billing_region
, (co.billing_contact -> 'address' ->> 'country')::CHAR(2) AS order_billing_country
, (co.billing_contact ->> 'email')::VARCHAR(128) AS order_billing_email
, (co.billing_contact ->> 'gender')::VARCHAR(16) AS order_billing_gender
, (co.billing_contact ->> 'company_name')::VARCHAR(128) AS order_billing_company_name
, (co.billing_contact ->> 'phone_number')::VARCHAR(32) AS order_billing_phone_number
, (co.billing_contact ->> 'mobile_phone_number')::VARCHAR(32) AS order_billing_mobile_phone_number
, (co.shipping_contact ->> 'first_name')::VARCHAR(64) AS order_shipping_first_name
, (co.shipping_contact ->> 'last_name')::VARCHAR(64) AS order_shipping_last_name
, (co.shipping_contact -> 'address' ->> 'street_address')::VARCHAR(128) AS order_shipping_street_address
, (co.shipping_contact -> 'address' ->> 'postal_code')::VARCHAR(16) AS order_shipping_postal_code
, (co.shipping_contact -> 'address' ->> 'locality')::VARCHAR(64) AS order_shipping_locality
, (co.shipping_contact -> 'address' ->> 'region')::VARCHAR(64) AS order_shipping_region
, (co.shipping_contact -> 'address' ->> 'country')::CHAR(2) AS order_shipping_country
, (co.shipping_contact ->> 'email')::VARCHAR(128) AS order_shipping_email
, (co.shipping_contact ->> 'gender')::VARCHAR(16) AS order_shipping_gender
, (co.shipping_contact ->> 'company_name')::VARCHAR(128) AS order_shipping_company_name
, (co.shipping_contact ->> 'phone_number')::VARCHAR(32) AS order_shipping_phone_number
, (co.shipping_contact ->> 'mobile_phone_number')::VARCHAR(32) AS order_shipping_mobile_phone_number
, s3.cable_quantity AS cable_quantity
, s4.device_quantity AS device_quantity
, (CASE
    WHEN co.magento_id IS NULL OR co.magento_id LIKE '77%' OR co.magento_id LIKE '78%'
      THEN (co.total_provisioned_price -> 'net' ->> 'amount')::DECIMAL(10,2)/100
    ELSE (left(ao."P.T. (Preis inkl. Steuer und Gutschrift)", length(ao."P.T. (Preis inkl. Steuer und Gutschrift)")-2)::DECIMAL(10,2)/1.19)
  END)::DECIMAL(10,2) AS order_net_invoice_amount
, CASE
    WHEN cr.id IS NOT NULL THEN TRUE
    ELSE FALSE
  END AS order_has_refund
, CASE
    WHEN ((co.magento_id IS NULL AND s1.refund_sum = s2.total_price) OR (co.magento_id IS NOT NULL AND ao."Magento-Status" = 'closed')) THEN TRUE
    ELSE FALSE
  END AS order_has_full_refund
, CASE
    WHEN ao.product_id IN ('2', '7', '10', '14, 15') AND ao."Magento-Status" IN ('complete', 'closed') THEN TRUE
    WHEN ao."Magento-Status" IS NULL AND s6.is_sold IS NOT NULL THEN s6.is_sold
    ELSE FALSE
  END AS order_is_sold
, CASE WHEN co.magento_id IS NOT NULL THEN TRUE ELSE FALSE END AS order_is_magento_customer
, CASE WHEN s5.device_quantity > 0 THEN TRUE ELSE FALSE END AS order_is_recurring
, CASE
    WHEN (nullif(s7.product_uuid, 'fake'))::UUID IN (SELECT product_uuid
                                                          FROM mapping.umd_product_map
                                                         WHERE is_subsequent_recurring IS TRUE
                                                           AND device_quantity_cnt IS TRUE) THEN TRUE
    ELSE FALSE
  END AS order_is_subsequent_recurring
, CASE WHEN co.creator = 'urn:vimcar:com.vimcar:application/importer' AND ci.id IS NOT NULL THEN TRUE ELSE FALSE END AS order_was_draft
, CASE WHEN s8.order_changed_to_onetime IS TRUE THEN TRUE ELSE FALSE END AS order_changed_to_onetime
, CASE WHEN DATE_TRUNC('month', co.created) = DATE_TRUNC('month', cc.created) THEN TRUE ELSE FALSE END AS order_is_cohort_analysis
, co.creator AS order_creator_urn
, co.created AS order_created_ts
, co.modifier AS order_modified_urn
, co.modified AS order_modified_ts

FROM vimcar.customers_order co
     LEFT JOIN vimcar.mg_all_orders ao
            ON ao."Bestellung Nr." = co.magento_id
     LEFT JOIN vimcar.customers_refund cr
            ON cr.order_id = co.id
     LEFT JOIN (SELECT cr.order_id,
                       sum((cr.amount -> 'net' ->> 'amount'):: INT) AS refund_sum
                  FROM vimcar.customers_refund cr
                 GROUP BY cr.order_id) s1
            ON s1.order_id = co.id
     LEFT JOIN (SELECT ci.order_id,
                       (ci.total_price -> 'net' ->> 'amount'):: INT AS total_price
                  FROM vimcar.customers_invoice ci) s2
            ON s2.order_id = co.id
     LEFT JOIN vimcar.customers_invoice ci
            ON ci.order_id = co.id
     LEFT JOIN (SELECT invoice_id,
                       status,
                       rank() over (PARTITION BY invoice_id ORDER BY status ASC) AS rank,
                       modified,
                       dwh_job_id
                  FROM vimcar.customers_payment
                 WHERE refund_id IS NULL) cp
            ON cp.invoice_id = ci.id
           AND rank = 1
     LEFT JOIN (SELECT id AS order_id,
                       sum(items.quantity) AS cable_quantity
                  FROM vimcar.customers_order o,
                       LATERAL (SELECT obj.value ->> 'product_id' AS product_uuid,
                                       (obj.value ->> 'quantity')::SMALLINT AS quantity
                                  FROM jsonb_array_elements(o.items) obj(value)) items
                 WHERE items.product_uuid IN ('67f2627c-d934-47bf-a04b-1eb94fb2e498')
                 GROUP BY order_id) s3
            ON co.id = s3.order_id
     LEFT JOIN (SELECT id AS order_id,
                       sum(items.quantity) AS device_quantity
                  FROM vimcar.customers_order o,
                       LATERAL (SELECT obj.value ->> 'product_id' AS product_uuid,
                                       (obj.value ->> 'quantity')::SMALLINT AS quantity,
                                       ss4.device_quantity_cnt
                                  FROM jsonb_array_elements(o.items) obj(value)
                                       JOIN (SELECT product_uuid,
                                                    device_quantity_cnt
                                               FROM mapping.umd_product_map) ss4
                                        ON ss4.product_uuid::TEXT = obj.value ->> 'product_id') items
                 WHERE items.device_quantity_cnt IS TRUE
                 GROUP BY order_id) s4
            ON co.id = s4.order_id
     LEFT JOIN (SELECT id AS order_id,
                       sum(items.quantity) AS device_quantity
                  FROM vimcar.customers_order o,
                       LATERAL (SELECT obj.value ->> 'product_id' AS product_uuid,
                                       (obj.value ->> 'quantity')::SMALLINT AS quantity,
                                       ss5.device_quantity_cnt,
                                       ss5.is_subsequent_recurring,
                                       ss5.is_change_to_onetime,
                                       ss5.is_recurring,
                                       ss5.kpi_annual_recurring_revenue
                                  FROM jsonb_array_elements(o.items) obj(value)
                                       JOIN (SELECT product_uuid,
                                                    device_quantity_cnt,
                                                    is_subsequent_recurring,
                                                    is_change_to_onetime,
                                                    is_recurring,
                                                    kpi_annual_recurring_revenue
                                               FROM mapping.umd_product_map) ss5
                                        ON ss5.product_uuid::TEXT = obj.value ->> 'product_id') items
                 WHERE items.device_quantity_cnt IS TRUE
                   AND items.is_subsequent_recurring IS FALSE
                   AND items.is_change_to_onetime IS FALSE
                   AND items.is_recurring IS TRUE
                   AND items.kpi_annual_recurring_revenue IS TRUE
                 GROUP BY order_id) s5
            ON co.id = s5.order_id
     LEFT JOIN (SELECT co.id AS order_id,
                       BOOL_OR(CASE
                                 WHEN items.is_sold_cnt IS TRUE AND items.device_quantity_cnt IS TRUE AND items.kpi_annual_recurring_revenue IS TRUE AND items.sales_rep_all_dongles IS TRUE
                                   AND ((cp.payment_method ->> 'type' IN ('sepa', 'card', 'paypal') AND cp.status = 'authorised')
                                    OR (cp.payment_method ->> 'type' = 'invoice')
                                    OR (cp.payment_method ->> 'type' IS NULL)) THEN TRUE
                                 ELSE FALSE
                               END) AS is_sold
                 FROM vimcar.customers_order co
                      LEFT JOIN vimcar.customers_payment cp
                             ON co.id = cp.order_id,
                      LATERAL (SELECT obj.value ->> 'product_id' AS product_uuid,
                                      ss6.is_sold_cnt,
                                      ss6.device_quantity_cnt,
                                      ss6.kpi_annual_recurring_revenue,
                                      ss6.sales_rep_all_dongles
                                 FROM jsonb_array_elements(co.items) obj(value)
                                      JOIN (SELECT product_uuid,
                                                   is_sold_cnt,
                                                   device_quantity_cnt,
                                                   kpi_annual_recurring_revenue,
                                                   sales_rep_all_dongles
                                              FROM mapping.umd_product_map) ss6
                                        ON ss6.product_uuid::TEXT = obj.value ->> 'product_id') items
                GROUP BY co.id) s6
            ON co.id = s6.order_id
     LEFT JOIN vimcar.customers_contract cc
            ON co.contract_id = cc.id
     LEFT JOIN (SELECT co2.id AS order_uuid,
                       ss1.product_uuid,
                       RANK() OVER (ORDER BY pmap.device_quantity_cnt DESC, pmap.is_subsequent_recurring DESC) AS rank
                  FROM vimcar.customers_order co2
                       JOIN (SELECT co3.id AS order_id,
                                    items.product_uuid
                               FROM vimcar.customers_order co3,
                                    LATERAL (SELECT obj.value ->> 'product_id' AS product_uuid
                                               FROM jsonb_array_elements(co3.items) obj(value)) items
                              ORDER BY co3.id) ss1
                         ON ss1.order_id = co2.id
                       JOIN mapping.umd_product_map pmap
                         ON pmap.product_uuid = (nullif(ss1.product_uuid, 'fake'))::UUID
                 ORDER BY co2.id) s7
            ON s7.order_uuid = co.id
           AND s7.rank = 1
     LEFT JOIN (SELECT co.id AS order_uuid,
                       TRUE AS order_changed_to_onetime
                  FROM vimcar.customers_order co,
                       LATERAL (SELECT obj.value ->> 'product_id' AS product_uuid
                                  FROM jsonb_array_elements(co.items) obj(value)) items
                 WHERE items.product_uuid = '974e92e4-fed9-499e-8b7e-582c986f8a6b') s8
            ON s8.order_uuid = co.id
WHERE co.status = 'placed'
/* orders to be ignored */
AND co.id IN ('3b647ac9-6014-4b51-9dba-13a0a0c4a48c');
;

WITH cte_contract_items AS (
  SELECT DISTINCT
    cc.outbound_id AS contract_id
  , (jsonb_array_elements(items) ->> 'product_id') AS product_uuid
  , (jsonb_array_elements(items) -> 'total_provisioned_price' -> 'net' ->> 'amount')::INT AS total_provisioned_price_net_amount
  , cc.discount ->> 'type' AS coupon_discount_type
  , (CASE cc.discount ->> 'type'
       WHEN 'percentage' THEN (cc.discount ->> 'value')
       WHEN 'amount' THEN (cc.discount -> 'value' ->> 'amount')
     END)::NUMERIC(8,2) AS coupon_discount_value
  FROM vimcar.customers_contract cc
  WHERE cc.outbound_id = '48278784'
)
SELECT * FROM cte_contract_items;

SELECT
  cc.outbound_id AS contract_id
, cc.id AS contract_uuid
, cc.customer_id AS customer_uuid
, lower(trim(trailing '.' from cc.domain)) AS domain_name
, cc.discount ->> 'type' AS coupon_discount_type
, (CASE cc.discount ->> 'type'
    WHEN 'percentage' THEN (cc.discount ->> 'value')
    WHEN 'amount' THEN (cc.discount -> 'value' ->> 'amount')
  END)::NUMERIC(8,2) AS coupon_discount_value
, cc.discount -> 'value' ->> 'currency' AS currency_code
, cc.coupon_code
, cc.status AS contract_status
, coalesce(items.contract_dongles_amt, 0) AS contract_dongles_amt
, cc.provisioning_period_start::DATE AS contract_provisioning_start_date_key
, cc.provisioning_period_end::DATE AS contract_provisioning_end_date_key
, cc.started::DATE AS contract_started_date_key
, cc.cancelation_reason AS contract_cancelation_reason
, cc.canceled::DATE AS contract_canceled_date_key
, cc.canceler AS contract_canceler_urn
, cc.items AS contract_item_array
, cc.created AS contract_created_ts
, cc.creator AS contract_creator_urn
, cc.modified AS contract_modified_ts
, cc.modifier AS contract_modifier_urn
FROM vimcar.customers_contract cc,
     LATERAL (SELECT sum((obj.value ->> 'quantity')::SMALLINT) AS contract_dongles_amt
                FROM jsonb_array_elements(cc.items) obj(value)
                     JOIN mapping.umd_product_map pmap
                       ON pmap.product_uuid = NULLIF(obj.value ->> 'product_id', 'fake')::UUID
                      AND pmap.kpi_active_cars IS TRUE) items
-- WHERE cc.outbound_id = 'V24502446'
WHERE cc.discount ->> 'type' = 'amount'
ORDER BY contract_id;


WITH cte_contract_items AS (
  SELECT DISTINCT
    cc.outbound_id AS contract_id
  , (jsonb_array_elements(items) ->> 'product_id') AS product_uuid
  , (jsonb_array_elements(items) -> 'total_provisioned_price' -> 'net' ->> 'amount')::INT AS total_provisioned_price_net_amount
  , cc.discount ->> 'type' AS coupon_discount_type
  , (CASE cc.discount ->> 'type'
       WHEN 'percentage' THEN (cc.discount ->> 'value')
       WHEN 'amount' THEN (cc.discount -> 'value' ->> 'amount')
     END)::NUMERIC(8,2) AS coupon_discount_value
  FROM vimcar.customers_contract cc
  WHERE cc.outbound_id = 'V34970486'
)
SELECT
  contract_id,
  round(sum(CASE
              WHEN coupon_discount_type = 'percentage' AND (monthly_payment IS FALSE OR monthly_payment IS NULL) THEN total_provisioned_price_net_amount * (1 - coupon_discount_value)
              WHEN coupon_discount_type = 'percentage' AND monthly_payment IS TRUE THEN total_provisioned_price_net_amount * (1 - coupon_discount_value) * 12
              WHEN coupon_discount_type = 'amount' AND (monthly_payment IS FALSE OR monthly_payment IS NULL) THEN total_provisioned_price_net_amount - (coupon_discount_value / 1.19)
              WHEN coupon_discount_type = 'amount' AND monthly_payment IS TRUE THEN (total_provisioned_price_net_amount - (coupon_discount_value / 1.19)) * 12
              WHEN coupon_discount_type IS NULL AND (monthly_payment IS FALSE OR monthly_payment IS NULL) THEN total_provisioned_price_net_amount
              WHEN coupon_discount_type IS NULL AND monthly_payment IS TRUE THEN total_provisioned_price_net_amount * 12
            END) / 100, 2)::DECIMAL(7,2) AS contract_total_net_value
  FROM cte_contract_items cte
       LEFT JOIN (SELECT product_uuid::TEXT,
                         monthly_payment
                    FROM mapping.umd_product_map) s
              ON s.product_uuid = cte.product_uuid
 WHERE cte.product_uuid IN (SELECT product_uuid::TEXT
                              FROM mapping.umd_product_map
                             WHERE kpi_annual_recurring_revenue IS TRUE)
GROUP BY contract_id
ORDER BY contract_id;


 SELECT DISTINCT
    cc.outbound_id AS contract_id
  , (jsonb_array_elements(items) ->> 'product_id') AS product_uuid
--   , (jsonb_array_elements(items) -> 'total_provisioned_price' -> 'net' ->> 'amount')::INT AS total_provisioned_price_net_amount
--   , cc.discount ->> 'type' AS coupon_discount_type
--   , (CASE cc.discount ->> 'type'
--        WHEN 'percentage' THEN (cc.discount ->> 'value')
--        WHEN 'amount' THEN (cc.discount -> 'value' ->> 'amount')
--      END)::NUMERIC(8,2) AS coupon_discount_value
  FROM vimcar.customers_contract cc
  WHERE cc.outbound_id = 'V24502446';

WITH cte_contract_items AS (
  SELECT DISTINCT
    cc.outbound_id AS contract_id
  , (jsonb_array_elements(items) ->> 'product_id') AS product_uuid
  , (jsonb_array_elements(items) -> 'total_provisioned_price' -> 'net' ->> 'amount')::INT AS total_provisioned_price_net_amount
  , cc.discount ->> 'type' AS coupon_discount_type
  , (CASE cc.discount ->> 'type'
       WHEN 'percentage' THEN (cc.discount ->> 'value')
       WHEN 'amount' THEN (cc.discount -> 'value' ->> 'amount')
     END)::NUMERIC(8,2) AS coupon_discount_value
  FROM vimcar.customers_contract cc
  WHERE cc.outbound_id = 'V34970486'
)
SELECT
  contract_id
  ,coupon_discount_value
  , monthly_payment
, total_provisioned_price_net_amount
  , round(sum(CASE
              WHEN coupon_discount_type = 'percentage' AND (monthly_payment IS FALSE OR monthly_payment IS NULL) THEN total_provisioned_price_net_amount * (1 - coupon_discount_value)
              WHEN coupon_discount_type = 'percentage' AND monthly_payment IS TRUE THEN total_provisioned_price_net_amount * (1 - coupon_discount_value) * 12
              WHEN coupon_discount_type = 'amount' AND (monthly_payment IS FALSE OR monthly_payment IS NULL) THEN total_provisioned_price_net_amount - (coupon_discount_value / 1.19)
              WHEN coupon_discount_type = 'amount' AND monthly_payment IS TRUE THEN (total_provisioned_price_net_amount - (coupon_discount_value / 1.19)) * 12
              WHEN coupon_discount_type IS NULL AND (monthly_payment IS FALSE OR monthly_payment IS NULL) THEN total_provisioned_price_net_amount
              WHEN coupon_discount_type IS NULL AND monthly_payment IS TRUE THEN total_provisioned_price_net_amount * 12
            END) / 100, 2)::DECIMAL(7,2) AS contract_total_net_value
  FROM cte_contract_items cte
       LEFT JOIN (SELECT product_uuid::TEXT,
                         monthly_payment
                    FROM mapping.umd_product_map) s
              ON s.product_uuid = cte.product_uuid
 WHERE cte.product_uuid IN (SELECT product_uuid::TEXT
                              FROM mapping.umd_product_map
                             WHERE kpi_annual_recurring_revenue IS TRUE)
GROUP BY contract_id,coupon_discount_value,monthly_payment,total_provisioned_price_net_amount
ORDER BY contract_id;

---- as it is now:
  SELECT DISTINCT
    cc.outbound_id AS contract_id
  , (jsonb_array_elements(items) ->> 'product_id') AS product_uuid
  , (jsonb_array_elements(items) -> 'total_provisioned_price' -> 'net' ->> 'amount')::INT AS total_provisioned_price_net_amount
  , cc.discount ->> 'type' AS coupon_discount_type
  , (CASE cc.discount ->> 'type'
       WHEN 'percentage' THEN (cc.discount ->> 'value')
       WHEN 'amount' THEN (cc.discount -> 'value' ->> 'amount')
     END)::NUMERIC(8,2) AS coupon_discount_value
  FROM vimcar.customers_contract cc
  WHERE cc.outbound_id = 'V34970486';

-- solution:
SELECT
s1.contract_id
, s1.product_uuid
, s1.total_provisioned_price_net_amount
, s1.coupon_discount_type
, s1.order_coupon_discount_value
, CASE
    WHEN s1.coupon_discount_type = 'percentage'
     THEN s1.order_coupon_discount_value
    WHEN s1.coupon_discount_type = 'amount'
     THEN order_coupon_discount_value * ((total_provisioned_price_net_amount::decimal(10,2) / sum(total_provisioned_price_net_amount) OVER (PARTITION BY contract_id))::decimal(10,2))
    END AS coupon_discount_value
FROM (
  SELECT DISTINCT
    cc.outbound_id AS contract_id
  , (jsonb_array_elements(items) ->> 'product_id') AS product_uuid
  , (jsonb_array_elements(items) -> 'total_provisioned_price' -> 'net' ->> 'amount')::INT AS total_provisioned_price_net_amount
  , cc.discount ->> 'type' AS coupon_discount_type
  , (CASE cc.discount ->> 'type'
       WHEN 'percentage' THEN (cc.discount ->> 'value')
       WHEN 'amount' THEN (cc.discount -> 'value' ->> 'amount')
     END)::NUMERIC(8,2) AS order_coupon_discount_value
  ,(jsonb_array_elements(items) -> 'total_provisioned_price' -> 'net' ->> 'amount')
  FROM vimcar.customers_contract cc
  WHERE cc.outbound_id = 'V34970486'
) s1
GROUP BY contract_id, product_uuid, total_provisioned_price_net_amount, coupon_discount_type, order_coupon_discount_value

SELECT * FROM vimcar.customers_contract cc WHERE cc.outbound_id = '48278784'
SELECT * FROM vimcar.customers_order WHERE contract_id = '855bb0d2-3067-4093-92ab-6979a6856f43'
;
SELECT * FROM fdw_customers.contract WHERE outbound_id = '48278784';

SELECT * FROM ext_sources.personio_timeoffs WHERE employee_id = 720061;
SELECT * FROM ext_sources.personio_timeoffs WHERE employee_first_name = 'Piotr';


SELECT
satisfaction_rating
, (satisfaction_rating ->> 'id')::BIGINT AS zd_ticket_sat_rat_id
, (satisfaction_rating ->> 'score')::VARCHAR(16) AS zd_ticket_sat_rat_score
, (satisfaction_rating ->> 'comment')::VARCHAR(2048) AS zd_ticket_sat_rat_comment
, (satisfaction_rating ->> 'reason')::VARCHAR(32) AS zd_ticket_sat_rat_reason
, (satisfaction_rating ->> 'created_at')::TIMESTAMP AS zd_ticket_sat_rat_created_ts
, (satisfaction_rating ->> 'updated_at')::TIMESTAMP AS zd_ticket_sat_rat_modified_ts
, zd_ticket_created_at AS zd_ticket_created_ts
, zd_ticket_updated_at AS zd_ticket_modified_ts
FROM ext_sources.zendesk_tickets
WHERE (satisfaction_rating ->> 'comment') IS NOT NULL;

SELECT * FROM ext_sources.zendesk_tickets WHERE id=542288

SELECT MAX(data_processed_to_job_id) AS curr
  FROM dwh_job.job_history
 WHERE job_name = 'zendesk_dwh'
   AND status = 'succeeded';

SELECT MAX(id) AS prev
  FROM dwh_job.job_history
 WHERE job_name = 'zendesk_stg'
   AND status = 'succeeded';

SELECT * FROM stg.ext_sources.personio_timeoffs WHERE employee_id = 323161
9020a2fe-1d03-42a6-a27b-b59e17801e55
;
SELECT id
, provisioning_start
, (provisioning_start AT TIME ZONE 'Europe/Berlin')::DATE
FROM fdw_customers."order" WHERE id = '9020a2fe-1d03-42a6-a27b-b59e17801e55'
;

SELECT DISTINCT lower(dc.domain_name) AS domain_name,
TRUE AS domain_uses_geo_time_fencing_ft
FROM stg.vimcar.foxbox_domain_configuration dc
         JOIN (SELECT left(gf.domain, length(gf.domain) - 1) AS domain
               FROM stg.vimcar.geolocation_fence gf
               WHERE gf.type = 'pool'
                 AND gf.state = 'active') s1 ON s1.domain = dc.domain_name
WHERE dc.configuration -> 'services' ->> 'fencing' = 'true';

WITH cte_contract_items AS (
  SELECT DISTINCT
    cc.outbound_id AS contract_id
  , (jsonb_array_elements(items) ->> 'product_id') AS product_uuid
  , (jsonb_array_elements(items) -> 'total_provisioned_price' -> 'net' ->> 'amount')::INT AS total_provisioned_price_net_amount
  , cc.discount ->> 'type' AS coupon_discount_type
  , (CASE cc.discount ->> 'type'
       WHEN 'percentage' THEN (cc.discount ->> 'value')
       WHEN 'amount' THEN (cc.discount -> 'value' ->> 'amount')
     END)::NUMERIC(8,2) AS coupon_discount_value
  FROM vimcar.customers_contract cc
)
SELECT
  contract_id,
  round(sum(CASE
              WHEN coupon_discount_type = 'percentage' AND (monthly_payment IS FALSE OR monthly_payment IS NULL) THEN total_provisioned_price_net_amount * (1 - coupon_discount_value)
              WHEN coupon_discount_type = 'percentage' AND monthly_payment IS TRUE THEN total_provisioned_price_net_amount * (1 - coupon_discount_value) * 12
              WHEN coupon_discount_type = 'amount' AND (monthly_payment IS FALSE OR monthly_payment IS NULL) THEN total_provisioned_price_net_amount - (coupon_discount_value / 1.19)
              WHEN coupon_discount_type = 'amount' AND monthly_payment IS TRUE THEN (total_provisioned_price_net_amount - (coupon_discount_value / 1.19)) * 12
              WHEN coupon_discount_type IS NULL AND (monthly_payment IS FALSE OR monthly_payment IS NULL) THEN total_provisioned_price_net_amount
              WHEN coupon_discount_type IS NULL AND monthly_payment IS TRUE THEN total_provisioned_price_net_amount * 12
            END) / 100, 2)::DECIMAL(7,2) AS contract_total_net_value
  FROM cte_contract_items cte
       LEFT JOIN (SELECT product_uuid::TEXT,
                         monthly_payment
                    FROM mapping.umd_product_map) s
              ON s.product_uuid = cte.product_uuid
 WHERE cte.product_uuid IN (SELECT product_uuid::TEXT
                              FROM mapping.umd_product_map
                             WHERE kpi_annual_recurring_revenue IS TRUE)
AND contract_id = '55646763'
GROUP BY contract_id
ORDER BY contract_id;

SELECT contract_id
FROM (SELECT ctr.contract_id
           , upm.product_name
           , (o.order_provisioning_end_ts AT TIME ZONE 'Europe/Berlin')::DATE                                        AS order_provisioning_end_dt
           , MAX(o.order_provisioning_end_ts AT TIME ZONE 'Europe/Berlin')
             OVER (PARTITION BY ctr.contract_id)::DATE                                                               AS max_order_provisioning_end_dt
      FROM dwh_main.fact_order_item foi
               JOIN dwh_main.dim_order o ON o.order_key = foi.order_key
               JOIN dwh_main.umd_product_map upm ON foi.order_item_product_key = upm.product_key
               JOIN dwh_main.dim_contract ctr ON ctr.contract_uuid = o.contract_uuid
      WHERE o.order_was_draft = FALSE
        AND o.order_has_full_refund = FALSE) s1
WHERE product_name LIKE '%Fleet%'
  AND order_provisioning_end_dt = max_order_provisioning_end_dt
GROUP BY contract_id
;

SELECT
  cc.outbound_id AS contract_id
, TRUE AS contract_is_fleet_customer
, product_uuid
FROM vimcar.customers_contract cc,
     LATERAL (SELECT obj.value ->> 'product_id' AS product_uuid,
                     pmap.product_name
                FROM jsonb_array_elements(cc.items) obj(value)
                     JOIN mapping.umd_product_map pmap
                       ON pmap.product_uuid = NULLIF(obj.value ->> 'product_id', 'fake')::UUID
                WHERE product_name LIKE '%Fleet%') contract_items
--WHERE cc.outbound_id = 'V42945808'
GROUP BY outbound_id, product_uuid;
;



SELECT cc.outbound_id AS contract_id, TRUE AS contract_is_fleet_customer
FROM vimcar.customers_contract cc
         JOIN mapping.map_customer_principal_domain mcpd
              ON mcpd.map_customer_domain = lower(trim(TRAILING '.' FROM cc.domain))
         JOIN (SELECT domain_name
               FROM vimcar.foxbox_principal fp
                        JOIN vimcar.foxbox_globalroles fg ON fg.principal_id = fp.id
               WHERE fg.role IN
                     ('bookingmanager', 'carmanager', 'contentmanager', 'contentreader', 'costmanager', 'domainmanager',
                      'inviter', 'locationreader', 'rolemanager', 'subscriber', 'usermanager', 'userrecordreader')
                 AND fg.role <> 'lockedaccount'
               GROUP BY domain_name) dp ON dp.domain_name = mcpd.map_principal_domain,
     LATERAL (SELECT obj.value ->> 'product_id' AS product_uuid
              FROM jsonb_array_elements(cc.items) obj(value)
                       JOIN mapping.umd_product_map pmap
                            ON pmap.product_uuid = NULLIF(obj.value ->> 'product_id', 'fake')::UUID
              WHERE (is_recurring IS TRUE OR is_subsequent_recurring IS TRUE)) contract_item
UNION
SELECT outbound_id AS contract_id, TRUE AS contract_is_fleet_customer
FROM (SELECT cc.outbound_id
           , order_items.product_name
           , (co.provisioning_end AT TIME ZONE 'Europe/Berlin')::DATE AS order_provisioning_end_dt
           , MAX(co.provisioning_end AT TIME ZONE 'Europe/Berlin')
             OVER (PARTITION BY cc.outbound_id)::DATE AS                 max_order_provisioning_end_dt
      FROM vimcar.customers_order co
               JOIN vimcar.customers_contract cc ON cc.id = co.contract_id,
           LATERAL (SELECT obj.value ->> 'product_id' AS product_uuid, pmap.product_name
                    FROM jsonb_array_elements(co.items) obj(value)
                             JOIN mapping.umd_product_map pmap
                                  ON pmap.product_uuid = NULLIF(obj.value ->> 'product_id', 'fake')::UUID
                    WHERE (is_recurring IS TRUE OR is_subsequent_recurring IS TRUE) ) order_items) s1
WHERE product_name LIKE '%Fleet%'
  AND order_provisioning_end_dt = max_order_provisioning_end_dt
GROUP BY outbound_id
;


SELECT
  domain_name
, TRUE AS domain_uses_contract_ft
FROM (
WITH cte AS (
    SELECT DISTINCT
        lower(fc.domain_name) AS domain_name
        , fc.id AS car_id
        , s1.contract_car_id
    FROM stg.vimcar.foxbox_cars fc
        LEFT JOIN (SELECT DISTINCT
                    lower(vc.domain) AS domain
                    , CAST(substr(vc.vehicle,strpos(vc.vehicle,'/')+1) AS INTEGER) AS contract_car_id
                    FROM stg.vimcar.vehicles_contract vc
                    WHERE vc.deleted IS NULL) s1
        ON s1.domain = lower(fc.domain_name)
         AND s1.contract_car_id = fc.id
    WHERE payload ->> 'endLogbookTimestamp' IS NULL
            )
 SELECT DISTINCT
    domain_name
    , count(car_id) OVER (PARTITION BY
        CASE
            WHEN domain_name = 'com.vimcar' THEN 'com.vimcar'
            WHEN domain_name NOT LIKE 'com.vimcar.de.%' THEN 'com.vimcar.' || split_part(domain_name, '.', 3)
            WHEN domain_name LIKE 'com.vimcar.de.%' THEN 'com.vimcar.de.' || split_part(domain_name, '.', 4)
         END ) AS car_amt  -- amount of cars on MAIN domain level
    , count(contract_car_id) OVER (PARTITION BY
        CASE
            WHEN domain_name = 'com.vimcar' THEN 'com.vimcar'
            WHEN domain_name NOT LIKE 'com.vimcar.de.%' THEN 'com.vimcar.' || split_part(domain_name, '.', 3)
            WHEN domain_name LIKE 'com.vimcar.de.%' THEN 'com.vimcar.de.' || split_part(domain_name, '.', 4)
         END ) AS contract_car_amt  -- amount of cars with contract on MAIN domain level
 FROM cte
) s2
--WHERE round(s2.contract_car_amt / s2.car_amt::DECIMAL, 2) >= 0.5
WHERE domain_name = 'com.vimcar.ultimate-lifestyle-group'
;
SELECT * FROM stg.vimcar.foxbox_cars WHERE domain_name like '%K38343656%' ;
SELECT * FROM stg.vimcar.vehicles_contract WHERE domain like '%K38343656%' ;

SELECT * FROM stg.vimcar.vehicles_contract  WHERE start::DATE = '2019-05-23';

SELECT * FROM stg.vimcar.vehicles_contract WHERE id = '16d0e204-5cc9-4a85-a712-75dedac83645' ;

V68947575
vimcar.customers_customer
com.vimcar.de.k38343656

com.vimcar.ultimate-lifestyle-group
com.vimcar.de.k19070304
;

    SELECT DISTINCT
        lower(fc.domain_name) AS domain_name
        , domain
        , mcpd.map_customer_domain
        , mcpd.map_principal_domain
    FROM stg.vimcar.foxbox_cars fc
        LEFT JOIN (SELECT DISTINCT
                    lower(vc.domain) AS domain
                    FROM stg.vimcar.vehicles_contract vc
                    WHERE vc.deleted IS NULL) s1
        ON s1.domain = lower(fc.domain_name)
    left join  mapping.map_customer_principal_domain mcpd
       ON mcpd.map_principal_domain = lower(fc.domain_name)
    WHERE payload ->> 'endLogbookTimestamp' IS NULL

;
SELECT lower(vc.domain) AS domain, lower(fc.domain_name) AS domain_name, mcpd.map_customer_domain, mcpd2.map_customer_domain
FROM stg.vimcar.vehicles_contract vc
LEFT JOIN stg.vimcar.foxbox_cars fc  ON lower(vc.domain) = lower(fc.domain_name) AND payload ->> 'endLogbookTimestamp' IS NULL
LEFT JOIN mapping.map_customer_principal_domain mcpd  ON lower(vc.domain) = mcpd.map_customer_domain
LEFT JOIN mapping.map_customer_principal_domain mcpd2  ON lower(vc.domain) = mcpd2.map_principal_domain
WHERE vc.deleted IS NULL
GROUP BY 1,2,3,4

;

SELECT
  lower(trim(TRAILING '.' FROM domain)) AS domain_name
, max(started)::DATE AS contract_started_date_key
, 'Customer'::VARCHAR(8) AS domain_contract_type
, (CASE WHEN max(provisioning_period_end) > current_date THEN 'Active' END)::VARCHAR(16) AS domain_contract_status
FROM vimcar.customers_contract
GROUP BY 1;

SELECT
*
FROM vimcar.customers_contract WHERE domain like '%aam-autoservice%';


SELECT
  lower(domain_name) AS domain_name
, sum(CASE WHEN payload ->> 'endLogbookTimestamp' IS NULL THEN 1 ELSE 0 END)::INT AS domain_active_car_amt
, min(created)::DATE AS first_usage_date_key
FROM vimcar.foxbox_cars
GROUP BY 1;