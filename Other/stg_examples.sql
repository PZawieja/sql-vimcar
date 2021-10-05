SELECT c.outbound_id,
       contract_items.product_uuid,
       contract_items.total_provisioned_price_net_amount,
       contract_items.product_name
FROM fdw_customers.contract c,

       LATERAL (SELECT obj.value ->> 'product_id' AS product_uuid,
                       (obj.value -> 'total_provisioned_price' -> 'net' ->> 'amount')::INT AS total_provisioned_price_net_amount,
                       s.product_name
                FROM jsonb_array_elements(c.items) obj(value)
                     JOIN (SELECT product_uuid,
                                  product_name,
                                  monthly_payment
                             FROM mapping.umd_product_map) s
                       ON s.product_uuid::TEXT = obj.value ->> 'product_id') contract_items
WHERE c.outbound_id = 'V80543341';

-------------------------------------

SELECT id AS trip_id,
       car_id,
       start_timestamp
  FROM vimcar.foxbox_trips
 WHERE start_timestamp < '2012-01-01' OR start_timestamp > now();

select id,
provisioning_period_start,
       provisioning_period_start AT TIME ZONE 'Europe/Berlin'
from fdw_customers.contract


SELECT
  domain_name
, car_amt
, contract_amt
, round(CASE WHEN contract_amt IS NOT NULL THEN contract_amt /car_amt ::DECIMAL END, 2) AS contract_car__ratio
FROM (
  WITH cars_cte AS (
    SELECT
      lower(fc.domain_name) AS domain_name
    , count(*) car_amt
    FROM stg.vimcar.foxbox_cars fc
    WHERE payload ->> 'endLogbookTimestamp' IS NULL
    GROUP BY fc.domain_name
  )
  SELECT
    cte.domain_name
  , car_amt
  , ss1.contract_amt
  FROM cars_cte cte
       LEFT JOIN (SELECT
                    lower(vc.domain) AS domain_name
                  , COUNT(*) AS contract_amt
                  FROM stg.vimcar.vehicles_contract vc
                  WHERE vc.deleted IS NULL
                  GROUP BY vc.domain) ss1
              ON ss1.domain_name = cte.domain_name) s1

SELECT * FROM  vimcar.calendar_booking limit 100


SELECT
  lower(domain) AS domain_name
, TRUE AS domain_uses_booking_ft
FROM vimcar.calendar_booking
WHERE created::DATE >= current_date - 30
GROUP BY 1
HAVING count(*) >= 2;


SELECT *
FROM vimcar.foxbox_principal
where domain_name IN ('com.vimcar.de.39011067', 'com.vimcar.39011067')

SELECT *
FROM vimcar.foxbox_principal
where domain_name like ('com.vimcar.de%')

SELECT id AS trip_id,
       car_id,
       start_timestamp
  FROM vimcar.foxbox_trips
 WHERE start_timestamp < '2012-01-01' OR start_timestamp > now();

select count (*) from stg.vimcar.calendar_booking --24280

WITH cte_stg_customers AS (SELECT lower(trim(TRAILING '.' FROM ctr.domain))                                              AS domain_name
                                , lower(contact ->> 'email')                                                             AS email
                                , max(started)::DATE                                                                     AS contract_started_date_key
                                , 'Customer'::VARCHAR(8)                                                                 AS domain_contract_type
                                , (CASE WHEN max(provisioning_period_end) > current_date THEN 'Active' END)::VARCHAR(16) AS domain_contract_status
                           FROM vimcar.customers_contract ctr
                                    LEFT JOIN vimcar.customers_customer c ON c.domain = ctr.domain
                           GROUP BY 1, 2
)

SELECT * FROM cte_stg_customers
-- WHERE domain_name IN ('com.vimcar.de.14247690','com.vimcar.de.k55685076')
;

SELECT * FROM stg.ext_sources.zendesk_tickets LIMIT 100;

SELECT * FROM  vimcar.customers_contract LIMIT 100;


