
WITH cte_fleet_products AS (SELECT
    fcl.customer_key,
    ctr.contract_provisioning_start_date_key,
    ctr.contract_provisioning_end_date_key,
    ctr.contract_status,
    string_agg(DISTINCT split_part(product_name, ' ', 3),',') AS product_trip
FROM dwh_main.fact_contract_line fcl
        JOIN dwh_main.dim_contract ctr ON ctr.contract_key = fcl.contract_key
        JOIN dwh_main.umd_product_map upmap ON fcl.contract_item_product_key = upmap.product_key
                 WHERE TRUE
                   AND product_name ~* 'fleet'
                   AND is_subsequent_recurring = FALSE
GROUP BY fcl.customer_key,contract_provisioning_start_date_key,contract_provisioning_end_date_key,contract_status
    )
,cte_trips AS (SELECT c.customer_key
                        , c.customer_id
                        , p.domain_name
                        , p.principal_id
                        , p.principal_is_active
                        , t.car_id
                        , car.car_type
                        , t.trip_id
                        , t.trip_distance / 1000 AS distance_km
                        , t.trip_start_country
                        , t.trip_start_postal_code
                        , t.trip_start_latitude
                        , t.trip_start_longitude
                        , t.trip_end_country
                        , t.trip_end_postal_code
                        , t.trip_end_latitude
                        , t.trip_end_longitude
                        , ddate.date_key         AS trip_start_dt
                        , left(ddate.datestring_yyyymmdd,6) AS yyyymm
                        , ddate.week_yyyyww      AS yyyyww
                        , product_trip
                        , contract_provisioning_start_date_key
                        , contract_provisioning_end_date_key
                        , contract_status
    FROM cte_fleet_products cfp
             JOIN  dwh_main.dim_customer c
                       ON cfp.customer_key = c.customer_key
             JOIN dwh_main.dim_principal p
                       ON c.domain_name = p.domain_name
             JOIN dwh_main.dim_trip t
                       ON t.trip_owner_id = p.principal_id
             JOIN dwh_main.dim_car car
                       ON t.car_id = car.car_id
             JOIN dwh_main.dim_date ddate
                       ON ddate.date_key = t.trip_start_ts::DATE
    WHERE TRUE
      AND merge_parent_trip_id IS NULL
      AND trip_distance IS NOT NULL
      AND t.trip_start_ts::DATE >= '2019-01-01'
      AND t.trip_start_ts::DATE <= '2019-06-30'
    -- trip within the contract period:
--       AND t.trip_start_ts::DATE >= contract_provisioning_start_date_key
--       AND t.trip_start_ts::DATE <= contract_provisioning_end_date_key

--AND c.customer_id IN ('K90792010','K95193490','K14798219','K14634726','K98554709','K14798219','K94302116')
     AND p.domain_name = 'com.vimcar.abdichtung-bautenschutz-gmbh'
)
SELECT contract_provisioning_start_date_key,contract_provisioning_end_date_key,min(trip_start_dt),max(trip_start_dt) FROM cte_trips
GROUP BY 1,2



-- SELECT count(*),count(DISTINCT customer_key) FROM (
SELECT
    foi.customer_key,
    ctr.contract_provisioning_start_date_key,
    ctr.contract_provisioning_end_date_key,
    ctr.contract_status,
    string_agg(DISTINCT split_part(product_name, ' ', 3),',') AS product_trip
FROM dwh_main.fact_order_item foi
        JOIN dwh_main.dim_contract ctr ON ctr.contract_key = foi.contract_key
        JOIN dwh_main.umd_product_map upmap ON foi.order_item_product_key = upmap.product_key
                 WHERE TRUE
                   AND product_name ~* 'fleet'
                   AND is_subsequent_recurring = FALSE
                    AND customer_key = 6666
GROUP BY foi.customer_key,contract_provisioning_start_date_key,contract_provisioning_end_date_key,contract_status
ORDER BY 1

SELECT * FROM dwh_main.dim_contract WHERE customer_uuid = 'cdb3271d-da68-4648-99d1-1060c9dc4f0c';
SELECT * FROM dwh_main.fact_order_item WHERE customer_key = 6666;
SELECT * FROM dwh_main.dim_customer WHERE customer_key = 6666;
SELECT * FROM dwh_main.fact_contract_line WHERE customer_key = 6666;

SELECT fcl.contract_key
     ,fcl.customer_key
,ctr.contract_status
FROM dwh_main.fact_contract_line fcl
JOIN dwh_main.dim_contract ctr ON ctr.contract_key=fcl.contract_key

SELECT count(*),count(DISTINCT customer_key) FROM data_mart_marketing.fleet_product_customer
SELECT count(*),count(DISTINCT trip_id) FROM data_mart_marketing.fleet_product_trip


    WITH cte_fleet_products AS (SELECT fcl.customer_key
                                         , ctr.domain_name
                                         , ctr.contract_provisioning_start_date_key
                                         , ctr.contract_provisioning_end_date_key
                                         , ctr.contract_status
                                         , string_agg(DISTINCT split_part(product_name, ' ', 3), ',') AS product_trip
                                    FROM dwh_main.fact_contract_line fcl
                                             JOIN dwh_main.dim_contract ctr
                                                 ON ctr.contract_key = fcl.contract_key
                                             JOIN dwh_main.umd_product_map upmap
                                                  ON fcl.contract_item_product_key = upmap.product_key
                                    WHERE TRUE
                                      AND product_name ~* 'fleet'
                                      AND is_subsequent_recurring = FALSE
                                    GROUP BY fcl.customer_key, ctr.domain_name,
                                             ctr.contract_provisioning_start_date_key,
                                             ctr.contract_provisioning_end_date_key, ctr.contract_status)
          SELECT * FROM cte_fleet_products WHERE customer_key=6666;

SELECT * FROM dwh_main.dim_contract WHERE customer_uuid = 'cdb3271d-da68-4648-99d1-1060c9dc4f0c';
SELECT * FROM dwh_main.fact_order_item WHERE customer_key = 6666;
SELECT * FROM dwh_main.dim_customer WHERE customer_key = 6666;

SELECT * FROM dwh_main.fact_contract_line WHERE customer_key = 6666;
SELECT * FROM dwh_main.fact_order_item WHERE customer_key = 6666;
