SELECT * FROM data_mart_internal_reporting.ir_sh_cb_customer WHERE customer_id = 'K76310569';
SELECT * FROM data_mart_internal_reporting.ir_sh_cb_subscription WHERE customer_id = 'K76310569';
SELECT next_renewal_dt FROM data_mart_internal_reporting.ir_m_investor_report WHERE customer_id = 'K76310569';
SELECT next_renewal_dt FROM data_mart_internal_reporting.tmp_revenue_reporting_dom WHERE domain_name like '%k76310569%';

WITH cte_revenue_raw AS (
    SELECT
        ctr.domain_name
         , ctr.contract_id                                                                                    AS contract_id
         , CASE
               WHEN ctr.contract_status = 'active'
                   AND MAX(o.order_provisioning_end_ts) OVER (PARTITION BY ctr.contract_id) ::DATE < current_date - INTERVAL '6 months'
                   THEN 'canceled'
               ELSE ctr.contract_status
        END AS contract_status
         , CASE
               WHEN contract_status != 'active'
                   OR MAX(o.order_provisioning_end_ts) OVER (PARTITION BY ctr.contract_id) ::DATE < current_date - INTERVAL '6 months'
                   THEN coalesce(contract_canceled_date_key, contract_modified_ts AT TIME ZONE 'Europe/Berlin')
        END::DATE AS contract_terminated_dt
         , CASE
               WHEN MIN(ctr.contract_status) OVER (PARTITION BY ctr.domain_name) != 'active'
                   OR MAX(o.order_provisioning_end_ts) OVER (PARTITION BY ctr.domain_name) ::DATE < current_date - INTERVAL '6 months'
                   THEN MAX(coalesce(contract_canceled_date_key, contract_modified_ts AT TIME ZONE 'Europe/Berlin')) OVER (PARTITION BY ctr.domain_name)::DATE
        END AS terminated_dt
         , ctr.contract_total_net_value
         , ctr.contract_creator_urn
         , (ctr.contract_created_ts AT TIME ZONE 'Europe/Berlin')::DATE                                      AS contract_created_dt
         , CASE WHEN o.order_creator_urn ILIKE '%renewal%' THEN TRUE ELSE FALSE END                          AS renewal_temp
         , upm.product_name
         , CASE
               WHEN upm.product_family IN ('Logbook B2B', 'Logbook B2C') THEN 'Logbook'
               WHEN upm.product_family IN ('Admin', 'Geo', 'Pro') THEN upm.product_family
               ELSE 'Other'
        END                                                                                            AS product_family
         , CASE
               WHEN upm.monthly_payment = FALSE
                   AND product_name LIKE '%3-jährige Laufzeit%'
                   THEN '36 mths, yearly'
               WHEN upm.monthly_payment = TRUE
                   AND product_name LIKE '%3-jährige Laufzeit%'
                   THEN '36 mths, monthly'
               WHEN upm.monthly_payment = FALSE
                   THEN '12 mths, yearly'
               WHEN upm.monthly_payment = TRUE
                   THEN '12 mths, monthly'
        END                                                                                              AS product_length_and_payment
         , upm.product_uuid
         , o.order_has_full_refund
         , o.order_has_refund
         , o.order_uuid
         , o.order_discount_percentage::decimal(10,2)                                                         AS order_discount_percentage
         , o.order_discount_amount                                                                            AS order_discount_amount
         , foi.order_item_total_price_net_amt                                                                 AS order_item_total_price_net_amt
         , foi.order_item_total_price_gross_amt                                                               AS order_item_total_price_gross_amt
         , coalesce((1 + o.order_total_price_tax), 1.19)                                                      AS tax_rate
         , foi.order_item_quantity                                                                            AS order_quantity
         , coalesce(refund_net_amount,0)                                                                      AS refund_net_amount
         , (o.order_created_ts AT TIME ZONE 'Europe/Berlin')::DATE                                            AS order_created_dt
         , (o.order_provisioning_start_ts AT TIME ZONE 'Europe/Berlin')::DATE                                 AS order_provisioning_start_dt
         , (o.order_provisioning_end_ts AT TIME ZONE 'Europe/Berlin')::DATE                                   AS original_order_provisioning_end_dt

         , CASE
               WHEN MIN(ctr.contract_status) OVER (PARTITION BY ctr.domain_name) <> 'active'
                   AND o.order_provisioning_end_ts::DATE = MAX(o.order_provisioning_end_ts::DATE) OVER (PARTITION BY ctr.domain_name)
                   AND date_trunc('month',coalesce(contract_canceled_date_key, contract_modified_ts AT TIME ZONE 'Europe/Berlin'))::DATE
                        >= MIN(date_trunc('month',o.order_provisioning_start_ts AT TIME ZONE 'Europe/Berlin')::DATE) OVER (PARTITION BY ctr.contract_id)  -- contract terminated once it started provisioning
                   AND (
                        (monthly_payment = TRUE
                            AND date_trunc('month', o.order_provisioning_start_ts AT TIME ZONE 'Europe/Berlin') = date_trunc('month', o.order_provisioning_end_ts AT TIME ZONE 'Europe/Berlin')) -- provisioning starts and ends in the same month (monthly billing)
--                             OR  (monthly_payment = FALSE AND date_trunc('month', o.order_provisioning_start_ts) + interval '1 year' > date_trunc('month', o.order_provisioning_end_ts)) -- provisioning ends within 12 mmths (yearly billing)
                        )
                   THEN date_trunc('month', o.order_provisioning_end_ts) + INTERVAL '1 month'
               ELSE o.order_provisioning_end_ts AT TIME ZONE 'Europe/Berlin'
        END::DATE AS order_provisioning_end_dt

         , CASE
               WHEN ctr.contract_status <> 'active'
                   AND date_trunc('month',coalesce(contract_canceled_date_key, contract_modified_ts AT TIME ZONE 'Europe/Berlin'))::DATE
                        <= MIN(date_trunc('month',o.order_provisioning_start_ts AT TIME ZONE 'Europe/Berlin')::DATE) OVER (PARTITION BY ctr.contract_id)
                   THEN TRUE ELSE FALSE
        END AS contract_terminated_in_the_first_month
         , COALESCE(emp_2.employee_team,'unknown')                                                            AS contract_creator
         , upm.monthly_payment
         , i.invoice_uuid
    FROM dwh_main.fact_order_item foi
             JOIN dwh_main.dim_order o ON o.order_key = foi.order_key
             JOIN dwh_main.umd_product_map upm ON foi.order_item_product_key = upm.product_key
             JOIN dwh_main.dim_contract ctr ON ctr.contract_uuid = o.contract_uuid
        --              JOIN (SELECT DISTINCT map_customer_domain_name
--                    FROM dwh_main.map_customer_to_foxbox_domain) mapcpd ON mapcpd.map_customer_domain_name = ctr.domain_name
             LEFT JOIN dwh_main.dim_invoice i ON i.order_uuid = o.order_uuid
             LEFT JOIN (SELECT invoice_id, SUM(refund_net_amount) AS refund_net_amount
                        FROM dwh_main.dim_refund
                        GROUP BY invoice_id) refund ON refund.invoice_id = i.invoice_id
             LEFT JOIN dwh_main.umd_employee_map emp_2 ON right(ctr.contract_creator_urn, 36) = emp_2.user_uuid::TEXT
        --AND record_is_active = TRUE
        AND (o.order_provisioning_start_ts AT TIME ZONE 'Europe/Berlin')::DATE BETWEEN emp_2.valid_from AND emp_2.valid_until
    WHERE foi.opcode != 'D'
      --AND foi.order_item_total_price_net_amt > 0
      AND (upm.is_recurring = TRUE OR upm.is_subsequent_recurring = TRUE)
      AND o.order_uuid <> 'a99d4f2d-cfb4-4c86-b358-4aeb0b1b5f3d' -- incorrectly created order
--	  AND ctr.contract_id <> 'V47217460' -- problematic contract, incorectly created by Ops, excluded as of 2021-05-19
--      AND ctr.domain_name = 'com.vimcar.de.90454623' -- 1 active, 2 terminated
--     AND ctr.domain_name = 'com.vimcar.de.k88380680'  -- multiple canceled dates, no active contracts
--     AND ctr.domain_name = 'com.vimcar.de.75518754' -- 1st contract is terminated in the first month, no active contracts
--    AND ctr.domain_name = 'com.vimcar.de.k51060031' -- in July21 the monthly invoice is counted in june and july (checked on 20210706)
--     AND ctr.domain_name = 'com.vimcar.de.k74111318'  -- special treatment for mthly payments where order provisioning start and end is the same month (see V39655147):
--     AND ctr.contract_id = 'V27045219' -- contract for both 1y and 3y products, good for next renewal dt calc
-- AND ctr.domain_name = 'com.vimcar.de.k58126111'  -- 3 year Admin contract, first year is over
)
-- SELECT * FROM cte_revenue_raw ;
        , cte_revenue_1 AS (
    SELECT
        cte_revenue_raw.*
         , MIN(contract_status) OVER (PARTITION BY domain_name) AS domain_status
         , date_trunc('month',current_date)::DATE                                                                                    AS current_mth
         , date_trunc('month',order_provisioning_start_dt AT TIME ZONE 'Europe/Berlin')::DATE                                        AS order_provisioning_start_mth
         , date_trunc('month',order_provisioning_end_dt AT TIME ZONE 'Europe/Berlin')::DATE                                          AS order_provisioning_end_mth

         , MIN(order_provisioning_start_dt) FILTER (WHERE order_has_full_refund = FALSE AND contract_terminated_in_the_first_month = FALSE) OVER (PARTITION BY domain_name)  AS first_order_dt
         , MIN(order_provisioning_start_dt) FILTER (WHERE order_has_full_refund = FALSE AND contract_terminated_in_the_first_month = FALSE) OVER (PARTITION BY contract_id)  AS first_order_dt_contract_lvl
         , MIN(date_trunc('month', order_provisioning_start_dt)) FILTER (WHERE order_has_full_refund = FALSE AND contract_terminated_in_the_first_month = FALSE) OVER (PARTITION BY domain_name)::DATE AS first_order_mth

         , MIN(order_provisioning_start_dt) OVER (PARTITION BY domain_name)::DATE                                                               AS first_order_dt_all_orders
         , MIN(date_trunc('month', order_provisioning_start_dt)) OVER (PARTITION BY domain_name)::DATE                                          AS first_order_mth_all_orders
         , MIN(order_created_dt) OVER (PARTITION BY domain_name)                                                                                AS first_order_created_dt_all_orders
         , MIN(date_trunc('month', order_provisioning_start_dt)) FILTER (WHERE monthly_payment = TRUE
        AND order_has_full_refund = FALSE
        AND contract_terminated_in_the_first_month = FALSE) OVER (PARTITION BY domain_name)::DATE                                        AS first_mthly_payment_order_mth

         , MIN(date_trunc('month', order_provisioning_start_dt)) FILTER (WHERE monthly_payment = TRUE) OVER (PARTITION BY domain_name)::DATE    AS first_mthly_payment_order_mth_all_orders
         , coalesce(MAX(order_provisioning_end_dt) FILTER (WHERE order_has_full_refund = FALSE)
        OVER (PARTITION BY domain_name), MAX(order_provisioning_end_dt) OVER (PARTITION BY domain_name))                      AS valid_to_dt
         , coalesce(MAX(order_provisioning_end_dt)
                    OVER (PARTITION BY contract_id), MAX(order_provisioning_end_dt) OVER (PARTITION BY contract_id))                      AS valid_to_dt_contract_level
         , coalesce(date_trunc('month',MAX(order_provisioning_end_dt) FILTER (WHERE order_has_full_refund = FALSE)
        OVER (PARTITION BY domain_name)), date_trunc('month',MAX(order_provisioning_end_dt) OVER (PARTITION BY domain_name)))::DATE AS valid_to_mth
         , round((CASE
                      WHEN order_has_full_refund = TRUE THEN 0
                      WHEN order_discount_percentage >0
                          THEN order_item_total_price_net_amt
                                   * (1-order_discount_percentage)
                          - (coalesce(refund_net_amount, 0) * ((order_item_total_price_net_amt / sum(order_item_total_price_net_amt) OVER (PARTITION BY order_uuid)::numeric)::numeric))
                      WHEN order_discount_amount >0
                          THEN order_item_total_price_net_amt
                          - (round((order_discount_amount   *  (order_item_total_price_gross_amt / sum(order_item_total_price_gross_amt) OVER (PARTITION BY order_uuid)::numeric) / tax_rate),3))  -- discount amount in order is gross!
                          - (coalesce(refund_net_amount, 0) * ((order_item_total_price_net_amt / sum(order_item_total_price_net_amt) OVER (PARTITION BY order_uuid)::numeric)::numeric))
                      ELSE order_item_total_price_net_amt
                          - (coalesce(refund_net_amount, 0) * ((order_item_total_price_net_amt / sum(order_item_total_price_net_amt) OVER (PARTITION BY order_uuid)::numeric)::numeric))
        END)/100,3) AS revenue_temp_ref_inc

         , round((CASE
                      WHEN order_discount_percentage >0
                          THEN
                              order_item_total_price_net_amt
                              * (1-order_discount_percentage)
                      WHEN order_discount_amount >0
                          THEN order_item_total_price_net_amt
                          - (round((order_discount_amount   *  (order_item_total_price_gross_amt / sum(order_item_total_price_gross_amt) OVER (PARTITION BY order_uuid)::numeric) / tax_rate),3))  -- discount amount in order is gross!
                      ELSE order_item_total_price_net_amt
        END)/100,3) AS revenue_temp_ref_excl

         , round((CASE
                      WHEN order_discount_percentage >0
                          THEN coalesce(refund_net_amount, 0) * ((order_item_total_price_net_amt / sum(order_item_total_price_net_amt) OVER (PARTITION BY order_uuid)::numeric)::numeric)
                      WHEN order_discount_amount >0
                          THEN coalesce(refund_net_amount, 0) * ((order_item_total_price_net_amt / sum(order_item_total_price_net_amt) OVER (PARTITION BY order_uuid)::numeric)::numeric)
                      ELSE coalesce(refund_net_amount, 0) * ((order_item_total_price_net_amt / sum(order_item_total_price_net_amt) OVER (PARTITION BY order_uuid)::numeric)::numeric)
        END)/100,3) AS refund

    FROM cte_revenue_raw
) ,
     cte_revenue_2 AS (
         SELECT cr1.*
              , dd.reporting_month
              , MIN(order_provisioning_start_dt) FILTER ( WHERE order_has_full_refund = FALSE AND contract_terminated_in_the_first_month = FALSE AND left(product_length_and_payment,1) = '3')
             OVER (PARTITION BY contract_id) AS first_3y_order_dt_contract_level
              , CASE
                    WHEN order_provisioning_end_dt > current_date
                        AND reporting_month != current_mth
                        AND dense_rank() OVER (PARTITION BY cr1.domain_name, cr1.order_uuid ORDER BY dd.reporting_month) >12 THEN 0

             -- current month only logic to show MRR upcoming this month:
                    WHEN monthly_payment = FALSE
                        AND reporting_month = current_mth
                        AND (first_order_dt >= current_date
                            OR (order_provisioning_start_dt >= current_date AND renewal_temp = FALSE))
                        THEN revenue_temp_ref_inc
                    WHEN monthly_payment = TRUE
                        AND reporting_month = current_mth
                        AND (first_order_dt >= current_date
                            OR (order_provisioning_start_dt >= current_date AND renewal_temp = FALSE))
                        THEN revenue_temp_ref_inc * 12

                    WHEN monthly_payment = FALSE
                        AND ((order_provisioning_end_mth > reporting_month AND order_provisioning_start_dt < current_date)   -- provisioning start in the past and provisioning end greater that duration month
                            OR (order_provisioning_end_dt >= current_date AND order_provisioning_start_dt < current_date)      -- provisioning start in the past and provisioning end greater or equal than today
                            OR (order_provisioning_end_dt > reporting_month AND order_provisioning_start_dt > current_date AND reporting_month != current_mth)  -- provisioning start in future and provisioning end greater that duration month (BUT duration mth IS NOT current mth)
                            OR (valid_to_mth = reporting_month AND order_provisioning_end_mth = reporting_month)
                             ) THEN revenue_temp_ref_inc
                    WHEN monthly_payment = TRUE
                        AND ((order_provisioning_end_mth > reporting_month AND order_provisioning_start_dt < current_date)
                            OR (order_provisioning_end_dt >= current_date AND order_provisioning_start_dt < current_date)
                            OR (order_provisioning_end_dt > reporting_month AND order_provisioning_start_dt > current_date AND reporting_month != current_mth)
                             ) THEN revenue_temp_ref_inc * 12
             -- special treatment for mthly payments where order provisioning start and end is the same month and day (see V39655147, Nov 2018):
                    WHEN monthly_payment = TRUE
                        AND
                         MAX(
                         (CASE WHEN monthly_payment = TRUE
                             AND ((order_provisioning_end_mth > reporting_month AND order_provisioning_start_dt < current_date)
                                 OR (order_provisioning_end_dt >= current_date AND order_provisioning_start_dt < current_date)
                                 OR (order_provisioning_end_dt > reporting_month AND order_provisioning_start_dt > current_date AND reporting_month != current_mth)
                                        ) THEN revenue_temp_ref_inc * 12 ELSE 0 END)
                             ) OVER (PARTITION BY domain_name, reporting_month) = 0
                        AND order_provisioning_end_mth = reporting_month
                        THEN revenue_temp_ref_inc * 12
                    ELSE 0
             END AS domain_value_temp_ref_inc

              , CASE
                    WHEN order_provisioning_end_dt != original_order_provisioning_end_dt
                        AND reporting_month = valid_to_mth
                        THEN 0
                    WHEN order_provisioning_end_dt > current_date
                        AND reporting_month != current_mth
                        AND dense_rank() OVER (PARTITION BY cr1.domain_name, cr1.order_uuid ORDER BY dd.reporting_month) >12 THEN 0

             -- current month only logic to show MRR upcoming this month:
                    WHEN monthly_payment = FALSE
                        AND reporting_month = current_mth
                        AND (first_order_dt >= current_date
                            OR (order_provisioning_start_dt >= current_date AND renewal_temp = FALSE))
                        THEN revenue_temp_ref_excl
                    WHEN monthly_payment = TRUE
                        AND reporting_month = current_mth
                        AND (first_order_dt >= current_date
                            OR (order_provisioning_start_dt >= current_date AND renewal_temp = FALSE))
                        THEN revenue_temp_ref_excl * 12

                    WHEN monthly_payment = FALSE
                        AND ((order_provisioning_end_mth > reporting_month AND order_provisioning_start_dt < current_date)   -- provisioning start in the past and provisioning end greater that duration month
                            OR (order_provisioning_end_dt >= current_date AND order_provisioning_start_dt < current_date)      -- provisioning start in the past and provisioning end greater or equal than today
                            OR (order_provisioning_end_dt > reporting_month AND order_provisioning_start_dt > current_date AND reporting_month != current_mth)  -- provisioning start in future and provisioning end greater that duration month (BUT duration mth IS NOT current mth)
                            OR (valid_to_mth = reporting_month AND order_provisioning_end_mth = reporting_month)
                             ) THEN revenue_temp_ref_excl
                    WHEN monthly_payment = TRUE
                        AND ((order_provisioning_end_mth > reporting_month AND order_provisioning_start_dt < current_date)
                            OR (order_provisioning_end_dt >= current_date AND order_provisioning_start_dt < current_date)
                            OR (order_provisioning_end_dt > reporting_month AND order_provisioning_start_dt > current_date AND reporting_month != current_mth)
                             ) THEN revenue_temp_ref_excl * 12
                    WHEN monthly_payment = TRUE
                        AND
                         MAX(
                         (CASE WHEN monthly_payment = TRUE
                             AND ((order_provisioning_end_mth > reporting_month AND order_provisioning_start_dt < current_date)
                                 OR (order_provisioning_end_dt >= current_date AND order_provisioning_start_dt < current_date)
                                 OR (order_provisioning_end_dt > reporting_month AND order_provisioning_start_dt > current_date AND reporting_month != current_mth)
                                        ) THEN revenue_temp_ref_inc * 12 ELSE 0 END)
                             ) OVER (PARTITION BY domain_name, reporting_month) = 0
                        AND order_provisioning_end_mth = reporting_month
                        THEN revenue_temp_ref_excl * 12
                    ELSE 0
             END AS domain_value_temp_ref_excl
         FROM cte_revenue_1 cr1
                  LEFT JOIN (SELECT DISTINCT date_trunc('month',date_key)::DATE AS reporting_month
                             FROM dwh_main.dim_date
                             WHERE date_key > '2013-01-01' AND date_key <= current_date + interval '5 years') dd
                            ON dd.reporting_month >= order_provisioning_start_mth
                                AND dd.reporting_month <= order_provisioning_end_mth
                                AND dd.reporting_month <= valid_to_mth
         WHERE dd.reporting_month IS NOT NULL
     ) --SELECT * FROM cte_revenue_2;
SELECT
    domain_name
     , domain_is_fleet
     , contract_id
     , contract_status -- to be removed later
--               , contract_terminated_dt
     , contract_terminated_in_the_first_month
     , domain_status
     , contract_creator
     , terminated_dt
     , valid_to_dt
     , valid_to_mth
     , first_order_dt
     , first_order_mth
     , first_order_dt_all_orders
     , first_order_mth_all_orders
     , first_mthly_payment_order_mth
     , first_mthly_payment_order_mth_all_orders
     , order_uuid
     , order_has_full_refund
     , order_discount_percentage
     , order_created_dt
     , order_provisioning_start_dt
     , order_provisioning_start_mth
     , order_provisioning_end_dt
     , order_provisioning_end_mth
     , invoice_uuid
     , product_family
     , product_length_and_payment
     , monthly_payment
     , product_uuid
     , order_quantity
     , CASE WHEN first_order_mth = reporting_month THEN TRUE ELSE FALSE END AS domain_is_new
     , CASE WHEN first_order_mth_all_orders = reporting_month THEN TRUE ELSE FALSE END AS domain_is_new_refund_orders_inc
     , CASE WHEN reporting_month > current_mth THEN TRUE ELSE FALSE END AS flag_future
     , CASE WHEN renewal_temp = TRUE AND order_has_full_refund = FALSE
    AND (
                         (product_length_and_payment LIKE '%annual payment%' AND MIN(reporting_month) OVER (PARTITION BY order_uuid) = reporting_month) -- for annual payments take the 1st duration month of each of each renewed order
                         OR (product_length_and_payment = '1y, monthly payment' AND (extract(year from age(reporting_month, first_mthly_payment_order_mth)) * 12 + extract(month from age(reporting_month, first_mthly_payment_order_mth)))::INT % 12 = 0 )
                         OR (product_length_and_payment = '3y, monthly payment' AND (extract(year from age(reporting_month, first_mthly_payment_order_mth)) * 12 + extract(month from age(reporting_month, first_mthly_payment_order_mth)))::INT % 36 = 0 )
                     ) THEN TRUE ELSE FALSE END AS renewal
--      , MAX(CASE WHEN monthly_payment = TRUE AND order_has_full_refund = FALSE AND contract_terminated_in_the_first_month = FALSE
--                     THEN (first_order_dt + (((extract(year from age(current_mth, first_mthly_payment_order_mth)) * 12 + extract(month from age(current_mth, first_mthly_payment_order_mth)))::INT / 12) + 1) * INTERVAL '1 year')::DATE
--                 ELSE valid_to_dt END) OVER (PARTITION BY contract_id) AS next_renewal_dt

     , coalesce(
                MIN(CASE WHEN product_length_and_payment LIKE '3%'
            AND first_3y_order_dt_contract_level + INTERVAL '3 years' > current_date
                             THEN first_3y_order_dt_contract_level + INTERVAL '3 years'
                         WHEN order_has_full_refund = FALSE
                             AND contract_terminated_in_the_first_month = FALSE
                             THEN first_order_dt_contract_lvl + ((extract(year from age(date_trunc('month',current_date), date_trunc('month',first_order_dt_contract_lvl) + INTERVAL '1 day'))
                             + extract(month from age(date_trunc('month',current_date), date_trunc('month',first_order_dt_contract_lvl) + INTERVAL '1 day'))::INT / 12) + 1)  * INTERVAL '1 year'
                         ELSE valid_to_dt_contract_level END::DATE)
                FILTER ( WHERE contract_status = 'active' AND valid_to_dt_contract_level >= current_date )
                    OVER (PARTITION BY contract_id),
                valid_to_dt_contract_level) AS next_renewal_dt

     , DATE_PART('day', terminated_dt::timestamp - contract_created_dt::timestamp) AS cancelled_after_days  -- TO BE REVIEWED!
--               , current_mth
     , CASE WHEN terminated_dt IS NOT NULL AND reporting_month = valid_to_mth THEN TRUE ELSE FALSE END churn_mth
     , contract_total_net_value AS shop_contract_total_net_value
     , reporting_month
     , CASE WHEN order_has_full_refund = TRUE AND contract_terminated_in_the_first_month = FALSE
    OR (terminated_dt IS NOT NULL AND reporting_month = valid_to_mth) -- churn month
                THEN 0 ELSE domain_value_temp_ref_inc END AS domain_value
     , domain_value_temp_ref_excl AS domain_value_ref_excl
     , CASE WHEN mth_last_mth_with_value IS NOT NULL THEN domain_value_temp_ref_inc END the_most_recent_domain_value
     , refund AS refund_net
     , coalesce(CASE WHEN order_has_full_refund = TRUE OR (terminated_dt IS NOT NULL AND reporting_month = valid_to_mth)
                         THEN 0
                     WHEN order_has_refund = TRUE
                         THEN round(SUM(order_quantity) FILTER ( WHERE domain_value_temp_ref_inc >0 ) OVER (PARTITION BY domain_name, reporting_month, product_family, product_length_and_payment)
                                        * (round(domain_value_temp_ref_inc / nullif(sum(domain_value_temp_ref_excl) OVER (PARTITION BY cr2.domain_name, cr2.product_family, cr2.product_length_and_payment, reporting_month),0)::numeric,3)) ,2)
                     ELSE round(SUM(order_quantity) FILTER ( WHERE domain_value_temp_ref_inc >0 ) OVER (PARTITION BY domain_name, reporting_month, product_family, product_length_and_payment)
                                    * (round(domain_value_temp_ref_inc / nullif(sum(domain_value_temp_ref_inc) OVER (PARTITION BY cr2.domain_name, cr2.product_family, cr2.product_length_and_payment, reporting_month),0)::numeric,3)) ,2) END, 0) AS domain_licenses
     , coalesce(round(SUM(order_quantity) FILTER ( WHERE domain_value_temp_ref_excl >0 )OVER (PARTITION BY domain_name, reporting_month, product_family, product_length_and_payment)
                          * (round(domain_value_temp_ref_excl / nullif(sum(domain_value_temp_ref_excl) OVER (PARTITION BY cr2.domain_name, cr2.product_family, cr2.product_length_and_payment, reporting_month),0)::numeric,3)) ,2), 0) AS domain_licenses_ref_excl
FROM cte_revenue_2 cr2
         LEFT JOIN (SELECT domain_name AS domain
                         , true = ANY(ARRAY_AGG(contract_is_fleet_customer)) AS domain_is_fleet
                    FROM dwh_main.dim_contract
                    GROUP BY domain_name) f
                   ON f.domain = cr2.domain_name
         LEFT JOIN (SELECT domain_name AS domain_last_mth_with_value
                         , coalesce(MAX(reporting_month) FILTER ( WHERE domain_status = 'active' AND domain_value_temp_ref_inc >0)
        , MAX(reporting_month) FILTER ( WHERE domain_status != 'active' AND domain_value_temp_ref_excl >0))
                                       AS mth_last_mth_with_value
                    FROM cte_revenue_2
                    WHERE reporting_month <= current_date
                    GROUP BY domain_name) v
                   ON v.domain_last_mth_with_value = cr2.domain_name
                       AND v.mth_last_mth_with_value = cr2.reporting_month
;
