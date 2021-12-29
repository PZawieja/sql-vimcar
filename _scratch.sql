WITH plan_map AS (SELECT cb_plan_id, MD5(cb_plan_id)::TEXT AS cb_plan_id_map
                  FROM dwh_main.dim_cb_plan
                  UNION
                  SELECT cb_addon_id AS cb_plan_id, MD5(cb_addon_id)::TEXT AS cb_plan_id_map
                  FROM dwh_main.dim_cb_addon)
   , cte_invoice_raw AS (
    SELECT
        c.customer_id
--          , ctr.contract_id  -- COMMENT THIS ONCE MIGRATION STARTS!! MAKE SURE ALL PRODUCTS ARE MAPPED
         , CASE
               WHEN cb.cb_cust_id IS NULL
                   OR pm.cb_plan_id_map IS NULL THEN ctr.contract_id
               ELSE ctr.contract_id ||'_'|| pm.cb_plan_id_map
        END AS contract_id  -- UNCOMMENT THIS ONCE MIGRATION STARTS!! MAKE SURE ALL PRODUCTS ARE MAPPED
         , CASE
               WHEN ctr.contract_status = 'active'
                   AND MAX(o.order_provisioning_end_ts) OVER (PARTITION BY ctr.contract_id) ::DATE < current_date - INTERVAL '6 months'
                   THEN 0 -- 'cancelled'
               WHEN ctr.contract_status = 'terminated'
                   THEN 0 --'cancelled'
               WHEN ctr.contract_status = 'canceled'
                   THEN 1 --'non-renewing'
               WHEN ctr.contract_status = 'active'
                   THEN 2 -- active
        END AS contract_status_map
         , (o.order_provisioning_start_ts AT TIME ZONE 'Europe/Berlin')::DATE                                 AS order_provisioning_start_dt
         , (o.order_provisioning_end_ts AT TIME ZONE 'Europe/Berlin')::DATE                                   AS order_provisioning_end_dt
         , o.order_has_full_refund
         , upm.monthly_payment
         , CASE WHEN upm.product_name LIKE '%3-jährige Laufzeit%' THEN TRUE ELSE FALSE END AS three_year_contract
         , CASE
               WHEN foi.order_item_total_price_net_amt > 0 AND (upm.is_recurring = TRUE OR upm.is_subsequent_recurring = TRUE)
                   THEN TRUE
               ELSE FALSE
        END AS is_mrr_relevant-- 20210826
         , CASE
               WHEN contract_status != 'active'
                   OR MAX(o.order_provisioning_end_ts) OVER (PARTITION BY ctr.contract_id) ::DATE < current_date - INTERVAL '6 months'
                   THEN coalesce(contract_canceled_date_key, contract_modified_ts AT TIME ZONE 'Europe/Berlin')
        END::DATE AS contract_terminated_dt
         , CASE WHEN (upm.is_recurring = TRUE OR upm.is_subsequent_recurring = TRUE) THEN TRUE ELSE FALSE END AS is_recurring_product
    FROM dwh_main.fact_order_item foi
             JOIN dwh_main.dim_order o
                  ON o.order_key = foi.order_key
             JOIN dwh_main.umd_product_map upm
                  ON foi.order_item_product_key = upm.product_key
             JOIN dwh_main.dim_customer c
                  ON o.customer_uuid = c.customer_uuid
             JOIN dwh_main.dim_contract ctr
                  ON ctr.contract_uuid = o.contract_uuid
             LEFT JOIN plan_map pm
                       ON pm.cb_plan_id = upm.cb_plan_id
             LEFT JOIN dwh_main.dim_cb_customer cb
                       ON cb.cb_cust_cf_kundennummer = c.customer_id
    WHERE foi.opcode != 'D'
      AND o.order_id <> 'a99d4f2d-cfb4-4c86-b358-4aeb0b1b5f3d' -- incorrectly created order
--       AND (upm.is_recurring = TRUE OR upm.is_subsequent_recurring = TRUE)  -- not relevant, checked on 2021-10-11
--         AND c.customer_id = 'K76310569' -- 3 years contract, good for checking the subscription end date (should be Dec'22)
--       AND foi.order_item_total_price_net_amt > 0
--     AND customer_id = '74879770' -- good for checking the subscription end date (2 contracts, yearly billing)
--     and contract_id = '99664692'
--        and customer_id = 'K78487906'  -- 2 contracts, 1 is Abgeltung
--        AND contract_id = 'V17314686'
       AND contract_id = 'V42030077'
)
--    SELECT * FROM cte_invoice_raw;
   , cte_1 AS (
    SELECT
        cte_invoice_raw.*
--          , MIN(order_provisioning_start_dt) FILTER (WHERE order_has_full_refund = FALSE AND is_mrr_relevant = TRUE) OVER (PARTITION BY domain_name)  AS first_order_dt
--          , MIN(order_provisioning_start_dt) FILTER (WHERE order_has_full_refund = FALSE AND is_mrr_relevant = TRUE AND product_length_and_payment LIKE '3%') OVER (PARTITION BY contract_id) AS first_3y_order_dt
--          , coalesce(MAX(order_provisioning_end_dt) FILTER (WHERE order_has_full_refund = FALSE AND is_mrr_relevant = TRUE) OVER (PARTITION BY domain_name), MAX(order_provisioning_end_dt) OVER (PARTITION BY customer_id)) AS valid_to_dt
         , MIN(order_provisioning_start_dt) FILTER (WHERE order_has_full_refund = FALSE AND is_mrr_relevant = TRUE) OVER (PARTITION BY customer_id) AS first_order_dt
         , MIN(order_provisioning_start_dt) FILTER (WHERE order_has_full_refund = FALSE AND is_mrr_relevant = TRUE) OVER (PARTITION BY contract_id) AS first_order_dt_contract_lvl
         , MAX(order_provisioning_start_dt) FILTER (WHERE is_mrr_relevant = TRUE) OVER (PARTITION BY contract_id) AS max_order_start_dt_contract_lvl
         ,
         , MIN(order_provisioning_start_dt) FILTER (WHERE order_has_full_refund = FALSE AND is_mrr_relevant = TRUE AND three_year_contract = True) OVER (PARTITION BY contract_id) AS first_3y_order_dt_contract_lvl
         , coalesce(MAX(order_provisioning_end_dt) FILTER (WHERE order_has_full_refund = FALSE AND is_mrr_relevant = TRUE) OVER (PARTITION BY customer_id)
         , MAX(order_provisioning_end_dt) OVER (PARTITION BY customer_id)) AS valid_to_dt
         , coalesce(MAX(order_provisioning_end_dt) FILTER (WHERE is_mrr_relevant = TRUE) OVER (PARTITION BY contract_id),
             MAX(order_provisioning_end_dt) OVER (PARTITION BY contract_id)) AS valid_to_dt_contract_lvl -- 20210831
    FROM cte_invoice_raw
)
-- select * from cte_1;
   , cte_2 AS (
    SELECT
        contract_id                                                                                AS subscription_id
         , coalesce(cb.cb_cust_id, customer_id)                                                    AS customer_id
         , CASE
               WHEN contract_status_map = 0 THEN 'cancelled'
               WHEN contract_status_map = 1 THEN 'non_renewing'
               WHEN contract_status_map = 2 THEN 'active'
            END                                                                                    AS subscription_status
         , MIN(order_provisioning_start_dt)                                                        AS subscription_start_dt
         , contract_terminated_dt                                                                  AS subscription_cancelled_dt
         , coalesce(
                    MIN(CASE WHEN three_year_contract = TRUE
                AND first_3y_order_dt_contract_lvl + INTERVAL '3 years' > current_date
                                 THEN first_3y_order_dt_contract_lvl + INTERVAL '3 years'
                             WHEN order_has_full_refund = FALSE
                                 AND monthly_payment = FALSE
                                 THEN valid_to_dt_contract_lvl
                             WHEN max_order_start_dt_contract_lvl = order_provisioning_start_dt
                                 THEN first_order_dt_contract_lvl + ((extract(year from age(date_trunc('month',current_date), date_trunc('month',first_order_dt_contract_lvl) + INTERVAL '1 day'))
                                 + extract(month from age(date_trunc('month',current_date), date_trunc('month',first_order_dt_contract_lvl) + INTERVAL '1 day'))::INT / 12) + 1)  * INTERVAL '1 year'
                             ELSE valid_to_dt_contract_lvl END::DATE)
                    FILTER ( WHERE contract_status_map = 2 AND valid_to_dt_contract_lvl >= current_date )
                , '1999-01-01') AS tmp_subscription_end_dt
         , valid_to_dt_contract_lvl
         , true = ANY(ARRAY_AGG(is_recurring_product)) AS subscription_has_recurring_product
    , first_order_dt_contract_lvl
    , order_has_full_refund
    FROM cte_1 c1
             LEFT JOIN dwh_main.dim_cb_customer cb
                       ON cb.cb_cust_cf_kundennummer = c1.customer_id
    GROUP BY cb.cb_cust_id, customer_id, contract_id, contract_status_map, contract_terminated_dt, valid_to_dt, valid_to_dt_contract_lvl,first_order_dt_contract_lvl, order_has_full_refund
)
SELECT * FROM cte_2;
SELECT
    subscription_id
     , customer_id
     , subscription_status
     , subscription_start_dt
     , subscription_cancelled_dt
     , CASE WHEN subscription_cancelled_dt IS NOT NULL THEN coalesce(ctr.contract_cancelation_reason, 'n/a') END AS subscription_cancellation_reason
     , CASE
           WHEN valid_to_dt_contract_lvl < subscription_start_dt + INTERVAL '360 days'
               THEN  subscription_start_dt + INTERVAL '1 year'
           WHEN ABS(ctr.contract_provisioning_end_date_key -
                    CASE WHEN valid_to_dt_contract_lvl > tmp_subscription_end_dt THEN valid_to_dt_contract_lvl ELSE tmp_subscription_end_dt END ) < 6  -- in case there is max 5 days difference between contract end date and calculated subscription_end_dt then we take the contract end date from the Shop
               THEN ctr.contract_provisioning_end_date_key
           ELSE CASE WHEN valid_to_dt_contract_lvl > tmp_subscription_end_dt THEN valid_to_dt_contract_lvl ELSE tmp_subscription_end_dt END
        END::DATE AS subscription_end_dt
     , subscription_has_recurring_product
FROM cte_2 c2
         LEFT JOIN dwh_main.dim_contract ctr
                   ON ctr.contract_id = c2.subscription_id
;