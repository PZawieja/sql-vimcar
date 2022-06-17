SELECT * FROM dwh_main.dim_fs_account

SELECT * FROM dwh_main.dim_cb_subscription WHERE cb_subscr_id = '30018086793';
SELECT * FROM dwh_main.dim_cb_subscription WHERE cb_subscr_id LIKE '11139822%';

SELECT cb_subscr_id AS sub_id
     , cb_subscr_status AS status
     , cb_subscr_plan_id AS plan_id
     , cb_subscr_billing_period_unit AS billing_period_unit
     , cb_subscr_started_ts::DATE AS started_dt
     , cb_subscr_current_term_start_ts::DATE AS curr_ter_start_dt
     , cb_subscr_current_term_end_ts::DATE AS curr_ter_end_dt
     , cb_subscr_billing_period AS billing_cycle
     , cb_subscr_remaining_billing_cycles AS remaining_billing_cycles
     , cb_subscr_contract_term_billing_cycle_on_renewal AS billing_cycles_on_renewal
     , cb_subscr_billing_period
FROM dwh_main.dim_cb_subscription
WHERE cb_subscr_cf_vertragsnummer IS NULL
AND cb_subscr_billing_period_unit = 'month'
-- AND cb_subscr_id = 'AzZMsNT8iI0h5HNw'
AND cb_subscr_plan_id NOT LIKE '%gbp%'
  AND cb_subscr_plan_id NOT LIKE '%trial%'
  AND cb_subscr_status NOT IN ('cancelled','future')
ORDER BY cb_subscr_started_ts DESC

--------------------------------------------------------------------------------
----- Chargebee subscriptions calculation:
--------------------------------------------------------------------------------
WITH cb_subs_precalculation AS (
    SELECT
        s.cb_subscr_customer_id                                                                               AS customer_id
         , s.cb_subscr_id                                                                                     AS subscription_id
         , s.cb_subscr_status                                                                                 AS subscription_status
         , (coalesce(s.cb_subscr_started_ts, s.cb_subscr_next_billing_ts
                , MIN(fil.cb_inv_line_item_from_ts) OVER (PARTITION BY s.cb_subscr_id)) AT TIME ZONE 'Europe/Berlin')::DATE AS subscription_start_dt
         , lower(s.cb_subscr_coupons -> 0 ->> 'coupon_id') AS subscription_coupon_1
         , lower(s.cb_subscr_coupons -> 1 ->> 'coupon_id') AS subscription_coupon_2
         , CASE
               WHEN s.cb_subscr_status IN ('non_renewing','cancelled')
                   THEN coalesce(e.event_subscr_cancelled_ts, ctr.contract_canceled_date_key, s.cb_subscr_updated_ts) AT TIME ZONE 'Europe/Berlin'
            END::DATE                                                                                          AS subscription_cancelled_dt
         , lower(btrim(
            CASE
                WHEN s.cb_subscr_status IN ('non_renewing','cancelled')
                    THEN coalesce(s.cb_subscr_cancel_reason_code, ctr.contract_cancelation_reason, 'n/a')
                END)) AS subscription_cancellation_reason
         , (coalesce(CASE
                         WHEN s.cb_subscr_status = 'cancelled'
                             THEN s.cb_subscr_current_term_end_ts
                         WHEN s.cb_subscr_status = 'future'
                             THEN s.cb_subscr_next_billing_ts + (p.cb_plan_duration_months * INTERVAL '1 month')
                         WHEN s.cb_subscr_remaining_billing_cycles > 0
                                AND s.cb_subscr_billing_period_unit = 'month'
                             THEN cb_subscr_current_term_end_ts + (s.cb_subscr_remaining_billing_cycles * INTERVAL '1 month')
                         WHEN s.cb_subscr_remaining_billing_cycles > 0
                             AND s.cb_subscr_billing_period_unit = 'year'
                             THEN cb_subscr_current_term_end_ts + (s.cb_subscr_remaining_billing_cycles * INTERVAL '1 year')
                         END, MAX(fil.cb_inv_line_item_to_ts) OVER (PARTITION BY s.cb_subscr_id)) AT TIME ZONE 'Europe/Berlin')::DATE AS subscription_end_dt
         , CASE
               WHEN p.cb_plan_id NOT IN ('logbook_b2c-one_time_buy-eur','logbook_b2c-from_rent2buy-eur') -- one time products
               -- AND i.cb_inv_total >0  -- only invoices with value
               -- AND fil.cb_inv_line_item_amount >0  -- only products with value, no addons
                   THEN TRUE
               ELSE FALSE
        END AS subscription_has_recurring_product -- exclude 1 time purchases, non-recurring ones
    FROM dwh_main.dim_cb_subscription s
             JOIN dwh_main.dim_cb_customer c
                  ON s.cb_subscr_customer_id = c.cb_cust_id
             JOIN dwh_main.dim_cb_plan p
                  ON p.cb_plan_id = s.cb_subscr_plan_id
             LEFT JOIN dwh_main.fact_cb_invoice_line_item fil
                       ON s.cb_subscr_key = fil.cb_subscr_key
             LEFT JOIN dwh_main.dim_cb_invoice i
                       ON i.cb_inv_key = fil.cb_inv_key
             LEFT JOIN dwh_main.dim_contract ctr
                       ON ctr.contract_id = s.cb_subscr_cf_vertragsnummer
             LEFT JOIN (SELECT
                            cb_event_type_id AS subscription_id
                             , MIN(cb_event_occurred_ts) AS event_subscr_cancelled_ts
                        FROM dwh_main.dim_cb_event
                        WHERE cb_event_type IN ('subscription_cancellation_scheduled', 'subscription_cancelled')  -- tracked as of 2022-02-15
                        GROUP BY cb_event_type_id) e
                       ON e.subscription_id = s.cb_subscr_id
    WHERE 1=1
      AND coalesce(s.cb_subscr_is_deleted,FALSE) != TRUE
      AND coalesce(i.cb_inv_is_deleted,FALSE) != TRUE
      AND c.cb_cust_email NOT LIKE '%@vimcar%'
      AND c.cb_cust_email NOT LIKE '%@internal.vimcar%'
--       AND fil.cb_inv_line_item_entity_id NOT LIKE 'hardware%'
--       AND fil.cb_inv_line_item_entity_id NOT LIKE 'addon%'
--       AND fil.cb_inv_line_item_entity_id NOT LIKE 'trial%'
--       AND cb_inv_line_item_id IS NOT NULL
--     AND cb_inv_customer_id = '30000866540' -- multiple invoices + one CN
--     AND cb_inv_customer_id = '30000673905'  -- find the credit note 80
-- AND cb_subscr_cf_vertragsnummer ='10097875'
--     AND cb_plan_id LIKE '%year%'
--    AND i.cb_inv_id = 'I-2021-01-215460'
-- AND cb_inv_customer_id = '30004929559' -- UK quarterly, good for checking as it "paused" in May for a month due to goodwill of Ops
--       AND cb_inv_customer_id = '30005149714' -- UK quarterly, good for checking as it starts on 1st of month

    UNION ALL
-- UK invoices not imported into CB: https://docs.google.com/spreadsheets/u/1/d/14PAZ_eMNi8WAAkxC7rjbqBMsYFEFt0jbEtUfLqozi1k/edit#gid=0
    SELECT
        u.customer_id
         , u.subscription_id AS subscription_id
         , u.subscription_status
         , (u.subscription_started_ts AT TIME ZONE 'Europe/Berlin')::DATE AS subscription_start_dt
         , NULL AS subscription_coupon_1
         , NULL AS subscription_coupon_2
         , (u.subscription_cancelled_ts AT TIME ZONE 'Europe/Berlin')::DATE AS subscription_cancelled_dt
         , CASE WHEN u.subscription_id IN ('non_renewing','cancelled')THEN 'n/a' END AS subscription_cancellation_reason
         , (u.subscription_started_ts AT TIME ZONE 'Europe/Berlin' + (p.cb_plan_duration_months * INTERVAL '1 month'))::DATE AS subscription_end_dt
         , TRUE AS subscription_has_recurring_product
    FROM dwh_main.umd_uk_invoices_not_imported_to_chargebee u
             JOIN dwh_main.dim_cb_plan p
                  ON p.cb_plan_id = u.plan_id
    WHERE NOT exists (SELECT 1 FROM dwh_main.dim_cb_invoice i WHERE i.cb_inv_id = u.invoice_id)
)
   , chargebee_subscriptions AS (
    SELECT subscription_id
         , customer_id
         , subscription_status
         , subscription_start_dt
         , subscription_coupon_1
         , subscription_coupon_2
         , subscription_cancelled_dt
         , subscription_cancellation_reason
         , subscription_end_dt
         , true = ANY(ARRAY_AGG(subscription_has_recurring_product)) AS subscription_has_recurring_product
--      , string_agg(DISTINCT subscription_id_cb::TEXT, ',') AS cb_subscription_id
    FROM cb_subs_precalculation
    WHERE subscription_start_dt IS NOT NULL
      AND subscription_end_dt IS NOT NULL
    GROUP BY subscription_id, customer_id, subscription_status, subscription_start_dt, subscription_coupon_1, subscription_coupon_2, subscription_cancelled_dt, subscription_cancellation_reason, subscription_end_dt
)
--------------------------------------------------------------------------------
----- SHOP subscriptions calculation:
--------------------------------------------------------------------------------

   , cte_invoice_raw AS (
    SELECT
        c.customer_id
         , CASE
               WHEN cbc.cb_cust_id IS NOT NULL
                   THEN coalesce(cbs_1.cb_subscr_id, cbs_2.cb_subscr_id, ctr.contract_id ||'_'|| md5(upm.cb_plan_id))
               ELSE  ctr.contract_id
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
         , CASE WHEN p.product_name LIKE '%3-jährige%' THEN TRUE ELSE FALSE END AS three_year_contract
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
             JOIN dwh_main.dim_product p
                  ON foi.order_item_product_key = p.product_key
             JOIN dwh_main.umd_product_map upm
                  ON foi.order_item_product_key = upm.product_key
             JOIN dwh_main.dim_customer c
                  ON o.customer_uuid = c.customer_uuid
             JOIN dwh_main.dim_contract ctr
                  ON ctr.contract_uuid = o.contract_uuid
             LEFT JOIN dwh_main.map_shop_extraction_cb_plan_id pm
                       ON pm.cb_plan_id = upm.cb_plan_id
             LEFT JOIN dwh_main.dim_cb_customer cbc
                       ON cbc.cb_cust_cf_kundennummer = c.customer_id
             LEFT JOIN dwh_main.dim_cb_subscription cbs_1
                       ON cbs_1.cb_subscr_id = ctr.contract_id ||'_'|| pm.cb_plan_id_map
             LEFT JOIN dwh_main.dim_cb_subscription cbs_2
                       ON cbs_2.cb_subscr_id = ctr.contract_id ||'_'|| pm.cb_plan_id_map_nbr
    WHERE foi.opcode != 'D'
      AND coalesce(ctr.contract_cancelation_reason, 'dummy') <> 'ghost contract' -- ADDED ON 2022-03-30, due to ghost contracts in Shop issue
      AND o.order_id <> 'a99d4f2d-cfb4-4c86-b358-4aeb0b1b5f3d' -- incorrectly created order
--       AND (upm.is_recurring = TRUE OR upm.is_subsequent_recurring = TRUE)  -- not relevant, checked on 2021-10-11
--         AND c.customer_id = 'K76310569' -- 3 years contract, good for checking the subscription end date (should be Dec'22)
--       AND foi.order_item_total_price_net_amt > 0
--     AND customer_id = '74879770' -- good for checking the subscription end date (2 contracts, yearly billing)
--     and contract_id = 'V90199604'
--        and customer_id = 'K78487906'  -- 2 contracts, 1 is Abgeltung
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
         , MIN(order_provisioning_start_dt) FILTER (WHERE order_has_full_refund = FALSE AND is_mrr_relevant = TRUE AND three_year_contract = True) OVER (PARTITION BY contract_id) AS first_3y_order_dt_contract_lvl
         , coalesce(MAX(order_provisioning_end_dt) FILTER (WHERE order_has_full_refund = FALSE AND is_mrr_relevant = TRUE) OVER (PARTITION BY customer_id)
        , MAX(order_provisioning_end_dt) OVER (PARTITION BY customer_id)) AS valid_to_dt
         , coalesce(MAX(order_provisioning_end_dt) FILTER (WHERE is_mrr_relevant = TRUE) OVER (PARTITION BY contract_id)
        , MAX(order_provisioning_end_dt) OVER (PARTITION BY contract_id)) AS valid_to_dt_contract_lvl -- 20210831
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
--          , coalesce(
--                     MIN(CASE WHEN three_year_contract = TRUE
--                                     AND first_3y_order_dt_contract_lvl + INTERVAL '3 years' > current_date
--                                  THEN first_3y_order_dt_contract_lvl + INTERVAL '3 years'
--                              WHEN monthly_payment = FALSE
--                                  AND order_has_full_refund = FALSE
--                                  THEN valid_to_dt_contract_lvl
--                              WHEN monthly_payment = TRUE
--                                  THEN first_order_dt_contract_lvl + ((extract(year from age(date_trunc('month',current_date), date_trunc('month',first_order_dt_contract_lvl) + INTERVAL '1 day'))
--                                  + extract(month from age(date_trunc('month',current_date), date_trunc('month',first_order_dt_contract_lvl) + INTERVAL '1 day'))::INT / 12) + 1)  * INTERVAL '1 year'
--                              ELSE valid_to_dt_contract_lvl END::DATE)
--                     FILTER ( WHERE contract_status_map = 2 AND valid_to_dt_contract_lvl >= current_date )
--         , '1999-01-01') AS tmp_subscription_end_dt

         , coalesce(
                    MIN(CASE WHEN three_year_contract = TRUE
                                AND monthly_payment = FALSE
                            THEN make_date(EXTRACT (YEAR FROM first_3y_order_dt_contract_lvl)::INT
                                     , EXTRACT (MONTH FROM valid_to_dt_contract_lvl)::INT
                                     , EXTRACT (DAY FROM valid_to_dt_contract_lvl)::INT) + INTERVAL '3 years'
                             WHEN three_year_contract = TRUE
                                     AND monthly_payment = TRUE
                                THEN first_3y_order_dt_contract_lvl + INTERVAL '3 years'
                             WHEN monthly_payment = FALSE
                                     AND order_has_full_refund = FALSE
                                 THEN valid_to_dt_contract_lvl
                             WHEN monthly_payment = TRUE
                                 THEN first_order_dt_contract_lvl + ((extract(year from age(date_trunc('month',current_date), date_trunc('month',first_order_dt_contract_lvl) + INTERVAL '1 day'))
                                     + extract(month from age(date_trunc('month',current_date), date_trunc('month',first_order_dt_contract_lvl) + INTERVAL '1 day'))::INT / 12) + 1)  * INTERVAL '1 year'
                             ELSE valid_to_dt_contract_lvl END::DATE)
                    FILTER ( WHERE contract_status_map = 2 AND valid_to_dt_contract_lvl >= current_date )
         , valid_to_dt_contract_lvl, '1999-01-01') AS tmp_subscription_end_dt
         , valid_to_dt_contract_lvl
         , true = ANY(ARRAY_AGG(is_recurring_product)) AS subscription_has_recurring_product
    FROM cte_1 c1
             LEFT JOIN dwh_main.dim_cb_customer cb
                       ON cb.cb_cust_cf_kundennummer = c1.customer_id
    GROUP BY cb.cb_cust_id, customer_id, contract_id, contract_status_map, contract_terminated_dt, valid_to_dt, valid_to_dt_contract_lvl,first_order_dt_contract_lvl
)
-- SELECT * FROM cte_2;
   , shop_subscriptions AS (
    SELECT
        subscription_id
         , customer_id
         , subscription_status
         , subscription_start_dt
         , lower(ctr.coupon_code) AS subscription_coupon_1
         , NULL AS subscription_coupon_2
         , subscription_cancelled_dt
         , lower(btrim(CASE WHEN subscription_cancelled_dt IS NOT NULL THEN coalesce(ctr.contract_cancelation_reason, 'n/a') END)) AS subscription_cancellation_reason
         , CASE
               WHEN subscription_status = 'cancelled'
                        OR (subscription_status = 'non_renewing' AND valid_to_dt_contract_lvl + INTERVAL '21 days' < current_date)
                    THEN valid_to_dt_contract_lvl
               WHEN valid_to_dt_contract_lvl < subscription_start_dt + INTERVAL '360 days'
                   THEN subscription_start_dt + INTERVAL '1 year'
               WHEN ABS(ctr.contract_provisioning_end_date_key -
                        CASE WHEN valid_to_dt_contract_lvl > tmp_subscription_end_dt THEN valid_to_dt_contract_lvl ELSE tmp_subscription_end_dt END ) < 6  -- in case there is max 5 days difference between contract end date and calculated subscription_end_dt then we take the contract end date from the Shop
                   THEN ctr.contract_provisioning_end_date_key
               ELSE CASE WHEN valid_to_dt_contract_lvl > tmp_subscription_end_dt THEN valid_to_dt_contract_lvl ELSE tmp_subscription_end_dt END
        END::DATE AS subscription_end_dt
         , subscription_has_recurring_product
    FROM cte_2 c2
             LEFT JOIN dwh_main.dim_contract ctr
                       ON ctr.contract_id = split_part(c2.subscription_id, '_', 1)
)
   , cte_3 AS (
    SELECT * FROM chargebee_subscriptions
    UNION ALL
    SELECT * FROM shop_subscriptions
-- we need to filter out the migrated Shop subscriptions which got reactivated in Chargebee after migration, ex. V66509368_8294003f207242cdc1babe5d3d987c7f
    WHERE NOT EXISTS(SELECT 1 FROM chargebee_subscriptions cb WHERE cb.subscription_id = shop_subscriptions.subscription_id )
)
-- SELECT * FROM cte_3 WHERE subscription_id LIKE '10097875%';
SELECT
    subscription_id
     , customer_id
     , CASE
           WHEN subscription_status = 'non_renewing' AND subscription_end_dt + INTERVAL '21 days' < current_date
               THEN 'cancelled'
           ELSE subscription_status
    END AS subscription_status
     , subscription_start_dt
     , subscription_coupon_1
     , subscription_coupon_2

     , CASE
           WHEN subscription_status = 'non_renewing' AND subscription_end_dt + INTERVAL '21 days' < current_date
               THEN subscription_end_dt
           WHEN subscription_cancelled_dt IS NOT NULL
             THEN LEAST(subscription_cancelled_dt, subscription_end_dt)
       END AS subscription_cancelled_dt
     , CASE
           WHEN subscription_cancellation_reason LIKE '%abgeschafft%'
               OR subscription_cancellation_reason LIKE '%abgeschaft%'
               OR subscription_cancellation_reason LIKE '%fahrzeug%'
               THEN 'Fahrzeug abgeschafft'
           WHEN subscription_cancellation_reason LIKE '%covid%'
               THEN 'Covid-19'
           WHEN subscription_cancellation_reason LIKE '%\%%'
               THEN '1% / 0.5% Regel'
           WHEN subscription_cancellation_reason LIKE '%abschreib%'
               OR subscription_cancellation_reason LIKE '%abscheiben%'
               OR subscription_cancellation_reason LIKE '%abscheiben%'
               OR subscription_cancellation_reason LIKE '%nichtzah%'
               OR subscription_cancellation_reason LIKE '%nictzahler%'
               THEN 'Abschreibung / Nichtzahler'
           WHEN subscription_cancellation_reason LIKE '%abgeltung%'
               THEN 'Abgeltung'
           WHEN subscription_cancellation_reason LIKE '%fehler%'
               --             OR subscription_cancellation_reason LIKE '%fehler%'
               THEN 'Fehler'
           WHEN subscription_cancellation_reason LIKE '%geschäftsaufgabe%'
               OR subscription_cancellation_reason LIKE '%insolvenz%'
               THEN 'Geschäftsaufgabe / Insolvenz'
           WHEN subscription_cancellation_reason LIKE '%alternativ%'
               THEN 'Nutzt alternatives Produkt'
           WHEN subscription_cancellation_reason LIKE '%preis%'
               OR subscription_cancellation_reason LIKE '% teuer%'
               THEN 'Zu teuer'
           WHEN subscription_cancellation_reason LIKE '%rentabel%'
               THEN 'Steuerlich nicht rentabel'
           WHEN subscription_cancellation_reason LIKE '%techn%'
               THEN 'Technische Probleme'
           WHEN subscription_cancellation_reason LIKE '%umstellung%'
               OR subscription_cancellation_reason LIKE '%vertrag%'
               THEN 'Vertragsanpassung'
           WHEN subscription_cancellation_reason LIKE '%kompliziert%'
               OR subscription_cancellation_reason LIKE '%zeitintensiv%'
               THEN 'Zeitintensiv / Kompliziert'
           WHEN subscription_cancellation_reason = 'n/a' -- this is the reason not provided
               THEN 'n/a'
           WHEN subscription_cancellation_reason IS NOT NULL
               THEN 'Sonstige'
    END AS subscription_cancellation_reason
     , subscription_end_dt
     , subscription_has_recurring_product
FROM cte_3
WHERE customer_id = '21537765'
;

