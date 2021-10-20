16CKTLSUJ57IL9cVk206
;
    SELECT
        s.cb_subscr_customer_id                                                                               AS customer_id
         , s.cb_subscr_id                                                                                     AS subscription_id
         , s.cb_subscr_status                                                                                 AS subscription_status
         , (coalesce(s.cb_subscr_started_ts, s.cb_subscr_next_billing_ts) AT TIME ZONE 'Europe/Berlin')::DATE AS subscription_start_dt
         , CASE
               WHEN s.cb_subscr_status IN ('non_renewing','cancelled')
                   THEN coalesce(cb_subscr_cancelled_ts, cb_subscr_updated_ts) AT TIME ZONE 'Europe/Berlin'
        END::DATE                                                                                          AS subscription_cancelled_dt
         , CASE
               WHEN s.cb_subscr_status IN ('non_renewing','cancelled')
                   THEN coalesce(s.cb_subscr_cancel_reason, 'n/a')
        END AS subscription_cancellation_reason
         , (CASE
                WHEN s.cb_subscr_status = 'future'
                    THEN s.cb_subscr_next_billing_ts + (p.cb_plan_duration_months * INTERVAL '1 month')
                WHEN s.cb_subscr_remaining_billing_cycles > 0
                    AND s.cb_subscr_billing_period_unit = 'month'
                    THEN cb_subscr_current_term_end_ts + (s.cb_subscr_billing_period * s.cb_subscr_remaining_billing_cycles * INTERVAL '1 month')
                WHEN s.cb_subscr_remaining_billing_cycles > 0
                    AND s.cb_subscr_billing_period_unit = 'year'
                    THEN cb_subscr_current_term_end_ts + (s.cb_subscr_billing_period * s.cb_subscr_remaining_billing_cycles * INTERVAL '1 year')
                ELSE MAX(fil.cb_inv_line_item_to_ts) OVER (PARTITION BY s.cb_subscr_id)
                END AT TIME ZONE 'Europe/Berlin')::DATE                                                       AS subscription_end_dt
        , cb_subscr_remaining_billing_cycles
        , cb_subscr_billing_period_unit
         , cb_subscr_current_term_end_ts
        , (s.cb_subscr_billing_period * s.cb_subscr_remaining_billing_cycles * INTERVAL '1 year')
         , CASE
               WHEN p.cb_plan_id NOT IN ('logbook_b2c-one_time_buy-eur','logbook_b2c-from_rent2buy-eur')
                   THEN TRUE
               ELSE FALSE
        END AS subscription_has_recurring_product -- exclude 1 time purchases, non-recurring ones
    FROM dwh_main.fact_cb_invoice_line_item fil
             JOIN dwh_main.dim_cb_invoice i
                  ON i.cb_inv_key = fil.cb_inv_key
             JOIN dwh_main.dim_cb_subscription s
                  ON s.cb_subscr_key = fil.cb_subscr_key
             JOIN dwh_main.dim_cb_customer c
                  ON s.cb_subscr_customer_id = c.cb_cust_id
             JOIN dwh_main.dim_cb_plan p
                  ON p.cb_plan_id = s.cb_subscr_plan_id
    WHERE TRUE
      AND i.cb_inv_total >0  -- only invoices with value
      AND fil.cb_inv_line_item_amount >0  -- only products with value, no addons
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
--     AND cb_plan_id LIKE '%year%'
--    AND i.cb_inv_id = 'I-2021-01-215460'
-- AND cb_inv_customer_id = '30004929559' -- UK quarterly, good for checking as it "paused" in May for a month due to goodwill of Ops
--       AND cb_inv_customer_id = '30005149714' -- UK quarterly, good for checking as it starts on 1st of month
    AND s.cb_subscr_id = '16CKTLSUJ57IL9cVk206' ;

    UNION ALL
-- UK invoices not imported into CB: https://docs.google.com/spreadsheets/u/1/d/14PAZ_eMNi8WAAkxC7rjbqBMsYFEFt0jbEtUfLqozi1k/edit#gid=0
    SELECT
        u.customer_id
         , u.subscription_id AS subscription_id
         , u.subscription_status
         , (u.subscription_started_ts AT TIME ZONE 'Europe/Berlin')::DATE AS subscription_start_dt
         , (u.subscription_cancelled_ts AT TIME ZONE 'Europe/Berlin')::DATE AS subscription_cancelled_dt
         , CASE WHEN u.subscription_id IN ('non_renewing','cancelled')THEN 'n/a' END AS subscription_cancelled_dt
         , (u.subscription_started_ts AT TIME ZONE 'Europe/Berlin' + (p.cb_plan_duration_months * INTERVAL '1 month'))::DATE AS subscription_cancellation_reason
         , TRUE AS subscription_has_recurring_product
    FROM dwh_main.umd_uk_invoices_not_imported_to_chargebee u
             JOIN dwh_main.dim_cb_plan p
                  ON p.cb_plan_id = u.plan_id
   WHERE customer_id = '30002501345xxx'
)
SELECT subscription_id
     , customer_id
     , subscription_status
     , subscription_start_dt
     , subscription_cancelled_dt
     , subscription_cancellation_reason
     , subscription_end_dt
     , subscription_has_recurring_product
--      , string_agg(DISTINCT subscription_id_cb::TEXT, ',') AS cb_subscription_id
FROM cte_precalculation
-- WHERE subscription_id = '30001162115'
GROUP BY subscription_id, customer_id, subscription_status, subscription_start_dt, subscription_cancelled_dt,subscription_cancellation_reason, subscription_end_dt, subscription_has_recurring_product
;