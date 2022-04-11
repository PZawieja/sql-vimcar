SELECT DISTINCT invoice_id
              ,invoice_provisioning_start_dt
              , invoice_provisioning_end_dt
              , CASE
                    WHEN extract(month from invoice_provisioning_start_dt) = 3 --March
                        AND extract(day from invoice_provisioning_start_dt) = 1
                        AND extract(month from invoice_provisioning_end_dt) = 3 --March
                        AND extract(day from invoice_provisioning_end_dt) = 29
                        THEN (extract(year from invoice_provisioning_start_dt) || '-02-28')::DATE
                    ELSE invoice_provisioning_start_dt
    END AS fix_invoice_provisioning_start_dt
              , CASE
                    WHEN extract(month from invoice_provisioning_start_dt) = 1 --January
                        AND extract(day from invoice_provisioning_start_dt) IN (29,30,31)
                        THEN (extract(year from invoice_provisioning_start_dt) || '-02-28')::DATE
                    ELSE invoice_provisioning_end_dt
    END AS fix_invoice_provisioning_end_dt
FROM dwh_main.dim_combined_invoice_line
WHERE source_system = 'SH'
  AND product_terms LIKE '%monthly payment'
  AND extract(year from invoice_provisioning_start_dt)::INT % 4 <> 0 -- not a leap_year
  AND extract(year from invoice_provisioning_start_dt) = 2022  -- first the fix for current year
        AND invoice_id IN ('R93420040','R77159373')
;



SELECT
invoice_provisioning_start_dt
, invoice_provisioning_end_dt
FROM dwh_main.dim_combined_invoice_line WHERE invoice_id IN ('R93420040','R77159373')



SELECT * FROM dwh_main.dim_combined_invoice_line WHERE customer_id = '16A1CTSsDJwC3HOsJ'

SELECT
    invoice_id
     , customer_id
     , entity_id
    , invoice_provisioning_start_dt
     , invoice_provisioning_end_dt
    , invoice_provisioning_end_dt - invoice_provisioning_start_dt
FROM dwh_main.dim_combined_invoice_line i
    JOIN (SELECT
              date_trunc('month', date_key)::DATE
               , max(date_key) AS max_date_of_month
          FROM dwh_main.dim_date
          GROUP BY 1) md
        ON md.max_date_of_month = i.invoice_provisioning_end_dt
WHERE source_system = 'CB'
-- AND extract(day from invoice_provisioning_start_dt) = 1
ORDER BY invoice_provisioning_end_dt - invoice_provisioning_start_dt
;
SELECT DISTINCT invoice_id
              , (invoice_provisioning_end_dt + INTERVAL '1 day')::DATE AS fix_invoice_provisioning_end_dt
FROM dwh_main.dim_combined_invoice_line i
         JOIN (SELECT
                   date_trunc('month', date_key)
                    , max(date_key) AS max_date_of_month
               FROM dwh_main.dim_date
               GROUP BY 1) md
            ON md.max_date_of_month = i.invoice_provisioning_end_dt
WHERE source_system = 'CB'
  AND product_terms NOT LIKE '%monthly payment'
  AND extract(year from invoice_provisioning_start_dt)::INT % 4 <> 0 -- not a leap_year
--       AND invoice_id IN ('I-2021-04-216160','I-2021-03-215159')
;






WITH cte_fix_shop_invoice_dates_february_march_monthly_billing AS (
    SELECT DISTINCT invoice_id
                  , CASE
                        WHEN extract(month from invoice_provisioning_start_dt) = 3 --March
                            AND extract(day from invoice_provisioning_start_dt) = 1
                            AND extract(month from invoice_provisioning_end_dt) = 3 --March
                            AND extract(day from invoice_provisioning_end_dt) = 29
                            THEN (extract(year from invoice_provisioning_start_dt) || '-02-28')::DATE
                        ELSE invoice_provisioning_start_dt
        END AS fix_invoice_provisioning_start_dt
                  , CASE
                        WHEN extract(month from invoice_provisioning_start_dt) = 1 --January
                            AND extract(day from invoice_provisioning_start_dt) IN (29,30,31)
                            THEN (extract(year from invoice_provisioning_start_dt) || '-02-28')::DATE
                        ELSE invoice_provisioning_end_dt
        END AS fix_invoice_provisioning_end_dt
    FROM dwh_main.dim_combined_invoice_line
    WHERE source_system = 'SH'
      AND product_terms LIKE '%monthly payment'
      AND extract(year from invoice_provisioning_start_dt)::INT % 4 <> 0 -- not a leap_year
      AND extract(year from invoice_provisioning_start_dt) >= 2022  -- first the fix for current year
--         AND invoice_id IN ('R93420040','R77159373')
)
   , cte_fix_cb_invoice_dates_last_date_of_month AS (
    SELECT DISTINCT invoice_id
                  , (invoice_provisioning_end_dt + INTERVAL '1 day')::DATE AS fix_invoice_provisioning_end_dt
    FROM dwh_main.dim_combined_invoice_line i
             JOIN (SELECT
                       date_trunc('month', date_key)
                        , max(date_key) AS max_date_per_month
                   FROM dwh_main.dim_date
                   GROUP BY 1) md
                  ON md.max_date_per_month = i.invoice_provisioning_end_dt
    WHERE source_system = 'CB'
      AND product_terms NOT LIKE '%monthly payment'
      AND extract(year from invoice_provisioning_start_dt)::INT % 4 <> 0 -- not a leap_year
--       AND invoice_id IN ('I-2021-04-216160','I-2021-03-215159')
)
   , cte_precalculation AS (
    SELECT
        i.customer_id
         , c.customer_country
         , c.customer_is_fleet
         , c.customer_status
         , c.min_subscription_start_dt AS first_customer_subs_start_dt
         , CASE WHEN c.customer_status IN ('non_renewing','cancelled') THEN c.max_subscription_cancelled_dt END AS customer_cancelled_dt
         , i.subscription_id
         , s.subscription_status
         , s.subscription_start_dt
         , s.subscription_end_dt
         , s.subscription_cancelled_dt
         , CASE
               WHEN date_trunc('month',s.subscription_cancelled_dt) <= date_trunc('month',s.subscription_start_dt)
                   THEN TRUE
               ELSE FALSE
        END AS subscription_cancelled_in_first_month
         , i.invoice_id
         , i.invoice_created_dt
         , coalesce(fix_sh.fix_invoice_provisioning_start_dt, i.invoice_provisioning_start_dt) AS invoice_provisioning_start_dt
         , coalesce(fix_sh.fix_invoice_provisioning_end_dt, fix_cb.fix_invoice_provisioning_end_dt, i.invoice_provisioning_end_dt) AS invoice_provisioning_end_dt
         , i.entity_id AS plan_id
         , i.product_name
         , CASE
               WHEN i.product_group IN ('Logbook B2B', 'Logbook B2C') THEN 'Logbook'
               WHEN i.product_group IN ('Admin', 'Geo', 'Pro') THEN i.product_group
               ELSE 'Other'
        END                                                                                               AS product_group
         , i.product_terms
         , i.discount_percentage
         , i.full_refund_flag AS full_refund
         , i.currency_code
         , i.exchange_rate
         , i.mrr_local_ccy
         , i.refund_mrr_local_ccy
         , i.item_quantity
         , i.refund_item_quantity
--         , CASE WHEN i.source_system = 'SH' THEN i.item_quantity * (i.refund_mrr_eur / nullif(i.mrr_eur,0)::numeric) ELSE 0 END AS refund_item_quantity   -- the quantity reduction is possible only in Shop. In CB in case of quantity reduction the subscription is cancelled and new one with new item quantity is created
    FROM dwh_main.dim_combined_invoice_line i
             JOIN dwh_main.dim_combined_subscription s
                  ON s.customer_id = i.customer_id
                      AND s.subscription_id = i.subscription_id
             JOIN dwh_main.dim_combined_customer c
                  ON c.customer_id = i.customer_id
             LEFT JOIN cte_fix_shop_invoice_dates_february_march_monthly_billing fix_sh
                       ON fix_sh.invoice_id = i.invoice_id
             LEFT JOIN cte_fix_cb_invoice_dates_last_date_of_month fix_cb
                       ON fix_cb.invoice_id = i.invoice_id
    WHERE i.invoice_status <> 'voided'
      AND i.product_group NOT IN ('Other', 'Hardware', 'Trial')
      AND i.entity_id NOT IN ('1db52694-baee-45fb-902f-8de7293621fe', '974e92e4-fed9-499e-8b7e-582c986f8a6b','logbook_b2c-one_time_buy-eur','logbook_b2c-from_rent2buy-eur') -- exclude 1 time products, non recurring ones
--       AND cb_inv_line_item_id IS NOT NULL
--     AND cb_inv_customer_id = '30000866540' -- multiple invoices + one CN
--     AND cb_inv_customer_id = '30000673905'  -- find the credit note 80
--     AND cb_plan_id LIKE '%year%'
--    AND i.cb_inv_id = 'I-2021-01-215460'
-- AND cb_inv_customer_id = '30004929559' -- UK quarterly, good for checking as it "paused" in May for a month due to goodwill of Ops
--       AND c.customer_id = '30005149714' -- UK quarterly, good for checking as it starts on 1st of month
--   AND i.customer_id = 'K36527423' --AND i.invoice_id IN ('R93420040','R77159373')  -- provisioning dates fix
--     and i.subscription_id = '300020404756980' -- good for refund testing as was both full (25 licenses) and partial (3) refund
--        and c.customer_id = '30006154481' -- CB last date of month fixed, otherwise reporting month 2022-03-01 shows 0 MRR
)
--    SELECT * FROM cte_precalculation;
   , cte_1 AS (SELECT customer_id
                    , customer_country
                    , customer_status
                    , customer_is_fleet
                    , first_customer_subs_start_dt
                    , customer_cancelled_dt
                    , subscription_id
                    , subscription_status
                    , subscription_start_dt
                    , subscription_end_dt
                    , subscription_cancelled_dt
                    , subscription_cancelled_in_first_month
                    , invoice_id
                    , invoice_created_dt
                    , invoice_provisioning_start_dt
                    , CASE
                          WHEN customer_status <> 'active'
                              AND invoice_provisioning_end_dt = MAX(invoice_provisioning_end_dt) OVER (PARTITION BY customer_id)
                              AND subscription_cancelled_in_first_month = FALSE
                              AND ( date_trunc('month', invoice_provisioning_start_dt) = date_trunc('month', invoice_provisioning_end_dt) -- provisioning starts and ends in the same month (monthly billing)
--                         OR date_trunc('month',invoice_provisioning_start_dt) + interval '1 year' > date_trunc('month',invoice_provisioning_end_dt) -- provisioning ends within 12 mmths (yearly billing)
                                   )
                              THEN date_trunc('month', invoice_provisioning_end_dt) + INTERVAL '1 month'
                          ELSE invoice_provisioning_end_dt
                    END::DATE invoice_provisioning_end_dt
                    , invoice_provisioning_end_dt AS original_invoice_provisioning_end_dt
                    , plan_id
                    , product_name
                    , product_group
                    , product_terms
                    , full_refund
                    , discount_percentage
                    , currency_code
                    , exchange_rate
                    , mrr_local_ccy
                    , refund_mrr_local_ccy
                    , item_quantity
                    , refund_item_quantity
                    , date_trunc('month',current_date)::DATE AS current_mth
               FROM cte_precalculation p
)
--    SELECT * FROM cte_1;
   , cte_2 AS (SELECT customer_id
                    , customer_country
                    , customer_is_fleet
                    , subscription_id
                    , subscription_status
                    , customer_status
                    , first_customer_subs_start_dt
                    , MIN(COALESCE(subscription_start_dt, invoice_provisioning_start_dt)) OVER (PARTITION BY subscription_id)::DATE             AS subscription_start_dt
                    , subscription_end_dt
                    , subscription_cancelled_dt
                    , subscription_cancelled_dt - MIN(COALESCE(subscription_start_dt, invoice_provisioning_start_dt)) OVER (PARTITION BY subscription_id)::DATE       AS cancelled_after_days
                    , subscription_cancelled_in_first_month
                    , customer_cancelled_dt
                    , MIN(invoice_provisioning_start_dt)
                      FILTER (WHERE full_refund = FALSE AND subscription_cancelled_in_first_month = FALSE) OVER (PARTITION BY customer_id)       AS first_invoice_dt
                    , MIN(date_trunc('month', invoice_provisioning_start_dt))
                      FILTER (WHERE full_refund = FALSE AND subscription_cancelled_in_first_month = FALSE) OVER (PARTITION BY customer_id)::DATE AS first_invoice_mth

                    , MIN(invoice_provisioning_start_dt) OVER (PARTITION BY customer_id)                                                          AS first_invoice_dt_all_invoices
                    , MIN(date_trunc('month', invoice_provisioning_start_dt)) OVER (PARTITION BY customer_id)::DATE                               AS first_invoice_mth_all_invoices

                    , coalesce(
                    MAX(invoice_provisioning_end_dt) FILTER (WHERE full_refund = FALSE) OVER (PARTITION BY customer_id),
                    MAX(invoice_provisioning_end_dt) OVER (PARTITION BY customer_id))                                                             AS valid_to_dt

                    , coalesce(date_trunc('month', MAX(invoice_provisioning_end_dt)
                                                   FILTER (WHERE full_refund = FALSE) OVER (PARTITION BY customer_id)),
                               date_trunc('month',
                                          MAX(invoice_provisioning_end_dt) OVER (PARTITION BY customer_id)))::DATE                                AS valid_to_mth
                    , invoice_id
                    , invoice_created_dt
                    , invoice_provisioning_start_dt
                    , date_trunc('month',invoice_provisioning_start_dt)::DATE                                                                     AS invoice_provisioning_start_mth
                    , invoice_provisioning_end_dt
                    , date_trunc('month',invoice_provisioning_end_dt)::DATE                                                                       AS invoice_provisioning_end_mth
                    , CASE WHEN invoice_provisioning_end_dt != original_invoice_provisioning_end_dt THEN TRUE ELSE FALSE END                      AS flag_modified_invoice_provisioning_end_dt
                    , full_refund
                    , plan_id
                    , product_name
                    , product_group
                    , product_terms
                    , discount_percentage
                    , currency_code
                    , exchange_rate
                    , mrr_local_ccy - refund_mrr_local_ccy AS mrr_temp_with_cn
                    , mrr_local_ccy AS mrr_temp_without_cn
                    , item_quantity
                    , refund_item_quantity
                    , current_mth
               FROM cte_1
)
--    SELECT * FROM cte_2;
   , cte_3 AS (
    SELECT c2.*
--                , dense_rank() OVER (PARTITION BY c2.customer_id, c2.invoice_id ORDER BY dd.reporting_month)
         , dd.reporting_month
         , CASE
               WHEN flag_modified_invoice_provisioning_end_dt = TRUE
                   AND invoice_provisioning_end_mth = reporting_month
                   THEN 0
               WHEN invoice_provisioning_end_dt > current_date
                   AND reporting_month != current_mth
                   AND dense_rank() OVER (PARTITION BY c2.customer_id, c2.invoice_id ORDER BY dd.reporting_month)
                        > CASE
                              WHEN split_part(product_terms, ' ', 3) = 'yearly' THEN 12
                              WHEN split_part(product_terms, ' ', 3) = 'bi-yearly' THEN 6
                              WHEN split_part(product_terms, ' ', 3) = 'quarterly' THEN 3
                           END
                   THEN 0

               WHEN reporting_month != current_mth
                   AND dense_rank() OVER (PARTITION BY c2.customer_id, c2.invoice_id ORDER BY dd.reporting_month)
                        > CASE
                              WHEN split_part(product_terms, ' ', 3) = 'bi-yearly' THEN 6
                              WHEN split_part(product_terms, ' ', 3) = 'quarterly' THEN 3
                           END
                   THEN 0

        -- new customers in the current month only:
               WHEN first_invoice_dt >= current_date
                   AND reporting_month = current_mth
                   THEN mrr_temp_with_cn

               WHEN ((invoice_provisioning_end_mth > reporting_month AND invoice_provisioning_start_dt < current_date)   -- provisioning start in the past and provisioning end greater that duration month
                   OR (invoice_provisioning_end_dt >= current_date AND invoice_provisioning_start_dt < current_date)      -- provisioning start in the past and provisioning end greater or equal than today
                   OR (invoice_provisioning_end_dt > reporting_month AND invoice_provisioning_start_dt > current_date AND reporting_month != current_mth)  -- provisioning start in future and provisioning end greater that duration month (BUT duration mth IS NOT current mth)
                   OR (valid_to_mth = reporting_month AND invoice_provisioning_end_mth = reporting_month)
                   OR (split_part(product_terms, ' ', 3) IN ('quarterly', 'bi-yearly') AND invoice_provisioning_start_dt = invoice_provisioning_start_mth)  -- special treatment for CB invoices starting on thee first day of month and end on last day of month +3/6 mths (see 30005149714, reporting month May'21 would have been 0 without this condition)
                   ) THEN mrr_temp_with_cn

        -- special treatment for mthly payments where order provisioning start and end is the same month and day (see V39655147, Nov 2018):
               WHEN split_part(product_terms, ' ', 3) = 'monthly'
                   AND
                    MAX(
                    (CASE WHEN split_part(product_terms, ' ', 3) = 'monthly'
                        AND ((invoice_provisioning_end_mth > reporting_month AND invoice_provisioning_start_dt < current_date)
                            OR (invoice_provisioning_end_dt >= current_date AND invoice_provisioning_start_dt < current_date)
                            OR (invoice_provisioning_end_dt > reporting_month AND invoice_provisioning_start_dt > current_date AND reporting_month != current_mth)
                                   ) THEN mrr_temp_with_cn ELSE 0 END)
                        ) OVER (PARTITION BY customer_id, reporting_month) = 0
                   AND invoice_provisioning_end_mth = reporting_month
                   THEN mrr_temp_with_cn
               ELSE 0
        END AS mrr_with_cn

         , CASE
               WHEN flag_modified_invoice_provisioning_end_dt = TRUE
                   AND invoice_provisioning_end_mth = reporting_month
                   THEN 0

               WHEN invoice_provisioning_end_dt > current_date
                   AND reporting_month != current_mth
                   AND dense_rank() OVER (PARTITION BY customer_id, invoice_id ORDER BY dd.reporting_month)
                        > CASE
                              WHEN split_part(product_terms, ' ', 3) = 'yearly' THEN 12
                              WHEN split_part(product_terms, ' ', 3) = 'bi-yearly' THEN 6
                              WHEN split_part(product_terms, ' ', 3) = 'quarterly' THEN 3
                           END
                   THEN 0

               WHEN reporting_month != current_mth
                   AND dense_rank() OVER (PARTITION BY customer_id, invoice_id ORDER BY dd.reporting_month)
                        > CASE
                              WHEN split_part(product_terms, ' ', 3) = 'yearly' THEN 12
                              WHEN split_part(product_terms, ' ', 3) = 'bi-yearly' THEN 6
                              WHEN split_part(product_terms, ' ', 3) = 'quarterly' THEN 3
                           END
                   THEN 0

        -- new customers in the current month only:
               WHEN first_invoice_dt_all_invoices >= current_date
                   AND reporting_month = current_mth
                   THEN mrr_temp_without_cn

               WHEN ((invoice_provisioning_end_mth > reporting_month AND invoice_provisioning_start_dt < current_date)   -- provisioning start in the past and provisioning end greater that duration month
                   OR (invoice_provisioning_end_dt >= current_date AND invoice_provisioning_start_dt < current_date)      -- provisioning start in the past and provisioning end greater or equal than today
                   OR (invoice_provisioning_end_dt > reporting_month AND invoice_provisioning_start_dt > current_date AND reporting_month != current_mth)  -- provisioning start in future and provisioning end greater that duration month (BUT duration mth IS NOT current mth)
                   OR (valid_to_mth = reporting_month AND invoice_provisioning_end_mth = reporting_month)
                   OR (split_part(product_terms, ' ', 3) IN ('quarterly', 'bi-yearly') AND invoice_provisioning_start_dt = invoice_provisioning_start_mth)  -- special treatment for CB invoices starting on thee first day of month and end on last day of month +3/6 mths (see 30005149714, reporting month May'21 would have been 0 without this condition)
                   ) THEN mrr_temp_without_cn

               WHEN split_part(product_terms, ' ', 3) = 'monthly'
                   AND
                    MAX(
                    (CASE WHEN split_part(product_terms, ' ', 3) = 'monthly'
                        AND ((invoice_provisioning_end_mth > reporting_month AND invoice_provisioning_start_dt < current_date)
                            OR (invoice_provisioning_end_dt >= current_date AND invoice_provisioning_start_dt < current_date)
                            OR (invoice_provisioning_end_dt > reporting_month AND invoice_provisioning_start_dt > current_date AND reporting_month != current_mth)
                                   ) THEN mrr_temp_without_cn ELSE 0 END)
                        ) OVER (PARTITION BY customer_id, reporting_month) = 0
                   AND invoice_provisioning_end_mth = reporting_month
                   THEN mrr_temp_without_cn
               ELSE 0
        END AS mrr_without_cn
    FROM cte_2 c2
             LEFT JOIN (SELECT DISTINCT date_trunc('month',date_key)::DATE AS reporting_month
                        FROM dwh_main.dim_date
                        WHERE date_key > '2013-01-01' AND date_key <= current_date + interval '5 years') dd
                       ON dd.reporting_month >= invoice_provisioning_start_mth
                           AND dd.reporting_month <= invoice_provisioning_end_mth
                           AND dd.reporting_month <= valid_to_mth
    WHERE dd.reporting_month IS NOT NULL
)
-- SELECT * FROM cte_3 ORDER BY reporting_month;
SELECT customer_id
     , customer_country
     , customer_is_fleet AS is_fleet_customer
     , subscription_id
     , subscription_status
     , customer_status
     , first_customer_subs_start_dt
     , subscription_start_dt
     , subscription_end_dt  -- next renewal date
     , subscription_cancelled_dt
     , cancelled_after_days
     , subscription_cancelled_in_first_month
     , customer_cancelled_dt
     , first_invoice_dt
     , first_invoice_mth
     , first_invoice_dt_all_invoices
     , first_invoice_mth_all_invoices
     , valid_to_dt
     , valid_to_mth
     , invoice_id
     , invoice_created_dt
     , invoice_provisioning_start_dt
     , invoice_provisioning_start_mth
     , invoice_provisioning_end_dt
     , invoice_provisioning_end_mth
--      , invoice_total_amount / 100::numeric                                                           AS invoice_total_amount
--      , invoice_adjusted_amount / 100::numeric                                                        AS invoice_adjusted_amount
     , discount_percentage AS invoice_discount_pct
     , currency_code
     , exchange_rate
--      , credit_note_pct
     , full_refund                                                                                   AS invoice_has_full_refund
     , FALSE AS is_renewal -- TO DO!!

--      , CASE WHEN DATE_TRUNC('month', subscription_end_dt) = reporting_month THEN TRUE ELSE FALSE END AS is_renewal
--      , CASE WHEN renewal_temp = TRUE AND order_has_full_refund = FALSE
--     AND (
--                          (product_length_and_payment LIKE '%annual payment%' AND MIN(reporting_month) OVER (PARTITION BY order_uuid) = reporting_month) -- for annual payments take the 1st duration month of each of each renewed order
--                          OR (product_length_and_payment = '1y, monthly payment' AND (extract(year from age(reporting_month, first_mthly_payment_order_mth)) * 12 + extract(month from age(reporting_month, first_mthly_payment_order_mth)))::INT % 12 = 0 )
--                          OR (product_length_and_payment = '3y, monthly payment' AND (extract(year from age(reporting_month, first_mthly_payment_order_mth)) * 12 + extract(month from age(reporting_month, first_mthly_payment_order_mth)))::INT % 36 = 0 )
--                      ) THEN TRUE ELSE FALSE END AS renewal
     , plan_id
     , product_name
     , product_group
     , product_terms
     , CASE WHEN first_invoice_mth = reporting_month THEN TRUE ELSE FALSE END                        AS customer_is_new
     , CASE
           WHEN first_invoice_mth_all_invoices = reporting_month THEN TRUE
           ELSE FALSE END                                                                            AS customer_is_new_credit_note_ignored
     , CASE WHEN reporting_month > current_mth THEN TRUE ELSE FALSE END                              AS flag_future
     , reporting_month
     , CASE
           WHEN customer_cancelled_dt IS NOT NULL AND reporting_month = valid_to_mth THEN TRUE
           ELSE FALSE END                                                                            AS churn_mth
     , CASE
           WHEN (customer_cancelled_dt IS NOT NULL AND reporting_month = valid_to_mth) -- churn month
               OR full_refund = TRUE
               OR subscription_cancelled_in_first_month = TRUE
               THEN 0
           ELSE mrr_with_cn END                                                                      AS mrr_with_credit_note

     , CASE
           WHEN (customer_cancelled_dt IS NOT NULL AND reporting_month = valid_to_mth) -- churn month
               THEN 0
           ELSE mrr_without_cn END                                                                   AS mrr_without_credit_note

     , CASE WHEN mth_last_mth_with_mrr IS NOT NULL THEN mrr_with_cn END                              AS the_most_recent_customer_mrr
     , CASE
           WHEN (customer_cancelled_dt IS NOT NULL AND reporting_month = valid_to_mth) -- churn month
               THEN 0
           ELSE mrr_without_cn - mrr_with_cn
    END                                                                                           AS credit_note_mrr
     , CASE
           WHEN (customer_cancelled_dt IS NOT NULL AND reporting_month = valid_to_mth) -- churn month
               OR full_refund = TRUE
               OR subscription_cancelled_in_first_month = TRUE
               OR mrr_with_cn = 0 THEN 0
           ELSE item_quantity - refund_item_quantity END  AS licenses
     , CASE
           WHEN (customer_cancelled_dt IS NOT NULL AND reporting_month = valid_to_mth) -- churn month
               OR mrr_without_cn = 0 THEN 0
           ELSE item_quantity
    END                                                                                          AS licenses_without_credit_note
--               , coalesce(CASE WHEN full_refund = TRUE OR (customer_terminated_dt IS NOT NULL AND reporting_month = valid_to_mth)
--                                   THEN 0
--                              WHEN full_refund = TRUE
--                    THEN round(item_quantity) FILTER ( WHERE mrr_with_cn >0 ) OVER (PARTITION BY customer_id, reporting_month, product_group, payment_frequency, plan_length)
--                                                         * (round(mrr_with_cn / nullif(sum(mrr_without_cn) OVER (PARTITION BY customer_id, reporting_month, product_group, payment_frequency, plan_length),0)::numeric,3)) ,2)
--                    ELSE round(SUM(item_quantity) FILTER ( WHERE mrr_with_cn >0 ) OVER (PARTITION BY customer_id, reporting_month, product_group, payment_frequency, plan_length)
--                                                         * (round(mrr_with_cn / nullif(sum(mrr_with_cn) OVER (PARTITION BY customer_id, reporting_month, product_group, payment_frequency, plan_length),0)::numeric,3)) ,2) END, 0) AS customer_licenses
--               , coalesce(round(SUM(item_quantity) FILTER ( WHERE mrr_without_cn >0 )OVER (PARTITION BY customer_id, reporting_month, product_group, payment_frequency, plan_length)
--                                    * (round(mrr_without_cn / nullif(sum(mrr_without_cn) OVER (PARTITION BY customer_id, reporting_month, product_group, payment_frequency, plan_length),0)::numeric,3)) ,2), 0) AS customer_licenses_without_credit_note
-- , credit_note_item_pct
-- , item_quantity * (1-credit_note_item_pct)
FROM cte_3 c3
         LEFT JOIN (SELECT customer_id AS customer_id_last_mth_with_mrr
                         , coalesce(MAX(reporting_month) FILTER ( WHERE customer_status = 'active' AND mrr_with_cn >0)
        , MAX(reporting_month) FILTER ( WHERE customer_status != 'active' AND mrr_with_cn >0)) AS mth_last_mth_with_mrr
                    FROM cte_3
                    WHERE reporting_month <= current_date
                    GROUP BY customer_id) v
                   ON v.customer_id_last_mth_with_mrr = c3.customer_id
                       AND v.mth_last_mth_with_mrr = c3.reporting_month
-- WHERE customer_id = '6olerSQP3x4fwAV'
;