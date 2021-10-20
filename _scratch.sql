-- NEW contracts which started in Jan-2021 and Jan-2020:
WITH customers AS (SELECT c.customer_id
                        , CASE WHEN c.customer_is_fleet = TRUE THEN 'B2B' ELSE 'B2C' END AS customer_type
                        , c.min_subscription_start_dt AS customer_first_subscription_start_dt
                        , s.subscription_id
                        , s.subscription_status
                        , s.subscription_start_dt
                        , s.subscription_end_dt
                        , c.customer_current_mrr_eur
                   FROM data_mart_internal_reporting.ir_sh_cb_subscription s
                            JOIN data_mart_internal_reporting.ir_sh_cb_customer c
                                 ON c.customer_id = s.customer_id
                   WHERE TRUE
                     AND c.invoice_only_in_shop = TRUE
                     AND s.subscription_status = 'active'
--                         AND c.customer_is_fleet = FALSE
-- AND c.customer_id = 'K29368419'
)
SELECT
    c.*
     , i.min_invoice_created_dt
     , CASE
           WHEN date_trunc('month',customer_first_subscription_start_dt) = '2020-01-01'
               AND min_invoice_created_dt < '2019-12-21'
               THEN TRUE
           WHEN date_trunc('month',customer_first_subscription_start_dt) = '2021-01-01'
               AND min_invoice_created_dt < '2020-12-21'
               THEN TRUE
           ELSE FALSE
    END AS invoice_before_Dec_21st
FROM customers c
         JOIN (SELECT customer_id, min(invoice_created_dt) AS min_invoice_created_dt
               FROM data_mart_internal_reporting.ir_sh_cb_invoice_line i
               WHERE invoice_status <> 'voided'
                 AND product_group <> 'Hardware'
                 AND full_refund_flag = FALSE
                 AND mrr_local_ccy >0
               GROUP BY 1) i
              ON c.customer_id = i.customer_id
WHERE date_trunc('month',customer_first_subscription_start_dt) IN ('2020-01-01', '2021-01-01')
  AND customer_first_subscription_start_dt = subscription_start_dt
;

SELECT
    CASE
        WHEN current_date + INTERVAL '1 year' < subscription_end_dt
            THEN subscription_end_dt -  INTERVAL '1 year'
        ELSE subscription_end_dt
        END::DATE AS subscription_end_dt_ignore_3y_prod
FROM data_mart_internal_reporting.ir_sh_cb_subscription WHERE customer_id = 'K76310569';


-- All renewal invoices of active contracts per month from November 2021 to March 2022:
-- the 3y contracts are treated as 1y ones (example: https://operations.vimcar.com/dashboard/contract/V62416887 this one has now renewal date of 1.12.2021 even though the "true" renewal of 3y product will occur on 1.12.2022)
WITH customers AS (SELECT c.customer_id
                        , CASE WHEN c.customer_is_fleet = TRUE THEN 'B2B' ELSE 'B2C' END AS customer_type
                        , c.min_subscription_start_dt                                    AS customer_first_subscription_start_dt
                        , s.subscription_id
                        , s.subscription_status
                        , s.subscription_start_dt
                        , s.subscription_end_dt
                        , date_trunc('month', s.subscription_end_dt)::DATE AS subscription_end_mth
                        , CASE
                              WHEN current_date + INTERVAL '3 year' < subscription_end_dt
                                  THEN subscription_end_dt - INTERVAL '3 year'
                              WHEN current_date + INTERVAL '2 year' < subscription_end_dt
                                  THEN subscription_end_dt - INTERVAL '2 year'
                              WHEN current_date + INTERVAL '1 year' < subscription_end_dt
                                  THEN subscription_end_dt -  INTERVAL '1 year'
                              ELSE subscription_end_dt
        END::DATE AS subscription_end_dt_ignore_3y_prod
                        , date_trunc('month',
                                     CASE
                                         WHEN current_date + INTERVAL '3 year' < subscription_end_dt
                                             THEN subscription_end_dt - INTERVAL '3 year'
                                         WHEN current_date + INTERVAL '2 year' < subscription_end_dt
                                             THEN subscription_end_dt - INTERVAL '2 year'
                                         WHEN current_date + INTERVAL '1 year' < subscription_end_dt
                                             THEN subscription_end_dt -  INTERVAL '1 year'
                                         ELSE subscription_end_dt
                                         END)::DATE AS subscription_end_mth_ignore_3y_prod
                   FROM data_mart_internal_reporting.ir_sh_cb_subscription s
                            JOIN data_mart_internal_reporting.ir_sh_cb_customer c ON c.customer_id = s.customer_id
                   WHERE TRUE
--                      AND c.customer_id = 'K15073145'
                     AND c.customer_country = 'DE'
                     AND s.subscription_status = 'active')
   , customers_2 AS (SELECT c.*, i.current_mrr
                     FROM customers c
                              LEFT JOIN (SELECT subscription_id, SUM(mrr_eur - refund_mrr_eur) AS current_mrr
                                         FROM data_mart_internal_reporting.ir_sh_cb_invoice_line
                                         WHERE voided_ts IS NULL
                                           AND product_group <> 'Hardware'
                                           AND invoice_provisioning_start_dt <= CURRENT_DATE
                                           AND invoice_provisioning_end_dt > CURRENT_DATE
                                           AND mrr_local_ccy >0
                                         GROUP BY 1) i ON i.subscription_id = c.subscription_id
                     WHERE subscription_end_mth_ignore_3y_prod BETWEEN '2021-11-01' AND '2022-03-01'
-- AND customer_id IN ('76268795','63068476','K64584860','K43540197')
)
SELECT
    customer_type
     , subscription_end_mth_ignore_3y_prod
     , count(DISTINCT customer_id) AS cnt_customers
     , count(DISTINCT subscription_id) AS cnt_subscriptions
     , sum(current_mrr) AS mrr
FROM customers_2
WHERE subscription_end_mth_ignore_3y_prod BETWEEN '2021-11-01' AND '2022-03-01'
GROUP BY 1,2
ORDER BY 1,2;