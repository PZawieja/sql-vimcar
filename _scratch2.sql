SELECT * FROM data_mart_internal_reporting.ir_ml_customer_revenue WHERE customer_id = 'K70873891'

SELECT *
FROM data_mart_internal_reporting.ir_m_sh_cb_investor_report_100d_mrr_new
WHERE customer_id = 'K82757655'
ORDER BY reporting_month
;
SELECT *
FROM data_mart_internal_reporting.ir_m_sh_cb_investor_report
WHERE customer_id = 'K71787876'
ORDER BY reporting_month
;


SELECT *
FROM data_mart_internal_reporting.ir_m_sh_cb_investor_report_100d_mrr_new
WHERE customer_id = 'K71787876'
ORDER BY reporting_month

;

SELECT *
FROM data_mart_internal_reporting.tmp_revenue_reporting_sh_cb
WHERE customer_id = 'K71787876'
AND invoice_id = 'R85126352'
;

SELECT
       reporting_month
     , CASE
           WHEN customer_is_new = TRUE
               -- to capture the new invoices from the additional two months:
               OR (ni.is_new_invoice_id IS NOT NULL
                   AND invoice_provisioning_start_mth = reporting_month
                   AND reporting_month BETWEEN t2.customer_is_new_start_mth AND
                       CASE
                           WHEN t2.new_mrr_window_mths =2 THEN t2.customer_is_new_start_mth + INTERVAL '2 months'
                           WHEN t2.new_mrr_window_mths =3 THEN t2.customer_is_new_start_mth + INTERVAL '3 months'
                        END)
               THEN TRUE
           ELSE FALSE
    END customer_is_new
FROM data_mart_internal_reporting.tmp_revenue_reporting_sh_cb t
         LEFT JOIN (SELECT DISTINCT invoice_id AS is_new_invoice_id FROM dwh_main.dim_combined_invoice_line WHERE is_new_invoice = TRUE) ni
                   ON ni.is_new_invoice_id = t.invoice_id
         LEFT JOIN (SELECT customer_id
                         , CASE WHEN extract('day' from first_customer_subs_start_dt) >20 THEN 3 ELSE 2 END new_mrr_window_mths
                         , MIN(CASE WHEN customer_is_new THEN reporting_month END) customer_is_new_start_mth
                         , MIN(CASE WHEN customer_is_new_credit_note_ignored THEN reporting_month END) customer_is_new_credit_note_ignored_start_mth
                    FROM data_mart_internal_reporting.tmp_revenue_reporting_sh_cb
                    GROUP BY customer_id, first_customer_subs_start_dt) t2
                   ON t.customer_id = t2.customer_id
WHERE first_invoice_dt = '2022-01-15'
AND t.customer_id = '11037121'
ORDER BY reporting_month
;

-- 20210901 Investor Report SQL, new based on combined data:
-- all the customer’s orders starting within the first 3 calendar months will be attributed to MRR New (JIRA BI-2387)
WITH cte_pdb_0 AS (SELECT t.customer_id
                        , customer_country
                        , is_fleet_customer
                        , subscription_id
                        , subscription_status
                        , customer_status
                        , first_customer_subs_start_dt
                        , subscription_start_dt
                        , subscription_end_dt
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
                        , invoice_discount_pct
                        , currency_code
                        , exchange_rate
                        , invoice_has_full_refund
                        , is_renewal
                        , plan_id
                        , product_name
                        , product_group
                        , product_terms
                        , customer_is_new
                        , CASE
                            WHEN t2.new_mrr_window_mths =2 THEN t2.customer_is_new_start_mth + INTERVAL '2 months'
                            WHEN t2.new_mrr_window_mths =3 THEN t2.customer_is_new_start_mth + INTERVAL '3 months'
                          END::DATE customer_is_new_3m_end_mth
                        /*
                        , CASE
                              WHEN customer_is_new = TRUE
                                  -- to capture the new invoices from the additional two months:
                                  OR (ni.is_new_invoice_id IS NOT NULL
                                      AND invoice_provisioning_start_mth = reporting_month
                                      AND reporting_month BETWEEN t2.customer_is_new_start_mth AND
                                          CASE
                                              WHEN t2.new_mrr_window_mths =2 THEN t2.customer_is_new_start_mth + INTERVAL '2 months'
                                              WHEN t2.new_mrr_window_mths =3 THEN t2.customer_is_new_start_mth + INTERVAL '3 months'
                                              END)
                                  THEN TRUE
                              ELSE FALSE
                        END customer_is_new
                         */
                        , flag_future
                        , reporting_month
                        , churn_mth
                        , mrr_with_credit_note
                        , mrr_without_credit_note
                        , the_most_recent_customer_mrr
                        , credit_note_mrr
                        , licenses
                        , licenses_without_credit_note
                   FROM data_mart_internal_reporting.tmp_revenue_reporting_sh_cb t
                            LEFT JOIN (SELECT DISTINCT invoice_id AS is_new_invoice_id FROM dwh_main.dim_combined_invoice_line WHERE is_new_invoice = TRUE) ni
                                      ON ni.is_new_invoice_id = t.invoice_id
                            LEFT JOIN (SELECT customer_id
                                            , CASE WHEN extract('day' from first_customer_subs_start_dt) >20 THEN 3 ELSE 2 END new_mrr_window_mths
                                            , MIN(CASE WHEN customer_is_new THEN reporting_month END) customer_is_new_start_mth
                                            , MIN(CASE WHEN customer_is_new_credit_note_ignored THEN reporting_month END) customer_is_new_credit_note_ignored_start_mth
                                       FROM data_mart_internal_reporting.tmp_revenue_reporting_sh_cb
                                       GROUP BY customer_id, first_customer_subs_start_dt) t2
                                      ON t.customer_id = t2.customer_id
                   WHERE invoice_has_full_refund = FALSE
                     AND subscription_cancelled_in_first_month = FALSE
--                        AND t.customer_id = 'K71787876'
--         AND t.customer_id = '71557870' -- new only in the first month, yearly payment
-- AND t.customer_id = '11053357' -- new only in the first month, yearly payment
--                    AND t.customer_id = '11025773' -- new the first and third month, yearly payment (new in Jan and Mar'22)
--         AND reporting_month BETWEEN '2016-10-01' AND '2017-05-01'
)
--    SELECT * FROM cte_pdb_0;
   , cte_pdb_1 AS (
    SELECT
        rev.*
         , coalesce(CASE WHEN customer_is_new = TRUE
                             THEN round(SUM(mrr_with_credit_note)
                                        OVER (PARTITION BY rev.customer_id, rev.product_group, rev.product_terms, rev.reporting_month), 3) END,0) AS mrr_new_temp
         , coalesce(CASE WHEN customer_is_new = TRUE
                             THEN round(SUM(licenses)
                                        OVER (PARTITION BY rev.customer_id, rev.product_group, rev.product_terms, rev.reporting_month), 3) END,0) AS licenses_new_temp

         , CASE WHEN customer_is_new = FALSE
        AND mrr_with_credit_note > 0
        AND first_invoice_dt <= current_date  -- expansion only if the contract exists already
--                      AND NOT ( MAX(order_provisioning_start_mth) OVER (PARTITION BY rev.domain_name, reporting_month)= date_trunc('month', current_date)
--                          AND  MAX(order_provisioning_start_dt) OVER (PARTITION BY rev.domain_name, reporting_month) >= current_date)
        AND SUM(mrr_with_credit_note)  OVER (PARTITION BY rev.customer_id, rev.product_group, rev.product_terms, rev.reporting_month) > coalesce(product_family_and_type_domain_value_previous_mth,0)
                    THEN (SUM(mrr_with_credit_note) OVER (PARTITION BY rev.customer_id, rev.product_group, rev.product_terms, rev.reporting_month) - coalesce(product_family_and_type_domain_value_previous_mth,0))  -- attributed to the month of actual expansion
        END AS mrr_expansion_temp

         , CASE WHEN customer_is_new = FALSE
                        AND mrr_with_credit_note > 0
                        AND first_invoice_dt <= current_date  -- expansion only if the contract exists already
--                      AND NOT ( MAX(order_provisioning_start_mth) OVER (PARTITION BY rev.domain_name, reporting_month)= date_trunc('month', current_date)
--                          AND  MAX(order_provisioning_start_dt) OVER (PARTITION BY rev.domain_name, reporting_month) >= current_date)
        AND SUM(licenses) OVER (PARTITION BY rev.customer_id, rev.product_group, rev.product_terms, rev.reporting_month) > coalesce(product_family_and_type_licenses_previous_mth,0)
                    THEN (SUM(licenses) OVER (PARTITION BY rev.customer_id, rev.product_group, rev.product_terms, rev.reporting_month) - coalesce(product_family_and_type_licenses_previous_mth,0))
        END AS licenses_expansion_temp

         , CASE
               WHEN customer_is_new = FALSE
                   AND SUM(mrr_with_credit_note)
                       OVER (PARTITION BY rev.customer_id, rev.product_group, rev.product_terms, rev.reporting_month) < coalesce(product_family_and_type_domain_value_previous_mth,0)
                   THEN (SUM(mrr_with_credit_note)
                         OVER (PARTITION BY rev.customer_id, rev.product_group, rev.product_terms, rev.reporting_month) - coalesce(product_family_and_type_domain_value_previous_mth,0)) -- reducing value of product is attributed to the month of actual contraction
               WHEN customer_is_new = FALSE
                   AND product_family_and_type_domain_value_next_mth IS NULL
                   AND MAX(invoice_provisioning_end_dt)
                       OVER (PARTITION BY rev.customer_id, rev.product_group, rev.product_terms, rev.reporting_month) < current_date
                   THEN (SUM(mrr_with_credit_note)
                         OVER (PARTITION BY rev.customer_id, rev.product_group, rev.product_terms, rev.reporting_month)) * -1 -- cancelling the product subscription is attributed to the month when the product was present last time (if product family does not appear next mth)
        END AS mrr_contraction_temp

         , CASE
               WHEN customer_is_new = FALSE
                   AND SUM(licenses)
                       OVER (PARTITION BY rev.customer_id, rev.product_group, rev.product_terms, rev.reporting_month) < coalesce(product_family_and_type_licenses_previous_mth,0)
                   THEN (SUM(licenses)
                         OVER (PARTITION BY rev.customer_id, rev.product_group, rev.product_terms, rev.reporting_month) - coalesce(product_family_and_type_licenses_previous_mth,0)) -- reducing value of product is attributed to the month of actual contraction
               WHEN customer_is_new = FALSE
                           AND product_family_and_type_licenses_next_mth IS NULL
                           AND MAX(invoice_provisioning_end_dt)
                       OVER (PARTITION BY rev.customer_id, rev.product_group, rev.product_terms, rev.reporting_month) < current_date
                   THEN (SUM(licenses)
                         OVER (PARTITION BY rev.customer_id, rev.product_group, rev.product_terms, rev.reporting_month))  -- cancelling the product subscription is attributed to the month when the product was present last time (if product family does not appear next mth)
        END AS licenses_contraction_temp

         , coalesce(CASE WHEN customer_is_new = FALSE
                                AND customer_cancelled_dt IS NOT NULL
                                AND reporting_month = valid_to_mth
                             THEN round(SUM(mrr_with_credit_note) OVER (PARTITION BY rev.customer_id, rev.product_group, rev.product_terms, rev.reporting_month) * -1, 3) END, 0) AS mrr_churned_temp -- attributed to the last order provisioning month

         , coalesce(CASE WHEN customer_is_new = FALSE
                                AND customer_cancelled_dt IS NOT NULL
                                AND reporting_month = valid_to_mth
                             THEN round(SUM(licenses) OVER (PARTITION BY rev.customer_id, rev.product_group, rev.product_terms, rev.reporting_month) * -1, 3) END, 0) AS licenses_churned_temp -- attributed to the last order provisioning month

         , round(mrr_with_credit_note / nullif(sum(mrr_with_credit_note) OVER (PARTITION BY rev.customer_id, rev.product_group, rev.product_terms, rev.reporting_month), 0)::numeric, 3) AS product_split_pct_revenue
         , CASE
               WHEN reporting_month = invoice_provisioning_start_mth
                   THEN round(mrr_with_credit_note / nullif(sum(mrr_with_credit_note) OVER (PARTITION BY rev.customer_id, rev.product_group, rev.product_terms, rev.reporting_month, invoice_provisioning_start_mth),0)::numeric,3)
        END AS product_split_pct_revenue_expansion -- attributed only to the records where reporting_month = order_provisioning_start_mth
         , round(licenses / nullif(sum(licenses) OVER (PARTITION BY rev.customer_id, rev.product_group, rev.product_terms, rev.reporting_month), 0)::numeric, 3) AS product_split_pct_licenses
         , CASE
               WHEN reporting_month = invoice_provisioning_start_mth
                   THEN round(licenses / nullif(sum(licenses) OVER (PARTITION BY rev.customer_id, rev.product_group, rev.product_terms, rev.reporting_month, invoice_provisioning_start_mth),0)::numeric,3)
        END AS product_split_pct_licenses_expansion -- attributed only to the records where reporting_month = order_provisioning_start_mth
         , product_family_and_type_domain_value_previous_mth -- max granularity
         , product_family_and_type_domain_value_next_mth
         , product_family_and_type_licenses_previous_mth
         , product_family_and_type_licenses_next_mth
         , product_family_domain_value_previous_mth  -- less granular
         , product_family_domain_value_next_mth
         , product_family_licenses_previous_mth
         , product_family_licenses_next_mth
    FROM cte_pdb_0 rev
--                   JOIN dwh_main.dim_contract ctr ON ctr.contract_id = rev.contract_id
             JOIN (SELECT customer_id, sum(mrr_with_credit_note)
                   FROM cte_pdb_0
                   WHERE invoice_has_full_refund = FALSE
                     AND subscription_cancelled_in_first_month = FALSE
                   GROUP BY customer_id
                   HAVING sum(mrr_with_credit_note) > 0) excl ON excl.customer_id = rev.customer_id -- exclude customer_id with no revenue value ever
    -- WINDOWS: previous mth & next month , Contract Value and Licenses by Contract, Product Family and Product Type (maximum granularity):
             LEFT JOIN (SELECT customer_id, product_group, product_terms, (reporting_month + INTERVAL '1 month')::DATE AS next_mth_for_join
                             , SUM(mrr_with_credit_note) AS product_family_and_type_domain_value_previous_mth
                             , SUM(licenses) AS product_family_and_type_licenses_previous_mth
                        FROM cte_pdb_0
                        GROUP BY 1,2,3,4) sub1 ON rev.reporting_month = sub1.next_mth_for_join
        AND rev.customer_id = sub1.customer_id
        AND rev.product_group = sub1.product_group
        AND rev.product_terms = sub1.product_terms
             LEFT JOIN (SELECT customer_id, product_group, product_terms, (reporting_month - INTERVAL '1 month')::DATE AS previous_mth_for_join
                             , SUM(mrr_with_credit_note) AS product_family_and_type_domain_value_next_mth
                             , SUM(licenses) AS product_family_and_type_licenses_next_mth
                        FROM cte_pdb_0
                        GROUP BY 1,2,3,4) sub2 ON rev.reporting_month = sub2.previous_mth_for_join
        AND rev.customer_id = sub2.customer_id
        AND rev.product_group = sub2.product_group
        AND rev.product_terms = sub2.product_terms

--          WINDOWS: previous mth & next month , Domain Value and Licenses by Domain, Product Family (less granular):
             LEFT JOIN (SELECT customer_id, product_group, (reporting_month + INTERVAL '1 month')::DATE AS next_mth_for_join
                             , SUM(mrr_with_credit_note) AS product_family_domain_value_previous_mth
                             , SUM(licenses) AS product_family_licenses_previous_mth
                        FROM cte_pdb_0
                        GROUP BY 1,2,3) sub3 ON rev.reporting_month = sub3.next_mth_for_join
        AND rev.customer_id = sub3.customer_id
        AND rev.product_group = sub3.product_group
             LEFT JOIN (SELECT customer_id, product_group, (reporting_month - INTERVAL '1 month')::DATE AS previous_mth_for_join
                             , SUM(mrr_with_credit_note) AS product_family_domain_value_next_mth
                             , SUM(licenses) AS product_family_licenses_next_mth
                        FROM cte_pdb_0
                        GROUP BY 1,2,3) sub4 ON rev.reporting_month = sub4.previous_mth_for_join
        AND rev.customer_id = sub4.customer_id
        AND rev.product_group = sub4.product_group
)
, cte_pdb_2 AS (
SELECT
    CASE
        WHEN c.customer_id_chargebee IS NOT NULL
            THEN 'CB'
        ELSE 'SH'
        END AS source_system
     , cte_pdb_1.customer_id
     , coalesce(c.customer_contact_company_name, c.customer_shipping_company_name, 'unknown') AS company_name
     , cte_pdb_1.customer_country
     , cte_pdb_1.customer_status
     , cte_pdb_1.is_fleet_customer
     , cte_pdb_1.subscription_id
     , cte_pdb_1.subscription_status
     , 'unknown' AS subscription_creator
--      , ctr.contract_is_fleet_customer AS fleet_customer
--      , ctr.contract_total_net_value AS shop_contract_total_net_value
     , cte_pdb_1.subscription_cancelled_dt AS subscription_terminated_dt-- subscription !!!!
     , cte_pdb_1.customer_cancelled_dt AS customer_terminated_dt  -- customer
     , cte_pdb_1.first_invoice_mth
--      , first_mthly_payment_order_mth
     , cte_pdb_1.valid_to_dt
     , cte_pdb_1.valid_to_mth
     , cte_pdb_1.churn_mth
     , FALSE is_renewal  -- CHECK!!
     , subscription_end_dt AS next_renewal_dt  -- CHECK!!
     , cte_pdb_1.plan_id
     , cte_pdb_1.product_group
     , cte_pdb_1.product_terms AS product_length_and_payment
     , cte_pdb_1.product_name AS plan_name
     , cte_pdb_1.invoice_id
     , cte_pdb_1.invoice_created_dt
     , cte_pdb_1.invoice_provisioning_start_dt
     , cte_pdb_1.invoice_provisioning_end_dt
     , cte_pdb_1.invoice_discount_pct
     , cte_pdb_1.currency_code
     , cte_pdb_1.exchange_rate::NUMERIC(10,5)
     , cte_pdb_1.customer_is_new
     , cte_pdb_1.flag_future
     , cte_pdb_1.reporting_month
     , round((mrr_with_credit_note / exchange_rate) * 12, 2)::DECIMAL(7,2) AS arr_eur
     , round(mrr_with_credit_note / exchange_rate, 2) AS mrr_eur
     , round(coalesce(CASE WHEN customer_is_new = TRUE THEN (mrr_new_temp + mrr_churned_temp) * product_split_pct_revenue ELSE 0 END, 0) / exchange_rate, 2) AS mrr_eur_new

--      , CASE WHEN (terminated_dt IS NOT NULL AND reporting_month = valid_to_mth) = FALSE -- expansion only if there is no mrr churn
--                 THEN coalesce(round(mrr_expansion_temp * (product_family_and_type_contract_value_next_mth / nullif(SUM(CASE WHEN mrr_expansion_temp > 0 THEN product_family_and_type_contract_value_next_mth END) OVER (PARTITION BY cte_pdb_1.contract_id, cte_pdb_1.product_family, cte_pdb_1.product_length_and_payment, reporting_month),0)), 3),0)
--             ELSE 0 END AS mrr_expansion

     , ROUND(CASE WHEN (customer_cancelled_dt IS NOT NULL AND reporting_month = valid_to_mth) = FALSE -- expansion only if there is no mrr churn
                      THEN coalesce(round(mrr_expansion_temp * product_split_pct_revenue_expansion, 3),0)
                  ELSE 0 END / exchange_rate, 2) AS mrr_eur_expansion

     , ROUND(CASE WHEN (customer_cancelled_dt IS NOT NULL AND reporting_month = valid_to_mth) = FALSE -- contraction only if there is no mrr churn
                      THEN coalesce(round(mrr_contraction_temp * (product_family_and_type_domain_value_previous_mth / nullif(SUM(CASE WHEN mrr_contraction_temp < 0 THEN product_family_and_type_domain_value_previous_mth END) OVER (PARTITION BY cte_pdb_1.customer_id, cte_pdb_1.product_group, cte_pdb_1.product_terms, reporting_month),0)),3),0)
                  ELSE 0 END / exchange_rate, 2) AS mrr_eur_contraction

     , ROUND(coalesce(round(CASE WHEN customer_cancelled_dt IS NOT NULL
    AND valid_to_mth = reporting_month
    AND mrr_churned_temp = 0
                                     THEN mrr_contraction_temp * (product_family_and_type_domain_value_previous_mth / nullif(SUM(CASE WHEN mrr_contraction_temp < 0 THEN product_family_and_type_domain_value_previous_mth END) OVER (PARTITION BY cte_pdb_1.customer_id, cte_pdb_1.product_group, cte_pdb_1.product_terms, reporting_month),0))
                                 WHEN customer_cancelled_dt IS NOT NULL
                                     AND valid_to_mth = reporting_month
                                     THEN (mrr_churned_temp + mrr_new_temp) * product_split_pct_revenue END, 3),0) / exchange_rate, 2) AS mrr_eur_churned
     -- Licenses:
     , cte_pdb_1.licenses
     , round(coalesce(CASE WHEN customer_is_new = TRUE THEN (licenses_new_temp + licenses_churned_temp) * product_split_pct_revenue ELSE 0 END, 0) / exchange_rate, 2) AS licenses_new
     , CASE WHEN (customer_cancelled_dt IS NOT NULL AND reporting_month = valid_to_mth) = FALSE -- expansion only if there is no mrr churn
                THEN coalesce(round(licenses_expansion_temp * product_split_pct_licenses_expansion, 3),0)
            ELSE 0 END AS licenses_expansion
     , CASE WHEN (customer_cancelled_dt IS NOT NULL AND reporting_month = valid_to_mth) = FALSE -- contraction only if there is no mrr churn
                THEN coalesce(round(licenses_contraction_temp * (product_family_and_type_licenses_previous_mth / nullif(SUM(CASE WHEN licenses_contraction_temp < 0 THEN product_family_and_type_licenses_previous_mth END) OVER (PARTITION BY cte_pdb_1.customer_id, cte_pdb_1.product_group, cte_pdb_1.product_terms, reporting_month),0)),3),0)
            ELSE 0 END AS licenses_contraction
     , ROUND(coalesce(round(CASE WHEN customer_cancelled_dt IS NOT NULL
    AND valid_to_mth = reporting_month
    AND mrr_churned_temp = 0
                                     THEN licenses_contraction_temp * (product_family_and_type_domain_value_previous_mth / nullif(SUM(CASE WHEN licenses_contraction_temp < 0 THEN product_family_and_type_domain_value_previous_mth END) OVER (PARTITION BY cte_pdb_1.customer_id, cte_pdb_1.product_group, cte_pdb_1.product_terms, reporting_month),0))
                                 WHEN customer_cancelled_dt IS NOT NULL
                                     AND valid_to_mth = reporting_month
                                     THEN (licenses_churned_temp + licenses_new_temp) * product_split_pct_revenue END, 3),0) / exchange_rate, 2) AS licenses_churned
     -- EXPANSION FIELDS:
     -- IMPORTANT NOTE: Calculation at the level reduced to Domain and Product Family (payment & length ignored) !!!
     , CASE WHEN customer_is_new = FALSE
    AND product_family_domain_value_previous_mth IS NULL
    AND product_name NOT LIKE '%Upgrade%'
    AND (mrr_expansion_temp > 0
        OR mrr_contraction_temp < 0)
                THEN TRUE ELSE FALSE END flg_expansion_crossselling
     , CASE WHEN customer_is_new = FALSE
    AND product_family_domain_value_previous_mth < nullif(sum(mrr_with_credit_note) OVER (PARTITION BY cte_pdb_1.customer_id, cte_pdb_1.product_group, reporting_month),0)::numeric
    AND (mrr_expansion_temp > 0
        OR mrr_contraction_temp < 0)
                THEN TRUE ELSE FALSE END flg_expansion_reselling
     , CASE WHEN customer_is_new = FALSE
    AND product_family_domain_value_previous_mth IS NULL
    AND product_name LIKE '%Upgrade%'
    AND (mrr_expansion_temp > 0
        OR mrr_contraction_temp < 0)
                THEN TRUE ELSE FALSE END flg_expansion_upgrade
    , customer_is_new_3m_end_mth
FROM cte_pdb_1
         JOIN dwh_main.dim_combined_customer c
              ON c.customer_id = cte_pdb_1.customer_id
)
SELECT source_system
     , customer_id
     , company_name
     , customer_country
     , customer_status
     , is_fleet_customer
     , subscription_id
     , subscription_status
     , subscription_creator
     , subscription_terminated_dt
     , customer_terminated_dt
     , first_invoice_mth
     , valid_to_dt
     , valid_to_mth
     , churn_mth
     , is_renewal
     , next_renewal_dt
     , plan_id
     , product_group
     , product_length_and_payment
     , plan_name
     , invoice_id
     , invoice_created_dt
     , invoice_provisioning_start_dt
     , invoice_provisioning_end_dt
     , invoice_discount_pct
     , currency_code
     , exchange_rate
     , customer_is_new
     , flag_future
     , reporting_month
     , arr_eur
     , mrr_eur
     , CASE
         WHEN reporting_month <= customer_is_new_3m_end_mth
             THEN mrr_eur_new + mrr_eur_expansion
         ELSE 0
        END AS mrr_eur_new
     , CASE
           WHEN reporting_month > customer_is_new_3m_end_mth
               THEN mrr_eur_expansion
           ELSE 0
        END AS mrr_eur_expansion
     , CASE
           WHEN reporting_month > customer_is_new_3m_end_mth
               THEN mrr_eur_contraction
           ELSE 0
        END AS mrr_eur_contraction
     , mrr_eur_churned
     , licenses
     , CASE
           WHEN reporting_month <= customer_is_new_3m_end_mth
               THEN licenses_new + licenses_expansion
           ELSE 0
        END AS licenses_new
     , CASE
           WHEN reporting_month > customer_is_new_3m_end_mth
               THEN licenses_expansion
           ELSE 0
        END AS licenses_expansion
     , CASE
           WHEN reporting_month > customer_is_new_3m_end_mth
               THEN licenses_contraction
           ELSE 0
        END AS licenses_contraction
     , licenses_churned
     , CASE
           WHEN reporting_month > customer_is_new_3m_end_mth
               THEN flg_expansion_crossselling
           ELSE FALSE
        END AS flg_expansion_crossselling
     , CASE
           WHEN reporting_month > customer_is_new_3m_end_mth
               THEN flg_expansion_reselling
           ELSE FALSE
        END AS flg_expansion_reselling
     , CASE
           WHEN reporting_month > customer_is_new_3m_end_mth
               THEN flg_expansion_upgrade
           ELSE FALSE
        END AS flg_expansion_upgrade
FROM cte_pdb_2
;


