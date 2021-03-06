WITH cte_invoice_raw AS (
    SELECT
        coalesce(cbc.cb_cust_id, c.customer_id) AS customer_id
         , c.domain_name
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
        END                                                                                               AS contract_status_map
         , coalesce(i.invoice_id, 'o-'|| order_id )                                                           AS invoice_id
         , o.invoice_status
         , o.order_id
         , (o.order_created_ts AT TIME ZONE 'Europe/Berlin')::DATE                                            AS order_created_dt
         , CASE
               WHEN split_part(coalesce(i.invoice_creator_urn, o.order_creator_urn),'/',2) NOT IN ('new-user-checkout','contract-renewal','importer')
                   THEN split_part(coalesce(i.invoice_creator_urn, o.order_creator_urn),'/',2)
        END::UUID AS creator_urn
         , (o.order_provisioning_start_ts AT TIME ZONE 'Europe/Berlin')::DATE                                 AS order_provisioning_start_dt
         , (o.order_provisioning_end_ts AT TIME ZONE 'Europe/Berlin')::DATE                                   AS order_provisioning_end_dt
         , CASE WHEN o.order_creator_urn NOT LIKE '%renewal%' THEN TRUE ELSE FALSE END                        AS is_new_invoice
--          , CASE WHEN o.order_creator_urn LIKE '%renewal%' THEN TRUE ELSE FALSE END                            AS flag_renewal_tmp
         , lower(btrim(o.coupon_code))                                                                        AS coupon_code
         , CASE
               WHEN cp.coupon_discount_type = 'percentage'
                   THEN cp.coupon_discount_value
               ELSE o.order_discount_percentage
        END::decimal(10,2)                                                                               AS order_discount_percentage
         , o.order_discount_amount                                                                            AS order_discount_amount
         , ROUND(order_discount_amount /
                 NULLIF(SUM(order_item_total_provisioned_price_gross_amt) OVER (PARTITION BY o.order_uuid),
                        0)::DECIMAL(10, 2), 3)                                                                AS calculated_order_discount_percentage -- best for checking: R31141546, correct is 0.473
         , foi.order_item_total_price_net_amt                                                                 AS order_item_total_price_net_amt
         , foi.order_item_total_price_gross_amt                                                               AS order_item_total_price_gross_amt
         , coalesce((1 + o.order_total_price_tax), 1.19)                                                      AS tax_rate
         , foi.order_item_quantity                                                                            AS order_quantity
         , o.order_has_full_refund
         , o.order_has_refund
         , coalesce(refund_net_amount,0)                                                                      AS refund_net_amount
         , upm.product_uuid
         , upm.product_name
         , upm.product_family
         , CASE
               WHEN upm.monthly_payment = FALSE
                   AND p.product_name LIKE '%3-j??hrig%'
                   THEN '36 mths, yearly payment'
               WHEN upm.monthly_payment = TRUE
                   AND p.product_name LIKE '%3-j??hrig%'
                   THEN '36 mths, monthly payment'
               WHEN upm.monthly_payment = FALSE
                   THEN '12 mths, yearly payment'
               WHEN upm.monthly_payment = TRUE
                   THEN '12 mths, monthly payment'
               WHEN p.product_name = 'Dummy Vimcar Miet-Modell'
                   THEN '12 mths, yearly payment'
        END                                                                                              AS product_length_and_payment
         , upm.monthly_payment
         , CASE
               WHEN foi.order_item_total_price_net_amt > 0 AND (upm.is_recurring = TRUE OR upm.is_subsequent_recurring = TRUE)
                   THEN TRUE
               ELSE FALSE
        END AS is_mrr_relevant
         , CASE
               WHEN contract_status != 'active'
                   OR MAX(o.order_provisioning_end_ts) OVER (PARTITION BY ctr.contract_id) ::DATE < current_date - INTERVAL '6 months'
                   THEN coalesce(contract_canceled_date_key, contract_modified_ts AT TIME ZONE 'Europe/Berlin')
        END::DATE AS contract_terminated_dt
    FROM dwh_main.fact_order_item foi
             JOIN dwh_main.dim_order o
                  ON o.order_key = foi.order_key
             JOIN dwh_main.umd_product_map upm
                  ON foi.order_item_product_key = upm.product_key
             JOIN dwh_main.umd_product_map p
                  ON foi.order_item_product_key = p.product_key
             JOIN dwh_main.dim_customer c
                  ON o.customer_uuid = c.customer_uuid
             JOIN dwh_main.dim_contract ctr
                  ON ctr.contract_uuid = o.contract_uuid
             LEFT JOIN dwh_main.dim_invoice i
                       ON i.order_uuid = o.order_uuid
             LEFT JOIN dwh_main.dim_coupon cp
                       ON cp.coupon_code = o.coupon_code
             LEFT JOIN (SELECT invoice_id, SUM(refund_net_amount) AS refund_net_amount
                        FROM dwh_main.dim_refund
                        GROUP BY invoice_id) refund ON refund.invoice_id = i.invoice_id
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
      AND NOT (i.invoice_id IS NULL AND o.order_created_ts >= '2020-12-31 23:00:00') -- exclude orders for recurring products without an invoice
--      AND NOT (c.customer_creator_urn = 'urn:vimcar::service/new-user-checkout' AND c.customer_modifier_urn = 'urn:vimcar::service/new-user-checkout') -- added 2022-02-11, mostlikely these are ghost customers,see BI-2175
--     AND ctr.contract_id = 'V13028084'
--         AND i.invoice_id = 'R14767314'  -- multiple products, discount amount split among them
--    AND customer_id = '21176414'
--     AND customer_id = '62315493'
--       AND foi.order_item_total_price_net_amt > 0
--       AND (upm.is_recurring = TRUE OR upm.is_subsequent_recurring = TRUE)
-- AND i.invoice_id = 'R72971822' -- no discount in invoice o order but on coupon there is 15%
)
-- SELECT * FROM cte_invoice_raw;
   , cte_revenue_1 AS (
    SELECT
        t.*
         , p.email AS invoice_creator_email
         , MIN(order_created_dt) OVER (PARTITION BY t.customer_id) AS first_order_created_dt
         , MIN(order_provisioning_start_dt) FILTER (WHERE order_has_full_refund = FALSE) OVER (PARTITION BY t.customer_id) AS first_order_dt
         , MIN(date_trunc('month',order_provisioning_start_dt)) FILTER (WHERE order_has_full_refund = FALSE AND monthly_payment = TRUE) OVER (PARTITION BY contract_id) AS first_mthly_payment_order_mth
         , MIN(order_provisioning_start_dt) FILTER (WHERE order_has_full_refund = FALSE AND product_length_and_payment LIKE '3%') OVER (PARTITION BY contract_id) AS first_3y_order_dt
         , MIN(order_created_dt) FILTER (WHERE order_has_full_refund = FALSE) OVER (PARTITION BY contract_id, product_family, product_length_and_payment) AS min_order_dt_by_product_terms
         , date_trunc('month',order_provisioning_start_dt) AS order_provisioning_start_mth
         , coalesce(MAX(order_provisioning_end_dt) FILTER (WHERE order_has_full_refund = FALSE) OVER (PARTITION BY t.customer_id), MAX(order_provisioning_end_dt) OVER (PARTITION BY customer_id)) AS valid_to_dt
         , coalesce(round(CASE WHEN is_mrr_relevant = TRUE AND monthly_payment = FALSE THEN
                                       (CASE
                                            WHEN order_discount_percentage >0
                                                THEN
                                                    order_item_total_price_net_amt
                                                    * (1-order_discount_percentage)
--                                                     - (COALESCE(refund_net_amount,0) * ((order_item_total_price_net_amt / sum(order_item_total_price_net_amt) OVER (PARTITION BY order_id)::NUMERIC)::NUMERIC))
                                            WHEN order_discount_amount >0
                                                THEN order_item_total_price_net_amt
                                                - (round((order_discount_amount * (order_item_total_price_gross_amt / sum(order_item_total_price_gross_amt) OVER (PARTITION BY order_id)::NUMERIC) / tax_rate)))
--                                                 - (COALESCE(refund_net_amount,0) * ((order_item_total_price_net_amt / sum(order_item_total_price_net_amt) OVER (PARTITION BY order_id)::NUMERIC)::NUMERIC))
                                            ELSE order_item_total_price_net_amt
--                                                 - (COALESCE(refund_net_amount,0) * ((order_item_total_price_net_amt / sum(order_item_total_price_net_amt) OVER (PARTITION BY order_id)::NUMERIC)::NUMERIC))
                                           END) /12
                               WHEN is_mrr_relevant = TRUE AND monthly_payment = TRUE THEN
                                   (CASE
                                        WHEN order_discount_percentage >0
                                            THEN
                                                order_item_total_price_net_amt
                                                * (1-order_discount_percentage)
--                                                 - (COALESCE(refund_net_amount,0) * ((order_item_total_price_net_amt / sum(order_item_total_price_net_amt) OVER (PARTITION BY order_id)::NUMERIC)::NUMERIC))
                                        WHEN order_discount_amount >0
                                            THEN order_item_total_price_net_amt
                                            - (round((order_discount_amount * (order_item_total_price_net_amt / sum(order_item_total_price_net_amt) OVER (PARTITION BY order_id)::NUMERIC) / tax_rate)))
--                                             - (COALESCE(refund_net_amount,0) * ((order_item_total_price_net_amt / sum(order_item_total_price_net_amt) OVER (PARTITION BY order_id)::NUMERIC)::NUMERIC))
                                        ELSE order_item_total_price_net_amt
--                                             - (COALESCE(refund_net_amount,0) * ((order_item_total_price_net_amt / sum(order_item_total_price_net_amt) OVER (PARTITION BY order_id)::NUMERIC)::NUMERIC))
                                       END)
                              END /100,2),0) AS mrr_eur

         , coalesce(round(CASE WHEN is_mrr_relevant = TRUE AND monthly_payment = FALSE THEN
                                   (CASE
                                        WHEN order_discount_percentage >0
                                            THEN coalesce(refund_net_amount, 0) * ((order_item_total_price_net_amt / sum(order_item_total_price_net_amt) OVER (PARTITION BY order_id)::numeric)::numeric)
                                        WHEN order_discount_amount >0
                                            THEN coalesce(refund_net_amount, 0) * ((order_item_total_price_net_amt / sum(order_item_total_price_net_amt) OVER (PARTITION BY order_id)::numeric)::numeric)
                                        ELSE coalesce(refund_net_amount, 0) * ((order_item_total_price_net_amt / sum(order_item_total_price_net_amt) OVER (PARTITION BY order_id)::numeric)::numeric)
                                        END /12)
                               WHEN is_mrr_relevant = TRUE AND monthly_payment = TRUE THEN
                                   (CASE
                                        WHEN order_discount_percentage >0
                                            THEN coalesce(refund_net_amount, 0) * ((order_item_total_price_net_amt / sum(order_item_total_price_net_amt) OVER (PARTITION BY order_id)::numeric)::numeric)
                                        WHEN order_discount_amount >0
                                            THEN coalesce(refund_net_amount, 0) * ((order_item_total_price_net_amt / sum(order_item_total_price_net_amt) OVER (PARTITION BY order_id)::numeric)::numeric)
                                        ELSE coalesce(refund_net_amount, 0) * ((order_item_total_price_net_amt / sum(order_item_total_price_net_amt) OVER (PARTITION BY order_id)::numeric)::numeric)
                                       END)
                              END /100,2),0) AS refund_mrr_eur
    FROM cte_invoice_raw t
             LEFT JOIN dwh_main.dim_principal p
                       ON t.creator_urn = p.principal_uuid
)
SELECT 'SH'                                     AS source_system
     , coalesce(invoice_id, 'o-'|| order_id )   AS invoice_id
     , contract_id								AS subscription_id
     , customer_id
     , order_created_dt                         AS invoice_created_dt
     , invoice_creator_email
     , invoice_status
     , NULL                                     AS voided_ts
     , order_provisioning_start_dt              AS invoice_provisioning_start_dt
     , order_provisioning_end_dt                AS invoice_provisioning_end_dt
     , CASE
           WHEN order_created_dt = first_order_created_dt
               THEN TRUE
           ELSE FALSE
    END AS is_customer_first_invoice
     , is_new_invoice
     , FALSE AS expansion_flag
     , coupon_code
     , CASE
           WHEN order_discount_percentage >0
               THEN 'percentage'
           WHEN order_discount_amount >0
               THEN 'amount'
    END AS discount_type
     , CASE
           WHEN order_discount_percentage >0
               THEN order_discount_percentage
           WHEN order_discount_amount >0
               THEN round(
                       (order_discount_amount * (order_item_total_price_gross_amt / NULLIF(sum(order_item_total_price_gross_amt) OVER (PARTITION BY order_id),0)::NUMERIC) / tax_rate)
                       /100, 2)
    END::DECIMAL(10, 2) AS discount_value -- in case of amount: NET discount amount per item (example: invoice R14767314)
     , coalesce(
        CASE
            WHEN order_discount_percentage >0
                THEN order_discount_percentage
            WHEN order_discount_amount >0
                THEN calculated_order_discount_percentage
            END,0)::DECIMAL(10, 2) 					AS discount_percentage
     , order_has_refund                         AS refund_flag
     , order_has_full_refund                    AS full_refund_flag
--      , CASE WHEN flag_renewal_tmp = TRUE
--                 AND order_has_full_refund = FALSE
--                 AND (
--                          (right(product_length_and_payment, 14) = 'yearly payment' AND MIN(order_provisioning_start_mth) OVER (PARTITION BY order_id) = order_provisioning_start_mth) -- for annual payments take the 1st duration month of each of each renewed order
--                          OR (product_length_and_payment = '12 mths, monthly payment' AND (extract(year from age(order_provisioning_start_mth, first_mthly_payment_order_mth)) * 12 + extract(month from age(order_provisioning_start_mth, first_mthly_payment_order_mth)))::INT % 12 = 0 )
--                          OR (product_length_and_payment = '36 mths, monthly payment' AND (extract(year from age(order_provisioning_start_mth, first_mthly_payment_order_mth)) * 12 + extract(month from age(order_provisioning_start_mth, first_mthly_payment_order_mth)))::INT % 36 = 0 )
--                      ) THEN TRUE ELSE FALSE END AS renewal_flag
     , product_uuid::varchar                    AS entity_id
     , 'plan' 								    AS entity_type
     , product_name
     , product_family                           AS product_group
     , coalesce(product_length_and_payment, 'one-time')               AS product_terms
     , 'EUR'                                    AS currency_code
     , 1::DECIMAL(10,2)                         AS exchange_rate
     , coalesce(mrr_eur,0)::DECIMAL(10,2) 		AS mrr_local_ccy
     , coalesce(
        CASE
            WHEN order_has_full_refund
                THEN mrr_eur
            ELSE refund_mrr_eur
            END,0)::DECIMAL(10,2) 			  AS refund_mrr_local_ccy
     , coalesce(mrr_eur,0)::DECIMAL(10,2) 	  AS mrr_eur
     , coalesce(
        CASE
            WHEN order_has_full_refund
                THEN mrr_eur
            ELSE refund_mrr_eur
            END,0)::DECIMAL(10,2) 	        AS refund_mrr_eur
     , order_quantity                       AS item_quantity
     , coalesce(
        CASE
            WHEN order_has_full_refund
                THEN order_quantity
            ELSE order_quantity * (refund_mrr_eur / nullif(mrr_eur,0))::numeric
            END,0)::DECIMAL(10,2) 	            AS refund_item_quantity
FROM cte_revenue_1 c1
-- WHERE customer_id = '62315493'
;