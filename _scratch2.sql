-- Shop migration 2022-07-19
WITH price_increase AS (
    SELECT ctr.customer_id
         , MAX((timestamp AT TIME ZONE 'Europe/Berlin'))::DATE AS max_contract_modified_dt_cet
    FROM event e
             JOIN contract ctr
                  ON ctr.id = split_part(e.subject,'/',2)::UUID
    WHERE e.type = 'ContractItemsModified'
      AND e.actor = 'urn:vimcar:com.vimcar:user/b59b3dca-c107-43e7-98a4-5910e32de133'  -- or Fabiano
      AND e.timestamp >= '2021-09-01'
    GROUP BY ctr.customer_id
)
   , recent_payment AS (
    SELECT
        domain
         , provider
         , payment_method
    FROM (SELECT p.domain
               , row_number() OVER (PARTITION BY p.domain ORDER BY p.authorized DESC) AS row_nbr
               , p.payment_method ->> 'type' AS payment_method
               , p.provider
          FROM public.payment p
          WHERE status = 'authorised') recent_payments
    WHERE row_nbr = 1
)
   , customer_status_product AS (
    SELECT
        cc.outbound_id AS customer_id
         , cc.id AS customer_uuid
         , ctr.customer_status
         , min(ci.provisioning_start AT TIME ZONE 'Europe/Berlin') AS min_provisioning_start_ts_cet
         , max(ci.provisioning_end AT TIME ZONE 'Europe/Berlin') AS max_provisioning_end_ts_cet
         , max(pi.max_contract_modified_dt_cet) AS price_increase_max_contract_modified_dt_cet
         , string_agg(DISTINCT product_family,', ') AS shop_product_types_ever
         , string_agg(DISTINCT coalesce(cb_addon_id, pmap.cb_plan_id),', ') AS cb_plan_ids_ever
         , string_agg(DISTINCT cbmap.cb_plan_id_map_nbr::TEXT,', ') AS cb_plan_id_map_nbr
         , coalesce(bool_or(CASE WHEN cbmap.cb_plan_id_map_nbr = 0 THEN TRUE END), FALSE) AS has_unmapped_product
         , coalesce(bool_or(
                                CASE
                                    WHEN co.discount ->> 'type' = 'amount'
                                        AND (co.discount -> 'value' ->> 'amount')::numeric > 0
                                        THEN (co.discount -> 'value' ->> 'amount')
                                    WHEN co.discount ->> 'type' = 'amount'
                                        AND (ci.discount_amount ->> 'amount')::numeric > 0
                                        THEN (ci.discount_amount ->> 'amount')
                                    WHEN co.discount ->> 'type' = 'percentage'
                                        THEN (co.discount ->> 'value')
                                    END::numeric >0), FALSE) AS has_discount  -- checking both the invoices and orders, sometimes invoice/coupon is visible in order only
         , bool_or(coalesce(ci.coupon_code, co.coupon_code) IS NOT NULL) AS has_coupon
         , coalesce(bool_or(CASE WHEN pmap.cb_plan_id IN ('logbook_b2c-from_rent2buy-eur', 'logbook_b2c-one_time_buy-eur') THEN TRUE END), FALSE) AS has_lifetime_product
         , coalesce(bool_or(CASE WHEN pmap.cb_plan_id LIKE '%3_years%' THEN TRUE END), FALSE) AS has_3y_product
         , bool_or(is_old_price) AS has_product_with_old_price
         , CASE
               WHEN rp.payment_method IN ('invoice', 'paypal')
                   THEN 'Invoice or Paypal'
               WHEN rp.payment_method IN ('card', 'sepa')
                   THEN 'Adyen (CC or SEPA)'
               ELSE 'unknown'
        END AS payment_method --payment method (card, sepa, invoice, paypal
         , CASE WHEN m.customer_id IS NOT NULL THEN TRUE ELSE FALSE END customer_is_migrated
    FROM public.shop_extraction_order_invoice ci
             JOIN public.order co
                  ON co.id = ci.order_id
             JOIN public.customer cc
                  ON cc.id = ci.customer_id
             LEFT JOIN recent_payment rp
                       ON rp.domain = cc.domain
             LEFT JOIN price_increase pi
                       ON pi.customer_id = ci.customer_id
             LEFT JOIN (SELECT DISTINCT customer_id FROM shop_extraction_batch WHERE set_is_migrated = true) m
                       ON m.customer_id = cc.outbound_id
             JOIN (SELECT customer_id
                        , MIN(status) AS customer_status
                   FROM public.contract
                   GROUP BY customer_id) ctr
                  ON ctr.customer_id = cc.id,
         LATERAL (SELECT (obj.value ->> 'product_id') AS product_id
                  FROM jsonb_array_elements(ci.items_added) obj(value)) items_added
             JOIN mapping.umd_product_map pmap
                  ON pmap.product_uuid = CASE items_added.product_id WHEN 'fake' THEN '00000000-0000-0000-0000-000000000000' ELSE items_added.product_id END::UUID
             LEFT JOIN public.shop_extraction_cb_plan_id_map cbmap
                       ON cbmap.cb_plan_id = pmap.cb_plan_id
--     WHERE cc.outbound_id = 'K48855744'
    GROUP BY cc.outbound_id, cc.id, ctr.customer_status, rp.payment_method, m.customer_id
)
--    SELECT * FROM customer_status_product;
   , batch_conditions AS (
    SELECT
        customer_id
         , customer_uuid
         , customer_status
--          , CASE
--                WHEN customer_status = 'active'
--                    AND max_provisioning_end_ts_cet < now() - INTERVAL '3 days'
--                    THEN 'canceled'
--                ELSE customer_status
--         END AS customer_status
         , CASE
               WHEN shop_product_types_ever LIKE '%Logbook B2B%'
                   OR shop_product_types_ever LIKE '%Pro%'
                   OR shop_product_types_ever LIKE '%Admin%'
                   OR shop_product_types_ever LIKE '%API%'
                   OR shop_product_types_ever LIKE '%Geo%'
                   OR shop_product_types_ever LIKE '%Anti Theft%'
                   OR shop_product_types_ever LIKE '%Driver%'
                   OR shop_product_types_ever LIKE '%Damage%'
                   OR shop_product_types_ever LIKE '%FSK%'
                   OR shop_product_types_ever LIKE '%LapID%'
                   OR shop_product_types_ever LIKE '%Share%'
                   OR shop_product_types_ever LIKE '%damage%'
                   OR shop_product_types_ever LIKE '%b2b%'
                   THEN 'B2B'
               ELSE 'B2C'
        END AS customer_type
         , shop_product_types_ever
         , cb_plan_ids_ever
         , array_length(string_to_array(cb_plan_ids_ever, ','), 1) AS number_of_plans_ever
         , array_length(string_to_array(cb_plan_id_map_nbr, ','), 1) AS number_of_plans_mapped_ever
         , has_unmapped_product
         , has_product_with_old_price
         , has_discount
         , has_coupon
         , has_lifetime_product
         , payment_method
         , has_3y_product
         , min_provisioning_start_ts_cet::DATE AS min_provisioning_start_ts_cet
         , max_provisioning_end_ts_cet::DATE AS max_provisioning_end_dt_cet
         , CASE WHEN max_provisioning_end_ts_cet::DATE - current_date BETWEEN 0 AND 14 THEN TRUE ELSE FALSE END AS next_invoice_within_n_days
         , price_increase_max_contract_modified_dt_cet
         , CASE
               WHEN current_date - max_provisioning_end_ts_cet::DATE > 30
                   AND customer_status = 'Active'
                   AND max_provisioning_end_ts_cet::DATE - price_increase_max_contract_modified_dt_cet BETWEEN 0 AND 32 THEN TRUE ELSE FALSE
        END AS price_increase_update_risk
         , customer_is_migrated
    FROM customer_status_product
       WHERE customer_is_migrated = FALSE
)
-- SELECT * FROM batch_conditions;
--    SELECT * FROM batch_conditions WHERE price_increase_update_risk = TRUE;
-- SELECT * FROM batch_conditions WHERE has_product_with_old_price = FALSE AND customer_is_migrated = FALSE and date_trunc('year',min_provisioning_start_ts_cet) = '2019-01-01 00:00:00.000000 +00:00';
-- SELECT date_trunc('year',min_provisioning_start_ts_cet), count(*) FROM batch_conditions WHERE has_product_with_old_price = FALSE AND customer_is_migrated = FALSE GROUP BY 1;
   , cte_special_batch_f1 AS (
    SELECT
        'F1: customers who purchased only the products with new prices and started in 2021-2022' AS batch_definition
         , customer_id
         , customer_uuid
    FROM batch_conditions
    WHERE (has_product_with_old_price = FALSE
--         AND customer_is_migrated = FALSE
        AND min_provisioning_start_ts_cet >= '2021-01-01')
       OR customer_id IN ('97832597',
                          '33950110',
                          '64915890',
                          '93880402',
                          '86161208',
                          '90783258',
                          '97019118',
                          '58165424',
                          '52713746',
                          '48454834',
                          '26826136',
                          '47524379',
                          '55110977',
                          '35852394',
                          '91450362',
                          '56795090',
                          'K66805501',
                          'K83920713',
                          'K16559822',
                          'K61360673',
                          'K49307855',
                          'K86275930',
                          'K14252492',
                          'K84012740',
                          'K72386302',
                          'K91544902',
                          'K71491214',
                          'K14654204',
                          'K51835435',
                          'K56984687',
                          'K44917399',
                          'K40305278',
                          'K70751035',
                          'K54639106',
                          'K65730746',
                          'K19153264',
                          'K28256483',
                          'K14042076',
                          'K95826372')
)
   , cte_special_batch_g1 AS (
    SELECT
        'G1: sample customers who experienced the price increase action' AS batch_definition
         , customer_id
         , customer_uuid
    FROM batch_conditions
    WHERE customer_id IN ('K48062619', -- B2B
                          'K86580169', -- B2B
                          '57232917', -- B2B
                          'K96877660', -- B2B
                          'K34790601', -- B2B
                          'K40873533', -- B2B, had logbook B2C which we replaced by.B2B eventually
                          'K70078345', -- B2B
                          'K10607246', -- B2B
                          'K76788985', -- B2B
                          'K51696968', -- B2B, interesting case with Pro upgrade
                          'K55757192',
                          '77722383',
                          'K97722414',
                          'K66158243',
                          'K82911856',
                          'K89329176',
                          '14826200',
                          'K53535038',
                          'K93641801',
                          'K95561653',
                          'K37940883',
                          'K74168743',
                          'K70700674',
                          'K75638883',
                          'K41922721',
                          'K20649237',
                          'K42279452',
                          'K54291243',
                          'K17704987',
                          '27788179',
                          'K56793645',
                          '74228249',
                          '94620003',
                          'K73649445',
                          'K43772983',
                          'K92631283',
                          '76892354',
                          'K83609083',
                          'K99296264',
                          '13370479',
                          'K79080031',
                          'K74898205',
                          'K44633393',
                          'K20942585',
                          '45680946',
                          'K77422498',
                          'K63915570',
                          'K46503723',
                          'K29871803',
                          '46264448')
)
--    SELECT * FROM cte_special_batch_f1;
   , migration_customers AS (
    SELECT * FROM cte_special_batch_f1
    UNION ALL
    SELECT * FROM cte_special_batch_g1
    UNION ALL

    SELECT
        'A1: status:terminated, plans:1, discounts:no, payment_method:no_restrictions, lifetime_product:no' AS batch_definition
         , customer_id
         , customer_uuid
    FROM batch_conditions
    WHERE TRUE
      AND customer_status = 'terminated'
      AND number_of_plans_ever = 1
      AND has_discount = FALSE
      AND has_lifetime_product = FALSE
      AND NOT exists(SELECT 1 FROM cte_special_batch_f1 WHERE batch_conditions.customer_uuid = cte_special_batch_f1.customer_uuid)
      AND NOT exists(SELECT 1 FROM cte_special_batch_g1 WHERE batch_conditions.customer_uuid = cte_special_batch_g1.customer_uuid)

    UNION ALL

    SELECT
        'B1: status:terminated, plans:1, discounts:yes, payment_method:no_restrictions, lifetime_product:no' AS batch_definition
         , customer_id
         , customer_uuid
    FROM batch_conditions
    WHERE TRUE
      AND customer_status = 'terminated'
      AND number_of_plans_ever = 1
      AND has_discount = TRUE
      AND has_lifetime_product = FALSE
      AND NOT exists(SELECT 1 FROM cte_special_batch_f1 WHERE batch_conditions.customer_uuid = cte_special_batch_f1.customer_uuid)
      AND NOT exists(SELECT 1 FROM cte_special_batch_g1 WHERE batch_conditions.customer_uuid = cte_special_batch_g1.customer_uuid)

    UNION ALL

    SELECT
        'B2: status:terminated, plans:2+, discounts:no, payment_method:no_restrictions, lifetime_product:no' AS batch_definition
         , customer_id
         , customer_uuid
    FROM batch_conditions
    WHERE TRUE
      AND customer_status = 'terminated'
      AND number_of_plans_ever > 1
      AND has_discount = FALSE
      AND has_lifetime_product = FALSE
      AND NOT exists(SELECT 1 FROM cte_special_batch_f1 WHERE batch_conditions.customer_uuid = cte_special_batch_f1.customer_uuid)
      AND NOT exists(SELECT 1 FROM cte_special_batch_g1 WHERE batch_conditions.customer_uuid = cte_special_batch_g1.customer_uuid)

    UNION ALL

    SELECT
        'B3: status:terminated, plans:2+, discounts:yes, payment_method:no_restrictions, lifetime_product:no' AS batch_definition
         , customer_id
         , customer_uuid
    FROM batch_conditions
    WHERE TRUE
      AND customer_status = 'terminated'
      AND number_of_plans_ever > 1
      AND has_discount = TRUE
      AND has_lifetime_product = FALSE
      AND NOT exists(SELECT 1 FROM cte_special_batch_f1 WHERE batch_conditions.customer_uuid = cte_special_batch_f1.customer_uuid)
      AND NOT exists(SELECT 1 FROM cte_special_batch_g1 WHERE batch_conditions.customer_uuid = cte_special_batch_g1.customer_uuid)

    UNION ALL

    SELECT
        'C1: status:canceled, plans:1, discounts:no, payment_method:no_restrictions, lifetime_product:no' AS batch_definition
         , customer_id
         , customer_uuid
    FROM batch_conditions
    WHERE TRUE
      AND customer_status = 'canceled'
      AND number_of_plans_ever = 1
      AND has_discount = FALSE
      AND has_lifetime_product = FALSE
      AND NOT exists(SELECT 1 FROM cte_special_batch_f1 WHERE batch_conditions.customer_uuid = cte_special_batch_f1.customer_uuid)
      AND NOT exists(SELECT 1 FROM cte_special_batch_g1 WHERE batch_conditions.customer_uuid = cte_special_batch_g1.customer_uuid)

    UNION ALL

    SELECT
        'C2: status:canceled, plans:1, discounts:yes, payment_method:no_restrictions, lifetime_product:no' AS batch_definition
         , customer_id
         , customer_uuid
    FROM batch_conditions
    WHERE TRUE
      AND customer_status = 'canceled'
      AND number_of_plans_ever = 1
      AND has_discount = TRUE
      AND has_lifetime_product = FALSE
      AND NOT exists(SELECT 1 FROM cte_special_batch_f1 WHERE batch_conditions.customer_uuid = cte_special_batch_f1.customer_uuid)
      AND NOT exists(SELECT 1 FROM cte_special_batch_g1 WHERE batch_conditions.customer_uuid = cte_special_batch_g1.customer_uuid)

    UNION ALL

    SELECT
        'C3: status:canceled, plans:2+, discounts:no, payment_method:no_restrictions, lifetime_product:no' AS batch_definition
         , customer_id
         , customer_uuid
    FROM batch_conditions
    WHERE TRUE
      AND customer_status = 'canceled'
      AND number_of_plans_ever > 1
      AND has_discount = FALSE
      AND has_lifetime_product = FALSE
      AND NOT exists(SELECT 1 FROM cte_special_batch_f1 WHERE batch_conditions.customer_uuid = cte_special_batch_f1.customer_uuid)
      AND NOT exists(SELECT 1 FROM cte_special_batch_g1 WHERE batch_conditions.customer_uuid = cte_special_batch_g1.customer_uuid)

    UNION ALL

    SELECT
        'C4: status:canceled, plans:2+, discounts:yes, payment_method:no_restrictions, lifetime_product:no' AS batch_definition
         , customer_id
         , customer_uuid
    FROM batch_conditions
    WHERE TRUE
      AND customer_status = 'canceled'
      AND number_of_plans_ever > 1
      AND has_discount = TRUE
      AND has_lifetime_product = FALSE
      AND NOT exists(SELECT 1 FROM cte_special_batch_f1 WHERE batch_conditions.customer_uuid = cte_special_batch_f1.customer_uuid)
      AND NOT exists(SELECT 1 FROM cte_special_batch_g1 WHERE batch_conditions.customer_uuid = cte_special_batch_g1.customer_uuid)

    UNION ALL

    SELECT
        'D1: status:terminated, plans:1, discounts:no, payment_method:no_restrictions, lifetime_product:yes' AS batch_definition
         , customer_id
         , customer_uuid
    FROM batch_conditions
    WHERE TRUE
      AND customer_status = 'terminated'
      AND number_of_plans_ever = 1
      AND has_discount = FALSE
      AND has_lifetime_product = TRUE
      AND NOT exists(SELECT 1 FROM cte_special_batch_f1 WHERE batch_conditions.customer_uuid = cte_special_batch_f1.customer_uuid)
      AND NOT exists(SELECT 1 FROM cte_special_batch_g1 WHERE batch_conditions.customer_uuid = cte_special_batch_g1.customer_uuid)

    UNION ALL

    SELECT
        'D2: status:terminated, plans:1, discounts:yes, payment_method:no_restrictions, lifetime_product:yes' AS batch_definition
         , customer_id
         , customer_uuid
    FROM batch_conditions
    WHERE TRUE
      AND customer_status = 'terminated'
      AND number_of_plans_ever = 1
      AND has_discount = TRUE
      AND has_lifetime_product = TRUE
      AND NOT exists(SELECT 1 FROM cte_special_batch_f1 WHERE batch_conditions.customer_uuid = cte_special_batch_f1.customer_uuid)
      AND NOT exists(SELECT 1 FROM cte_special_batch_g1 WHERE batch_conditions.customer_uuid = cte_special_batch_g1.customer_uuid)

    UNION ALL

    SELECT
        'D3: status:terminated, plans:2+, discounts:no, payment_method:no_restrictions, lifetime_product:yes' AS batch_definition
         , customer_id
         , customer_uuid
    FROM batch_conditions
    WHERE TRUE
      AND customer_status = 'terminated'
      AND number_of_plans_ever > 1
      AND has_discount = FALSE
      AND has_lifetime_product = TRUE
      AND NOT exists(SELECT 1 FROM cte_special_batch_f1 WHERE batch_conditions.customer_uuid = cte_special_batch_f1.customer_uuid)
      AND NOT exists(SELECT 1 FROM cte_special_batch_g1 WHERE batch_conditions.customer_uuid = cte_special_batch_g1.customer_uuid)

    UNION ALL

    SELECT
        'D4: status:terminated, plans:2+, discounts:yes, payment_method:no_restrictions, lifetime_product:yes' AS batch_definition
         , customer_id
         , customer_uuid
    FROM batch_conditions
    WHERE TRUE
      AND customer_status = 'terminated'
      AND number_of_plans_ever > 1
      AND has_discount = TRUE
      AND has_lifetime_product = TRUE
      AND NOT exists(SELECT 1 FROM cte_special_batch_f1 WHERE batch_conditions.customer_uuid = cte_special_batch_f1.customer_uuid)
      AND NOT exists(SELECT 1 FROM cte_special_batch_g1 WHERE batch_conditions.customer_uuid = cte_special_batch_g1.customer_uuid)

    UNION ALL

    SELECT
        'D5: status:canceled, plans:1, discounts:no, payment_method:no_restrictions, lifetime_product:yes' AS batch_definition
         , customer_id
         , customer_uuid
    FROM batch_conditions
    WHERE TRUE
      AND customer_status = 'canceled'
      AND number_of_plans_ever = 1
      AND has_discount = FALSE
      AND has_lifetime_product = TRUE
      AND NOT exists(SELECT 1 FROM cte_special_batch_f1 WHERE batch_conditions.customer_uuid = cte_special_batch_f1.customer_uuid)
      AND NOT exists(SELECT 1 FROM cte_special_batch_g1 WHERE batch_conditions.customer_uuid = cte_special_batch_g1.customer_uuid)

    UNION ALL

    SELECT
        'D6: status:canceled, plans:1, discounts:yes, payment_method:no_restrictions, lifetime_product:yes' AS batch_definition
         , customer_id
         , customer_uuid
    FROM batch_conditions
    WHERE TRUE
      AND customer_status = 'canceled'
      AND number_of_plans_ever = 1
      AND has_discount = TRUE
      AND has_lifetime_product = TRUE
      AND NOT exists(SELECT 1 FROM cte_special_batch_f1 WHERE batch_conditions.customer_uuid = cte_special_batch_f1.customer_uuid)
      AND NOT exists(SELECT 1 FROM cte_special_batch_g1 WHERE batch_conditions.customer_uuid = cte_special_batch_g1.customer_uuid)

    UNION ALL

    SELECT
        'D7: status:canceled, plans:2+, discounts:no, payment_method:no_restrictions, lifetime_product:yes' AS batch_definition
         , customer_id
         , customer_uuid
    FROM batch_conditions
    WHERE TRUE
      AND customer_status = 'canceled'
      AND number_of_plans_ever > 1
      AND has_discount = FALSE
      AND has_lifetime_product = TRUE
      AND NOT exists(SELECT 1 FROM cte_special_batch_f1 WHERE batch_conditions.customer_uuid = cte_special_batch_f1.customer_uuid)
      AND NOT exists(SELECT 1 FROM cte_special_batch_g1 WHERE batch_conditions.customer_uuid = cte_special_batch_g1.customer_uuid)

    UNION ALL

    SELECT
        'D8: status:canceled, plans:2+, discounts:yes, payment_method:no_restrictions, lifetime_product:yes' AS batch_definition
         , customer_id
         , customer_uuid
    FROM batch_conditions
    WHERE TRUE
      AND customer_status = 'canceled'
      AND number_of_plans_ever > 1
      AND has_discount = TRUE
      AND has_lifetime_product = TRUE
      AND NOT exists(SELECT 1 FROM cte_special_batch_f1 WHERE batch_conditions.customer_uuid = cte_special_batch_f1.customer_uuid)
      AND NOT exists(SELECT 1 FROM cte_special_batch_g1 WHERE batch_conditions.customer_uuid = cte_special_batch_g1.customer_uuid)

    UNION ALL
------ Active 3 years:
    SELECT
        'E1: status:active, plans:1, discounts:no_restrictions, payment_method:Adyen (CC or SEPA), lifetime_product:no_restrictions, 3-years_product:true' AS batch_definition
         , customer_id
         , customer_uuid
    FROM batch_conditions
    WHERE TRUE
      AND customer_status = 'active'
      AND payment_method = 'Adyen (CC or SEPA)'
      AND number_of_plans_ever = 1
      AND has_3y_product = TRUE
      AND NOT exists(SELECT 1 FROM cte_special_batch_f1 WHERE batch_conditions.customer_uuid = cte_special_batch_f1.customer_uuid)
      AND NOT exists(SELECT 1 FROM cte_special_batch_g1 WHERE batch_conditions.customer_uuid = cte_special_batch_g1.customer_uuid)

    UNION ALL

    SELECT
        'E2: status:active, plans:1, discounts:no_restrictions, payment_method:Invoice or Paypal, lifetime_product:no_restrictions, 3-years_product:true' AS batch_definition
         , customer_id
         , customer_uuid
    FROM batch_conditions
    WHERE TRUE
      AND customer_status = 'active'
      AND payment_method = 'Invoice or Paypal'
      AND number_of_plans_ever = 1
      AND has_3y_product = TRUE
      AND NOT exists(SELECT 1 FROM cte_special_batch_f1 WHERE batch_conditions.customer_uuid = cte_special_batch_f1.customer_uuid)
      AND NOT exists(SELECT 1 FROM cte_special_batch_g1 WHERE batch_conditions.customer_uuid = cte_special_batch_g1.customer_uuid)

    UNION ALL

    SELECT
        'E3: status:active, plans:1, discounts:no_restrictions, payment_method:unknown, lifetime_product:no_restrictions, 3-years_product:true' AS batch_definition
         , customer_id
         , customer_uuid
    FROM batch_conditions
    WHERE TRUE
      AND customer_status = 'active'
      AND payment_method = 'unknown'
      AND number_of_plans_ever = 1
      AND has_3y_product = TRUE
      AND NOT exists(SELECT 1 FROM cte_special_batch_f1 WHERE batch_conditions.customer_uuid = cte_special_batch_f1.customer_uuid)
      AND NOT exists(SELECT 1 FROM cte_special_batch_g1 WHERE batch_conditions.customer_uuid = cte_special_batch_g1.customer_uuid)

    UNION ALL

    SELECT
        'X1: status:active, plans:2+, discounts:no_restrictions, payment_method:Adyen (CC or SEPA), lifetime_product:no_restrictions, 3-years_product:true' AS batch_definition
         , customer_id
         , customer_uuid
    FROM batch_conditions
    WHERE TRUE
      AND customer_status = 'active'
      AND payment_method = 'Adyen (CC or SEPA)'
      AND number_of_plans_ever > 1
      AND has_3y_product = TRUE
      AND NOT exists(SELECT 1 FROM cte_special_batch_f1 WHERE batch_conditions.customer_uuid = cte_special_batch_f1.customer_uuid)
      AND NOT exists(SELECT 1 FROM cte_special_batch_g1 WHERE batch_conditions.customer_uuid = cte_special_batch_g1.customer_uuid)

    UNION ALL

    SELECT
        'X2: status:active, plans:2+, discounts:no_restrictions, payment_method:Invoice or Paypal, lifetime_product:no_restrictions, 3-years_product:true' AS batch_definition
         , customer_id
         , customer_uuid
    FROM batch_conditions
    WHERE TRUE
      AND customer_status = 'active'
      AND payment_method = 'Invoice or Paypal'
      AND number_of_plans_ever > 1
      AND has_3y_product = TRUE
      AND NOT exists(SELECT 1 FROM cte_special_batch_f1 WHERE batch_conditions.customer_uuid = cte_special_batch_f1.customer_uuid)
      AND NOT exists(SELECT 1 FROM cte_special_batch_g1 WHERE batch_conditions.customer_uuid = cte_special_batch_g1.customer_uuid)

    UNION ALL

    SELECT
        'X3: status:active, plans:2+, discounts:no_restrictions, payment_method:unknown, lifetime_product:no_restrictions, 3-years_product:true' AS batch_definition
         , customer_id
         , customer_uuid
    FROM batch_conditions
    WHERE TRUE
      AND customer_status = 'active'
      AND payment_method = 'unknown'
      AND number_of_plans_ever > 1
      AND has_3y_product = TRUE
      AND NOT exists(SELECT 1 FROM cte_special_batch_f1 WHERE batch_conditions.customer_uuid = cte_special_batch_f1.customer_uuid)
      AND NOT exists(SELECT 1 FROM cte_special_batch_g1 WHERE batch_conditions.customer_uuid = cte_special_batch_g1.customer_uuid)

    UNION ALL

    SELECT
        'X4: status:active, plans:no_restrictions, discounts:no, payment_method:Adyen (CC or SEPA), lifetime_product:no_restrictions, 3-years_product:false' AS batch_definition
         , customer_id
         , customer_uuid
    FROM batch_conditions
    WHERE TRUE
      AND customer_status = 'active'
      AND has_discount = FALSE
      AND payment_method = 'Adyen (CC or SEPA)'
      AND has_3y_product = FALSE
--       AND customer_type = 'B2C'
      AND NOT exists(SELECT 1 FROM cte_special_batch_f1 WHERE batch_conditions.customer_uuid = cte_special_batch_f1.customer_uuid)
      AND NOT exists(SELECT 1 FROM cte_special_batch_g1 WHERE batch_conditions.customer_uuid = cte_special_batch_g1.customer_uuid)


    UNION ALL

    SELECT
        'X5: status:active, plans:no_restrictions, discounts:no, payment_method:Invoice or Paypal, lifetime_product:no_restrictions, 3-years_product:false' AS batch_definition
         , customer_id
         , customer_uuid
    FROM batch_conditions
    WHERE TRUE
      AND customer_status = 'active'
      AND has_discount = FALSE
      AND payment_method = 'Invoice or Paypal'
      AND has_3y_product = FALSE
--       AND customer_type = 'B2C'
      AND NOT exists(SELECT 1 FROM cte_special_batch_f1 WHERE batch_conditions.customer_uuid = cte_special_batch_f1.customer_uuid)
      AND NOT exists(SELECT 1 FROM cte_special_batch_g1 WHERE batch_conditions.customer_uuid = cte_special_batch_g1.customer_uuid)

    UNION ALL

    SELECT
        'X6: status:active, plans:no_restrictions, discounts:no, payment_method:unknown, lifetime_product:no_restrictions, 3-years_product:false' AS batch_definition
         , customer_id
         , customer_uuid
    FROM batch_conditions
    WHERE TRUE
      AND customer_status = 'active'
      AND has_discount = FALSE
      AND payment_method = 'unknown'
      AND has_3y_product = FALSE
--       AND customer_type = 'B2C'
      AND NOT exists(SELECT 1 FROM cte_special_batch_f1 WHERE batch_conditions.customer_uuid = cte_special_batch_f1.customer_uuid)
      AND NOT exists(SELECT 1 FROM cte_special_batch_g1 WHERE batch_conditions.customer_uuid = cte_special_batch_g1.customer_uuid)

    UNION ALL

    SELECT
        'X7: status:active, plans:no_restrictions, discounts:yes, payment_method:Adyen (CC or SEPA), lifetime_product:no_restrictions, 3-years_product:false' AS batch_definition
         , customer_id
         , customer_uuid
    FROM batch_conditions
    WHERE TRUE
      AND customer_status = 'active'
      AND has_discount = TRUE
      AND payment_method = 'Adyen (CC or SEPA)'
      AND has_3y_product = FALSE
      AND NOT exists(SELECT 1 FROM cte_special_batch_f1 WHERE batch_conditions.customer_uuid = cte_special_batch_f1.customer_uuid)
      AND NOT exists(SELECT 1 FROM cte_special_batch_g1 WHERE batch_conditions.customer_uuid = cte_special_batch_g1.customer_uuid)


    UNION ALL

    SELECT
        'X8: status:active, plans:no_restrictions, discounts:yes, payment_method:Invoice or Paypal, lifetime_product:no_restrictions, 3-years_product:false' AS batch_definition
         , customer_id
         , customer_uuid
    FROM batch_conditions
    WHERE TRUE
      AND customer_status = 'active'
      AND has_discount = TRUE
      AND payment_method = 'Invoice or Paypal'
      AND has_3y_product = FALSE
      AND NOT exists(SELECT 1 FROM cte_special_batch_f1 WHERE batch_conditions.customer_uuid = cte_special_batch_f1.customer_uuid)
      AND NOT exists(SELECT 1 FROM cte_special_batch_g1 WHERE batch_conditions.customer_uuid = cte_special_batch_g1.customer_uuid)

    UNION ALL

    SELECT
        'X9: status:active, plans:no_restrictions, discounts:yes, payment_method:unknown, lifetime_product:no_restrictions, 3-years_product:false' AS batch_definition
         , customer_id
         , customer_uuid
    FROM batch_conditions
    WHERE TRUE
      AND customer_status = 'active'
      AND has_discount = TRUE
      AND payment_method = 'unknown'
      AND has_3y_product = FALSE
      AND NOT exists(SELECT 1 FROM cte_special_batch_f1 WHERE batch_conditions.customer_uuid = cte_special_batch_f1.customer_uuid)
      AND NOT exists(SELECT 1 FROM cte_special_batch_g1 WHERE batch_conditions.customer_uuid = cte_special_batch_g1.customer_uuid)
)
   , customer_contract AS (SELECT mc.customer_id
                                , mc.customer_uuid
                                , ctr.id     AS contract_uuid
                                , ctr.domain AS domain_name
                                , mc.batch_definition
                           FROM migration_customers mc
                                    JOIN public.contract ctr ON ctr.customer_id = mc.customer_uuid)
   , sample_batch AS
    (SELECT
         customer_id
          , customer_uuid
          , contract_uuid
          , domain_name
          , 'D9: sample_batch, test od active subs close to renewal' AS batch_definition
          , CASE
                WHEN customer_id = '86915228' THEN 'Renewal on 2022-02-17, discount:no, 1 logbook'
                WHEN customer_id = 'K12747073' THEN 'Renewal on 2022-02-18, discount:yes, 1 logbook'
                WHEN customer_id = 'K99986918' THEN 'Renewal on 2022-02-19, discount:yes, 1 logbook'
                WHEN customer_id = 'K30846175' THEN 'Renewal on 2022-02-21, discount:no, 1 logbook'
            END AS comment
     FROM customer_contract
--           WHERE customer_id IN ('86915228','K12747073','K99986918','K30846175')
     WHERE FALSE --2022-05-16 this time we don't send any sample test customers
    )
--CHECKS:
-- SELECT customer_id, count(DISTINCT batch_definition), string_agg(batch_definition,' | ') FROM migration_customers GROUP BY 1 HAVING count(DISTINCT batch_definition) >1; -- check if any customer falls into more than one batch definition
-- SELECT count(DISTINCT customer_uuid) FROM migration_customers
-- SELECT batch_definition, count(DISTINCT customer_id) FROM migration_customers GROUP BY 1 ;
-- SELECT count(DISTINCT customer_uuid) FROM migration_customers
-- SELECT * FROM must_checks_before_migration  -- customer_contract
   , cte_final AS (
    SELECT * , NULL AS comment FROM customer_contract cc WHERE NOT EXISTS(SELECT 1 FROM sample_batch sb WHERE cc.customer_uuid = sb.customer_uuid) -- AND batch_definition LIKE 'G%'
    UNION ALL
    SELECT * FROM sample_batch
)
-- SELECT * FROM cte_final ORDER BY contract_uuid;

SELECT batch_definition, count(DISTINCT customer_id) FROM migration_customers GROUP BY 1
UNION ALL
SELECT batch_definition, count(DISTINCT customer_id) FROM sample_batch GROUP BY 1

-- SELECT count(DISTINCT customer_id) FROM migration_customers
-- SELECT customer_id, count(DISTINCT batch_definition), string_agg(batch_definition,' | ') FROM migration_customers GROUP BY 1 HAVING count(DISTINCT batch_definition) >1

;--
SELECT * FROM shop_extraction_batch WHERE batch_definition LIKE 'G%'

SELECT batch_definition, count(DISTINCT customer_uuid) FROM shop_extraction_batch WHERE set_is_migrated IS TRUE  GROUP BY 1 ORDER BY 1;

UPDATE shop_extraction_batch SET batch_definition = 'E3: status:active, plans:1, discounts:no_restrictions, payment_method:unknown, lifetime_product:no_restrictions, 3-years_product:true'
WHERE batch_definition = 'E: status:active, plans:1, discounts:no_restrictions, payment_method:unknown, lifetime_product:no_restrictions, 3-years_product:true'



