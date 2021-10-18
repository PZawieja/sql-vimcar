
WITH prod_map_raw AS (
    SELECT
        product_uuid AS original_product_uuid
         , p.product_name AS original_product_name
         , p.product_price_net AS original_product_price_net
         , CASE
               WHEN p.product_price_net= 590 THEN 790
               WHEN p.product_price_net= 2290 THEN 3290
               WHEN p.product_price_net= 3480 THEN 5880
               WHEN p.product_price_net= 8400 THEN 12000
               WHEN p.product_price_net= 12000 THEN 20400
               WHEN p.product_price_net= 19080 THEN 23880
               WHEN p.product_price_net= 19900 THEN 23880
               WHEN p.product_price_net= 27480 THEN 35880
        END AS map_product_price_net
    FROM dwh_main.dim_product p
)
   , prod_map AS (SELECT *
                       , coalesce(map_product_price_net / original_product_price_net::decimal, 1) AS price_increase_factor
                  FROM prod_map_raw)
   , cte_raw AS (
    SELECT
        i.subscription_id AS shop_contract
         , subscription_status
         , c.customer_id
         , i.invoice_id
         , invoice_created_dt
         , invoice_provisioning_start_dt
         , invoice_provisioning_end_dt
         , CASE WHEN i.invoice_created_dt >= '2021-09-18' THEN TRUE ELSE FALSE END AS flag_renewed_with_price_increase
--                                      , current_date - invoice_created_dt AS days_since_last_invoice
         , CASE WHEN c.max_subscription_end_dt >= current_date THEN c.max_subscription_end_dt END AS next_renewal_dt
         , CASE WHEN product_group LIKE 'Logbook%' THEN 'Logbook' ELSE product_group END AS product_group
         , p.original_product_name AS product_name
         , plan_id AS product_uuid
         , p.original_product_price_net AS product_price_net
         , p.price_increase_factor
         , mrr_eur
         , item_quantity
         , round(mrr_eur * price_increase_factor, 2) AS mrr_eur_expected
         , (i2.invoice_total_price_gross_amount / 100::decimal) / count(i2.invoice_id) OVER (PARTITION BY i2.invoice_ID) AS invoice_total_price_gross_amount
         , round((i2.invoice_total_price_gross_amount / count(i2.invoice_id) OVER (PARTITION BY i2.invoice_ID) * price_increase_factor) / 100,2) AS invoice_total_price_gross_amount_expected
    FROM data_mart_internal_reporting.ir_sh_cb_invoice_line i
             JOIN data_mart_internal_reporting.ir_sh_cb_subscription s
                  ON s.subscription_id = i.subscription_id
             JOIN data_mart_internal_reporting.ir_sh_cb_customer c
                  ON c.customer_id = i.customer_id
             JOIN dwh_main.dim_invoice i2
                  ON i.invoice_id = i2.invoice_id
             JOIN prod_map p
                  ON p.original_product_uuid::text = i.plan_id
    WHERE TRUE
      AND product_group <> 'Hardware'
      AND invoice_provisioning_end_dt >= '2021-09-18'
--       AND i.subscription_id IN ('V81183643') -- invoice created for 1 item instead of 10, product with 9 dongles didn't trigger a renewal
--       AND i.subscription_id IN ('V81183643','V17304741')
      AND i.subscription_id = '67383155'
      AND i.subscription_id IN ('V90106427',
                                'V26775883',
                                'V11872676',
                                'V48018446',
                                'V71771715',
                                'V71123281',
                                'V55027367',
                                'V25281037',
                                'V98585126',
                                'V63724433',
                                'V83226035',
                                '13807871',
                                'V17304741',
                                '40740323',
                                'V29049110',
                                'V59148793',
                                'V35390171',
                                'V92263061',
                                'V85518883',
                                'V72730760',
                                'V53965371',
                                'V50058748',
                                'V73084820',
                                'V50173327',
                                'V96462436',
                                'V23045221',
                                '75971679',
                                'V81183643',
                                'V92887544',
                                'V41805471',
                                'V22161864',
                                'V54767685',
                                'V84061509',
                                'V76199329',
                                '17439727',
                                '42213954',
                                'V87681541',
                                '22215828',
                                'V78134153',
                                'V79239471',
                                '36261740',
                                'V67009924',
                                '24201451',
                                '60937338',
                                'V67579277',
                                'V79360254',
                                'V64040705',
                                'V22367063',
                                'V37918057',
                                '13597353',
                                'V78735159',
                                'V85952859',
                                'V61289119',
                                'V57991918',
                                'V81882938',
                                'V38368058',
                                'V16937766',
                                'V38451863',
                                '56471918',
                                'V38905423',
                                '75331173',
                                '74162846',
                                'V46856466',
                                'V53919299',
                                'V81639141',
                                '71575821',
                                'V43286986',
                                'V92036264',
                                'V12400221',
                                '40728495',
                                '87048862',
                                '61264296',
                                'V39880072',
                                'V76533656',
                                'V22574950',
                                '97839195',
                                '85368972',
                                'V15524833',
                                '65449636',
                                'V71914960',
                                '72375463',
                                '63988519',
                                'V91812985',
                                '20215403',
                                'V12127102',
                                '67383155',
                                'V53680829',
                                '38475991',
                                'V16348870',
                                'V63816572',
                                'V20550883',
                                'V86040962',
                                'V43778309',
                                'V98595682',
                                '84426842',
                                'V33268991',
                                'V86808684',
                                '35075243',
                                'V38230762',
                                '17669048',
                                'V32587141',
                                'V67936309',
                                '37547199',
                                'V22102393',
                                'V39367606',
                                'V80423329',
                                '49925949',
                                '52355653',
                                '78784097',
                                'V19413340',
                                'V39679749',
                                '20215099',
                                'V48851535',
                                'V34013576',
                                '57963642',
                                'V28245992',
                                'V48110629',
                                'V99382480',
                                'V47775709',
                                'V62874300',
                                'V73216317',
                                'V17575855',
                                'V34059545',
                                'V91136330',
                                'V15045105',
                                'V13211037',
                                'V49273366',
                                'V49709424',
                                'V41157507',
                                '44608780',
                                'V91376647',
                                'V96082036',
                                'V27080658',
                                'V67283385',
                                'V53466183',
                                'V86288247',
                                'V76958679',
                                'V89193355',
                                'V33075035',
                                'V79783536',
                                'V88591955',
                                'V91692655',
                                '10625891',
                                'V19269741',
                                'V39428698',
                                'V66130049',
                                'V25730317',
                                '16775319',
                                '76733339',
                                '25774373',
                                '48914727',
                                '72614913',
                                '49174958',
                                'V64034207',
                                'V22268852',
                                '93938750',
                                'V87085591',
                                'V13932609',
                                'V58086028',
                                '12305820',
                                'V45747662',
                                'V77211903',
                                'V16205012',
                                'V64370743',
                                'V29748834',
                                'V17605411',
                                'V59167159',
                                'V99584518',
                                'V22826261',
                                '47507899',
                                'V96028311',
                                'V46237336',
                                'V17653349',
                                '73054875',
                                'V18361719',
                                'V18263646',
                                'V81478452',
                                'V22184933',
                                'V52359370',
                                'V55594263',
                                'V49765541')

    ORDER BY i.subscription_id, invoice_provisioning_start_dt DESC
)
-- SELECT * FROM cte_raw;
   , renewal_invoice AS (SELECT DISTINCT shop_contract
                                       , invoice_id AS renewal_invoice_id
                                       , invoice_created_dt AS renewal_invoice_created_dt
                         FROM cte_raw
                         WHERE flag_renewed_with_price_increase = True)
   , cte_1 AS (
    SELECT
        shop_contract
         , subscription_status
         , customer_id
         , next_renewal_dt
         , flag_renewed_with_price_increase
         , product_group
         , sum(mrr_eur) AS mrr_eur
         , sum(mrr_eur_expected) FILTER ( WHERE flag_renewed_with_price_increase = FALSE ) AS mrr_eur_expected
         , sum(invoice_total_price_gross_amount) AS invoice_total_price_gross_amount
         , sum(invoice_total_price_gross_amount_expected) FILTER ( WHERE flag_renewed_with_price_increase = FALSE ) AS invoice_total_price_gross_amount_expected
         , sum(item_quantity) AS item_quantity
    FROM cte_raw
    GROUP BY shop_contract, subscription_status, customer_id, next_renewal_dt, flag_renewed_with_price_increase, product_group)
-- SELECT * FROM cte_1;
   , cte_final AS (
    SELECT before.shop_contract
         , before.subscription_status
         , before.customer_id
         , before.next_renewal_dt
--          , after.invoice_id AS renewal_invoice_id
--          , after.invoice_created_dt AS renewal_invoice_created_dt
         , before.product_group
         , SUM(before.mrr_eur) AS mrr_eur_before
         , after.mrr_eur AS mrr_eur_after
         , SUM(before.mrr_eur_expected) AS mrr_eur_expected
         , after.invoice_total_price_gross_amount
         , SUM(before.invoice_total_price_gross_amount_expected) AS invoice_total_price_gross_amount_expected
         , SUM(before.item_quantity) AS item_quantity_before
         , after.item_quantity AS item_quantity_after
    FROM cte_1 before
             LEFT JOIN (SELECT shop_contract
                             , product_group
                             , invoice_total_price_gross_amount
                             , mrr_eur
                             , item_quantity
                        FROM cte_1 c
                        WHERE flag_renewed_with_price_increase = TRUE
                        GROUP BY shop_contract, product_group, invoice_total_price_gross_amount, mrr_eur, item_quantity) after
                       ON before.shop_contract = after.shop_contract
                           AND before.product_group = after.product_group
    WHERE TRUE
      AND before.flag_renewed_with_price_increase = FALSE
    GROUP BY before.shop_contract, before.subscription_status, before.customer_id, before.next_renewal_dt, before.product_group, after.mrr_eur, after.invoice_total_price_gross_amount, after.item_quantity
    ORDER BY before.shop_contract
)
-- SELECT count(DISTINCT shop_contract) FROM cte_final ;
-- SELECT * FROM cte_final;
SELECT f.shop_contract
     , subscription_status
     , customer_id
     , next_renewal_dt
--      , product_group
     , SUM(mrr_eur_before) AS mrr_eur_before
     , SUM(mrr_eur_after) AS mrr_eur_after
     , SUM(mrr_eur_expected) AS mrr_eur_expected
     , SUM(invoice_total_price_gross_amount) AS invoice_total_price_gross_amount
     , SUM(invoice_total_price_gross_amount_expected) AS invoice_total_price_gross_amount_expected
     , SUM(item_quantity_before) AS item_quantity_before
     , SUM(item_quantity_after) AS item_quantity_after
FROM cte_final f
--     LEFT JOIN (SELECT DISTINCT shop_contract, invoice_id AS renewal_invoice_id, invoice_created_dt AS renewal_invoice_created_dt
--                 FROM cte_raw
--                 WHERE flag_renewed_with_price_increase = True) r
--         ON f.shop_contract = r.shop_contract
-- WHERE shop_contract = 'V11872676'
GROUP BY f.shop_contract, subscription_status, customer_id, next_renewal_dt


;
