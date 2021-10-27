WITH cte_tax_advisors AS (
    -- coupon_code / coupon_category
    SELECT customer_id
        , TRUE AS is_tax_advisor
    FROM data_mart_internal_reporting.ir_sh_cb_invoice_line
    WHERE coupon_code LIKE 'datev\_stb%'
       OR coupon_code LIKE 'dstv\_stb%'
       OR coupon_code LIKE 'ecovisstb%'
       OR coupon_code LIKE 'etl\_stb%'
       OR coupon_code LIKE 'hsp\_stb%'
       OR coupon_code LIKE 'wst-stb%'
       OR coupon_code LIKE 'lohnkonzept\_stb%'
       OR coupon_code LIKE 'lohnsteuer\_stb%'
       OR coupon_code LIKE 'dstrauto%'
       OR coupon_code LIKE 'datevauto%'
       OR coupon_code LIKE '%skg_15%'

    UNION
    -- customer_contact_company_name or customer_contact_email
    SELECT customer_id
        , TRUE AS is_tax_advisor
    FROM data_mart_internal_reporting.ir_sh_cb_customer
    WHERE lower(customer_contact_company_name) LIKE '%stb%'
       OR lower(customer_contact_company_name) LIKE '%steuer%'
       OR lower(customer_contact_company_name) LIKE '%treuhand%'
       OR customer_contact_company_name LIKE '%ETL%'
       OR customer_contact_company_name LIKE '%HSP%'
       OR lower(customer_contact_company_name) LIKE '%ecovis%'
       OR lower(customer_contact_company_name) LIKE '%tax %'
       OR lower(customer_contact_company_name) LIKE '%tax-%'
       OR customer_contact_email LIKE '%stb%'
       OR customer_contact_email LIKE '%steuer%'
       OR customer_contact_email LIKE '%treuhand%'
       OR customer_contact_email LIKE '%@etl%'
       OR customer_contact_email LIKE '%hsp%'
       OR customer_contact_email LIKE '%ecovis%'
       OR customer_contact_email LIKE '%tax.%'
       OR customer_contact_email LIKE '%tax-%'
       OR customer_contact_email LIKE '%bbjmail%')
        ,
     cte_tax_advisor_client AS (
         SELECT customer_id
              , TRUE AS is_tax_advisor
         FROM data_mart_internal_reporting.ir_sh_cb_invoice_line
         WHERE coupon_code LIKE 'datev\_m%'
            OR coupon_code LIKE 'm\_datev\_%'
            OR coupon_code LIKE 'datev\_ad\_%'
            OR coupon_code LIKE 'dstv\_m\_%'
            OR coupon_code LIKE 'dstv\_man\_%'
            OR coupon_code LIKE 'dstvman\_%'
            OR coupon_code LIKE 'm\_dstv\_%'
            OR coupon_code LIKE 'ecovis\_m\_%'
            OR coupon_code LIKE 'etl\_man\_%'
            OR coupon_code LIKE 'hsp15\_man\_%'
            OR coupon_code LIKE 'wst15\_man\_%'
            OR coupon_code LIKE 'lohnkonzept\_m\_%'
            OR coupon_code LIKE 'lohnsteuer\_man\_%'
            OR coupon_code LIKE 'datev\_man\_cc\_%'
            OR coupon_code LIKE 'dstv\_man\_cc\_%'
            OR coupon_code LIKE 'hsp\_man\_cc\_%'
            OR coupon_code LIKE 'stpro_%'
            OR coupon_code LIKE 'bvbc%'
     )
, final AS (SELECT DISTINCT ta.customer_id, TRUE AS customer_is_tax_advisor, FALSE AS customer_is_tax_advisor_client
            FROM cte_tax_advisors ta

            UNION

            SELECT DISTINCT tac.customer_id, FALSE AS customer_is_tax_advisor, TRUE AS customer_is_tax_advisor_client
            FROM cte_tax_advisor_client tac
            WHERE NOT EXISTS(SELECT 1 FROM cte_tax_advisors ta WHERE tac.customer_id = ta.customer_id))
SELECT
customer_id, customer_is_tax_advisor, customer_is_tax_advisor_client
FROM final
ORDER BY customer_id
;