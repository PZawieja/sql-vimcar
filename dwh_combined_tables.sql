
/*
1. How to get customers with a currently running Geo subscription?
- Documentation of combined Shop + Chargebee reporting tables: https://www.notion.so/Shop-CB-combined-reporting-tables-c351ec4d16b845f7bad8d9d3362e3a0e
 */

SELECT
    c.customer_id
     , c.customer_country
     , c.customer_status
     , CASE WHEN c.customer_id_shop IS NOT NULL THEN 'SH' ELSE 'CB' END AS customer_origin_system
     , CASE WHEN c.customer_id_chargebee IS NOT NULL THEN 'CB' ELSE 'SH' END AS customer_current_system
     , sum(i.mrr_eur - i.refund_mrr_eur) AS mrr_eur
FROM dwh_main.dim_combined_invoice_line i
         JOIN dwh_main.dim_combined_subscription s
              ON s.subscription_id = i.subscription_id
         JOIN dwh_main.dim_combined_customer c
              ON c.customer_id = i.customer_id
WHERE 1=1
  AND i.invoice_status <> 'voided'                          -- exclude voided invoices
  AND i.invoice_provisioning_start_dt <= current_date       -- get only currently running subscriptions
  AND i.invoice_provisioning_end_dt > current_date
  AND i.product_group = 'Geo'                               -- only customers with min. 1 Geo order
GROUP BY c.customer_id, c.customer_country, c.customer_status,
         c.customer_id_shop, c.customer_id_chargebee
;
------------------------------------------------------------------------------------------------------------------------------------------------

-- 2. Which subs tables can be connected with the Freshsales Leads  and Account tables

-- This is how to join Freshsales deals with CB/Shop customer.
-- Please note that this works only for the WON deals where the invoice ID has been entered in Freshsales.
SELECT DISTINCT
    d.fs_deal_id
    , o.fs_owner_display_name
    , i.customer_id
FROM dwh_main.dim_fs_deal d
    JOIN dwh_main.dim_fs_owner o
        ON o.fs_owner_id = d.fs_deal_owner_id
    JOIN dwh_main.dim_combined_invoice_line i
        ON i.invoice_id = d.fs_deal_cf_invoice_number
;


SELECT
fsc.fs_contact_id
, fsc.fs_contact_email
, fsa.fs_account_name
, string_agg(c.customer_id, ', ') AS customer_ids
FROM dwh_main.dim_fs_contact fsc
    JOIN dwh_main.dim_fs_account fsa
        ON fsa.fs_account_contact_id = fsc.fs_contact_id
    JOIN dwh_main.dim_combined_customer c
        ON c.customer_contact_email = fsc.fs_contact_email
WHERE fs_contact_email = 'adrian@coolsolutionsac.co.uk'
GROUP BY fsc.fs_contact_id, fsc.fs_contact_email, fsa.fs_account_name;

-- Regarding Leads please contact Blessing

------------------------------------------------------------------------------------------------------------------------------------------------
-- 3. In which table can I find product data especially about the domains, domain creation date,  the dongle id and car trips.
SELECT
    dd.domain_name AS foxbox_domain_name
    , (dd.domain_created_ts AT TIME ZONE 'Europe/Berlin')::DATE AS domain_created_date_cet
    , count(dc.car_id) AS cnt_cars_ever_created
    , count(dc.car_id) FILTER ( WHERE car_end_logbook_ts IS NULL ) AS cnt_active_cars
FROM dwh_main.dim_v_dom_domain dd
    LEFT JOIN dwh_main.dim_car dc
        ON dc.domain_name = dd.domain_name
WHERE dd.domain_is_test = false
GROUP BY dd.domain_name, dd.domain_created_ts
;


-- Cars and trips tables:
SELECT domain_name, car_id, car_imei AS dongle_id FROM dwh_main.dim_car LIMIT 10;
SELECT * FROM dwh_main.dim_v_gl_trip limit 10; -- trips of Geo customers
SELECT * FROM dwh_main.dim_trip limit 10; -- trips of Logbook customers
-- customers that have logbook and geo have the trips in both tables of course
------------------------------------------------------------------------------------------------------------------------------------------------
/*
4. What are the foreign keys to connect them ?

- Here is how you can join CB/Shop tables with Foxbox tables by using a mapping table.
- Full documentation of the mapping table: https://www.notion.so/vimcar/Mapping-table-101-f72dbce2058244a09e8c71ad2832badd
- Why not all CB/Shop customers can be linked to Foxbox (and the other way around?) -> https://www.notion.so/vimcar/Foxbox-and-Shop-Chargebee-Domains-875325714bc041a29ab5a43857655126
 */


SELECT
    c.customer_id
    , CASE WHEN c.customer_id_shop IS NOT NULL THEN 'SH' ELSE 'CB' END AS customer_origin_system
    , CASE WHEN c.customer_id_chargebee IS NOT NULL THEN 'CB' ELSE 'SH' END AS customer_current_system
    , map.map_fb_domain_name AS foxbox_domain_name
    , map.mapping_method
    , dd.domain_configuration_template_match
FROM dwh_main.dim_combined_customer c
         JOIN dwh_main.map_customer_to_foxbox_domain map
              ON c.customer_id = map.map_unified_customer_id
         LEFT JOIN dwh_main.dim_v_dom_domain dd
              ON dd.domain_name = map.map_fb_domain_name
WHERE map.record_nbr_per_unified_customer_id = 1
;
