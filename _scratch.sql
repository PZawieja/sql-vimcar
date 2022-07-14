-- Aggregation at MAIN Domain level:
WITH cte_freshsales AS (
    SELECT m.map_fb_main_domain_name
         , string_agg(DISTINCT
                      CASE
                          WHEN btrim(lower(a.fs_account_cf_customer_success_manager)) = 'milena.lassainato@vimcar.com'
                              THEN 'Milena L''Assainato'
                          WHEN btrim(lower(a.fs_account_cf_customer_success_manager)) = 'kai.gaertner@vimcar.com'
                              THEN 'Kai Gärtner'
                          WHEN btrim(a.fs_account_cf_customer_success_manager) LIKE '%@vimcar%'
                              THEN initcap(replace(split_part(btrim(a.fs_account_cf_customer_success_manager), '@', 1), '.', ' '))
                          ELSE a.fs_account_cf_customer_success_manager
                          END, ', ') AS customer_success_manager_name

    FROM dwh_main.dim_fs_deal d
             JOIN dwh_main.dim_fs_account a
                  ON a.fs_account_id = d.fs_deal_sales_account_id
             JOIN dwh_main.dim_fs_owner o
                  ON a.fs_account_owner_id = o.fs_owner_id
             JOIN dwh_main.dim_combined_invoice_line ci
                  ON ci.invoice_id = d.fs_deal_cf_invoice_number
             JOIN dwh_main.map_customer_to_foxbox_domain m
                  ON m.map_unified_customer_id = ci.customer_id
    WHERE d.fs_deal_cf_invoice_number IS NOT NULL
      AND m.record_nbr_per_unified_customer_id = 1
      AND m.cnt_foxbox_domains_per_unified_customer_id >0
    GROUP BY m.map_fb_main_domain_name
)
, cte_subscriptions AS (
    SELECT
        m.map_fb_main_domain_name AS foxbox_main_domain
         , CASE
               WHEN m.map_fb_main_domain_name LIKE '%.gb.%' THEN 'GB'
               WHEN m.map_fb_main_domain_name LIKE '%.at.%' THEN 'AT'
               WHEN m.map_fb_main_domain_name LIKE '%.ch.%' THEN 'CH'
               ELSE 'DE'
        END AS foxbox_domain_country
         , m.map_unified_customer_id
         , c.customer_status
         , c.customer_is_fleet
         , c.customer_contact_company_name
         , c.customer_contact_postal_code
         , CASE
             WHEN c.customer_id_chargebee IS NOT NULL
                 THEN 'Chargebee'
            ELSE 'Shop'
            END AS subscription_system
         , dom.has_logbook_b2c
         , dom.has_fleet_logbook
         , dom.has_fleet_admin
         , greatest(dom.has_fleet_geo, dom.has_fleet_geo_live) AS has_fleet_geo
         , dom.has_fleet_pro
         , coalesce(sum(mrr_eur-refund_mrr_eur),0) AS current_mrr_eur
         , coalesce(sum(mrr_eur-refund_mrr_eur) FILTER ( WHERE product_group LIKE 'Logbook%'),0) AS current_mrr_eur_logbook
         , coalesce(sum(mrr_eur-refund_mrr_eur) FILTER ( WHERE product_group = 'Admin'),0) AS current_mrr_eur_admin
         , coalesce(sum(mrr_eur-refund_mrr_eur) FILTER ( WHERE product_group = 'Geo'),0) AS current_mrr_eur_geo
         , coalesce(sum(mrr_eur-refund_mrr_eur) FILTER ( WHERE product_group = 'Pro'),0) AS current_mrr_eur_pro
         , coalesce(sum(mrr_eur-refund_mrr_eur) FILTER ( WHERE product_group NOT IN ('Logbook B2C','Logbook B2B','Admin','Geo','Pro')),0) AS current_mrr_eur_other
         , coalesce(sum(item_quantity-refund_item_quantity) FILTER ( WHERE product_group LIKE 'Logbook%'),0) AS current_items_logbook
         , coalesce(sum(item_quantity-refund_item_quantity) FILTER ( WHERE product_group = 'Admin'),0) AS current_items_admin
         , coalesce(sum(item_quantity-refund_item_quantity) FILTER ( WHERE product_group = 'Geo'),0) AS current_items_geo
         , coalesce(sum(item_quantity-refund_item_quantity) FILTER ( WHERE product_group = 'Pro'),0) AS current_items_pro
         , coalesce(sum(item_quantity-refund_item_quantity) FILTER ( WHERE product_group IN ('Logbook B2C','Logbook B2B','Admin','Geo','Pro')),0) AS current_items
         , coalesce(sum(item_quantity-refund_item_quantity) FILTER ( WHERE product_group NOT IN ('Logbook B2C','Logbook B2B','Admin','Geo','Pro')),0) AS current_items_other
    FROM dwh_main.dim_combined_invoice_line i
             JOIN dwh_main.dim_combined_subscription s
                  ON s.subscription_id = i.subscription_id
             JOIN dwh_main.dim_combined_customer c
                  ON c.customer_id = i.customer_id
             JOIN dwh_main.map_customer_to_foxbox_domain m
                  ON i.customer_id = m.map_unified_customer_id
                      AND m.record_nbr_per_unified_customer_id = 1
                      AND m.cnt_foxbox_domains_per_unified_customer_id > 0
             JOIN dwh_main.dim_v_dom_domain dom
                  ON dom.domain_name = m.map_fb_domain_name
    WHERE i.invoice_status <> 'voided'
      AND i.full_refund_flag = false
      AND i.product_group NOT IN ('Hardware', 'Trial')
      AND i.invoice_provisioning_start_dt <= current_date
      AND i.invoice_provisioning_end_dt > current_date
      AND dom.domain_is_test = FALSE
      AND c.customer_status NOT IN ('cancelled', 'non_renewing')
      AND c.customer_is_fleet = TRUE
--      AND m.map_fb_main_domain_name IN ('com.vimcar.2p-media-net')
    GROUP BY i.customer_id, m.map_fb_main_domain_name, m.map_unified_customer_id, c.customer_status, c.customer_is_fleet,
             c.customer_contact_company_name, c.customer_contact_postal_code, c.customer_id_chargebee, dom.has_logbook_b2c, dom.has_fleet_logbook, dom.has_fleet_admin,
             dom.has_fleet_geo, dom.has_fleet_geo_live, dom.has_fleet_pro
)
   , cte_company_info AS (
    SELECT DISTINCT ON (foxbox_main_domain)
        foxbox_main_domain
        , customer_contact_company_name AS company_name
        , map_unified_customer_id AS customer_id
        , customer_contact_postal_code AS postal_code
        , subscription_system
    FROM cte_subscriptions
    ORDER BY foxbox_main_domain, customer_status, current_mrr_eur DESC, map_unified_customer_id
)
, cte_tracking AS (
    SELECT d.main_domain_name
         , SUM(1) FILTER (WHERE s.gl_vehicle_setting_has_tracking) AS cnt_cars_with_tracking -- Geo-Funktionen aktivieren
         , SUM(1) FILTER (WHERE s.gl_vehicle_setting_has_dynamic_tracking) AS cnt_cars_with_dynamic_tracking -- Fahrer/in ermoglichen Geo-Funktionen zu aktivieren
    FROM dwh_main.dim_gl_vehicle_setting s
             JOIN dwh_main.dim_v_fb_vehicle v
                  ON v.fb_vehicle_uuid = s.gl_vehicle_uuid
             JOIN dwh_main.dim_vehicle dv
                  ON dv.vehicle_uuid = v.fb_vehicle_uuid
             JOIN dwh_main.dim_v_dom_domain d
                  ON v.domain_name = d.domain_name
    WHERE greatest(gl_vehicle_setting_has_tracking, gl_vehicle_setting_has_dynamic_tracking) = true
      AND v.fb_vehicle_end_logbook_ts IS NULL
      AND dv.vehicle_archived_ts IS NULL
    GROUP BY d.main_domain_name
)
   , cte_inventory_booking AS (
    SELECT d.main_domain_name
         , SUM(1) cnt_cars_with_booking_enabled
    FROM dwh_main.dim_v_cal_inventory i
             JOIN dwh_main.dim_v_fb_vehicle v
                  ON v.fb_vehicle_uuid = split_part(i.inventory_resource_urn, '/', 2)::UUID
             JOIN dwh_main.dim_vehicle dv
                  ON dv.vehicle_uuid = v.fb_vehicle_uuid
             JOIN dwh_main.dim_v_dom_domain d
                  ON v.domain_name = d.domain_name
    WHERE i.inventory_is_bookable = true
      AND i.inventory_archived_ts IS NULL
      AND v.fb_vehicle_end_logbook_ts IS NULL
    GROUP BY d.main_domain_name
)
   , cte_foxbox AS (SELECT DISTINCT
                           d.main_domain_name
                         , mdi.active_vehicles_amt AS cnt_active_cars
                         , mdi.active_logbook_vehicles_amt AS cnt_active_cars_with_logbook_enabled
                         , coalesce(t.cnt_cars_with_tracking, 0) AS cnt_cars_with_tracking
                         , coalesce(t.cnt_cars_with_dynamic_tracking, 0) AS cnt_cars_with_dynamic_tracking
                         , coalesce(ib.cnt_cars_with_booking_enabled, 0) AS cnt_cars_with_booking_enabled
                    FROM dwh_main.dim_v_dom_domain d
                             JOIN dwh_main.dim_main_domain_info mdi
                                ON mdi.foxbox_main_domain = d.main_domain_name
                             LEFT JOIN cte_tracking t
                                       ON t.main_domain_name = d.main_domain_name
                             LEFT JOIN cte_inventory_booking ib
                                       ON ib.main_domain_name = d.main_domain_name
                    WHERE d.domain_is_test = false)
   , cte_domain AS (SELECT DISTINCT s.foxbox_main_domain
                                       , s.foxbox_domain_country
                                       , string_agg(DISTINCT s.map_unified_customer_id ||' ('||customer_status||')', ', ') AS subscription_customer_id
                                       , ci.company_name
                                       , ci.customer_id
                                       , ci.postal_code
                                       , ci.subscription_system
                                       , SUM(current_mrr_eur) AS current_mrr_eur
                                       , SUM(current_mrr_eur_logbook) AS current_mrr_eur_logbook
                                       , SUM(current_mrr_eur_admin) AS current_mrr_eur_admin
                                       , SUM(current_mrr_eur_geo) AS current_mrr_eur_geo
                                       , SUM(current_mrr_eur_pro) AS current_mrr_eur_pro
                                       , SUM(current_mrr_eur_other) AS current_mrr_eur_other
                                       , SUM(current_items) AS current_items
                                       , SUM(current_items_logbook) AS current_items_logbook
                                       , SUM(current_items_admin) AS current_items_admin
                                       , SUM(current_items_geo) AS current_items_geo
                                       , SUM(current_items_pro) AS current_items_pro
                                       , SUM(current_items_other) AS current_items_other
                                       , true = ANY(ARRAY_AGG(s.has_logbook_b2c)) AS config_logbook_b2c
                                       , true = ANY(ARRAY_AGG(s.has_fleet_logbook)) AS config_logbook_b2b
                                       , true = ANY(ARRAY_AGG(s.has_fleet_admin)) AS config_admin
                                       , true = ANY(ARRAY_AGG(s.has_fleet_geo)) AS config_geo
                                       , true = ANY(ARRAY_AGG(s.has_fleet_pro)) AS config_pro
                         FROM cte_subscriptions s
                                  LEFT JOIN cte_company_info ci
                                            ON ci.foxbox_main_domain = s.foxbox_main_domain
                         GROUP BY s.foxbox_main_domain, s.foxbox_domain_country, ci.company_name,
                                  ci.customer_id, ci.postal_code, ci.subscription_system
)
SELECT foxbox_main_domain
     , fs.customer_success_manager_name
     , company_name
--      , customer_id_company_name
     , postal_code
     , subscription_system
     , subscription_customer_id
     , '' AS "Domain configuration"
     , config_admin
     , config_geo
     , config_pro
     , config_logbook_b2c
     , config_logbook_b2b
     , greatest(config_logbook_b2c, config_logbook_b2b) AS config_logbook
     , '' AS "MRR"
     , coalesce(current_mrr_eur,0) AS "Current MRR"
     , coalesce(current_mrr_eur_logbook,0) AS "MRR Logbook"
     , coalesce(current_mrr_eur_admin,0) AS "MRR Admin"
     , coalesce(current_mrr_eur_geo,0) AS "MRR Geo"
     , coalesce(current_mrr_eur_pro,0) AS "MRR Pro"
     , coalesce(current_mrr_eur_other,0) AS "MRR Other"
     , '' AS "Cars vs licenses"
     , coalesce(current_items,0) AS "Current Items"
     , coalesce(current_items_logbook,0) AS "Items Logbook"
     , coalesce(current_items_admin,0) AS "Items Admin"
     , coalesce(current_items_geo,0) AS "Items Geo"
     , coalesce(current_items_pro,0) AS "Items Pro"
     , coalesce(current_items_other,0) AS "Items Other"
     , f.cnt_active_cars AS "Active Cars"
     , CASE
           WHEN f.cnt_active_cars > current_items
               THEN 'cars > licenses'
           WHEN f.cnt_active_cars <= current_items
               THEN 'cars <= licenses'
        END AS "Cars vs Items"
     , current_items - f.cnt_active_cars AS "DIFF Cars vs Items"
     -- logbook:
     , f.cnt_active_cars_with_logbook_enabled AS "Active Cars with Logbook enabled"
     , CASE
           WHEN f.cnt_active_cars_with_logbook_enabled > current_items
               THEN 'cars > licenses'
           WHEN f.cnt_active_cars_with_logbook_enabled <= current_items
               THEN 'cars <= licenses'
    END AS "Cars with Logbook enabled vs Items"
     , current_items - f.cnt_active_cars_with_logbook_enabled AS "DIFF Cars with Logbook enabled vs Items"
     -- tracking:
     , f.cnt_cars_with_tracking AS "Active Cars with Tracking enabled"
     , CASE
           WHEN f.cnt_cars_with_tracking > (current_items_geo + current_items_pro)
               THEN 'cars > licenses'
           WHEN f.cnt_cars_with_tracking <= (current_items_geo + current_items_pro)
               THEN 'cars <= licenses'
    END AS "Cars with Tracking vs Items"
     , (current_items_geo + current_items_pro) - f.cnt_cars_with_tracking AS "DIFF Cars with Tracking vs Items"
     -- booking
     , f.cnt_cars_with_booking_enabled AS "Active Cars with Booking enabled"
     , CASE
           WHEN f.cnt_cars_with_booking_enabled > current_items
               THEN 'cars > licenses'
           WHEN f.cnt_cars_with_booking_enabled <= current_items
               THEN 'cars <= licenses'
    END AS "Cars with Booking enabled vs Items"
     , (current_items - f.cnt_cars_with_booking_enabled)::DECIMAL(10,2) AS "DIFF Cars with Booking enabled vs Items"
--      , sum(f.cnt_cars_with_dynamic_tracking) AS cnt_cars_with_dynamic_tracking
FROM cte_domain d
         LEFT JOIN cte_freshsales fs
                   ON fs.map_fb_main_domain_name = d.foxbox_main_domain
         LEFT JOIN cte_foxbox f
                   ON d.foxbox_main_domain = f.main_domain_name
WHERE 1=1
  AND foxbox_domain_country <> 'GB'
 AND foxbox_main_domain = 'com.vimcar.veoneer'
;

SELECT * FROM dwh_main.dim_main_domain_info WHERE foxbox_main_domain = 'com.vimcar.veoneer'
;
