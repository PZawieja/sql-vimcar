
WITH cte_revenue AS (
    SELECT i.customer_id
         , product_group
         , coalesce(sum(mrr_eur-refund_mrr_eur),0) AS current_mrr_eur
    FROM dwh_main.dim_combined_invoice_line i
        JOIN dwh_main.dim_combined_customer c
            ON c.customer_id = i.customer_id
    WHERE invoice_status <> 'voided'
      AND product_group NOT IN ('Hardware', 'Trial')
      AND invoice_provisioning_start_dt <= current_date
      AND invoice_provisioning_end_dt > current_date
--       AND c.customer_id = '30003266438'
    GROUP BY i.customer_id, product_group
)
   , cte_foxbox_domains AS (SELECT d.domain_name
--                                  , d.domain_configuration_template_match
                                 , REPLACE(d.domain_configuration_template_match, ',', ' | ') AS domain_configuration_template_match
                                 , cnt_cars
                                 , cnt_users
                                 , emails
                            FROM dwh_main.dim_v_dom_domain d
                                     LEFT JOIN (SELECT domain_name, SUM(1) AS cnt_cars
                                                FROM dwh_main.dim_car
                                                WHERE car_end_logbook_ts IS NOT NULL
                                                GROUP BY domain_name) cars ON cars.domain_name = d.domain_name
                                     LEFT JOIN (SELECT domain_name, SUM(1) AS cnt_users
                                                , string_agg(email, ' | ') AS emails
                                                FROM dwh_main.dim_principal
                                                WHERE principal_deleted_ts IS NULL
                                                GROUP BY domain_name) u ON u.domain_name = d.domain_name
                            WHERE d.domain_is_test = FALSE)
, cte_final AS (SELECT f.domain_name AS foxbox_domain_name
                     , f.domain_configuration_template_match               AS domain_configuration_template
                     , f.cnt_cars
                     , f.cnt_users
                     , f.emails
                     , STRING_AGG(DISTINCT m.map_unified_customer_id ||' ('|| c.customer_status ||')', ' | ') AS subscription_customer_id
                     , SUM(r.current_mrr_eur)                              AS current_mrr_eur
                     , STRING_AGG(DISTINCT r.product_group, ' | ')         AS subscribed_products
                FROM cte_foxbox_domains f
                         LEFT JOIN dwh_main.map_customer_to_foxbox_domain m
                                   ON m.map_fb_domain_name = f.domain_name AND record_nbr_per_unified_customer_id = 1
                         LEFT JOIN cte_revenue r ON r.customer_id = m.map_unified_customer_id
                         LEFT JOIN dwh_main.dim_combined_customer c ON c.customer_id = m.map_unified_customer_id
                GROUP BY f.domain_name, f.cnt_cars, f.cnt_users, f.emails, f.domain_configuration_template_match)
SELECT
foxbox_domain_name
, domain_configuration_template
, coalesce(cnt_cars, 0) AS cnt_cars
, coalesce(cnt_users, 0) AS cnt_users
, emails
, subscription_customer_id
, subscribed_products
, CASE
    WHEN subscription_customer_id IS NOT NULL
        THEN coalesce(current_mrr_eur, 0)
    END AS current_mrr_eur
, CASE
    WHEN subscription_customer_id IS NULL
        THEN 'unknown'
    WHEN subscription_customer_id IS NOT NULL
            AND current_mrr_eur > 200
        THEN 'yes'
    ELSE 'no'
    END AS "is_m+_customer"
FROM cte_final
;

SELECT domain_name
, sum(1) AS cnt_fences
FROM dwh_main.dim_gl_fence
GROUP BY domain_name
;

SELECT * FROM dwh_main.dim_gl_fence_viol;

