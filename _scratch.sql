SELECT * FROM dwh_main.dim_fs_account

SELECT * FROM dwh_main.dim_cb_subscription WHERE cb_subscr_id = '30018086793';
SELECT * FROM dwh_main.dim_cb_subscription WHERE cb_subscr_id LIKE '11139822%';

SELECT cb_subscr_id AS sub_id
     , cb_subscr_status AS status
     , cb_subscr_plan_id AS plan_id
     , cb_subscr_billing_period_unit AS billing_period_unit
     , cb_subscr_started_ts::DATE AS started_dt
     , cb_subscr_current_term_start_ts::DATE AS curr_ter_start_dt
     , cb_subscr_current_term_end_ts::DATE AS curr_ter_end_dt
     , cb_subscr_billing_period AS billing_cycle
     , cb_subscr_remaining_billing_cycles AS remaining_billing_cycles
     , cb_subscr_contract_term_billing_cycle_on_renewal AS billing_cycles_on_renewal
     , cb_subscr_billing_period
FROM dwh_main.dim_cb_subscription
WHERE cb_subscr_cf_vertragsnummer IS NULL
AND cb_subscr_billing_period_unit = 'month'
-- AND cb_subscr_id = 'AzZMsNT8iI0h5HNw'
AND cb_subscr_plan_id NOT LIKE '%gbp%'
  AND cb_subscr_plan_id NOT LIKE '%trial%'
  AND cb_subscr_status NOT IN ('cancelled','future')
ORDER BY cb_subscr_started_ts DESC
;
