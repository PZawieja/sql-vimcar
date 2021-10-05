/*
 CHECK: Every recurring plan ID should be categorized so that it doesn't get into group 'Other'
 ACTION: Adjust the mapping in PDI: pdi/chargebee_dwh/tr_dim_cb_plan.ktr
 */
SELECT cb_plan_product_group, cb_plan_name, cb_plan_id, cb_plan_price
FROM dwh_main.dim_cb_plan
WHERE cb_plan_product_group IN ('Other')
AND cb_plan_id NOT IN ('webcheckout-test-plan', 'dummy-vimcar_miet_modell')
GROUP BY 1,2,3,4;

