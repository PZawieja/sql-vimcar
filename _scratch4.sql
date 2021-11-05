SELECT * FROM dwh_main.dim_cb_subscription WHERE cb_subscr_customer_id = '14232859';
SELECT * FROM dwh_main.dim_cb_customer WHERE cb_cust_id = '14232859';
SELECT * FROM dwh_main.dim_combined_subscription WHERE customer_id = '14232859';
SELECT * FROM dwh_main.dim_combined_customer WHERE customer_id = '14232859';
SELECT cb_subscr_id, cb_subscr_cf_vertragsnummer
FROM dwh_main.dim_cb_subscription
WHERE cb_subscr_customer_id= '14232859'
;
SELECT cb_subscr_id, cb_subscr_remaining_billing_cycles, cb_subscr_contract_term_billing_cycle_on_renewal
FROM dwh_main.dim_cb_subscription
WHERE cb_subscr_status <> 'cancelled'
    14232859;

