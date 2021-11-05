select oi.domain_name as domain_name,
    oi.id      as odometer_id,
    tr.id      as trip_id,
    oi.created as odometer_created,
    tr.created as trip_created,
    to_char(oi.created, 'YYYY-MM-DD HH24:MI:SS') as odometer_created_sec,
    to_char(tr.created, 'YYYY-MM-DD HH24:MI:SS') as trip_created_sec
from odometer_inquiries oi
         join trips tr
             on oi.payload ->> 'carId' = tr.payload ->> 'carId'
-- where CAST(tr.created as text) like (left(CAST(oi.created as text), 19) || '%')
WHERE to_char(tr.created, 'YYYY-MM-DD HH24:MI:SS') = to_char(oi.created, 'YYYY-MM-DD HH24:MI:SS')
-- AND oi.created > '2021-11-01' AND tr.created > '2021-11-01';
;
SELECT * FROM contract WHERE id = '27291486-b700-4139-ad6b-37446600abfc'

;
SELECT
    cc.outbound_id AS contract_id
     , cc.id AS contract_uuid
     , cc.customer_id AS customer_uuid
     , lower(trim(trailing '.' from cc.domain)) AS domain_name
     , cc.discount ->> 'type' AS coupon_discount_type
     , (CASE cc.discount ->> 'type'
            WHEN 'percentage' THEN (cc.discount ->> 'value')
            WHEN 'amount' THEN (cc.discount -> 'value' ->> 'amount')
    END)::NUMERIC(8,2) AS coupon_discount_value
     , cc.discount -> 'value' ->> 'currency' AS currency_code
     , cc.coupon_code
     , cc.status AS contract_status
     , coalesce(items.contract_dongles_amt, 0) AS contract_dongles_amt
     , (cc.provisioning_period_start AT TIME ZONE 'Europe/Berlin')::DATE AS contract_provisioning_start_date_key
     , (cc.provisioning_period_end AT TIME ZONE 'Europe/Berlin')::DATE AS contract_provisioning_end_date_key
     , (cc.started AT TIME ZONE 'Europe/Berlin')::DATE AS contract_started_date_key
     , cc.cancelation_reason AS contract_cancelation_reason
     , (cc.canceled AT TIME ZONE 'Europe/Berlin')::DATE AS contract_canceled_date_key
     , cc.canceler AS contract_canceler_urn
     , cc.items AS contract_item_array
     , cc.created AS contract_created_ts
     , cc.creator AS contract_creator_urn
     , cc.modified AS contract_modified_ts
     , cc.modifier AS contract_modifier_urn
FROM public.contract cc
         JOIN public.customer ccu
              ON ccu.id = cc.customer_id,
     LATERAL (SELECT sum((obj.value ->> 'quantity')::SMALLINT) AS contract_dongles_amt
              FROM jsonb_array_elements(cc.items) obj(value)
                       JOIN mapping.umd_product_map pmap
                            ON pmap.product_uuid = NULLIF(obj.value ->> 'product_id', 'fake')::UUID
                                AND pmap.device_quantity_cnt IS TRUE
                                AND pmap.is_recurring IS TRUE) items
WHERE 1=1
    AND cc.id = '27291486-b700-4139-ad6b-37446600abfc'
-- exclude test contracts
--   AND ccu.contact ->> 'email' NOT LIKE '%vimcar.com'
-- exclude ghost contracts
--   AND EXISTS (SELECT 1
--               FROM public.invoice ci
--                        LEFT JOIN public.payment dp
--                                  ON dp.invoice_id = ci.id
--               WHERE cc.id = ci.contract_id
--                 AND NOT ((ci.creator = 'urn:vimcar::service/new-user-checkout' AND ci.modifier = 'urn:vimcar::service/new-user-checkout')
--                   OR (ci.creator = 'urn:vimcar::service/new-user-checkout' AND ci.modifier LIKE 'urn:vimcar:com.vimcar:user%' AND dp.status <> 'authorised'))
-- --     )
ORDER BY cc.outbound_id;


SELECT *
FROM public.invoice ci
         LEFT JOIN public.payment dp
                   ON dp.invoice_id = ci.id
WHERE ci.outbound_id = 'R97773906'
;

SELECT * FROM shop_extraction_order_invoice WHERE outbound_id = 'R97773906'







SELECT
    id AS odometer_inquiry_id
     , owner_id AS odometer_inquiry_owner_id
     , domain_name::VARCHAR(128)
     , (payload ->> 'carId')::INT AS car_id
     , (payload ->> 'totalOdometerInKm')::BIGINT AS total_odometer_value_km
     , (payload ->> 'isValid')::BOOLEAN AS odometer_inquiry_is_valid
     , ((payload ->> 'inquiryDate')::TIMESTAMP AT TIME ZONE 'UTC')::TIMESTAMPTZ AS odometer_inquiry_ts
     , (payload ->> 'actorId')::INT AS odometer_inquiry_actor_id
     , (payload ->> 'creator')::VARCHAR(128) AS odometer_inquiry_creator_urn
     , (created AT TIME ZONE 'UTC')::TIMESTAMPTZ AS odometer_inquiry_created_ts
     , (modified AT TIME ZONE 'UTC')::TIMESTAMPTZ AS odometer_inquiry_modified_ts
     , created::TIMESTAMPTZ AS odometer_inquiry_created_ts
     , modified::TIMESTAMPTZ AS odometer_inquiry_modified_ts
FROM public.odometer_inquiries
WHERE 1=1
--   AND domain_name = '${DOMAIN_NAME}'
AND id = 145591233
ORDER BY id;


SELECT
    id AS trip_id
     , owner_id AS trip_owner_id
     , domain_name
     , is_merged_trip
     , merge_parent_id AS merge_parent_trip_id
     , (payload ->> 'mergedTripCount')::SMALLINT AS merged_trip_count
     , (payload ->> 'carId')::INT AS car_id
     , (payload ->> 'categorizer')::INT AS trip_categorizer
     , payload ->> 'category' AS trip_category
     , (payload -> 'categorySplit' ->> 'BUSINESS')::DECIMAL(4,1) AS category_split_business
     , (payload -> 'categorySplit' ->> 'COMMUTE')::DECIMAL(4,1) AS category_split_commute
     , (payload -> 'categorySplit' ->> 'PRIVATE')::DECIMAL(4,1) AS category_split_private
     , ((payload ->> 'startTimestamp')::TIMESTAMP AT TIME ZONE 'UTC')::TIMESTAMPTZ AS trip_start_ts
     , ((payload ->> 'endTimestamp')::TIMESTAMP AT TIME ZONE 'UTC')::TIMESTAMPTZ AS trip_end_ts
     , payload -> 'startCoordinates' AS trip_start_coordinates
     , payload -> 'endCoordinates' AS trip_end_coordinates
     , payload -> 'startAddress' AS trip_start_address
     , payload -> 'endAddress' AS trip_end_address
     , (payload ->> 'distanceInMeter')::INT AS trip_distance
     , (payload ->> 'distanceInMeterAdjustment')::INT AS distance_adjustment
     , payload ->> 'driverInformation' AS trip_driver_information
     , payload ->> 'reason' AS trip_reason
     , payload ->> 'remark' AS trip_remark
     , payload ->> 'comment' AS trip_comment
     , payload ->> 'contactName' AS trip_contact_name
     , payload ->> 'contactCompany' AS trip_contact_company
     , payload ->> 'creator' AS trip_creator
     , (created AT TIME ZONE 'UTC')::TIMESTAMPTZ AS trip_created_ts
     , (modified AT TIME ZONE 'UTC')::TIMESTAMPTZ AS trip_modified_ts
     , payload -> 'tripChangeLogs' AS tcl_json
FROM public.trips
WHERE merge_parent_id IS NULL
  AND domain_name = '${DOMAIN_NAME}'
--AND created > '2019-10-01'
--and id = 17093023;