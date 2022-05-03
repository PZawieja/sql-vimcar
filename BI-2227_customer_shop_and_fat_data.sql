-- Geo tracking active
SELECT domain_name
     , split_part(gl_vehicle_urn, '/', 2)::UUID AS vehicle_uuid
FROM dwh_main.dim_gl_vehicle_setting s
WHERE gl_vehicle_setting_is_tracking = true
;
SELECT count(*) FROM dwh_main.dim_vehicle --264740
SELECT count(*) FROM dwh_main.dim_car --263273
SELECT count(*) FROM dwh_main.dim_v_fb_vehicle --263273