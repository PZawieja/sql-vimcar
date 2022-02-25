-- Problem:
SELECT * FROM dwh_main.dim_domain_scd2 WHERE domain_name = 'com.vimcar.adac-wuerttemberg-ev' ORDER BY version_nr;


-- DELETE FROM dwh_main.dim_domain_scd2
SELECT * FROM dwh_main.dim_domain_scd2 -- comment this line and uncomment delete lime
WHERE domain_key IN
    (SELECT domain_key
    FROM
        (SELECT domain_key
              , version_nr
              , ROW_NUMBER() OVER( PARTITION BY domain_name, version_nr ORDER BY domain_key ) AS row_num
        FROM dwh_main.dim_domain_scd2
        WHERE domain_name = 'com.vimcar.adac-wuerttemberg-ev'  -- testing
            ) t
        WHERE t.row_num > 1 )
;

-- After deletion the table will look like this. There are still fixes for some records in "valid_from_ts" column (see rows 29 and 30) and later "valid_to_ts" (see rows 36, 37):
WITH cte_after_deletion AS (
    SELECT * FROM dwh_main.dim_domain_scd2
    WHERE domain_key IN
        (SELECT domain_key
        FROM
            (SELECT domain_key
                  , version_nr
                  , ROW_NUMBER() OVER( PARTITION BY domain_name, version_nr ORDER BY domain_key ) AS row_num
            FROM dwh_main.dim_domain_scd2
            WHERE domain_name IN ('com.vimcar.adac-wuerttemberg-ev', 'com.vimcar.b-und-s-gmbh-de')  -- testing
                ) t
            WHERE t.row_num = 1 )
    ORDER BY domain_name, domain_key
)
SELECT * FROM cte_after_deletion
;

-- In order to fix it we need to get the correct values for "valid_from_ts" first. Update of both fields at the same time would fail therefore we need to do this separately.
-- Showcase:
WITH cte_after_deletion AS (
    SELECT * FROM dwh_main.dim_domain_scd2
    WHERE domain_key IN
        (SELECT domain_key
        FROM
            (SELECT domain_key
                  , version_nr
                  , ROW_NUMBER() OVER( PARTITION BY domain_name, version_nr ORDER BY domain_key ) AS row_num
            FROM dwh_main.dim_domain_scd2
            WHERE domain_name IN ('com.vimcar.adac-wuerttemberg-ev', 'com.vimcar.b-und-s-gmbh-de') -- testing
                ) t
            WHERE t.row_num = 1 )
    ORDER BY domain_name, domain_key
)
SELECT domain_key
     ,valid_from_ts
     ,valid_from_ts_correct
FROM (SELECT domain_name
             , domain_key
             , valid_from_ts
             , valid_to_ts
             , version_nr
             , LAG(valid_to_ts) OVER (PARTITION BY domain_name ORDER BY domain_name, domain_key)
             , CASE WHEN LAG(valid_to_ts) OVER (PARTITION BY domain_name ORDER BY domain_name, domain_key) > valid_from_ts
                 AND LAG(valid_to_ts) OVER (PARTITION BY domain_name ORDER BY domain_name, domain_key) < current_timestamp
                        THEN LAG(valid_to_ts) OVER (PARTITION BY domain_name ORDER BY domain_name, domain_key)
                 END valid_from_ts_correct
      FROM cte_after_deletion) t
WHERE valid_from_ts_correct IS NOT NULL
ORDER BY domain_name, domain_key
;
-- Actual UPDATE statement for "valid_from_ts" field:
UPDATE dwh_main.dim_domain_scd2 scd2
SET valid_from_ts = sub.valid_from_ts_correct
FROM (
SELECT domain_key
--      , valid_from_ts
     , valid_from_ts_correct
FROM (SELECT domain_name
             , domain_key
--              , valid_from_ts
--              , valid_to_ts
--              , version_nr
             , LAG(valid_to_ts) OVER (PARTITION BY domain_name ORDER BY domain_name, domain_key)
             , CASE WHEN LAG(valid_to_ts) OVER (PARTITION BY domain_name ORDER BY domain_name, domain_key) > valid_from_ts
                     AND LAG(valid_to_ts) OVER (PARTITION BY domain_name ORDER BY domain_name, domain_key) < current_timestamp
                        THEN LAG(valid_to_ts) OVER (PARTITION BY domain_name ORDER BY domain_name, domain_key)
--                     ELSE valid_from_ts
                 END valid_from_ts_correct
      FROM dwh_main.dim_domain_scd2) t
WHERE valid_from_ts_correct IS NOT NULL
    AND domain_name IN ('com.vimcar.adac-wuerttemberg-ev', 'com.vimcar.b-und-s-gmbh-de')  -- testing
ORDER BY domain_name, domain_key
) sub
WHERE scd2.domain_key = sub.domain_key
;

SELECT * FROM dwh_main.dim_domain_scd2 WHERE domain_name IN ('com.vimcar.adac-wuerttemberg-ev', 'com.vimcar.b-und-s-gmbh-de') ORDER BY version_nr;


-- UPDATE statement for "valid_to_ts" field:
UPDATE dwh_main.dim_domain_scd2 scd2
SET valid_to_ts = sub.valid_to_ts_correct
FROM (
SELECT domain_key
     , valid_to_ts_correct
FROM (SELECT domain_name
             , domain_key
             , CASE WHEN LEAD(valid_from_ts) OVER (PARTITION BY domain_name ORDER BY domain_name, domain_key) < valid_to_ts
                         AND LEAD(valid_from_ts) OVER (PARTITION BY domain_name ORDER BY domain_name, domain_key) < current_timestamp
                        THEN LEAD(valid_from_ts) OVER (PARTITION BY domain_name ORDER BY domain_name, domain_key)
--                     ELSE valid_to_ts
                 END valid_to_ts_correct
      FROM dwh_main.dim_domain_scd2) t
WHERE valid_to_ts_correct IS NOT NULL
    AND domain_name IN ('com.vimcar.adac-wuerttemberg-ev', 'com.vimcar.b-und-s-gmbh-de')  -- testing
ORDER BY domain_name, domain_key
) sub
WHERE scd2.domain_key = sub.domain_key
;

-- Cleaning up of the "is_last_version" flag. It can be true only if "valid_to_ts" is in future:
UPDATE dwh_main.dim_domain_scd2 scd2
SET is_last_version = FALSE
WHERE valid_to_ts < current_timestamp
;




/*
If we want to keep the table 100% clean we should probably delete all the duplicates which are there after the update we made.
There will be records which are identical except from "domain_key" and "version_nr" therefore they are redundant.
 */



/*
WITH cte_after_deletion AS (
    SELECT * FROM dwh_main.dim_domain_scd2
    WHERE domain_key IN
        (SELECT domain_key
        FROM
            (SELECT domain_key
                  , version_nr
                  , ROW_NUMBER() OVER( PARTITION BY domain_name, version_nr ORDER BY domain_key ) AS row_num
            FROM dwh_main.dim_domain_scd2
            WHERE domain_name IN ('com.vimcar.adac-wuerttemberg-ev', 'com.vimcar.b-und-s-gmbh-de')
                ) t
            WHERE t.row_num = 1 )
    ORDER BY domain_name, domain_key
)
SELECT domain_key
     , domain_name
     , valid_from_ts
     , valid_from_ts_correct
     , valid_to_ts
     , valid_to_ts_correct
--      , test
     , is_last_version
FROM (SELECT domain_name
             , domain_key
             , valid_from_ts AS valid_from_ts
             , valid_to_ts
             , version_nr
--              , LAG(valid_to_ts) OVER (PARTITION BY domain_name ORDER BY domain_name, domain_key) AS test_lag
             , CASE WHEN LAG(valid_to_ts) OVER (PARTITION BY domain_name ORDER BY domain_name, domain_key) > valid_from_ts
                         AND LAG(valid_to_ts) OVER (PARTITION BY domain_name ORDER BY domain_name, domain_key) < current_timestamp
                        THEN LAG(valid_to_ts) OVER (PARTITION BY domain_name ORDER BY domain_name, domain_key)
                 END valid_from_ts_correct

             , CASE WHEN LEAD(valid_from_ts) OVER (PARTITION BY domain_name ORDER BY domain_name, domain_key) < valid_to_ts
                         AND LEAD(valid_from_ts) OVER (PARTITION BY domain_name ORDER BY domain_name, domain_key) < current_timestamp
                        THEN LEAD(valid_from_ts) OVER (PARTITION BY domain_name ORDER BY domain_name, domain_key)
                 END valid_to_ts_correct
--              , LEAD(valid_from_ts) OVER (PARTITION BY domain_name ORDER BY domain_name, domain_key) AS test_lead
             , is_last_version
      FROM cte_after_deletion) t
-- WHERE valid_from_ts_correct IS NOT NULL
ORDER BY domain_name, domain_key
;

 */