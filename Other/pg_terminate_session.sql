SELECT datname, pid, usename, query_start
, (DATE_PART('day', now() - query_start) * 24 + DATE_PART('hour', now() - query_start)) * 60 + DATE_PART('minute', now() - query_start) AS query_time_minutes
, wait_event_type, wait_event, state, query, backend_type
FROM pg_stat_activity
WHERE query not like 'SET application%' AND query not like 'SHOW TRANSACTION%'
--AND usename = 'pentaho_di'
ORDER BY usename, query;

SELECT pg_cancel_backend(4569);
SELECT pg_terminate_backend(709);
