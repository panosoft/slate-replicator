--get the total events inserted into the events table from insert groups data
WITH get_eventcount_from_insert_groups AS (
SELECT sum(e.eventcount)::bigint AS eventcount FROM
	(SELECT
		event::jsonb->'testingStats'->>'programId' AS programid,
		event::jsonb->'testingStats'->>'dbOperationId' AS dboperationid,
		event::jsonb->'testingStats'->'dbOperationNum' AS dboperationnum,
		(event::jsonb->'testingStats'->>'dbOperationEventCount')::bigint AS eventcount,
		array_agg(id) AS ids,
		min(id) AS minid,
		max(id) AS maxid,
		count(id) AS count
	FROM events
	GROUP BY programId, dboperationid, dboperationnum, eventcount) AS e
),
--get id statistics from all rows in the events table
get_stats_from_all_rows AS (
SELECT min(id) AS minid, max(id) AS maxid, array_agg(id) AS ids, count(id) as count
	FROM events
),
--get id statistics and total events inserted into the table from insert groups data.  validate event ids and total events inserted.
--event ids must start at 1 and be consecutive integers.  total events inserted must equal total row count.
get_stats_from_insert_groups AS (
SELECT min(s.minid) AS minid, max(s.maxid) AS maxid, sum(s.count)::bigint AS count, sum(s.eventcount)::bigint AS eventcount FROM
	(SELECT r.programid, r.dboperationid, r.dboperationnum, r.ids, r.generatedids, r.minid, r.maxid, r.count, r.eventcount FROM
		(SELECT  q.programId, q.dboperationid, q.dboperationnum,
			--sort q.ids
			ARRAY(SELECT unnest(q.ids) ORDER BY 1) as ids,
			q.minid, q.maxid, q.count, q.eventcount,
			ARRAY(SELECT generate_series(q.minid, q.maxid)) AS generatedids FROM
			(SELECT
				event::jsonb->'testingStats'->>'programId' AS programid,
				event::jsonb->'testingStats'->>'dbOperationId' AS dboperationid,
				event::jsonb->'testingStats'->'dbOperationNum' AS dboperationnum,
				(event::jsonb->'testingStats'->>'dbOperationEventCount')::bigint AS eventcount,
				array_agg(id) AS ids,
				min(id) AS minid,
				max(id) AS maxid,
				count(id) AS count
			FROM events
			GROUP BY programId, dboperationid, dboperationnum, eventcount) AS q
		) AS r
	--should select all insert operation groups
	WHERE (r.count = r.eventcount) IS NOT UNKNOWN AND r.count = r.eventcount AND (r.ids = r.generatedids) IS NOT UNKNOWN AND r.ids = r.generatedids
	ORDER BY r.minid
	) AS s
),
--merge id and total events inserted stats from all rows.  validate event ids and total event inserted.
--event ids must start at 1 and be consecutive integers.  total events inserted must equal total row count.
merge_stats_from_all_rows AS (
SELECT a.minid, a.maxid, a.count, e.eventcount
FROM get_stats_from_all_rows AS a, get_eventcount_from_insert_groups AS e
WHERE (a.minid = 1) IS NOT UNKNOWN
	AND a.minid = 1
	AND (ARRAY(SELECT unnest(a.ids) ORDER BY 1) = ARRAY(SELECT generate_series(a.minid, a.maxid))) IS NOT UNKNOWN
	AND ARRAY(SELECT unnest(a.ids) ORDER BY 1) = ARRAY(SELECT generate_series(a.minid, a.maxid))
	AND (a.count = array_length(ARRAY(SELECT generate_series(a.minid, a.maxid) ORDER BY 1), 1)) IS NOT UNKNOWN
	AND a.count = array_length(ARRAY(SELECT generate_series(a.minid, a.maxid) ORDER BY 1), 1)
	AND (a.count = e.eventcount) IS NOT UNKNOWN
	AND a.count = e.eventcount
)

--events table is valid if one row of stats is returned where all statistics derived from insert groups data equals the same statistics derived from all rows.
SELECT 'VALID' AS events_table_status, g.minid AS minid_insert_groups, a.minid AS minid_all_rows, g.maxid AS maxid_insert_groups, a.maxid AS maxid_all_rows,
	g.count AS rowcount_insert_groups, a.count AS rowcount_all_rows, g.eventcount AS total_events_insert_groups, a.eventcount AS total_events_all_rows
FROM get_stats_from_insert_groups AS g,	merge_stats_from_all_rows AS a
WHERE (g.minid = 1) IS NOT UNKNOWN
	AND g.minid = 1
	AND (a.minid = 1) IS NOT UNKNOWN
	AND a.minid = 1
	AND (g.minid = a.minid) IS NOT UNKNOWN AND (g.maxid = a.maxid) IS NOT UNKNOWN AND (g.count = a.count) IS NOT UNKNOWN AND (g.eventcount = a.eventcount) IS NOT UNKNOWN
	AND g.minid = a.minid AND g.maxid = a.maxid AND g.count = a.count AND g.eventcount = a.eventcount
	AND g.count = a.eventcount
	AND (g.count = a.eventcount) IS NOT UNKNOWN
