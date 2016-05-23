WITH get_stats_by_insert_groups AS (
	SELECT
		event::jsonb->'testingStats'->>'programId' AS programid,
		event::jsonb->'testingStats'->>'dbOperationId' AS dboperationid,
		event::jsonb->'testingStats'->'dbOperationNum' AS dboperationnum,
		(event::jsonb->'testingStats'->>'dbOperationEventCount')::bigint AS eventcount,
		array_agg(id) AS ids,
		min(id) AS minid,
		max(id) AS maxid,
		count(id) AS count
	FROM events
	GROUP BY programId, dboperationid, dboperationnum, eventcount
),
update_stats_by_insert_groups AS (
	SELECT  programId, dboperationid, dboperationnum,
		--sort q.ids
		ARRAY(SELECT unnest(ids) ORDER BY 1) as ids,
		minid, maxid, count, eventcount,
		ARRAY(SELECT generate_series(minid, maxid)) AS generatedids
	FROM get_stats_by_insert_groups
),
--get the total events inserted into the events table from insert groups data
sum_eventcount_from_insert_groups AS (
	SELECT sum(eventcount)::bigint AS eventcount
	FROM get_stats_by_insert_groups
),
--get id statistics from all rows in the events table
get_agg_stats_from_all_rows AS (
	SELECT min(id) AS minid, max(id) AS maxid, count(id) as count
		FROM events
),
--get ordered ids from all rows in the events table.  id column in events table should be consecutive when ordered by ts, id.
get_ordered_ids_from_all_rows AS (
	SELECT ARRAY(SELECT id from events ORDER by ts, id) AS ids
),
--get id statistics and total events inserted into the table from insert groups data.  validate event ids and total events inserted.
--event ids must start at 1 and be consecutive integers.  total events inserted must equal total row count.
get_agg_stats_from_insert_groups AS (
	SELECT bool_and(s.insert_group_valid) AS insert_groups_valid, min(s.minid) AS minid, max(s.maxid) AS maxid,
		sum(s.count)::bigint AS count, sum(s.eventcount)::bigint AS eventcount
	FROM
		(SELECT
			CASE
				WHEN (count = eventcount) IS NOT UNKNOWN AND count = eventcount AND (ids = generatedids) IS NOT UNKNOWN AND ids = generatedids
				THEN true
				ELSE false
			END AS insert_group_valid,
			programid, dboperationid, dboperationnum, ids, generatedids, minid, maxid, count, eventcount
		FROM update_stats_by_insert_groups
		ORDER BY minid
		) AS s
),
--merge id and total events inserted stats from all rows.  validate event ids and total event inserted.
--event ids must start at 1 and be consecutive integers.  total events inserted must equal total row count.
merge_agg_stats_from_all_rows AS (
	SELECT
		CASE
			WHEN
				(a.minid = 1) IS NOT UNKNOWN
				AND a.minid = 1
				AND (oi.ids = ARRAY(SELECT generate_series(a.minid, a.maxid))) IS NOT UNKNOWN
				AND oi.ids = ARRAY(SELECT generate_series(a.minid, a.maxid))
				AND (a.count = array_length(ARRAY(SELECT generate_series(a.minid, a.maxid) ORDER BY 1), 1)) IS NOT UNKNOWN
				AND a.count = array_length(ARRAY(SELECT generate_series(a.minid, a.maxid) ORDER BY 1), 1)
				AND (a.count = e.eventcount) IS NOT UNKNOWN
				AND a.count = e.eventcount
			THEN true
			ELSE false
		END AS all_rows_valid,
		a.minid, a.maxid, a.count, e.eventcount
	FROM get_agg_stats_from_all_rows AS a, get_ordered_ids_from_all_rows AS oi, sum_eventcount_from_insert_groups AS e
)

SELECT
	CASE
		WHEN a.count = 0
		THEN 'VALID (NO ROWS IN TABLE)'
		WHEN a.all_rows_valid = false AND g.insert_groups_valid = false
		THEN 'INVALID (ALL ROWS and INSERT GROUPS)'
		WHEN a.all_rows_valid = false AND g.insert_groups_valid = false
		THEN 'INVALID (ALL ROWS)'
		WHEN g.insert_groups_valid = false
		THEN 'INVALID (INSERT GROUPS)'
		WHEN (g.minid = 1) IS NOT UNKNOWN
			AND g.minid = 1
			AND (a.minid = 1) IS NOT UNKNOWN
			AND a.minid = 1
			AND (g.minid = a.minid) IS NOT UNKNOWN
			AND (g.maxid = a.maxid) IS NOT UNKNOWN
			AND (g.count = a.count) IS NOT UNKNOWN
			AND (g.eventcount = a.eventcount) IS NOT UNKNOWN
			AND g.minid = a.minid
			AND g.maxid = a.maxid
			AND g.count = a.count
			AND g.eventcount = a.eventcount
			AND g.count = a.eventcount
			AND (g.count = a.eventcount) IS NOT UNKNOWN
		THEN 'VALID'
		ELSE 'INVALID'
	END AS events_table_status,
	g.minid AS minid_insert_groups, a.minid AS minid_all_rows, g.maxid AS maxid_insert_groups, a.maxid AS maxid_all_rows,
	g.count AS rowcount_insert_groups, a.count AS rowcount_all_rows, g.eventcount AS total_events_insert_groups, a.eventcount AS total_events_all_rows
FROM get_agg_stats_from_insert_groups AS g, merge_agg_stats_from_all_rows AS a
