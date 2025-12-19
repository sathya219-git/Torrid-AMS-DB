-- FUNCTION: public.sp_getdashboardkpis(timestamp without time zone, timestamp without time zone, text, text, text, text, text)

-- DROP FUNCTION IF EXISTS public.sp_getdashboardkpis(timestamp without time zone, timestamp without time zone, text, text, text, text, text);

CREATE OR REPLACE FUNCTION public.sp_getdashboardkpis(
	p_fromdate timestamp without time zone DEFAULT NULL::timestamp without time zone,
	p_todate timestamp without time zone DEFAULT NULL::timestamp without time zone,
	p_assignmentgroup text DEFAULT NULL::text,
	p_category text DEFAULT NULL::text,
	p_priority text DEFAULT NULL::text,
	p_assignedtoname text DEFAULT NULL::text,
	p_state text DEFAULT NULL::text)
    RETURNS TABLE(totalincidents integer, breached_count integer, open_count integer, open_more_15_days integer, open_less_15_days integer, state_counts jsonb) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
BEGIN
RETURN QUERY
WITH FilteredIncidents AS (
    SELECT i.state, i.priority, i.opened, i.resolved
    FROM incidents i
    WHERE
        (p_fromdate IS NULL OR i.opened >= p_fromdate)
        AND (p_todate IS NULL OR i.opened <= p_todate)
        AND (p_assignmentgroup IS NULL OR UPPER(i.assignment_group) IN (SELECT TRIM(UPPER(v)) FROM UNNEST(STRING_TO_ARRAY(p_assignmentgroup, ',')) v))
        AND (p_category IS NULL OR UPPER(i.category) IN (SELECT TRIM(UPPER(v)) FROM UNNEST(STRING_TO_ARRAY(p_category, ',')) v))
        AND (p_priority IS NULL OR UPPER(i.priority) IN (SELECT TRIM(UPPER(v)) FROM UNNEST(STRING_TO_ARRAY(p_priority, ',')) v))
        AND (p_assignedtoname IS NULL OR UPPER(i.assigned_to) IN (SELECT TRIM(UPPER(v)) FROM UNNEST(STRING_TO_ARRAY(p_assignedtoname, ',')) v))
        AND (p_state IS NULL OR UPPER(i.state) IN (SELECT TRIM(UPPER(v)) FROM UNNEST(STRING_TO_ARRAY(p_state, ',')) v))
),
SLA_Processing AS (
    SELECT
        fi.state, fi.resolved,
        SLABM.businessminutes AS minutes_current,
        CASE LEFT(TRIM(COALESCE(fi.priority,'4')),1)
            WHEN '1' THEN 120 WHEN '2' THEN 240 WHEN '3' THEN 1440 ELSE 7200
        END AS sla_threshold
    FROM FilteredIncidents fi
    JOIN LATERAL public.fn_slaminutes_itvf(
        fi.opened,
        COALESCE(fi.resolved, NOW()::timestamp),
        CASE WHEN LEFT(TRIM(COALESCE(fi.priority, '4')),1) IN ('1','2') THEN TRUE ELSE FALSE END
    ) AS SLABM ON TRUE
),
DynamicStates AS (
    -- Group by state and create a key-value pair for each
    SELECT jsonb_object_agg(COALESCE(state, 'Unknown'), count_val) as counts
    FROM (
        SELECT state, COUNT(*)::int as count_val 
        FROM SLA_Processing 
        GROUP BY state
    ) s
)
SELECT
    COUNT(*)::int AS totalincidents,
    SUM(CASE WHEN sp.minutes_current > sp.sla_threshold THEN 1 ELSE 0 END)::int AS breached_count,
    SUM(CASE WHEN sp.resolved IS NULL THEN 1 ELSE 0 END)::int AS open_count,
    SUM(CASE WHEN sp.resolved IS NULL AND (sp.minutes_current / 1440.0) > 15 THEN 1 ELSE 0 END)::int AS open_more_15_days,
    SUM(CASE WHEN sp.resolved IS NULL AND (sp.minutes_current / 1440.0) <= 15 THEN 1 ELSE 0 END)::int AS open_less_15_days,
    (SELECT counts FROM DynamicStates)
FROM SLA_Processing sp;
END;
$BODY$;

ALTER FUNCTION public.sp_getdashboardkpis(timestamp without time zone, timestamp without time zone, text, text, text, text, text)
    OWNER TO postgres;

