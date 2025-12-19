-- FUNCTION: public.sp_incidentcountbypriority(timestamp without time zone, timestamp without time zone, text, text, text, text, text)

-- DROP FUNCTION IF EXISTS public.sp_incidentcountbypriority(timestamp without time zone, timestamp without time zone, text, text, text, text, text);

CREATE OR REPLACE FUNCTION public.sp_incidentcountbypriority(
	p_fromdate timestamp without time zone DEFAULT NULL::timestamp without time zone,
	p_todate timestamp without time zone DEFAULT NULL::timestamp without time zone,
	p_category text DEFAULT NULL::text,
	p_assignmentgroup text DEFAULT NULL::text,
	p_priority text DEFAULT NULL::text,
	p_assignedtoname text DEFAULT NULL::text,
	p_state text DEFAULT NULL::text)
    RETURNS TABLE(priority text, state text, incidentcount bigint, totalcount bigint, avgresolvedtime text, totalresolvedtime text, breachedcount bigint, open_more_15_days bigint, open_less_15_days bigint) 
    LANGUAGE 'sql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$

WITH FilteredIncidents AS (
    SELECT 
        i."number", i.priority, i.state, i.opened, i.resolved
    FROM 
        public.incidents i
    WHERE 
        (p_fromdate IS NULL OR i.opened >= p_fromdate)
        AND (p_todate IS NULL OR i.opened <= p_todate)
        AND (p_assignmentgroup IS NULL OR UPPER(i.assignment_group) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_assignmentgroup, ',')) u))
        AND (p_category IS NULL OR UPPER(i.category) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_category, ',')) u))
        AND (p_priority IS NULL OR UPPER(i.priority) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_priority, ',')) u))
        AND (p_assignedtoname IS NULL OR UPPER(i.assigned_to) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_assignedtoname, ',')) u))
        AND (p_state IS NULL OR UPPER(i.state) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_state, ',')) u))
)

, BusinessCalc AS (
    SELECT
        fi.*,
        SLABM.businessminutes AS minutes_current
    FROM 
        FilteredIncidents fi
    JOIN LATERAL public.fn_slaminutes_itvf(
        fi.opened,
        COALESCE(fi.resolved, NOW()::timestamp), -- Live aging logic
        CASE WHEN LEFT(TRIM(COALESCE(fi.priority, '4')),1) IN ('1','2') THEN TRUE ELSE FALSE END
    ) AS SLABM ON TRUE
)

, ProcessedData AS (
    SELECT 
        bc.priority,
        bc.state,
        bc.resolved,
        bc.minutes_current,
        CASE LEFT(TRIM(COALESCE(bc.priority,'4')),1)
            WHEN '1' THEN 120
            WHEN '2' THEN 240
            WHEN '3' THEN 1440
            ELSE 7200
        END AS sla_threshold
    FROM BusinessCalc bc
)

, AggregatedMetrics AS (
    SELECT 
        pd.priority,
        pd.state,
        COUNT(*)::bigint AS incidentcount,
        SUM(pd.minutes_current)::numeric AS totalminutes,
        AVG(pd.minutes_current)::double precision AS avgminutes,
        -- Correct Breach Count: uses business minutes vs threshold
        SUM(CASE WHEN pd.minutes_current > pd.sla_threshold THEN 1 ELSE 0 END)::bigint AS breachedcount,
        -- Aging: Business Days > 15 for tickets where resolved IS NULL
        SUM(CASE WHEN pd.resolved IS NULL AND (pd.minutes_current / 1440.0) > 15 THEN 1 ELSE 0 END)::bigint AS open_more_15_days,
        SUM(CASE WHEN pd.resolved IS NULL AND (pd.minutes_current / 1440.0) <= 15 THEN 1 ELSE 0 END)::bigint AS open_less_15_days
    FROM ProcessedData pd
    GROUP BY pd.priority, pd.state
)

, TotalPerPriority AS (
    SELECT 
        priority, 
        SUM(incidentcount)::bigint AS totalcount
    FROM AggregatedMetrics
    GROUP BY priority
)

SELECT 
    am.priority,
    am.state,
    am.incidentcount,
    tp.totalcount,
    CASE 
        WHEN am.avgminutes IS NULL THEN NULL
        ELSE 
            CONCAT(
                CASE WHEN am.avgminutes >= 1440 THEN CONCAT(FLOOR(am.avgminutes / 1440)::bigint, ' days ') ELSE '' END,
                CASE WHEN (FLOOR(am.avgminutes)::bigint % 1440) / 60 > 0 THEN CONCAT((FLOOR(am.avgminutes)::bigint % 1440) / 60, ' hours ') ELSE '' END,
                CONCAT(FLOOR(am.avgminutes)::bigint % 60, ' minutes')
            )
    END AS avgresolvedtime,
    CASE 
        WHEN am.totalminutes IS NULL THEN NULL
        ELSE 
            CONCAT(
                CASE WHEN am.totalminutes >= 1440 THEN CONCAT(FLOOR(am.totalminutes / 1440)::bigint, ' days ') ELSE '' END,
                CASE WHEN (FLOOR(am.totalminutes)::bigint % 1440) / 60 > 0 THEN CONCAT((FLOOR(am.totalminutes)::bigint % 1440) / 60, ' hours ') ELSE '' END,
                CONCAT(FLOOR(am.totalminutes)::bigint % 60, ' minutes')
            )
    END AS totalresolvedtime,
    am.breachedcount,
    am.open_more_15_days,
    am.open_less_15_days
FROM AggregatedMetrics am
LEFT JOIN TotalPerPriority tp ON am.priority = tp.priority
ORDER BY am.priority, am.state;

$BODY$;

ALTER FUNCTION public.sp_incidentcountbypriority(timestamp without time zone, timestamp without time zone, text, text, text, text, text)
    OWNER TO postgres;

