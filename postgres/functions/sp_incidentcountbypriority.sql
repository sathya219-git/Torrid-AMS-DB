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
    RETURNS TABLE(priority text, state text, incidentcount bigint, totalcount bigint, avgresolvedtime text, totalresolvedtime text, breachedcount bigint) 
    LANGUAGE 'sql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$

WITH FilteredIncidents AS (
    -- STEP 1: Filter incidents first
    SELECT 
        i."number", i.priority, i.state, i.opened, i.resolved, i.category, i.assigned_to, i.assignment_group
    FROM 
        public.incidents i
    WHERE 
        (p_fromdate IS NULL OR i.opened >= p_fromdate)
        AND (p_todate IS NULL OR i.opened <= p_todate)
        -- Multiselect Filters using UNNEST(string_to_array())
        AND (p_assignmentgroup IS NULL OR UPPER(i.assignment_group) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_assignmentgroup, ',')) u))
        AND (p_category IS NULL OR UPPER(i.category) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_category, ',')) u))
        AND (p_priority IS NULL OR UPPER(i.priority) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_priority, ',')) u))
        AND (p_assignedtoname IS NULL OR UPPER(i.assigned_to) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_assignedtoname, ',')) u))
        AND (p_state IS NULL OR UPPER(i.state) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_state, ',')) u))
)

, BusinessCalc AS (
    -- STEP 2: compute SLA-aware minutes per incident (using SLA TVF)
    SELECT
        fi.*,
        SLABM.businessminutes AS businessminutesresolved
    FROM 
        FilteredIncidents fi
    -- Replace CROSS APPLY with JOIN LATERAL
    JOIN LATERAL public.fn_slaminutes_itvf(
        fi.opened,
        fi.resolved,
        -- SLA includes weekends for P1/P2, excludes for P3/P4
        CASE WHEN LEFT(TRIM(COALESCE(fi.priority, '4')),1) IN ('1','2') THEN TRUE ELSE FALSE END
    ) AS SLABM ON TRUE
)

, ResolvedData AS (
    -- STEP 3: only closed incidents (use TVF minutes as MinutesTaken)
    SELECT 
        bc.priority,
        bc.state,
        bc.businessminutesresolved AS minutestaken,
        -- SLA mapping by priority (first digit)
        CASE LEFT(TRIM(COALESCE(bc.priority,'4')),1)
            WHEN '1' THEN 120    -- Critical: 2 hours
            WHEN '2' THEN 240    -- High:     4 hours
            WHEN '3' THEN 1440   -- Moderate: 24 hours (1 day)
            WHEN '4' THEN 7200   -- Low:      120 hours (5 days)
            ELSE 7200
        END AS sla_minutes,
        -- breached flag: 1 if TVF minutes (Opened->Resolved) > SLA, else 0
        CASE
            WHEN bc.businessminutesresolved > 
                 CASE LEFT(TRIM(COALESCE(bc.priority,'4')),1)
                     WHEN '1' THEN 120 WHEN '2' THEN 240 WHEN '3' THEN 1440 WHEN '4' THEN 7200 ELSE 7200
                 END
            THEN 1 ELSE 0
        END AS isbreached
    FROM BusinessCalc bc
    WHERE bc.resolved IS NOT NULL
)

, TotalAndAvg AS (
    -- STEP 4: Totals and averages aggregated by Priority, State
    SELECT 
        rd.priority,
        rd.state,
        COUNT(*)::bigint AS closedcount,
        SUM(rd.minutestaken)::numeric AS totalminutes, -- Keep as numeric for AVG calc
        AVG(rd.minutestaken)::double precision AS avgminutes, -- Use double precision for AVG
        SUM(rd.isbreached)::bigint AS breachedcount
    FROM ResolvedData rd
    GROUP BY rd.priority, rd.state
)

, IncidentCount AS (
    -- Total incident counts per priority/state
    SELECT 
        priority, 
        state, 
        COUNT(*)::bigint AS incidentcount
    FROM FilteredIncidents
    GROUP BY priority, state
)

, TotalPerPriority AS (
    -- Total incidents per priority across all states
    SELECT 
        priority, 
        SUM(incidentcount)::bigint AS totalcount
    FROM IncidentCount
    GROUP BY priority
)

SELECT 
    ic.priority,
    ic.state,
    ic.incidentcount,
    tp.totalcount, -- Total incidents per priority across states

    -- AvgResolvedTime formatted as days, hours, minutes (from AvgMinutes)
    CASE 
        WHEN ta.avgminutes IS NULL THEN NULL
        ELSE 
            CONCAT(
                CASE WHEN ta.avgminutes >= 1440 THEN CONCAT(FLOOR(ta.avgminutes / 1440)::bigint, ' days ') ELSE '' END,
                CASE WHEN (FLOOR(ta.avgminutes)::bigint % 1440) / 60 > 0 THEN CONCAT((FLOOR(ta.avgminutes)::bigint % 1440) / 60, ' hours ') ELSE '' END,
                CONCAT(FLOOR(ta.avgminutes)::bigint % 60, ' minutes')
            )
    END AS avgresolvedtime,

    -- TotalResolvedTime formatted as days, hours, minutes (from TotalMinutes)
    CASE 
        WHEN ta.totalminutes IS NULL THEN NULL
        ELSE 
            CONCAT(
                CASE WHEN ta.totalminutes >= 1440 THEN CONCAT(FLOOR(ta.totalminutes / 1440)::bigint, ' days ') ELSE '' END,
                CASE WHEN (FLOOR(ta.totalminutes)::bigint % 1440) / 60 > 0 THEN CONCAT((FLOOR(ta.totalminutes)::bigint % 1440) / 60, ' hours ') ELSE '' END,
                CONCAT(FLOOR(ta.totalminutes)::bigint % 60, ' minutes')
            )
    END AS totalresolvedtime,

    -- count of breached incidents
    COALESCE(ta.breachedcount, 0) AS breachedcount

FROM IncidentCount ic
LEFT JOIN TotalAndAvg ta 
    ON ic.priority = ta.priority AND ic.state = ta.state
LEFT JOIN TotalPerPriority tp
    ON ic.priority = tp.priority
ORDER BY ic.priority, ic.state;

$BODY$;

ALTER FUNCTION public.sp_incidentcountbypriority(timestamp without time zone, timestamp without time zone, text, text, text, text, text)
    OWNER TO postgres;

