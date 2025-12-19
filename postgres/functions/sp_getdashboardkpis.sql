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
    RETURNS TABLE(totalincidents integer, openincidents integer, inprogressincidents integer, closedincidents integer, breached integer) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
BEGIN
RETURN QUERY
WITH split_assignment AS (
    SELECT trim(upper(value)) AS v
    FROM regexp_split_to_table(p_assignmentgroup, ',') AS value
),
split_category AS (
    SELECT trim(upper(value)) AS v
    FROM regexp_split_to_table(p_category, ',') AS value
),
split_priority AS (
    SELECT trim(upper(value)) AS v
    FROM regexp_split_to_table(p_priority, ',') AS value
),
split_assigned AS (
    SELECT trim(upper(value)) AS v
    FROM regexp_split_to_table(p_assignedtoname, ',') AS value
),
split_state AS (
    SELECT trim(upper(value)) AS v
    FROM regexp_split_to_table(p_state, ',') AS value
),
FilteredIncidents AS (
    SELECT
        i.number,
        i.assigned_to,
        i.state,
        i.opened,
        i.resolved,
        i.priority,
        i.assignment_group,
        i.category
    FROM incidents i
    WHERE
        (p_fromdate IS NULL OR i.opened >= p_fromdate)
        AND (p_todate IS NULL OR i.opened <= p_todate)
        AND (p_assignmentgroup IS NULL OR upper(i.assignment_group) IN (SELECT v FROM split_assignment))
        AND (p_category IS NULL OR upper(i.category) IN (SELECT v FROM split_category))
        AND (p_priority IS NULL OR upper(i.priority) IN (SELECT v FROM split_priority))
        AND (p_assignedtoname IS NULL OR upper(i.assigned_to) IN (SELECT v FROM split_assigned))
        AND (p_state IS NULL OR upper(i.state) IN (SELECT v FROM split_state))
)
SELECT
    COUNT(*)::int AS totalIncidents,
    SUM(CASE WHEN upper(state) = 'OPEN' THEN 1 ELSE 0 END)::int AS openIncidents,
    SUM(CASE WHEN upper(state) = 'IN PROGRESS' THEN 1 ELSE 0 END)::int AS inProgressIncidents,
    SUM(CASE WHEN upper(state) = 'CLOSED' THEN 1 ELSE 0 END)::int AS closedIncidents,
    SUM(
        CASE
            WHEN fi.resolved IS NOT NULL
            AND (
                SELECT businessminutes
                FROM fn_slaminutes_itvf(
                    fi.opened,
                    fi.resolved,
                    CASE WHEN left(trim(coalesce(fi.priority,'4')),1) IN ('1','2')
                         THEN true ELSE false END
                )
            ) > (
                CASE left(trim(coalesce(fi.priority,'4')),1)
                    WHEN '1' THEN 120
                    WHEN '2' THEN 240
                    WHEN '3' THEN 1440
                    WHEN '4' THEN 7200
                    ELSE 7200
                END
            )
            THEN 1 ELSE 0
        END
    )::int AS breached
FROM FilteredIncidents fi;
END;
$BODY$;

ALTER FUNCTION public.sp_getdashboardkpis(timestamp without time zone, timestamp without time zone, text, text, text, text, text)
    OWNER TO postgres;

