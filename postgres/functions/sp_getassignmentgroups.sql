-- FUNCTION: public.sp_getassignmentgroups(timestamp without time zone, timestamp without time zone, text, text, text, text, text)

-- DROP FUNCTION IF EXISTS public.sp_getassignmentgroups(timestamp without time zone, timestamp without time zone, text, text, text, text, text);

CREATE OR REPLACE FUNCTION public.sp_getassignmentgroups(
	p_fromdate timestamp without time zone DEFAULT NULL::timestamp without time zone,
	p_todate timestamp without time zone DEFAULT NULL::timestamp without time zone,
	p_assignmentgroup text DEFAULT NULL::text,
	p_category text DEFAULT NULL::text,
	p_state text DEFAULT NULL::text,
	p_priority text DEFAULT NULL::text,
	p_assignedtoname text DEFAULT NULL::text)
    RETURNS TABLE(assignmentgroupname text, incidentcount bigint) 
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
    split_state AS (
        SELECT trim(upper(value)) AS v
        FROM regexp_split_to_table(p_state, ',') AS value
    ),
    split_assigned AS (
        SELECT trim(upper(value)) AS v
        FROM regexp_split_to_table(p_assignedtoname, ',') AS value
    )
    SELECT
        i.assignment_group AS assignmentgroupname,
        COUNT(*) AS incidentcount
    FROM incidents i
    WHERE
        ((p_fromdate IS NULL AND p_todate IS NULL) OR (i.opened BETWEEN p_fromdate AND p_todate))
        AND (p_assignmentgroup IS NULL OR upper(i.assignment_group) IN (SELECT v FROM split_assignment))
        AND (p_category IS NULL OR upper(i.category) IN (SELECT v FROM split_category))
        AND (p_priority IS NULL OR upper(i.priority) IN (SELECT v FROM split_priority))
        AND (p_state IS NULL OR upper(i.state) IN (SELECT v FROM split_state))
        AND (p_assignedtoname IS NULL OR upper(i.assigned_to) IN (SELECT v FROM split_assigned))
    GROUP BY i.assignment_group
    ORDER BY i.assignment_group;
END;
$BODY$;

ALTER FUNCTION public.sp_getassignmentgroups(timestamp without time zone, timestamp without time zone, text, text, text, text, text)
    OWNER TO postgres;

