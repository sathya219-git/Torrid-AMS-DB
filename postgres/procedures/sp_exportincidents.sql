-- FUNCTION: public.sp_exportincidents(timestamp without time zone, timestamp without time zone, text, text, text, text, text)

-- DROP FUNCTION IF EXISTS public.sp_exportincidents(timestamp without time zone, timestamp without time zone, text, text, text, text, text);

CREATE OR REPLACE FUNCTION public.sp_exportincidents(
	p_fromdate timestamp without time zone DEFAULT NULL::timestamp without time zone,
	p_todate timestamp without time zone DEFAULT NULL::timestamp without time zone,
	p_category text DEFAULT NULL::text,
	p_assignmentgroup text DEFAULT NULL::text,
	p_priority text DEFAULT NULL::text,
	p_assignedtoname text DEFAULT NULL::text,
	p_state text DEFAULT NULL::text)
    RETURNS TABLE(number text, opened timestamp without time zone, short_description text, caller text, priority text, state text, category text, assignment_group text, assigned_to text, updated timestamp without time zone, updated_by text, child_incidents integer, sla_due numeric, severity text, subcategory text, resolution_notes text, resolved timestamp without time zone, sla_calculation numeric, parent_incident numeric, parent text, task_type text) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
BEGIN
    RETURN QUERY
    WITH split_assignment AS (
        SELECT trim(upper(value)) AS v FROM regexp_split_to_table(p_assignmentgroup, ',') AS value
    ),
    split_category AS (
        SELECT trim(upper(value)) AS v FROM regexp_split_to_table(p_category, ',') AS value
    ),
    split_priority AS (
        SELECT trim(upper(value)) AS v FROM regexp_split_to_table(p_priority, ',') AS value
    ),
    split_assigned AS (
        SELECT trim(upper(value)) AS v FROM regexp_split_to_table(p_assignedtoname, ',') AS value
    ),
    split_state AS (
        SELECT trim(upper(value)) AS v FROM regexp_split_to_table(p_state, ',') AS value
    )
    SELECT
        i.number,
        i.opened,
        i.short_description,
        i.caller,
        i.priority,
        i.state,
        i.category,
        i.assignment_group,
        i.assigned_to,
        i.updated,
        i.updated_by,
        i.child_incidents,
        i.sla_due,
        i.severity,
        i.subcategory,
        i.resolution_notes,
        i.resolved,
        i.sla_calculation,
        i.parent_incident,   -- numeric
        i.parent,
        i.task_type
    FROM incidents i
    WHERE
        ((p_fromdate IS NULL AND p_todate IS NULL) OR (i.opened BETWEEN p_fromdate AND p_todate))
        AND (p_assignmentgroup IS NULL OR upper(i.assignment_group) IN (SELECT v FROM split_assignment))
        AND (p_category IS NULL OR upper(i.category) IN (SELECT v FROM split_category))
        AND (p_priority IS NULL OR upper(i.priority) IN (SELECT v FROM split_priority))
        AND (p_assignedtoname IS NULL OR upper(i.assigned_to) IN (SELECT v FROM split_assigned))
        AND (p_state IS NULL OR upper(i.state) IN (SELECT v FROM split_state))
    ORDER BY i.opened DESC;
END;
$BODY$;

ALTER FUNCTION public.sp_exportincidents(timestamp without time zone, timestamp without time zone, text, text, text, text, text)
    OWNER TO postgres;

