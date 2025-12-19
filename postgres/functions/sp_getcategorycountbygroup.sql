-- FUNCTION: public.sp_getcategorycountbygroup(timestamp without time zone, timestamp without time zone, text)

-- DROP FUNCTION IF EXISTS public.sp_getcategorycountbygroup(timestamp without time zone, timestamp without time zone, text);

CREATE OR REPLACE FUNCTION public.sp_getcategorycountbygroup(
	p_fromdate timestamp without time zone DEFAULT NULL::timestamp without time zone,
	p_todate timestamp without time zone DEFAULT NULL::timestamp without time zone,
	p_assignmentgroup text DEFAULT NULL::text)
    RETURNS TABLE(categoryname text, incidentcount bigint) 
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
    )
    SELECT
        i.category AS categoryname,
        COUNT(*) AS incidentcount
    FROM incidents i
    WHERE
        ((p_fromdate IS NULL AND p_todate IS NULL) OR (i.opened BETWEEN p_fromdate AND p_todate))
        AND (p_assignmentgroup IS NULL OR upper(i.assignment_group) IN (SELECT v FROM split_assignment))
    GROUP BY i.category
    ORDER BY incidentcount DESC;
END;
$BODY$;

ALTER FUNCTION public.sp_getcategorycountbygroup(timestamp without time zone, timestamp without time zone, text)
    OWNER TO postgres;

