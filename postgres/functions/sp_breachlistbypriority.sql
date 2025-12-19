-- FUNCTION: public.sp_breachlistbypriority(timestamp without time zone, timestamp without time zone, text, text, text, text, text, text, integer, integer, character varying, character varying, text, text, text)

-- DROP FUNCTION IF EXISTS public.sp_breachlistbypriority(timestamp without time zone, timestamp without time zone, text, text, text, text, text, text, integer, integer, character varying, character varying, text, text, text);

CREATE OR REPLACE FUNCTION public.sp_breachlistbypriority(
	p_fromdate timestamp without time zone DEFAULT NULL::timestamp without time zone,
	p_todate timestamp without time zone DEFAULT NULL::timestamp without time zone,
	p_category text DEFAULT NULL::text,
	p_assignmentgroup text DEFAULT NULL::text,
	p_assignedtoname text DEFAULT NULL::text,
	p_state text DEFAULT NULL::text,
	p_search text DEFAULT NULL::text,
	p_priority text DEFAULT NULL::text,
	p_pagenumber integer DEFAULT 1,
	p_pagesize integer DEFAULT 8,
	p_sortby character varying DEFAULT 'Updated'::character varying,
	p_sortorder character varying DEFAULT 'DESC'::character varying,
	p_incidentnumber text DEFAULT NULL::text,
	p_actualresolvedtime text DEFAULT NULL::text,
	p_breachsla text DEFAULT NULL::text)
    RETURNS TABLE(pagenumber integer, pagesize integer, totalpages integer, totalelements integer, incidentnumber text, assignedto text, shortdescription text, category text, state text, created timestamp without time zone, resolveddatetime timestamp without time zone, updated timestamp without time zone, actualresolvedtime text, breachsla text) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE
    v_page_number INT := COALESCE(p_pagenumber, 1);
    v_page_size INT := COALESCE(p_pagesize, 8);
    v_offset INT;
    v_total_count INT;
    v_total_pages INT;
BEGIN
    -- Guard rails
    IF v_page_number < 1 THEN v_page_number := 1; END IF;
    IF v_page_size < 0 THEN v_page_size := 8; END IF;
    v_offset := (v_page_number - 1) * CASE WHEN v_page_size = 0 THEN 1 ELSE v_page_size END;

    -- STEP 1: Initial filtering
    CREATE TEMP TABLE filtered_incidents ON COMMIT DROP AS
    SELECT
        i."number", i.assigned_to, i.short_description, i.category, i.state,
        i.opened, i.resolved, i.updated, i.priority, i.assignment_group
    FROM public.incidents i
    WHERE
        ((p_fromdate IS NULL AND p_todate IS NULL) OR (i.opened BETWEEN p_fromdate AND p_todate))
        AND (p_assignmentgroup IS NULL OR UPPER(i.assignment_group) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_assignmentgroup, ',')) u))
        AND (p_category IS NULL OR UPPER(i.category) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_category, ',')) u))
        AND (p_assignedtoname IS NULL OR UPPER(i.assigned_to) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_assignedtoname, ',')) u))
        AND (p_state IS NULL OR UPPER(i.state) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_state, ',')) u))
        AND (p_priority IS NULL OR UPPER(i.priority) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_priority, ',')) u));

    -- STEP 2: Logic for Breach calculation (Applying NOW() logic)
    EXECUTE '
    CREATE TEMP TABLE final_incidents_search ON COMMIT DROP AS
    WITH Final AS (
        SELECT
            f."number" AS "Incident Number",
            f.assigned_to AS "Assigned To",
            f.short_description AS "Short Description",
            f.category AS "Category",
            f.state AS "State",
            f.opened AS "Created",
            f.resolved AS "Resolved Date & Time",
            f.updated AS "Updated",
            f.priority AS "Priority",
            ARHBM.businessminutes AS "ActualResolvedMinutes",
            CASE LEFT(TRIM(COALESCE(f.priority,''4'')),1)
                WHEN ''1'' THEN 120
                WHEN ''2'' THEN 240
                WHEN ''3'' THEN 1440
                ELSE 7200
            END AS "SLA_Minutes"
        FROM
            filtered_incidents f
        JOIN LATERAL public.fn_slaminutes_itvf(
            f.opened,
            COALESCE(f.resolved, NOW()::timestamp),
            CASE WHEN LEFT(TRIM(COALESCE(f.priority,''4'')),1) IN (''1'', ''2'') THEN TRUE ELSE FALSE END
        ) AS ARHBM ON TRUE
        WHERE ARHBM.businessminutes > 
             CASE LEFT(TRIM(COALESCE(f.priority,''4'')),1)
                 WHEN ''1'' THEN 120 WHEN ''2'' THEN 240 WHEN ''3'' THEN 1440 ELSE 7200
             END
    )
    SELECT
        f.*,
        f."ActualResolvedMinutes" - f."SLA_Minutes" AS "BreachMinutes",
        CASE
            WHEN f."ActualResolvedMinutes" IS NULL THEN ''N/A''
            ELSE TRIM(
                CASE WHEN f."ActualResolvedMinutes"/1440 >= 1 THEN CONCAT(FLOOR(f."ActualResolvedMinutes"/1440), '' days '') ELSE '''' END ||
                CASE WHEN (f."ActualResolvedMinutes" % 1440) / 60 >= 1 THEN CONCAT(FLOOR((f."ActualResolvedMinutes" % 1440) / 60), '' hours '') ELSE '''' END ||
                CASE WHEN f."ActualResolvedMinutes" % 60 > 0 THEN CONCAT(FLOOR(f."ActualResolvedMinutes" % 60), '' mins'') ELSE '''' END
            )
        END AS "Actual Resolved Time",
        CASE
            WHEN f."ActualResolvedMinutes" <= f."SLA_Minutes" THEN ''No Breach''
            ELSE
                TRIM(
                    CASE WHEN (f."ActualResolvedMinutes" - f."SLA_Minutes")/1440 >= 1 
                        THEN CONCAT(FLOOR((f."ActualResolvedMinutes" - f."SLA_Minutes")/1440), '' days '') ELSE '''' END ||
                    CASE WHEN ((f."ActualResolvedMinutes" - f."SLA_Minutes") % 1440) / 60 >= 1 
                        THEN CONCAT(FLOOR(((f."ActualResolvedMinutes" - f."SLA_Minutes") % 1440) / 60), '' hours '') ELSE '''' END ||
                    CASE WHEN (f."ActualResolvedMinutes" - f."SLA_Minutes") % 60 > 0 
                        THEN CONCAT(FLOOR((f."ActualResolvedMinutes" - f."SLA_Minutes") % 60), '' mins'') ELSE '''' END
                )
        END AS "Breach SLA"
    FROM
        Final f
    WHERE
        ($1 IS NULL OR UPPER(f."Incident Number") LIKE ''%'' || UPPER($1) || ''%'')
        AND ($2 IS NULL OR 
               UPPER(f."Incident Number") LIKE ''%'' || UPPER($2) || ''%'' OR 
               UPPER(f."Assigned To") LIKE ''%'' || UPPER($2) || ''%'' OR 
               UPPER(f."Short Description") LIKE ''%'' || UPPER($2) || ''%'' OR 
               UPPER(f."Category") LIKE ''%'' || UPPER($2) || ''%'' OR 
               UPPER(f."State") LIKE ''%'' || UPPER($2) || ''%''
            );' 
    USING p_incidentnumber, p_search;

    -- STEP 3: Pagination calculation
    SELECT COUNT(*) INTO v_total_count FROM final_incidents_search;
    v_total_pages := CASE WHEN v_page_size = 0 THEN 1 ELSE CEIL(1.0 * v_total_count / v_page_size)::INT END;
    IF v_total_count = 0 THEN v_total_pages := 1; END IF;

    -- STEP 4: Sorting and Final Output
    RETURN QUERY EXECUTE format('
        SELECT
            %s::INTEGER, %s::INTEGER, %s::INTEGER, %s::INTEGER,
            "Incident Number", "Assigned To", "Short Description", "Category", "State",
            "Created", "Resolved Date & Time", "Updated", "Actual Resolved Time", "Breach SLA"
        FROM final_incidents_search
        ORDER BY
            CASE WHEN $1 = ''Number'' AND $2 = ''ASC'' THEN "Incident Number" END ASC,
            CASE WHEN $1 = ''Number'' AND $2 = ''DESC'' THEN "Incident Number" END DESC,
            CASE WHEN $1 = ''AssignedTo'' AND $2 = ''ASC'' THEN "Assigned To" END ASC,
            CASE WHEN $1 = ''AssignedTo'' AND $2 = ''DESC'' THEN "Assigned To" END DESC,
            CASE WHEN $1 = ''ShortDescription'' AND $2 = ''ASC'' THEN "Short Description" END ASC,
            CASE WHEN $1 = ''ShortDescription'' AND $2 = ''DESC'' THEN "Short Description" END DESC,
            CASE WHEN $1 = ''Category'' AND $2 = ''ASC'' THEN "Category" END ASC,
            CASE WHEN $1 = ''Category'' AND $2 = ''DESC'' THEN "Category" END DESC,
            CASE WHEN $1 = ''State'' AND $2 = ''ASC'' THEN "State" END ASC,
            CASE WHEN $1 = ''State'' AND $2 = ''DESC'' THEN "State" END DESC,
            CASE WHEN $1 = ''Created'' AND $2 = ''ASC'' THEN "Created" END ASC,
            CASE WHEN $1 = ''Created'' AND $2 = ''DESC'' THEN "Created" END DESC,
            CASE WHEN $1 = ''Resolved'' AND $2 = ''ASC'' THEN "Resolved Date & Time" END ASC,
            CASE WHEN $1 = ''Resolved'' AND $2 = ''DESC'' THEN "Resolved Date & Time" END DESC,
            CASE WHEN $1 = ''Updated'' AND $2 = ''ASC'' THEN "Updated" END ASC,
            CASE WHEN $1 = ''Updated'' AND $2 = ''DESC'' THEN "Updated" END DESC,
            CASE WHEN $1 = ''ActualResolvedTime'' AND $2 = ''ASC'' THEN COALESCE("ActualResolvedMinutes", 9223372036854775807) END ASC,
            CASE WHEN $1 = ''ActualResolvedTime'' AND $2 = ''DESC'' THEN COALESCE("ActualResolvedMinutes", -9223372036854775808) END DESC,
            CASE WHEN $1 = ''BreachSLA'' AND $2 = ''ASC'' THEN COALESCE("BreachMinutes", 9223372036854775807) END ASC,
            CASE WHEN $1 = ''BreachSLA'' AND $2 = ''DESC'' THEN COALESCE("BreachMinutes", -9223372036854775808) END DESC
        %s %s'
        , v_page_number, v_page_size, v_total_pages, v_total_count
        , CASE WHEN v_page_size > 0 THEN CONCAT(' OFFSET ', v_offset, ' ROWS ') ELSE '' END
        , CASE WHEN v_page_size > 0 THEN CONCAT(' FETCH NEXT ', v_page_size, ' ROWS ONLY ') ELSE '' END
    ) USING p_sortby, p_sortorder;
    
END;
$BODY$;

ALTER FUNCTION public.sp_breachlistbypriority(timestamp without time zone, timestamp without time zone, text, text, text, text, text, text, integer, integer, character varying, character varying, text, text, text)
    OWNER TO postgres;

