-- FUNCTION: public.sp_getincidentdetailsbypriority(timestamp without time zone, timestamp without time zone, text, text, text, text, text, text, integer, integer, character varying, character varying)

-- DROP FUNCTION IF EXISTS public.sp_getincidentdetailsbypriority(timestamp without time zone, timestamp without time zone, text, text, text, text, text, text, integer, integer, character varying, character varying);

CREATE OR REPLACE FUNCTION public.sp_getincidentdetailsbypriority(
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
	p_sortorder character varying DEFAULT 'DESC'::character varying)
    RETURNS TABLE(pagenumber integer, pagesize integer, totalpages integer, totalelements integer, incidentno text, assignedto text, shortdescription text, category text, state text, created timestamp without time zone, resolveddatetime timestamp without time zone, updated timestamp without time zone, priority text, actualresolvedtime text, breachsla text) 
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

    -- STEP 1: Base filtered set
    CREATE TEMP TABLE filtered_incidents ON COMMIT DROP AS
    SELECT
        i."number", i.assigned_to, i.short_description, i.category, i.state,
        i.opened, i.resolved, i.updated, i.priority, i.assignment_group
    FROM
        public.incidents i
    WHERE
        ((p_fromdate IS NULL) OR (i.opened >= p_fromdate))
        AND ((p_todate IS NULL) OR (i.opened <= p_todate))
        AND (p_assignmentgroup IS NULL OR UPPER(i.assignment_group) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_assignmentgroup, ',')) u))
        AND (p_category IS NULL OR UPPER(i.category) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_category, ',')) u))
        AND (p_assignedtoname IS NULL OR UPPER(i.assigned_to) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_assignedtoname, ',')) u))
        AND (p_state IS NULL OR UPPER(i.state) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_state, ',')) u))
        AND (p_priority IS NULL OR UPPER(i.priority) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_priority, ',')) u));

    -- STEP 2: Compute calculated fields with Live Aging Breach Logic
    CREATE TEMP TABLE final_temp ON COMMIT DROP AS
    WITH Final AS (
        SELECT
            f."number" AS incidentno,
            f.assigned_to AS assignedto,
            f.short_description AS shortdescription,
            f.category AS category,
            f.state AS state,
            f.opened AS created,
            f.resolved AS resolveddatetime,
            f.updated AS updated,
            f.priority AS priority,

            -- UPDATED LOGIC: Calculate minutes from Opened to (Resolved OR Now)
            ARHBM.businessminutes AS actualresolvedminutes,

            -- SLA mapping
            CASE LEFT(TRIM(COALESCE(f.priority,'4')),1)
                WHEN '1' THEN 120
                WHEN '2' THEN 240
                WHEN '3' THEN 1440
                ELSE 7200
            END AS sla_minutes
        FROM filtered_incidents f
        JOIN LATERAL public.fn_slaminutes_itvf(
            f.opened,
            COALESCE(f.resolved, NOW()::timestamp), -- Use NOW for non-resolved states
            CASE WHEN LEFT(TRIM(COALESCE(f.priority,'4')),1) IN ('1','2') THEN TRUE ELSE FALSE END
        ) AS ARHBM ON TRUE
    ),
    Calculated AS (
        SELECT
            *,
            -- Breach minutes calculation
            COALESCE(actualresolvedminutes, 0) - sla_minutes AS breachminutes_raw
        FROM Final
    )
    SELECT 
        c.incidentno, c.assignedto, c.shortdescription, c.category, c.state, c.created, c.resolveddatetime, c.updated, c.priority,
        c.actualresolvedminutes, c.sla_minutes, 
        GREATEST(0, c.breachminutes_raw) as breachminutes,
        
        -- Formatted ActualResolvedTime
        CASE
            WHEN c.actualresolvedminutes IS NULL OR c.actualresolvedminutes = 0 THEN 'N/A'
            ELSE TRIM(
                CASE WHEN c.actualresolvedminutes / 1440 >= 1 THEN CONCAT(FLOOR(c.actualresolvedminutes / 1440), ' days ') ELSE '' END ||
                CASE WHEN (c.actualresolvedminutes % 1440) / 60 >= 1 THEN CONCAT(FLOOR((c.actualresolvedminutes % 1440) / 60), ' hours ') ELSE '' END ||
                CASE WHEN c.actualresolvedminutes % 60 > 0 THEN CONCAT(FLOOR(c.actualresolvedminutes % 60), ' mins') ELSE '' END
            )
        END AS actualresolvedtime,

        -- Formatted BreachSLA
        CASE
            WHEN c.actualresolvedminutes <= c.sla_minutes THEN 'No Breach'
            ELSE
                TRIM(
                    CASE WHEN (c.breachminutes_raw / 1440) >= 1 THEN CONCAT(FLOOR(c.breachminutes_raw / 1440), ' days ') ELSE '' END ||
                    CASE WHEN ((c.breachminutes_raw % 1440) / 60) >= 1 THEN CONCAT(FLOOR(((c.breachminutes_raw % 1440) / 60)), ' hours ') ELSE '' END ||
                    CASE WHEN (c.breachminutes_raw % 60) > 0 THEN CONCAT(FLOOR(c.breachminutes_raw % 60), ' mins') ELSE '' END
                )
        END AS breachsla
    FROM Calculated c
    WHERE
        p_search IS NULL
        OR UPPER(c.incidentno) LIKE '%' || UPPER(p_search) || '%'
        OR UPPER(c.assignedto) LIKE '%' || UPPER(p_search) || '%'
        OR UPPER(c.shortdescription) LIKE '%' || UPPER(p_search) || '%'
        OR UPPER(c.category) LIKE '%' || UPPER(p_search) || '%'
        OR UPPER(c.state) LIKE '%' || UPPER(p_search) || '%'
        OR UPPER(c.priority) LIKE '%' || UPPER(p_search) || '%';

    -- STEP 3: Sorting and Pagination (Original Sorting logic untouched)
    SELECT COUNT(*) INTO v_total_count FROM final_temp;
    v_total_pages := CASE WHEN v_page_size = 0 THEN 1 ELSE CEIL(1.0 * v_total_count / v_page_size)::INT END;
    IF v_total_count = 0 THEN v_total_pages := 1; END IF;

    RETURN QUERY EXECUTE format('
        SELECT
            %s::INTEGER, %s::INTEGER, %s::INTEGER, %s::INTEGER,
            incidentno, assignedto, shortdescription, category, state,
            created, resolveddatetime, updated, priority,
            actualresolvedtime, breachsla
        FROM final_temp
        ORDER BY
            CASE WHEN $1 = ''Number'' AND $2 = ''ASC'' THEN incidentno END ASC,
            CASE WHEN $1 = ''Number'' AND $2 = ''DESC'' THEN incidentno END DESC,
            CASE WHEN $1 = ''AssignedTo'' AND $2 = ''ASC'' THEN assignedto END ASC,
            CASE WHEN $1 = ''AssignedTo'' AND $2 = ''DESC'' THEN assignedto END DESC,
            CASE WHEN $1 = ''ShortDescription'' AND $2 = ''ASC'' THEN shortdescription END ASC,
            CASE WHEN $1 = ''ShortDescription'' AND $2 = ''DESC'' THEN shortdescription END DESC,
            CASE WHEN $1 = ''Category'' AND $2 = ''ASC'' THEN category END ASC,
            CASE WHEN $1 = ''Category'' AND $2 = ''DESC'' THEN category END DESC,
            CASE WHEN $1 = ''State'' AND $2 = ''ASC'' THEN state END ASC,
            CASE WHEN $1 = ''State'' AND $2 = ''DESC'' THEN state END DESC,
            CASE WHEN $1 = ''Created'' AND $2 = ''ASC'' THEN created END ASC,
            CASE WHEN $1 = ''Created'' AND $2 = ''DESC'' THEN created END DESC,
            CASE WHEN $1 = ''Resolved'' AND $2 = ''ASC'' THEN resolveddatetime END ASC,
            CASE WHEN $1 = ''Resolved'' AND $2 = ''DESC'' THEN resolveddatetime END DESC,
            CASE WHEN $1 = ''Updated'' AND $2 = ''ASC'' THEN updated END ASC,
            CASE WHEN $1 = ''Updated'' AND $2 = ''DESC'' THEN updated END DESC,
            CASE WHEN $1 = ''Priority'' AND $2 = ''ASC'' THEN priority END ASC,
            CASE WHEN $1 = ''Priority'' AND $2 = ''DESC'' THEN priority END DESC,
            CASE WHEN $1 = ''ActualResolvedTime'' AND $2 = ''ASC'' THEN COALESCE(CAST(actualresolvedminutes AS BIGINT), 9223372036854775807) END ASC,
            CASE WHEN $1 = ''ActualResolvedTime'' AND $2 = ''DESC'' THEN COALESCE(CAST(actualresolvedminutes AS BIGINT), -9223372036854775808) END DESC,
            CASE WHEN $1 = ''BreachSLA'' AND $2 = ''ASC'' THEN COALESCE(CAST(breachminutes AS BIGINT), 9223372036854775807) END ASC,
            CASE WHEN $1 = ''BreachSLA'' AND $2 = ''DESC'' THEN COALESCE(CAST(breachminutes AS BIGINT), -9223372036854775808) END DESC
        %s %s'
        , v_page_number, v_page_size, v_total_pages, v_total_count
        , CASE WHEN v_page_size > 0 THEN CONCAT(' OFFSET ', v_offset, ' ROWS ') ELSE '' END
        , CASE WHEN v_page_size > 0 THEN CONCAT(' FETCH NEXT ', v_page_size, ' ROWS ONLY ') ELSE '' END
    ) USING p_sortby, p_sortorder;
    
END;
$BODY$;

ALTER FUNCTION public.sp_getincidentdetailsbypriority(timestamp without time zone, timestamp without time zone, text, text, text, text, text, text, integer, integer, character varying, character varying)
    OWNER TO postgres;

