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
    v_is_return_all BOOLEAN := (v_page_size = 0);
    v_sort_key TEXT := LOWER(COALESCE(p_sortBy, 'resolveddatetime'));
    v_sort_order TEXT := UPPER(COALESCE(p_sortOrder, 'DESC'));
BEGIN
    -- Guard rails
    IF v_page_number < 1 THEN v_page_number := 1; END IF;
    IF v_page_size < 0 THEN v_page_size := 8; END IF;

    v_offset := (v_page_number - 1) * CASE WHEN v_page_size = 0 THEN 1 ELSE v_page_size END;

    -- STEP 1: Base filtered set - APPLY ONLY NON-SEARCH FILTERS
    CREATE TEMP TABLE filtered_incidents ON COMMIT DROP AS
    SELECT
        i."number",
        i.assigned_to,
        i.short_description,
        i.category,
        i.state,
        i.opened,
        i.resolved,
        i.updated,
        i.priority,
        i.assignment_group
    FROM
        public.incidents i
    WHERE
        -- Date Range Filter
        ((p_fromdate IS NULL) OR (i.opened >= p_fromdate))
        AND ((p_todate IS NULL) OR (i.opened <= p_todate))
        -- Multiselect Filters using UNNEST(string_to_array())
        AND (p_assignmentgroup IS NULL OR UPPER(i.assignment_group) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_assignmentgroup, ',')) u))
        AND (p_category IS NULL OR UPPER(i.category) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_category, ',')) u))
        AND (p_assignedtoname IS NULL OR UPPER(i.assigned_to) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_assignedtoname, ',')) u))
        AND (p_state IS NULL OR UPPER(i.state) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_state, ',')) u))
        AND (p_priority IS NULL OR UPPER(i.priority) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_priority, ',')) u));

    -- STEP 2 & 3: Compute calculated fields, apply search filters, and store in final_temp
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

            -- Actual resolved minutes: Opened -> Resolved (using LATERAL join to function)
            ARHBM.businessminutes AS actualresolvedminutes,

            -- SLA minutes (hardcoded mapping from previous SP)
            CASE LEFT(TRIM(COALESCE(f.priority,'4')),1)
                WHEN '1' THEN 120
                WHEN '2' THEN 240
                WHEN '3' THEN 1440
                WHEN '4' THEN 7200
                ELSE 7200
            END AS sla_minutes,

            -- Breach minutes = MAX(0, ActualResolvedMinutes - SLA). GREATEST() replaces T-SQL's complex CASE/MIN(0, ...)
            GREATEST(0, COALESCE(ARHBM.businessminutes, 0) - 
                CASE LEFT(TRIM(COALESCE(f.priority,'4')),1)
                    WHEN '1' THEN 120 WHEN '2' THEN 240 WHEN '3' THEN 1440 WHEN '4' THEN 7200 ELSE 7200
                END
            ) AS breachminutes,

            -- formatted ActualResolvedTime
            CASE
                WHEN ARHBM.businessminutes IS NULL OR ARHBM.businessminutes = 0 THEN 'N/A'
                ELSE TRIM(
                    CASE WHEN ARHBM.businessminutes / 1440 >= 1 THEN CONCAT(ARHBM.businessminutes / 1440, ' days ') ELSE '' END ||
                    CASE WHEN (ARHBM.businessminutes % 1440) / 60 > 0 THEN CONCAT((ARHBM.businessminutes % 1440) / 60, ' hours ') ELSE '' END ||
                    CASE WHEN ARHBM.businessminutes % 60 > 0 THEN CONCAT(ARHBM.businessminutes % 60, ' mins') ELSE '' END
                )
            END AS actualresolvedtime,

            -- formatted BreachSLA
            CASE
                WHEN COALESCE(ARHBM.businessminutes, 0) <= 
                    CASE LEFT(TRIM(COALESCE(f.priority,'4')),1)
                        WHEN '1' THEN 120 WHEN '2' THEN 240 WHEN '3' THEN 1440 WHEN '4' THEN 7200 ELSE 7200
                    END
                THEN 'No Breach'
                ELSE
                    TRIM(
                        CASE WHEN (GREATEST(0, COALESCE(ARHBM.businessminutes, 0) - (CASE LEFT(TRIM(COALESCE(f.priority,'4')),1) WHEN '1' THEN 120 WHEN '2' THEN 240 WHEN '3' THEN 1440 WHEN '4' THEN 7200 ELSE 7200 END))) / 1440 >= 1
                            THEN CONCAT((GREATEST(0, COALESCE(ARHBM.businessminutes, 0) - (CASE LEFT(TRIM(COALESCE(f.priority,'4')),1) WHEN '1' THEN 120 WHEN '2' THEN 240 WHEN '3' THEN 1440 WHEN '4' THEN 7200 ELSE 7200 END))) / 1440, ' days ') ELSE '' END ||
                        CASE WHEN ((GREATEST(0, COALESCE(ARHBM.businessminutes, 0) - (CASE LEFT(TRIM(COALESCE(f.priority,'4')),1) WHEN '1' THEN 120 WHEN '2' THEN 240 WHEN '3' THEN 1440 WHEN '4' THEN 7200 ELSE 7200 END))) % 1440) / 60 > 0
                            THEN CONCAT(((GREATEST(0, COALESCE(ARHBM.businessminutes, 0) - (CASE LEFT(TRIM(COALESCE(f.priority,'4')),1) WHEN '1' THEN 120 WHEN '2' THEN 240 WHEN '3' THEN 1440 WHEN '4' THEN 7200 ELSE 7200 END))) % 1440) / 60, ' hours ') ELSE '' END ||
                        CASE WHEN (GREATEST(0, COALESCE(ARHBM.businessminutes, 0) - (CASE LEFT(TRIM(COALESCE(f.priority,'4')),1) WHEN '1' THEN 120 WHEN '2' THEN 240 WHEN '3' THEN 1440 WHEN '4' THEN 7200 ELSE 7200 END))) % 60 > 0
                            THEN CONCAT((GREATEST(0, COALESCE(ARHBM.businessminutes, 0) - (CASE LEFT(TRIM(COALESCE(f.priority,'4')),1) WHEN '1' THEN 120 WHEN '2' THEN 240 WHEN '3' THEN 1440 WHEN '4' THEN 7200 ELSE 7200 END))) % 60, ' mins') ELSE '' END
                    )
            END AS breachsla

        FROM filtered_incidents f
        JOIN LATERAL public.fn_slaminutes_itvf(
            f.opened,
            f.resolved,
            CASE WHEN LEFT(TRIM(COALESCE(f.priority,'4')),1) IN ('1','2') THEN TRUE ELSE FALSE END
        ) AS ARHBM ON TRUE
    )
    -- Select from the CTE (Final) and apply the p_search filter
    SELECT 
        f.incidentno, f.assignedto, f.shortdescription, f.category, f.state, f.created, f.resolveddatetime, f.updated, f.priority,
        f.actualresolvedminutes, f.sla_minutes, f.breachminutes, f.actualresolvedtime, f.breachsla
    FROM Final f
    WHERE
        p_search IS NULL
        OR UPPER(f.incidentno) LIKE '%' || UPPER(p_search) || '%'
        OR UPPER(f.assignedto) LIKE '%' || UPPER(p_search) || '%'
        OR UPPER(f.shortdescription) LIKE '%' || UPPER(p_search) || '%'
        OR UPPER(f.category) LIKE '%' || UPPER(p_search) || '%'
        OR UPPER(f.state) LIKE '%' || UPPER(p_search) || '%'
        OR UPPER(f.priority) LIKE '%' || UPPER(p_search) || '%'
        OR UPPER(CAST(f.created AS TEXT)) LIKE '%' || UPPER(p_search) || '%'
        OR UPPER(CAST(f.resolveddatetime AS TEXT)) LIKE '%' || UPPER(p_search) || '%'
        OR UPPER(CAST(f.updated AS TEXT)) LIKE '%' || UPPER(p_search) || '%'
        OR UPPER(f.actualresolvedtime) LIKE '%' || UPPER(p_search) || '%'
        OR UPPER(f.breachsla) LIKE '%' || UPPER(p_search) || '%';
        
    -- Normalizing sort key and setting counts
    v_sort_key := REPLACE(REPLACE(REPLACE(v_sort_key, ' ', ''), '_', ''), '-', '');
    IF v_sort_key = 'incidentno' OR v_sort_key = 'incidentnumber' OR v_sort_key = 'number' OR v_sort_key = 'numbered' THEN v_sort_key := 'incidentno'; END IF;
    IF v_sort_key = 'assignedto' OR v_sort_key = 'assignto' OR v_sort_key = 'assigned' THEN v_sort_key := 'assignedto'; END IF;
    IF v_sort_key = 'shortdescription' OR v_sort_key = 'shortdesc' OR v_sort_key = 'short_description' THEN v_sort_key := 'shortdescription'; END IF;
    IF v_sort_key = 'category' THEN v_sort_key := 'category'; END IF;
    IF v_sort_key = 'state' THEN v_sort_key := 'state'; END IF;
    IF v_sort_key = 'created' OR v_sort_key = 'opened' OR v_sort_key = 'opendate' THEN v_sort_key := 'created'; END IF;
    IF v_sort_key = 'resolveddatetime' OR v_sort_key = 'resolved' OR v_sort_key = 'resolveddate' THEN v_sort_key := 'resolveddatetime'; END IF;
    IF v_sort_key = 'updated' OR v_sort_key = 'lastupdated' OR v_sort_key = 'updateddate' THEN v_sort_key := 'updated'; END IF;
    IF v_sort_key = 'priority' THEN v_sort_key := 'priority'; END IF;
    IF v_sort_key = 'actualresolvedtime' OR v_sort_key = 'actualresolved' OR v_sort_key = 'actualresolvedtimeminutes' OR v_sort_key = 'actualresolvedminutes' OR v_sort_key = 'actual' THEN v_sort_key := 'actualresolvedtime'; END IF;
    IF v_sort_key = 'breachsla' OR v_sort_key = 'breachs' OR v_sort_key = 'breach' THEN v_sort_key := 'breachsla'; END IF;
    IF v_sort_key = '' THEN v_sort_key := 'resolveddatetime'; END IF;

    SELECT COUNT(*) INTO v_total_count FROM final_temp;

    IF v_page_size = 0 THEN
        v_total_pages := 1;
    ELSE
        v_total_pages := CEIL(1.0 * v_total_count / v_page_size)::INT;
        IF v_total_count = 0 THEN v_total_pages := 1; END IF;
    END IF;

    -- Final SELECT with Pagination Metadata and Dynamic Sorting
    RETURN QUERY EXECUTE format('
        SELECT
            %s::INTEGER,
            %s::INTEGER,
            %s::INTEGER,
            %s::INTEGER,
            incidentno,
            assignedto,
            shortdescription,
            category,
            state,
            created,
            resolveddatetime,
            updated,
            priority,
            actualresolvedtime,
            breachsla
        FROM final_temp
        ORDER BY
            CASE WHEN $1 = ''incidentno'' AND $2 = ''ASC'' THEN incidentno END ASC,
            CASE WHEN $1 = ''incidentno'' AND $2 = ''DESC'' THEN incidentno END DESC,
            CASE WHEN $1 = ''assignedto'' AND $2 = ''ASC'' THEN assignedto END ASC,
            CASE WHEN $1 = ''assignedto'' AND $2 = ''DESC'' THEN assignedto END DESC,
            CASE WHEN $1 = ''shortdescription'' AND $2 = ''ASC'' THEN shortdescription END ASC,
            CASE WHEN $1 = ''shortdescription'' AND $2 = ''DESC'' THEN shortdescription END DESC,
            CASE WHEN $1 = ''category'' AND $2 = ''ASC'' THEN category END ASC,
            CASE WHEN $1 = ''category'' AND $2 = ''DESC'' THEN category END DESC,
            CASE WHEN $1 = ''state'' AND $2 = ''ASC'' THEN state END ASC,
            CASE WHEN $1 = ''state'' AND $2 = ''DESC'' THEN state END DESC,
            CASE WHEN $1 = ''created'' AND $2 = ''ASC'' THEN created END ASC,
            CASE WHEN $1 = ''created'' AND $2 = ''DESC'' THEN created END DESC,
            CASE WHEN $1 = ''resolveddatetime'' AND $2 = ''ASC'' THEN resolveddatetime END ASC,
            CASE WHEN $1 = ''resolveddatetime'' AND $2 = ''DESC'' THEN resolveddatetime END DESC,
            CASE WHEN $1 = ''updated'' AND $2 = ''ASC'' THEN updated END ASC,
            CASE WHEN $1 = ''updated'' AND $2 = ''DESC'' THEN updated END DESC,
            CASE WHEN $1 = ''priority'' AND $2 = ''ASC'' THEN priority END ASC,
            CASE WHEN $1 = ''priority'' AND $2 = ''DESC'' THEN priority END DESC,
            -- Sorting for numeric columns (actualresolvedminutes, breachminutes) with NULL handling
            CASE WHEN $1 = ''actualresolvedtime'' AND $2 = ''ASC'' THEN COALESCE(CAST(actualresolvedminutes AS BIGINT), 9223372036854775807) END ASC,
            CASE WHEN $1 = ''actualresolvedtime'' AND $2 = ''DESC'' THEN COALESCE(CAST(actualresolvedminutes AS BIGINT), -9223372036854775808) END DESC,
            CASE WHEN $1 = ''breachsla'' AND $2 = ''ASC'' THEN COALESCE(CAST(breachminutes AS BIGINT), 9223372036854775807) END ASC,
            CASE WHEN $1 = ''breachsla'' AND $2 = ''DESC'' THEN COALESCE(CAST(breachminutes AS BIGINT), -9223372036854775808) END DESC
        %s %s'
        , v_page_number, v_page_size, v_total_pages, v_total_count
        , CASE WHEN v_page_size > 0 THEN CONCAT(' OFFSET ', v_offset, ' ROWS ') ELSE '' END
        , CASE WHEN v_page_size > 0 THEN CONCAT(' FETCH NEXT ', v_page_size, ' ROWS ONLY ') ELSE '' END
    ) USING v_sort_key, v_sort_order;
    
END;
$BODY$;

ALTER FUNCTION public.sp_getincidentdetailsbypriority(timestamp without time zone, timestamp without time zone, text, text, text, text, text, text, integer, integer, character varying, character varying)
    OWNER TO postgres;

