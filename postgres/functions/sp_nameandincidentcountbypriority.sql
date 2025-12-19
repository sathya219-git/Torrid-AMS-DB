-- FUNCTION: public.sp_nameandincidentcountbypriority(timestamp without time zone, timestamp without time zone, text, text, text, text, text, integer, integer, character varying, character varying)

-- DROP FUNCTION IF EXISTS public.sp_nameandincidentcountbypriority(timestamp without time zone, timestamp without time zone, text, text, text, text, text, integer, integer, character varying, character varying);

CREATE OR REPLACE FUNCTION public.sp_nameandincidentcountbypriority(
	p_fromdate timestamp without time zone DEFAULT NULL::timestamp without time zone,
	p_todate timestamp without time zone DEFAULT NULL::timestamp without time zone,
	p_category text DEFAULT NULL::text,
	p_assignmentgroup text DEFAULT NULL::text,
	p_priority text DEFAULT NULL::text,
	p_assignedtoname text DEFAULT NULL::text,
	p_state text DEFAULT NULL::text,
	p_pagenumber integer DEFAULT 1,
	p_pagesize integer DEFAULT 4,
	p_sortby character varying DEFAULT 'Name'::character varying,
	p_sortorder character varying DEFAULT 'ASC'::character varying)
    RETURNS TABLE(currentpage integer, pagesize integer, totalpages numeric, totalrecords bigint, name text, p1 bigint, p2 bigint, p3 bigint, p4 bigint, totalcount bigint, actualresolvedtime text, lastupdated timestamp without time zone, actualresolvedtime_min numeric) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE
    v_page_number INT := COALESCE(p_pagenumber, 1);
    v_page_size INT := COALESCE(p_pagesize, 4);
    v_offset INT;
    v_total_count BIGINT;
    v_total_pages NUMERIC;
    v_sort_by TEXT := LOWER(COALESCE(p_sortBy, 'name'));
    v_sort_order TEXT := UPPER(COALESCE(p_sortOrder, 'ASC'));
BEGIN
    -- Guard rails
    IF v_page_number < 1 THEN v_page_number := 1; END IF;
    IF v_page_size < 0 THEN v_page_size := 4; END IF;

    v_offset := (v_page_number - 1) * CASE WHEN v_page_size = 0 THEN 1 ELSE v_page_size END;

    -- STEP 1: Filtered Incidents and Per-Incident Business Minutes (combined CTEs)
    CREATE TEMP TABLE results_temp ON COMMIT DROP AS
    WITH Filtered AS
    (
        SELECT 
            i."number", i.opened, i.resolved, i.updated, i.priority, i.assigned_to, i.assignment_group, i.category, i.state
        FROM public.incidents i
        WHERE
            (p_fromDate IS NULL OR i.opened >= p_fromDate)
            AND (p_toDate IS NULL OR i.opened <= p_toDate)
            -- Multiselect Filters using UNNEST(string_to_array())
            AND (p_assignmentGroup IS NULL OR UPPER(i.assignment_group) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_assignmentGroup, ',')) u))
            AND (p_category IS NULL OR UPPER(i.category) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_category, ',')) u))
            AND (p_priority IS NULL OR UPPER(i.priority) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_priority, ',')) u))
            AND (p_assignedToName IS NULL OR UPPER(i.assigned_to) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_assignedToName, ',')) u))
            AND (p_state IS NULL OR UPPER(i.state) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_state, ',')) u))
    ),
    PerIncident AS
    (
        SELECT
            -- COALESCE(NULLIF(LTRIM(RTRIM(f.Assigned_to)), ''), N'Unassigned') AS AssignedName
            COALESCE(NULLIF(TRIM(f.assigned_to), ''), 'Unassigned') AS AssignedName,
            f.priority,
            f.updated,
            -- Cross Apply replaced with JOIN LATERAL
            br.businessminutes AS MinutesTaken
        FROM Filtered f
        JOIN LATERAL public.fn_slaminutes_itvf(
            f.opened,
            f.resolved,
            -- Case logic: 1/2 priority includes weekends, else excludes
            CASE WHEN LEFT(TRIM(COALESCE(f.priority, '4')), 1) IN ('1','2') THEN TRUE ELSE FALSE END
        ) AS br ON TRUE
    )
    -- STEP 2 & 3: Aggregate per assignee and format time
    SELECT
        p.AssignedName AS Name,
        SUM(CASE WHEN p.Priority LIKE '1%' THEN 1 ELSE 0 END)::BIGINT AS P1,
        SUM(CASE WHEN p.Priority LIKE '2%' THEN 1 ELSE 0 END)::BIGINT AS P2,
        SUM(CASE WHEN p.Priority LIKE '3%' THEN 1 ELSE 0 END)::BIGINT AS P3,
        SUM(CASE WHEN p.Priority LIKE '4%' THEN 1 ELSE 0 END)::BIGINT AS P4,
        COUNT(*)::BIGINT AS TotalCount,
        MAX(p.Updated) AS LastUpdated,
        -- Calculate average minutes (uses numeric for precision)
        AVG(p.MinutesTaken)::NUMERIC(18, 2) AS ActualResolvedTime_Min,

        -- Format Average Resolved Time (mimics T-SQL logic)
        CASE 
            WHEN AVG(p.MinutesTaken) IS NULL THEN 'N/A'
            ELSE
                CONCAT(
                    CASE WHEN AVG(p.MinutesTaken) >= 1440 THEN CONCAT(FLOOR(AVG(p.MinutesTaken)/1440)::INT, ' days ') ELSE '' END,
                    CASE WHEN AVG(p.MinutesTaken) >= 60 AND (FLOOR(AVG(p.MinutesTaken) % 1440) / 60) > 0 THEN CONCAT(FLOOR((AVG(p.MinutesTaken) % 1440)/60)::INT, ' hours ') ELSE '' END,
                    CONCAT(FLOOR(AVG(p.MinutesTaken) % 60)::INT, ' mins')
                )
        END AS ActualResolvedTime
    FROM PerIncident p
    GROUP BY p.AssignedName;

    -- Calculate total count for pagination metadata
    SELECT COUNT(*) INTO v_total_count FROM results_temp;

    -- Calculate TotalPages
    v_total_pages := CEILING(v_total_count::FLOAT / NULLIF(v_page_size, 0));
    IF v_page_size = 0 THEN v_total_pages := 1; END IF;

    -- Final SELECT with Pagination Metadata and Sorting (Dynamic SQL)
    RETURN QUERY EXECUTE format('
        SELECT
            %s::INTEGER AS CurrentPage,
            %s::INTEGER AS PageSize,
            %s::NUMERIC AS TotalPages,
            %s::BIGINT AS TotalRecords,
            Name, P1, P2, P3, P4, TotalCount, ActualResolvedTime, LastUpdated, ActualResolvedTime_Min
        FROM results_temp
        ORDER BY
            CASE WHEN $1 = ''name'' AND $2 = ''ASC'' THEN Name END ASC,
            CASE WHEN $1 = ''name'' AND $2 = ''DESC'' THEN Name END DESC,
            CASE WHEN $1 = ''p1'' AND $2 = ''ASC'' THEN P1 END ASC,
            CASE WHEN $1 = ''p1'' AND $2 = ''DESC'' THEN P1 END DESC,
            CASE WHEN $1 = ''p2'' AND $2 = ''ASC'' THEN P2 END ASC,
            CASE WHEN $1 = ''p2'' AND $2 = ''DESC'' THEN P2 END DESC,
            CASE WHEN $1 = ''p3'' AND $2 = ''ASC'' THEN P3 END ASC,
            CASE WHEN $1 = ''p3'' AND $2 = ''DESC'' THEN P3 END DESC,
            CASE WHEN $1 = ''p4'' AND $2 = ''ASC'' THEN P4 END ASC,
            CASE WHEN $1 = ''p4'' AND $2 = ''DESC'' THEN P4 END DESC,
            CASE WHEN $1 = ''totalcount'' AND $2 = ''ASC'' THEN TotalCount END ASC,
            CASE WHEN $1 = ''totalcount'' AND $2 = ''DESC'' THEN TotalCount END DESC,
            CASE WHEN $1 = ''lastupdated'' AND $2 = ''ASC'' THEN LastUpdated END ASC,
            CASE WHEN $1 = ''lastupdated'' AND $2 = ''DESC'' THEN LastUpdated END DESC,
            -- Sort by the numeric minute value for accurate time sorting
            CASE WHEN $1 = ''actualresolvedtime'' AND $2 = ''ASC'' THEN COALESCE(ActualResolvedTime_Min, 999999999999999) END ASC,
            CASE WHEN $1 = ''actualresolvedtime'' AND $2 = ''DESC'' THEN COALESCE(ActualResolvedTime_Min, -999999999999999) END DESC
        %s %s'
        -- Arguments for format placeholders
        , v_page_number, v_page_size, v_total_pages, v_total_count
        -- Arguments for OFFSET/FETCH limits
        , CASE WHEN v_page_size > 0 THEN CONCAT(' OFFSET ', v_offset, ' ROWS ') ELSE '' END
        , CASE WHEN v_page_size > 0 THEN CONCAT(' FETCH NEXT ', v_page_size, ' ROWS ONLY ') ELSE '' END
    ) USING v_sort_by, v_sort_order; -- Arguments for $1 and $2
    
END;
$BODY$;

ALTER FUNCTION public.sp_nameandincidentcountbypriority(timestamp without time zone, timestamp without time zone, text, text, text, text, text, integer, integer, character varying, character varying)
    OWNER TO postgres;

