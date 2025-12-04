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
    v_resolved_op TEXT := NULL;
    v_resolved_val_minutes NUMERIC := NULL;
    v_breach_op TEXT := NULL;
    v_breach_val_minutes NUMERIC := NULL;
    v_total_count INT;
    v_total_pages INT;
    v_sla_minutes INT;
    v_breach_minutes INT;
    v_priority_char CHAR(1);
    v_match_array TEXT[];
BEGIN
    -- Guard rails
    IF v_page_number < 1 THEN v_page_number := 1; END IF;
    IF v_page_size < 0 THEN v_page_size := 8; END IF;

    v_offset := (v_page_number - 1) * CASE WHEN v_page_size = 0 THEN 1 ELSE v_page_size END;

    -- STEP 1: Base filtered set - APPLY ONLY NON-SEARCH FILTERS
    -- Use alias 'i' for incidents table to avoid parameter/column ambiguity
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
        public.incidents i -- Alias 'i' is essential here
    WHERE
        -- Date Range Filter: Use i.opened
        ((p_fromdate IS NULL AND p_todate IS NULL) OR (i.opened BETWEEN p_fromdate AND p_todate))
        -- Multiselect Filters using UNNEST(string_to_array()): Use i.column_name
        AND (p_assignmentgroup IS NULL OR UPPER(i.assignment_group) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_assignmentgroup, ',')) u))
        AND (p_category IS NULL OR UPPER(i.category) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_category, ',')) u))
        AND (p_assignedtoname IS NULL OR UPPER(i.assigned_to) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_assignedtoname, ',')) u))
        AND (p_state IS NULL OR UPPER(i.state) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_state, ',')) u))
        AND (p_priority IS NULL OR UPPER(i.priority) IN (SELECT TRIM(UPPER(u)) FROM UNNEST(STRING_TO_ARRAY(p_priority, ',')) u));

    ----------------------------------------------------------------
    -- Parse UI filters (p_actualresolvedtime, p_breachsla)
    ----------------------------------------------------------------

    -- Helper to parse complex filter strings like '>= 2 days' or '< 60 mins'
    -- Regex captures: (1: Op) (2: Number) (3: Unit)
    IF p_actualresolvedtime IS NOT NULL THEN
        v_match_array := REGEXP_MATCHES(TRIM(p_actualresolvedtime), '^\s*(>=|<=|>|<|=)?\s*([0-9]+\.?[0-9]*)\s*([a-zA-Z]+)?\s*$', 'i');
        IF ARRAY_LENGTH(v_match_array, 1) = 3 THEN
            v_resolved_op := COALESCE(NULLIF(v_match_array[1], ''), '=');
            v_resolved_val_minutes := v_match_array[2]::NUMERIC;
            
            IF v_resolved_val_minutes IS NOT NULL THEN
                CASE LOWER(COALESCE(v_match_array[3], ''))
                    WHEN 'day', 'days', 'd' THEN v_resolved_val_minutes := v_resolved_val_minutes * 1440;
                    WHEN 'hour', 'hours', 'hr', 'h' THEN v_resolved_val_minutes := v_resolved_val_minutes * 60;
                    WHEN 'min', 'mins', 'm', '' THEN v_resolved_val_minutes := v_resolved_val_minutes * 1;
                    ELSE v_resolved_val_minutes := NULL; -- Invalid unit, treat as NULL filter
                END CASE;
            END IF;
        END IF;
    END IF;

    IF p_breachsla IS NOT NULL THEN
        v_match_array := REGEXP_MATCHES(TRIM(p_breachsla), '^\s*(>=|<=|>|<|=)?\s*([0-9]+\.?[0-9]*)\s*([a-zA-Z]+)?\s*$', 'i');
        IF ARRAY_LENGTH(v_match_array, 1) = 3 THEN
            v_breach_op := COALESCE(NULLIF(v_match_array[1], ''), '=');
            v_breach_val_minutes := v_match_array[2]::NUMERIC;

            IF v_breach_val_minutes IS NOT NULL THEN
                CASE LOWER(COALESCE(v_match_array[3], ''))
                    WHEN 'day', 'days', 'd' THEN v_breach_val_minutes := v_breach_val_minutes * 1440;
                    WHEN 'hour', 'hours', 'hr', 'h' THEN v_breach_val_minutes := v_breach_val_minutes * 60;
                    WHEN 'min', 'mins', 'm', '' THEN v_breach_val_minutes := v_breach_val_minutes * 1;
                    ELSE v_breach_val_minutes := NULL; -- Invalid unit, treat as NULL filter
                END CASE;
            END IF;
        END IF;
    END IF;

    -- STEP 2 & 3: Compute calculated fields and apply search filters.
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

            -- Actual resolved minutes: Opened -> Resolved (from iTVF)
            ARHBM.businessminutes AS "ActualResolvedMinutes",

            -- SLA mapping (priority -> minutes)
            CASE LEFT(TRIM(COALESCE(f.priority,''4'')),1)
                WHEN ''1'' THEN 120
                WHEN ''2'' THEN 240
                WHEN ''3'' THEN 1440
                WHEN ''4'' THEN 7200
                ELSE 7200
            END AS "SLA_Minutes"
        FROM
            filtered_incidents f
        -- compute business minutes using the SLA function (JOIN LATERAL is PostgreSQL''s CROSS APPLY)
        JOIN LATERAL public.fn_slaminutes_itvf(
            f.opened,
            f.resolved,
            CASE WHEN LEFT(TRIM(COALESCE(f.priority,''4'')),1) IN (''1'',''2'') THEN TRUE ELSE FALSE END
        ) AS ARHBM ON TRUE
        
        -- filter only incidents that have Resolved & Updated and breach > 0
        WHERE f.resolved IS NOT NULL 
          AND f.updated IS NOT NULL
          AND ARHBM.businessminutes > 
             CASE LEFT(TRIM(COALESCE(f.priority,''4'')),1)
                 WHEN ''1'' THEN 120 WHEN ''2'' THEN 240 WHEN ''3'' THEN 1440 WHEN ''4'' THEN 7200 ELSE 7200
             END
    )
    SELECT
        f."Incident Number",
        f."Assigned To",
        f."Short Description",
        f."Category",  -- FIX: Quotes added here
        f."State",     -- FIX: Quotes added here
        f."Created",   -- FIX: Quotes added here
        f."Resolved Date & Time",
        f."Updated",   -- FIX: Quotes added here
        f."ActualResolvedMinutes",
        -- BreachMinutes := minutes over SLA (clamped to 0)
        f."ActualResolvedMinutes" - f."SLA_Minutes" AS "BreachMinutes",
        
        -- Formatted text (Actual Resolved Time)
        CASE
            WHEN f."ActualResolvedMinutes" IS NULL THEN ''N/A''
            ELSE TRIM(
                CASE WHEN f."ActualResolvedMinutes"/1440 >= 1 THEN CONCAT(f."ActualResolvedMinutes"/1440, '' days '') ELSE '''' END ||
                CASE WHEN (f."ActualResolvedMinutes" % 1440) / 60 > 0 THEN CONCAT((f."ActualResolvedMinutes" % 1440) / 60, '' hours '') ELSE '''' END ||
                CASE WHEN f."ActualResolvedMinutes" % 60 > 0 THEN CONCAT(f."ActualResolvedMinutes" % 60, '' mins'') ELSE '''' END
            )
        END AS "Actual Resolved Time",

        -- Formatted text (Breach SLA)
        CASE
            WHEN f."ActualResolvedMinutes" <= f."SLA_Minutes" THEN ''No Breach''
            ELSE
                TRIM(
                    CASE WHEN (f."ActualResolvedMinutes" - f."SLA_Minutes")/1440 >= 1 
                        THEN CONCAT((f."ActualResolvedMinutes" - f."SLA_Minutes")/1440, '' days '') ELSE '''' END ||
                    CASE WHEN ((f."ActualResolvedMinutes" - f."SLA_Minutes") % 1440) / 60 > 0 
                        THEN CONCAT(((f."ActualResolvedMinutes" - f."SLA_Minutes") % 1440) / 60, '' hours '') ELSE '''' END ||
                    CASE WHEN (f."ActualResolvedMinutes" - f."SLA_Minutes") % 60 > 0 
                        THEN CONCAT((f."ActualResolvedMinutes" - f."SLA_Minutes") % 60, '' mins'') ELSE '''' END
                )
        END AS "Breach SLA"
    FROM
        Final f
    WHERE
        -- Filter 1: Incident Number (Case-insensitive search)
        ($1 IS NULL OR UPPER(f."Incident Number") LIKE ''%'' || UPPER($1) || ''%'')
        
        -- Filter 2: Actual Resolved Time (Numeric filter)
        AND ($2 IS NULL OR $3 IS NULL
            OR (
                ($2 = ''>='' AND f."ActualResolvedMinutes" >= $3) OR
                ($2 = ''<='' AND f."ActualResolvedMinutes" <= $3) OR
                ($2 = ''>''  AND f."ActualResolvedMinutes" >  $3) OR
                ($2 = ''<''  AND f."ActualResolvedMinutes" <  $3) OR
                ($2 = ''=''  AND f."ActualResolvedMinutes" =  $3)
            )
        )
        
        -- Filter 3: Breach SLA (Numeric filter)
        AND ($4 IS NULL OR $5 IS NULL
            OR (
                ($4 = ''>='' AND (f."ActualResolvedMinutes" - f."SLA_Minutes") >= $5) OR
                ($4 = ''<='' AND (f."ActualResolvedMinutes" - f."SLA_Minutes") <= $5) OR
                ($4 = ''>''  AND (f."ActualResolvedMinutes" - f."SLA_Minutes") >  $5) OR
                ($4 = ''<''  AND (f."ActualResolvedMinutes" - f."SLA_Minutes") <  $5) OR
                ($4 = ''=''  AND (f."ActualResolvedMinutes" - f."SLA_Minutes") =  $5)
            )
        )
        
        -- Filter 4: General Search (p_search) - **NOW COMBINED HERE**
        AND ($6 IS NULL
            OR UPPER(f."Incident Number") LIKE ''%'' || UPPER($6) || ''%''
            OR UPPER(f."Assigned To") LIKE ''%'' || UPPER($6) || ''%''
            OR UPPER(f."Short Description") LIKE ''%'' || UPPER($6) || ''%''
            OR UPPER(f."Category") LIKE ''%'' || UPPER($6) || ''%''
            OR UPPER(f."State") LIKE ''%'' || UPPER($6) || ''%''
            OR UPPER(CAST(f."Created" AS TEXT)) LIKE ''%'' || UPPER($6) || ''%''
            OR UPPER(CAST(f."Resolved Date & Time" AS TEXT)) LIKE ''%'' || UPPER($6) || ''%''
            OR UPPER(CAST(f."Updated" AS TEXT)) LIKE ''%'' || UPPER($6) || ''%''
        );'
    USING p_incidentnumber, v_resolved_op, v_resolved_val_minutes, v_breach_op, v_breach_val_minutes, p_search;

    -- Calculate total count and total pages for pagination metadata
    SELECT COUNT(*) INTO v_total_count FROM final_incidents_search;

    IF v_page_size = 0 THEN
        v_total_pages := 1;
    ELSE
        v_total_pages := CEIL(1.0 * v_total_count / v_page_size)::INT;
        IF v_total_count = 0 THEN v_total_pages := 1; END IF;
    END IF;

    -- Final SELECT with Pagination Metadata and Sorting
    RETURN QUERY EXECUTE format('
        SELECT
            %s::INTEGER,
            %s::INTEGER,
            %s::INTEGER,
            %s::INTEGER,
            "Incident Number",
            "Assigned To",
            "Short Description",
            "Category",
            "State",
            "Created",
            "Resolved Date & Time",
            "Updated",
            "Actual Resolved Time",
            "Breach SLA"
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
            -- Sorting for numeric columns (ActualResolvedMinutes, BreachMinutes) with NULL handling
            CASE WHEN $1 = ''ActualResolvedTime'' AND $2 = ''ASC'' THEN COALESCE(CAST("ActualResolvedMinutes" AS BIGINT), 9223372036854775807) END ASC,
            CASE WHEN $1 = ''ActualResolvedTime'' AND $2 = ''DESC'' THEN COALESCE(CAST("ActualResolvedMinutes" AS BIGINT), -9223372036854775808) END DESC,
            CASE WHEN $1 = ''BreachSLA'' AND $2 = ''ASC'' THEN COALESCE(CAST("BreachMinutes" AS BIGINT), 9223372036854775807) END ASC,
            CASE WHEN $1 = ''BreachSLA'' AND $2 = ''DESC'' THEN COALESCE(CAST("BreachMinutes" AS BIGINT), -9223372036854775808) END DESC
        %s %s'
        , v_page_number, v_page_size, v_total_pages, v_total_count
        , CASE WHEN v_page_size > 0 THEN CONCAT(' OFFSET ', v_offset, ' ROWS ') ELSE '' END
        , CASE WHEN v_page_size > 0 THEN CONCAT(' FETCH NEXT ', v_page_size, ' ROWS ONLY ') ELSE '' END
    ) USING p_sortby, p_sortorder;
    
END;
$BODY$;

ALTER FUNCTION public.sp_breachlistbypriority(timestamp without time zone, timestamp without time zone, text, text, text, text, text, text, integer, integer, character varying, character varying, text, text, text)
    OWNER TO postgres;

