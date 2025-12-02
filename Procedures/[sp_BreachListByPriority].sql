USE [TorridAMS]
GO

/****** Object:  StoredProcedure [dbo].[sp_BreachListByPriority]    Script Date: 26-11-2025 11:40:26 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [dbo].[sp_BreachListByPriority]
    @p_fromDate DATETIME = NULL,
    @p_toDate   DATETIME = NULL,
    @p_category NVARCHAR(MAX) = NULL,
    @p_assignmentGroup NVARCHAR(MAX) = NULL,
    @p_assignedToName NVARCHAR(MAX) = NULL,
    @p_state NVARCHAR(MAX) = NULL,
    @p_search NVARCHAR(MAX) = NULL,
    @p_priority NVARCHAR(MAX) = NULL,
    @p_pageNumber INT = 1,
    @p_pageSize INT = 8,
    @p_sortBy VARCHAR(50) = 'Updated',
    @p_sortOrder VARCHAR(4) = 'DESC',
    @p_incidentNumber NVARCHAR(MAX) = NULL,
    @p_actualResolvedTime NVARCHAR(100) = NULL,
    @p_breachSLA NVARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- Guard rails
    IF @p_pageNumber IS NULL OR @p_pageNumber < 1 SET @p_pageNumber = 1;
    IF @p_pageSize   IS NULL OR @p_pageSize   < 0 SET @p_pageSize   = 8;

    DECLARE @Offset INT = (@p_pageNumber - 1) * CASE WHEN @p_pageSize = 0 THEN 1 ELSE @p_pageSize END;

    IF OBJECT_ID('tempdb..#Filtered') IS NOT NULL DROP TABLE #Filtered;

    -- STEP 1: Base filtered set - APPLY ONLY NON-SEARCH FILTERS
    SELECT
        Number,
        CAST(Assigned_to AS NVARCHAR(MAX))      AS Assigned_to,
        CAST(Short_description AS NVARCHAR(MAX)) AS Short_description,
        CAST(Category AS NVARCHAR(MAX))         AS Category,
        CAST(State AS NVARCHAR(MAX))            AS State,
        Opened,
        Resolved,
        Updated,
        CAST(Priority AS NVARCHAR(MAX))         AS Priority,
        Assignment_group
    INTO #Filtered
    FROM dbo.incidents
    WHERE
        ((@p_fromDate IS NULL AND @p_toDate IS NULL) OR (Opened BETWEEN @p_fromDate AND @p_toDate))
        AND (@p_assignmentGroup IS NULL OR UPPER(CAST(Assignment_group AS NVARCHAR(MAX))) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_assignmentGroup, ',')))
        AND (@p_category        IS NULL OR UPPER(CAST(Category        AS NVARCHAR(MAX))) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_category, ',')))
        AND (@p_assignedToName IS NULL OR UPPER(CAST(Assigned_to     AS NVARCHAR(MAX))) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_assignedToName, ',')))
        AND (@p_state           IS NULL OR UPPER(CAST(State           AS NVARCHAR(MAX))) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_state, ',')))
        AND (@p_priority        IS NULL OR UPPER(CAST(Priority        AS NVARCHAR(MAX))) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_priority, ',')));
        -- **REMOVED @p_search FILTERING FROM HERE**

    ----------------------------------------------------------------
    -- Parse UI filters (@p_actualResolvedTime, @p_breachSLA)
    ----------------------------------------------------------------

    DECLARE
        @ResolvedOp NVARCHAR(2) = NULL,
        @ResolvedValMinutes FLOAT = NULL,
        @BreachOp NVARCHAR(2) = NULL,
        @BreachValMinutes FLOAT = NULL;

    DECLARE @s NVARCHAR(200), @startNum INT, @lenNum INT, @numText NVARCHAR(50), @unitText NVARCHAR(50), @opText NVARCHAR(2);

    /* Parse @p_actualResolvedTime */
    IF @p_actualResolvedTime IS NOT NULL
    BEGIN
        SET @s = LTRIM(RTRIM(@p_actualResolvedTime));
        IF LEFT(@s,2) IN ('>=','<=') SET @opText = LEFT(@s,2)
        ELSE IF LEFT(@s,1) IN ('>','<') SET @opText = LEFT(@s,1)
        ELSE SET @opText = '=';
        SET @startNum = PATINDEX('%[0-9]%', @s);
        IF @startNum > 0
        BEGIN
            SET @lenNum = PATINDEX('%[^0-9.]%', SUBSTRING(@s, @startNum, 200));
            IF @lenNum = 0 SET @lenNum = LEN(@s) - @startNum + 2;
            ELSE SET @lenNum = @lenNum - 1;
            SET @numText = SUBSTRING(@s, @startNum, @lenNum);
            SET @unitText = LTRIM(RTRIM(REPLACE(REPLACE(SUBSTRING(@s, @startNum + @lenNum, 200), @opText, ''), @numText, '')));

            SET @ResolvedOp = @opText;
            SET @ResolvedValMinutes = TRY_CAST(@numText AS FLOAT);

            IF @ResolvedValMinutes IS NOT NULL
            BEGIN
                SET @unitText = LOWER(@unitText);
                IF @unitText LIKE '%day%' OR @unitText LIKE '%d%' SET @ResolvedValMinutes = @ResolvedValMinutes * 1440;
                ELSE IF @unitText LIKE '%hour%' OR @unitText LIKE '%hr%' OR @unitText LIKE '%h%' SET @ResolvedValMinutes = @ResolvedValMinutes * 60;
                ELSE IF @unitText LIKE '%min%' OR @unitText LIKE '%m%' SET @ResolvedValMinutes = @ResolvedValMinutes * 1;
            END
        END
    END

    /* Parse @p_breachSLA */
    IF @p_breachSLA IS NOT NULL
    BEGIN
        SET @s = LTRIM(RTRIM(@p_breachSLA));
        IF LEFT(@s,2) IN ('>=','<=') SET @opText = LEFT(@s,2)
        ELSE IF LEFT(@s,1) IN ('>','<') SET @opText = LEFT(@s,1)
        ELSE SET @opText = '=';
        SET @startNum = PATINDEX('%[0-9]%', @s);
        IF @startNum > 0
        BEGIN
            SET @lenNum = PATINDEX('%[^0-9.]%', SUBSTRING(@s, @startNum, 200));
            IF @lenNum = 0 SET @lenNum = LEN(@s) - @startNum + 2;
            ELSE SET @lenNum = @lenNum - 1;
            SET @numText = SUBSTRING(@s, @startNum, @lenNum);
            SET @unitText = LTRIM(RTRIM(REPLACE(REPLACE(SUBSTRING(@s, @startNum + @lenNum, 200), @opText, ''), @numText, '')));

            SET @BreachOp = @opText;
            SET @BreachValMinutes = TRY_CAST(@numText AS FLOAT);

            IF @BreachValMinutes IS NOT NULL
            BEGIN
                SET @unitText = LOWER(@unitText);
                IF @unitText LIKE '%day%' OR @unitText LIKE '%d%' SET @BreachValMinutes = @BreachValMinutes * 1440;
                ELSE IF @unitText LIKE '%hour%' OR @unitText LIKE '%hr%' OR @unitText LIKE '%h%' SET @BreachValMinutes = @BreachValMinutes * 60;
                ELSE IF @unitText LIKE '%min%' OR @unitText LIKE '%m%' SET @BreachValMinutes = @BreachValMinutes * 1;
            END
        END
    END

    IF OBJECT_ID('tempdb..#Final') IS NOT NULL DROP TABLE #Final;

    -- STEP 2: Compute minutes and formatted text (CTE Final)
    ;WITH Final AS (
        SELECT
            Number AS [Incident Number],
            Assigned_to AS [Assigned To],
            Short_description AS [Short Description],
            Category,
            State AS [State],
            Opened AS [Created],
            Resolved AS [Resolved Date & Time],
            Updated AS [Updated],
            Priority, -- Keep Priority for search

            -- Actual resolved minutes: Opened -> Resolved (from iTVF)
            ARHBM.BusinessMinutes AS ActualResolvedMinutes,

            -- SLA mapping (priority -> minutes)  <-- UPDATED SLA VALUES (24h & 120h for P3/P4)
            CASE LEFT(LTRIM(RTRIM(ISNULL(Priority,'4'))),1)
                WHEN '1' THEN 120   -- Critical: 2 hours
                WHEN '2' THEN 240   -- High:     4 hours
                WHEN '3' THEN 1440  -- Moderate: 24 hours
                WHEN '4' THEN 7200  -- Low:      120 hours (5 days)
                ELSE 7200
            END AS SLA_Minutes,

            -- BreachMinutes := minutes over SLA (clamped to 0)  <-- uses updated SLA values
            CASE 
                WHEN ARHBM.BusinessMinutes IS NULL THEN 0
                WHEN ARHBM.BusinessMinutes - 
                     CASE LEFT(LTRIM(RTRIM(ISNULL(Priority,'4'))),1)
                         WHEN '1' THEN 120 WHEN '2' THEN 240 WHEN '3' THEN 1440 WHEN '4' THEN 7200 ELSE 7200
                     END > 0
                THEN ARHBM.BusinessMinutes - 
                     CASE LEFT(LTRIM(RTRIM(ISNULL(Priority,'4'))),1)
                         WHEN '1' THEN 120 WHEN '2' THEN 240 WHEN '3' THEN 1440 WHEN '4' THEN 7200 ELSE 7200
                     END
                ELSE 0
            END AS BreachMinutes,

            -- formatted text (Actual Resolved Time)
            CASE
                WHEN ARHBM.BusinessMinutes IS NULL THEN 'N/A'
                ELSE LTRIM(RTRIM(
                    CASE WHEN ARHBM.BusinessMinutes/1440 >= 1 THEN CONCAT(ARHBM.BusinessMinutes/1440, ' days ') ELSE '' END +
                    CASE WHEN (ARHBM.BusinessMinutes % 1440) / 60 > 0 THEN CONCAT((ARHBM.BusinessMinutes % 1440) / 60, ' hours ') ELSE '' END +
                    CASE WHEN ARHBM.BusinessMinutes % 60 > 0 THEN CONCAT(ARHBM.BusinessMinutes % 60, ' mins') ELSE '' END
                ))
            END AS [Actual Resolved Time],

            -- formatted text (Breach SLA)
            CASE
                WHEN (ARHBM.BusinessMinutes IS NULL) OR
                     (ARHBM.BusinessMinutes - 
                        CASE LEFT(LTRIM(RTRIM(ISNULL(Priority,'4'))),1)
                            WHEN '1' THEN 120 WHEN '2' THEN 240 WHEN '3' THEN 1440 WHEN '4' THEN 7200 ELSE 7200
                        END
                     ) <= 0
                THEN 'No Breach'
                ELSE LTRIM(RTRIM(
                    CASE WHEN (ARHBM.BusinessMinutes - 
                                CASE LEFT(LTRIM(RTRIM(ISNULL(Priority,'4'))),1)
                                    WHEN '1' THEN 120 WHEN '2' THEN 240 WHEN '3' THEN 1440 WHEN '4' THEN 7200 ELSE 7200
                                END)/1440 >= 1 
                         THEN CONCAT((ARHBM.BusinessMinutes - 
                                CASE LEFT(LTRIM(RTRIM(ISNULL(Priority,'4'))),1)
                                    WHEN '1' THEN 120 WHEN '2' THEN 240 WHEN '3' THEN 1440 WHEN '4' THEN 7200 ELSE 7200
                                END)/1440, ' days ') ELSE '' END +
                    CASE WHEN ((ARHBM.BusinessMinutes - 
                                CASE LEFT(LTRIM(RTRIM(ISNULL(Priority,'4'))),1)
                                    WHEN '1' THEN 120 WHEN '2' THEN 240 WHEN '3' THEN 1440 WHEN '4' THEN 7200 ELSE 7200
                                END) % 1440) / 60 > 0 
                         THEN CONCAT(((ARHBM.BusinessMinutes - 
                                CASE LEFT(LTRIM(RTRIM(ISNULL(Priority,'4'))),1)
                                    WHEN '1' THEN 120 WHEN '2' THEN 240 WHEN '3' THEN 1440 WHEN '4' THEN 7200 ELSE 7200
                                END) % 1440) / 60, ' hours ') ELSE '' END +
                    CASE WHEN (ARHBM.BusinessMinutes - 
                                CASE LEFT(LTRIM(RTRIM(ISNULL(Priority,'4'))),1)
                                    WHEN '1' THEN 120 WHEN '2' THEN 240 WHEN '3' THEN 1440 WHEN '4' THEN 7200 ELSE 7200
                                END) % 60 > 0 
                         THEN CONCAT((ARHBM.BusinessMinutes - 
                                CASE LEFT(LTRIM(RTRIM(ISNULL(Priority,'4'))),1)
                                    WHEN '1' THEN 120 WHEN '2' THEN 240 WHEN '3' THEN 1440 WHEN '4' THEN 7200 ELSE 7200
                                END) % 60, ' mins') ELSE '' END
                ))
            END AS [Breach SLA]

        FROM #Filtered f
        -- compute business minutes using the SLA iTVF (explicit casts to DATETIME2)
        -- NOTE: IncludeWeekends = 1 for priorities 1 & 2, else 0 (exclude weekends for 3 & 4)
        CROSS APPLY dbo.fn_SLAMinutes_iTVF(
            CAST(f.Opened AS DATETIME2),
            CAST(f.Resolved AS DATETIME2),
            CASE WHEN LEFT(LTRIM(RTRIM(ISNULL(f.Priority,'4'))),1) IN ('1','2') THEN 1 ELSE 0 END
        ) AS ARHBM

        -- filter only incidents that have Resolved & Updated and breach > 0 (based on SLA)
        WHERE f.Resolved IS NOT NULL 
          AND f.Updated IS NOT NULL
          AND 
          (
            CASE 
                WHEN ARHBM.BusinessMinutes IS NULL THEN 0
                WHEN ARHBM.BusinessMinutes - 
                     CASE LEFT(LTRIM(RTRIM(ISNULL(f.Priority,'4'))),1)
                         WHEN '1' THEN 120 WHEN '2' THEN 240 WHEN '3' THEN 1440 WHEN '4' THEN 7200 ELSE 7200
                     END > 0
                THEN 1
                ELSE 0
            END
          ) = 1
    )

    -- STEP 3: Apply ALL remaining filters (@p_search, @p_incidentNumber, @p_actualResolvedTime, @p_breachSLA)
    SELECT
        [Incident Number],
        [Assigned To],
        [Short Description],
        Category,
        [State],
        [Created],
        [Resolved Date & Time],
        [Updated],
        [Actual Resolved Time],
        [Breach SLA],
        ActualResolvedMinutes,
        BreachMinutes
    INTO #Final
    FROM Final
    WHERE
        -- Filter 1: Incident Number (Exact search)
        (@p_incidentNumber IS NULL OR UPPER([Incident Number]) LIKE '%' + UPPER(@p_incidentNumber) + '%')
        
        -- Filter 2: Actual Resolved Time (Numeric filter)
        AND (
            @p_actualResolvedTime IS NULL
            OR (
                (@ResolvedOp = '>=' AND ActualResolvedMinutes >= @ResolvedValMinutes) OR
                (@ResolvedOp = '<=' AND ActualResolvedMinutes <= @ResolvedValMinutes) OR
                (@ResolvedOp = '>'  AND ActualResolvedMinutes >  @ResolvedValMinutes) OR
                (@ResolvedOp = '<'  AND ActualResolvedMinutes <  @ResolvedValMinutes) OR
                (@ResolvedOp = '='  AND ActualResolvedMinutes =  @ResolvedValMinutes)
            )
        )
        
        -- Filter 3: Breach SLA (Numeric filter)
        AND (
            @p_breachSLA IS NULL
            OR (
                (@BreachOp = '>=' AND BreachMinutes >= @BreachValMinutes) OR
                (@BreachOp = '<=' AND BreachMinutes <= @BreachValMinutes) OR
                (@BreachOp = '>'  AND BreachMinutes >  @BreachValMinutes) OR
                (@BreachOp = '<'  AND BreachMinutes <  @BreachValMinutes) OR
                (@BreachOp = '='  AND BreachMinutes =  @BreachValMinutes)
            )
        )
        
        -- Filter 4: General Search (@p_search) - **NOW COMBINED HERE**
        AND (
            @p_search IS NULL
            OR UPPER([Incident Number]) LIKE '%' + UPPER(@p_search) + '%'
            OR UPPER([Assigned To]) LIKE '%' + UPPER(@p_search) + '%'
            OR UPPER([Short Description]) LIKE '%' + UPPER(@p_search) + '%'
            OR UPPER(Category) LIKE '%' + UPPER(@p_search) + '%'
            OR UPPER([State]) LIKE '%' + UPPER(@p_search) + '%'
            OR UPPER(CONVERT(VARCHAR(30), [Created], 120)) LIKE '%' + UPPER(@p_search) + '%'
            OR UPPER(CONVERT(VARCHAR(30), [Resolved Date & Time], 120)) LIKE '%' + UPPER(@p_search) + '%'
            OR UPPER(CONVERT(VARCHAR(30), [Updated], 120)) LIKE '%' + UPPER(@p_search) + '%'
            OR UPPER([Actual Resolved Time]) LIKE '%' + UPPER(@p_search) + '%'
            OR UPPER([Breach SLA]) LIKE '%' + UPPER(@p_search) + '%'
        );


    DECLARE @TotalCount INT = (SELECT COUNT(*) FROM #Final);
    DECLARE @TotalPages INT;
    IF @p_pageSize = 0
        SET @TotalPages = 1;
    ELSE
        SET @TotalPages = CASE WHEN @TotalCount = 0 THEN 1 ELSE CEILING(1.0 * @TotalCount / @p_pageSize) END;

    -- Sort logic and final SELECT remains the same (unchanged)
    IF @p_pageSize = 0
    BEGIN
        SELECT
            1              AS PageNumber,
            0              AS PageSize,
            1              AS TotalPages,
            @TotalCount    AS TotalElements,
            [Incident Number] AS incidentNumber,
            [Assigned To] AS assignedTo,
            [Short Description] AS shortDescription,
            Category,
            [State],
            [Created],
            [Resolved Date & Time] AS resolvedDateTime,
            [Updated],
            [Actual Resolved Time] AS actualResolvedTime,
            [Breach SLA] AS breachSLA
        FROM #Final
        ORDER BY
            CASE WHEN @p_sortBy = 'Number'                     AND @p_sortOrder = 'ASC'  THEN [Incident Number] END ASC,
            CASE WHEN @p_sortBy = 'Number'                     AND @p_sortOrder = 'DESC' THEN [Incident Number] END DESC,
            CASE WHEN @p_sortBy = 'AssignedTo'                 AND @p_sortOrder = 'ASC'  THEN [Assigned To] END ASC,
            CASE WHEN @p_sortBy = 'AssignedTo'                 AND @p_sortOrder = 'DESC' THEN [Assigned To] END DESC,
            CASE WHEN @p_sortBy = 'ShortDescription'           AND @p_sortOrder = 'ASC'  THEN [Short Description] END ASC,
            CASE WHEN @p_sortBy = 'ShortDescription'           AND @p_sortOrder = 'DESC' THEN [Short Description] END DESC,
            CASE WHEN @p_sortBy = 'Category'                   AND @p_sortOrder = 'ASC'  THEN Category END ASC,
            CASE WHEN @p_sortBy = 'Category'                   AND @p_sortOrder = 'DESC' THEN Category END DESC,
            CASE WHEN @p_sortBy = 'State'                      AND @p_sortOrder = 'ASC'  THEN [State] END ASC,
            CASE WHEN @p_sortBy = 'State'                      AND @p_sortOrder = 'DESC' THEN [State] END DESC,
            CASE WHEN @p_sortBy = 'Created'                    AND @p_sortOrder = 'ASC'  THEN [Created] END ASC,
            CASE WHEN @p_sortBy = 'Created'                    AND @p_sortOrder = 'DESC' THEN [Created] END DESC,
            CASE WHEN @p_sortBy = 'Resolved'                   AND @p_sortOrder = 'ASC'  THEN [Resolved Date & Time] END ASC,
            CASE WHEN @p_sortBy = 'Resolved'                   AND @p_sortOrder = 'DESC' THEN [Resolved Date & Time] END DESC,
            CASE WHEN @p_sortBy = 'Updated'                    AND @p_sortOrder = 'ASC'  THEN [Updated] END ASC,
            CASE WHEN @p_sortBy = 'Updated'                    AND @p_sortOrder = 'DESC' THEN [Updated] END DESC,
            CASE WHEN @p_sortBy = 'ActualResolvedTime'         AND @p_sortOrder = 'ASC'  THEN COALESCE(CAST(ActualResolvedMinutes AS BIGINT), 9223372036854775807) END ASC,
            CASE WHEN @p_sortBy = 'ActualResolvedTime'         AND @p_sortOrder = 'DESC' THEN COALESCE(CAST(ActualResolvedMinutes AS BIGINT), -9223372036854775808) END DESC,
            CASE WHEN @p_sortBy = 'BreachSLA'                  AND @p_sortOrder = 'ASC'  THEN COALESCE(CAST(BreachMinutes AS BIGINT), 9223372036854775807) END ASC,
            CASE WHEN @p_sortBy = 'BreachSLA'                  AND @p_sortOrder = 'DESC' THEN COALESCE(CAST(BreachMinutes AS BIGINT), -9223372036854775808) END DESC;
    END
    ELSE
    BEGIN
        SELECT
            @p_pageNumber AS PageNumber,
            @p_pageSize   AS PageSize,
            @TotalPages   AS TotalPages,
            @TotalCount   AS TotalElements,
            [Incident Number] AS incidentNumber,
            [Assigned To] AS assignedTo,
            [Short Description] AS shortDescription,
            Category,
            [State],
            [Created],
            [Resolved Date & Time] AS resolvedDateTime,
            [Updated],
            [Actual Resolved Time] AS actualResolvedTime,
            [Breach SLA] AS breachSLA
        FROM #Final
        ORDER BY
            CASE WHEN @p_sortBy = 'Number'                     AND @p_sortOrder = 'ASC'  THEN [Incident Number] END ASC,
            CASE WHEN @p_sortBy = 'Number'                     AND @p_sortOrder = 'DESC' THEN [Incident Number] END DESC,
            CASE WHEN @p_sortBy = 'AssignedTo'                 AND @p_sortOrder = 'ASC'  THEN [Assigned To] END ASC,
            CASE WHEN @p_sortBy = 'AssignedTo'                 AND @p_sortOrder = 'DESC' THEN [Assigned To] END DESC,
            CASE WHEN @p_sortBy = 'ShortDescription'           AND @p_sortOrder = 'ASC'  THEN [Short Description] END ASC,
            CASE WHEN @p_sortBy = 'ShortDescription'           AND @p_sortOrder = 'DESC' THEN [Short Description] END DESC,
            CASE WHEN @p_sortBy = 'Category'                   AND @p_sortOrder = 'ASC'  THEN Category END ASC,
            CASE WHEN @p_sortBy = 'Category'                   AND @p_sortOrder = 'DESC' THEN Category END DESC,
            CASE WHEN @p_sortBy = 'State'                      AND @p_sortOrder = 'ASC'  THEN [State] END ASC,
            CASE WHEN @p_sortBy = 'State'                      AND @p_sortOrder = 'DESC' THEN [State] END DESC,
            CASE WHEN @p_sortBy = 'Created'                    AND @p_sortOrder = 'ASC'  THEN [Created] END ASC,
            CASE WHEN @p_sortBy = 'Created'                    AND @p_sortOrder = 'DESC' THEN [Created] END DESC,
            CASE WHEN @p_sortBy = 'Resolved'                   AND @p_sortOrder = 'ASC'  THEN [Resolved Date & Time] END ASC,
            CASE WHEN @p_sortBy = 'Resolved'                   AND @p_sortOrder = 'DESC' THEN [Resolved Date & Time] END DESC,
            CASE WHEN @p_sortBy = 'Updated'                    AND @p_sortOrder = 'ASC'  THEN [Updated] END ASC,
            CASE WHEN @p_sortBy = 'Updated'                    AND @p_sortOrder = 'DESC' THEN [Updated] END DESC,
            CASE WHEN @p_sortBy = 'ActualResolvedTime'         AND @p_sortOrder = 'ASC'  THEN COALESCE(CAST(ActualResolvedMinutes AS BIGINT), 9223372036854775807) END ASC,
            CASE WHEN @p_sortBy = 'ActualResolvedTime'         AND @p_sortOrder = 'DESC' THEN COALESCE(CAST(ActualResolvedMinutes AS BIGINT), -9223372036854775808) END DESC,
            CASE WHEN @p_sortBy = 'BreachSLA'                  AND @p_sortOrder = 'ASC'  THEN COALESCE(CAST(BreachMinutes AS BIGINT), 9223372036854775807) END ASC,
            CASE WHEN @p_sortBy = 'BreachSLA'                  AND @p_sortOrder = 'DESC' THEN COALESCE(CAST(BreachMinutes AS BIGINT), -9223372036854775808) END DESC
        OFFSET @Offset ROWS
        FETCH NEXT @p_pageSize ROWS ONLY;
    END

    IF OBJECT_ID('tempdb..#Filtered') IS NOT NULL DROP TABLE #Filtered;
    IF OBJECT_ID('tempdb..#Final') IS NOT NULL     DROP TABLE #Final;
END;

GO


