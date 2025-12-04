USE [TorridAMS]
GO

/****** Object:  StoredProcedure [dbo].[sp_GetIncidentDetailsByPriority]    Script Date: 26-11-2025 11:43:29 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [dbo].[sp_GetIncidentDetailsByPriority]
    @p_fromDate DATETIME = NULL,
    @p_toDate DATETIME = NULL,
    @p_category NVARCHAR(MAX) = NULL,
    @p_assignmentGroup NVARCHAR(MAX) = NULL,
    @p_assignedToName NVARCHAR(MAX) = NULL,
    @p_state NVARCHAR(MAX) = NULL,
    @p_search NVARCHAR(MAX) = NULL,
    @p_priority NVARCHAR(MAX) = NULL,
    @p_pageNumber INT = 1,
    @p_pageSize INT = 8,
    @p_sortBy NVARCHAR(50) = 'Updated',
    @p_sortOrder NVARCHAR(4) = 'DESC'
AS
BEGIN
    SET NOCOUNT ON;

    IF @p_pageNumber IS NULL OR @p_pageNumber < 1 SET @p_pageNumber = 1;
    IF @p_pageSize IS NULL OR @p_pageSize < 0 SET @p_pageSize = 8;

    DECLARE @IsReturnAll BIT = CASE WHEN @p_pageSize = 0 THEN 1 ELSE 0 END;
    DECLARE @Offset INT = (@p_pageNumber - 1) * CASE WHEN @p_pageSize = 0 THEN 1 ELSE @p_pageSize END;

    IF OBJECT_ID('tempdb..#Filtered') IS NOT NULL DROP TABLE #Filtered;

    -- STEP 1: Apply all non-search filters and select base columns into #Filtered
    SELECT
        Number,
        CAST(Assigned_to AS NVARCHAR(MAX)) AS Assigned_to,
        CAST(Short_description AS NVARCHAR(MAX)) AS Short_description,
        CAST(Category AS NVARCHAR(MAX)) AS Category,
        CAST(State AS NVARCHAR(MAX)) AS State,
        Opened,
        Resolved,
        Updated,
        Priority
    INTO #Filtered
    FROM incidents
    WHERE
        (@p_fromDate IS NULL OR Opened >= @p_fromDate)
        AND (@p_toDate IS NULL OR Opened <= @p_toDate)
        AND (@p_assignmentGroup IS NULL OR UPPER(CAST(Assignment_group AS NVARCHAR(MAX))) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_assignmentGroup, ',')))
        AND (@p_category IS NULL OR UPPER(CAST(Category AS NVARCHAR(MAX))) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_category, ',')))
        AND (@p_assignedToName IS NULL OR UPPER(CAST(Assigned_to AS NVARCHAR(MAX))) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_assignedToName, ',')))
        AND (@p_state IS NULL OR UPPER(CAST(State AS NVARCHAR(MAX))) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_state, ',')))
        AND (@p_priority IS NULL OR UPPER(CAST(Priority AS NVARCHAR(MAX))) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_priority, ',')))
        -- **REMOVED @p_search FILTERING FROM HERE**

    -- STEP 2: Compute and format all final columns, including calculated ones
    IF OBJECT_ID('tempdb..#FinalTemp') IS NOT NULL DROP TABLE #FinalTemp;

    ;WITH Final AS
    (
        SELECT
            f.Number AS IncidentNo,
            f.Assigned_to AS AssignedTo,
            f.Short_description AS [ShortDescription],
            f.Category AS Category,
            f.State AS State,
            f.Opened AS Created,
            f.Resolved AS ResolvedDateTime,
            f.Updated AS Updated,
            f.Priority AS Priority,

            -- use SLA-aware TVF for minutes between Opened and Resolved
            ARHBM.BusinessMinutes AS ActualResolvedMinutes,

            -- SLA minutes (updated to your requested values)
            CASE LEFT(LTRIM(RTRIM(ISNULL(f.Priority,'4'))),1)
                WHEN '1' THEN 120  -- Critical: 2 hours
                WHEN '2' THEN 240  -- High:     4 hours
                WHEN '3' THEN 1440 -- Moderate: 24 hours
                WHEN '4' THEN 7200 -- Low:      120 hours (5 days)
                ELSE 7200
            END AS SLA_Minutes,

            -- Breach minutes = MAX(0, ActualResolvedMinutes - SLA)
            CASE 
                WHEN ARHBM.BusinessMinutes IS NULL THEN 0
                WHEN ARHBM.BusinessMinutes - 
                    CASE LEFT(LTRIM(RTRIM(ISNULL(f.Priority,'4'))),1)
                        WHEN '1' THEN 120 WHEN '2' THEN 240 WHEN '3' THEN 1440 WHEN '4' THEN 7200 ELSE 7200
                    END > 0
                THEN ARHBM.BusinessMinutes - 
                    CASE LEFT(LTRIM(RTRIM(ISNULL(f.Priority,'4'))),1)
                        WHEN '1' THEN 120 WHEN '2' THEN 240 WHEN '3' THEN 1440 WHEN '4' THEN 7200 ELSE 7200
                    END
                ELSE 0
            END AS BreachMinutes,

            -- formatted ActualResolvedTime using computed alias
            CASE
                WHEN ARHBM.BusinessMinutes IS NULL OR ARHBM.BusinessMinutes = 0 THEN 'N/A'
                ELSE LTRIM(RTRIM(
                    CASE WHEN ARHBM.BusinessMinutes / 1440 >= 1 THEN CONCAT(ARHBM.BusinessMinutes / 1440, ' days ') ELSE '' END +
                    CASE WHEN (ARHBM.BusinessMinutes % 1440) / 60 > 0 THEN CONCAT((ARHBM.BusinessMinutes % 1440) / 60, ' hours ') ELSE '' END +
                    CASE WHEN ARHBM.BusinessMinutes % 60 > 0 THEN CONCAT(ARHBM.BusinessMinutes % 60, ' mins') ELSE '' END
                ))
            END AS ActualResolvedTime,

            -- formatted BreachSLA using computed alias
            CASE
                WHEN (ARHBM.BusinessMinutes IS NULL) OR
                     (ARHBM.BusinessMinutes - 
                        CASE LEFT(LTRIM(RTRIM(ISNULL(f.Priority,'4'))),1)
                            WHEN '1' THEN 120 WHEN '2' THEN 240 WHEN '3' THEN 1440 WHEN '4' THEN 7200 ELSE 7200
                        END
                     ) <= 0
                THEN 'No Breach'
                ELSE LTRIM(RTRIM(
                    CASE WHEN (ARHBM.BusinessMinutes - 
                                CASE LEFT(LTRIM(RTRIM(ISNULL(f.Priority,'4'))),1)
                                    WHEN '1' THEN 120 WHEN '2' THEN 240 WHEN '3' THEN 1440 WHEN '4' THEN 7200 ELSE 7200
                                END)/1440 >= 1 
                         THEN CONCAT((ARHBM.BusinessMinutes - 
                                CASE LEFT(LTRIM(RTRIM(ISNULL(f.Priority,'4'))),1)
                                    WHEN '1' THEN 120 WHEN '2' THEN 240 WHEN '3' THEN 1440 WHEN '4' THEN 7200 ELSE 7200
                                END)/1440, ' days ') ELSE '' END +
                    CASE WHEN ((ARHBM.BusinessMinutes - 
                                CASE LEFT(LTRIM(RTRIM(ISNULL(f.Priority,'4'))),1)
                                    WHEN '1' THEN 120 WHEN '2' THEN 240 WHEN '3' THEN 1440 WHEN '4' THEN 7200 ELSE 7200
                                END) % 1440) / 60 > 0 
                         THEN CONCAT(((ARHBM.BusinessMinutes - 
                                CASE LEFT(LTRIM(RTRIM(ISNULL(f.Priority,'4'))),1)
                                    WHEN '1' THEN 120 WHEN '2' THEN 240 WHEN '3' THEN 1440 WHEN '4' THEN 7200 ELSE 7200
                                END) % 1440) / 60, ' hours ') ELSE '' END +
                    CASE WHEN (ARHBM.BusinessMinutes - 
                                CASE LEFT(LTRIM(RTRIM(ISNULL(f.Priority,'4'))),1)
                                    WHEN '1' THEN 120 WHEN '2' THEN 240 WHEN '3' THEN 1440 WHEN '4' THEN 7200 ELSE 7200
                                END) % 60 > 0 
                         THEN CONCAT((ARHBM.BusinessMinutes - 
                                CASE LEFT(LTRIM(RTRIM(ISNULL(f.Priority,'4'))),1)
                                    WHEN '1' THEN 120 WHEN '2' THEN 240 WHEN '3' THEN 1440 WHEN '4' THEN 7200 ELSE 7200
                                END) % 60, ' mins') ELSE '' END
                ))
            END AS BreachSLA

        FROM #Filtered f

        -- compute minutes using SLA-aware TVF
        -- IncludeWeekends = 1 for Priority 1 & 2 (count weekends), ELSE 0 (exclude weekend days)
        CROSS APPLY dbo.fn_SLAMinutes_iTVF(
            CAST(f.Opened AS DATETIME2),
            CAST(f.Resolved AS DATETIME2),
            CASE WHEN LEFT(LTRIM(RTRIM(ISNULL(f.Priority,'4'))),1) IN ('1','2') THEN 1 ELSE 0 END
        ) AS ARHBM
    )
    -- STEP 3: Apply the search filter to ALL columns (base and computed)
    SELECT *
    INTO #FinalTemp
    FROM Final
    WHERE
        @p_search IS NULL
        OR UPPER(IncidentNo) LIKE '%' + UPPER(@p_search) + '%'
        OR UPPER(AssignedTo) LIKE '%' + UPPER(@p_search) + '%'
        OR UPPER([ShortDescription]) LIKE '%' + UPPER(@p_search) + '%'
        OR UPPER(Category) LIKE '%' + UPPER(@p_search) + '%'
        OR UPPER(State) LIKE '%' + UPPER(@p_search) + '%'
        OR UPPER(Priority) LIKE '%' + UPPER(@p_search) + '%'
        OR UPPER(CONVERT(VARCHAR(30), Created, 120)) LIKE '%' + UPPER(@p_search) + '%'
        OR UPPER(CONVERT(VARCHAR(30), ResolvedDateTime, 120)) LIKE '%' + UPPER(@p_search) + '%'
        OR UPPER(CONVERT(VARCHAR(30), Updated, 120)) LIKE '%' + UPPER(@p_search) + '%'
        OR UPPER(ActualResolvedTime) LIKE '%' + UPPER(@p_search) + '%'
        OR UPPER(BreachSLA) LIKE '%' + UPPER(@p_search) + '%';

    -- Remaining logic (TotalCount, TotalPages, sorting, and final SELECT) remains the same.

    DECLARE @TotalCount INT = (SELECT COUNT(*) FROM #FinalTemp); -- Count from #FinalTemp
    DECLARE @TotalPages INT = CASE WHEN @TotalCount = 0 THEN 1 ELSE CEILING(1.0 * @TotalCount / CASE WHEN @p_pageSize = 0 THEN 1 ELSE @p_pageSize END) END;

    DECLARE @sortKey NVARCHAR(50) = LOWER(ISNULL(@p_sortBy, ''));
    SET @sortKey = REPLACE(REPLACE(REPLACE(@sortKey, ' ', ''), '_', ''), '-', '');
    IF @sortKey IN ('incidentno','incidentnumber','number','numbered') SET @sortKey = 'incidentno';
    IF @sortKey IN ('assignedto','assignto','assigned') SET @sortKey = 'assignedto';
    IF @sortKey IN ('shortdescription','shortdesc','short_description') SET @sortKey = 'shortdescription';
    IF @sortKey IN ('category') SET @sortKey = 'category';
    IF @sortKey IN ('state') SET @sortKey = 'state';
    IF @sortKey IN ('created','opened','opendate') SET @sortKey = 'created';
    IF @sortKey IN ('resolveddatetime','resolved','resolveddate') SET @sortKey = 'resolveddatetime';
    IF @sortKey IN ('updated','lastupdated','updateddate') SET @sortKey = 'updated';
    IF @sortKey IN ('priority') SET @sortKey = 'priority';
    IF @sortKey IN ('actualresolvedtime','actualresolved','actualresolvedtimeminutes','actualresolvedminutes','actual') SET @sortKey = 'actualresolvedtime';
    IF @sortKey IN ('breachsla','breachs','breach') SET @sortKey = 'breachsla';
    IF @sortKey = '' SET @sortKey = 'resolveddatetime';

    IF @IsReturnAll = 1
    BEGIN
        SELECT
            1 AS PageNumber,
            0 AS PageSize,
            1 AS TotalPages,
            @TotalCount AS TotalElements,
            IncidentNo,
            AssignedTo,
            [ShortDescription],
            Category,
            State,
            Created,
            ResolvedDateTime,
            Updated,
            Priority,
            ActualResolvedTime,
            BreachSLA
        FROM #FinalTemp
        ORDER BY
            CASE WHEN @sortKey = 'incidentno' AND UPPER(@p_sortOrder) = 'ASC'  THEN IncidentNo END ASC,
            CASE WHEN @sortKey = 'incidentno' AND UPPER(@p_sortOrder) = 'DESC' THEN IncidentNo END DESC,
            CASE WHEN @sortKey = 'assignedto' AND UPPER(@p_sortOrder) = 'ASC'  THEN AssignedTo END ASC,
            CASE WHEN @sortKey = 'assignedto' AND UPPER(@p_sortOrder) = 'DESC' THEN AssignedTo END DESC,
            CASE WHEN @sortKey = 'shortdescription' AND UPPER(@p_sortOrder) = 'ASC'  THEN [ShortDescription] END ASC,
            CASE WHEN @sortKey = 'shortdescription' AND UPPER(@p_sortOrder) = 'DESC' THEN [ShortDescription] END DESC,
            CASE WHEN @sortKey = 'category' AND UPPER(@p_sortOrder) = 'ASC'  THEN Category END ASC,
            CASE WHEN @sortKey = 'category' AND UPPER(@p_sortOrder) = 'DESC' THEN Category END DESC,
            CASE WHEN @sortKey = 'state' AND UPPER(@p_sortOrder) = 'ASC'  THEN State END ASC,
            CASE WHEN @sortKey = 'state' AND UPPER(@p_sortOrder) = 'DESC' THEN State END DESC,
            CASE WHEN @sortKey = 'created' AND UPPER(@p_sortOrder) = 'ASC'  THEN Created END ASC,
            CASE WHEN @sortKey = 'created' AND UPPER(@p_sortOrder) = 'DESC' THEN Created END DESC,
            CASE WHEN @sortKey = 'resolveddatetime' AND UPPER(@p_sortOrder) = 'ASC'  THEN ResolvedDateTime END ASC,
            CASE WHEN @sortKey = 'resolveddatetime' AND UPPER(@p_sortOrder) = 'DESC' THEN ResolvedDateTime END DESC,
            CASE WHEN @sortKey = 'updated' AND UPPER(@p_sortOrder) = 'ASC'  THEN Updated END ASC,
            CASE WHEN @sortKey = 'updated' AND UPPER(@p_sortOrder) = 'DESC' THEN Updated END DESC,
            CASE WHEN @sortKey = 'actualresolvedtime' AND UPPER(@p_sortOrder) = 'ASC'
                 THEN COALESCE(CAST(ActualResolvedMinutes AS BIGINT), 9223372036854775807) END ASC,
            CASE WHEN @sortKey = 'actualresolvedtime' AND UPPER(@p_sortOrder) = 'DESC'
                 THEN COALESCE(CAST(ActualResolvedMinutes AS BIGINT), -9223372036854775808) END DESC,
            CASE WHEN @sortKey = 'breachsla' AND UPPER(@p_sortOrder) = 'ASC'
                 THEN COALESCE(CAST(BreachMinutes AS BIGINT), 9223372036854775807) END ASC,
            CASE WHEN @sortKey = 'breachsla' AND UPPER(@p_sortOrder) = 'DESC'
                 THEN COALESCE(CAST(BreachMinutes AS BIGINT), -9223372036854775808) END DESC;
    END
    ELSE
    BEGIN
        SELECT
            @p_pageNumber AS PageNumber,
            @p_pageSize AS PageSize,
            @TotalPages AS TotalPages,
            @TotalCount AS TotalElements,
            IncidentNo,
            AssignedTo,
            [ShortDescription],
            Category,
            State,
            Created,
            ResolvedDateTime,
            Updated,
            Priority,
            ActualResolvedTime,
            BreachSLA
        FROM #FinalTemp
        ORDER BY
            CASE WHEN @sortKey = 'incidentno' AND UPPER(@p_sortOrder) = 'ASC'  THEN IncidentNo END ASC,
            CASE WHEN @sortKey = 'incidentno' AND UPPER(@p_sortOrder) = 'DESC' THEN IncidentNo END DESC,
            CASE WHEN @sortKey = 'assignedto' AND UPPER(@p_sortOrder) = 'ASC'  THEN AssignedTo END ASC,
            CASE WHEN @sortKey = 'assignedto' AND UPPER(@p_sortOrder) = 'DESC' THEN AssignedTo END DESC,
            CASE WHEN @sortKey = 'shortdescription' AND UPPER(@p_sortOrder) = 'ASC'  THEN [ShortDescription] END ASC,
            CASE WHEN @sortKey = 'shortdescription' AND UPPER(@p_sortOrder) = 'DESC' THEN [ShortDescription] END DESC,
            CASE WHEN @sortKey = 'category' AND UPPER(@p_sortOrder) = 'ASC'  THEN Category END ASC,
            CASE WHEN @sortKey = 'category' AND UPPER(@p_sortOrder) = 'DESC' THEN Category END DESC,
            CASE WHEN @sortKey = 'state' AND UPPER(@p_sortOrder) = 'ASC'  THEN State END ASC,
            CASE WHEN @sortKey = 'state' AND UPPER(@p_sortOrder) = 'DESC' THEN State END DESC,
            CASE WHEN @sortKey = 'created' AND UPPER(@p_sortOrder) = 'ASC'  THEN Created END ASC,
            CASE WHEN @sortKey = 'created' AND UPPER(@p_sortOrder) = 'DESC' THEN Created END DESC,
            CASE WHEN @sortKey = 'resolveddatetime' AND UPPER(@p_sortOrder) = 'ASC'  THEN ResolvedDateTime END ASC,
            CASE WHEN @sortKey = 'resolveddatetime' AND UPPER(@p_sortOrder) = 'DESC' THEN ResolvedDateTime END DESC,
            CASE WHEN @sortKey = 'updated' AND UPPER(@p_sortOrder) = 'ASC'  THEN Updated END ASC,
            CASE WHEN @sortKey = 'updated' AND UPPER(@p_sortOrder) = 'DESC' THEN Updated END DESC,
            CASE WHEN @sortKey = 'actualresolvedtime' AND UPPER(@p_sortOrder) = 'ASC'
                 THEN COALESCE(CAST(ActualResolvedMinutes AS BIGINT), 9223372036854775807) END ASC,
            CASE WHEN @sortKey = 'actualresolvedtime' AND UPPER(@p_sortOrder) = 'DESC'
                 THEN COALESCE(CAST(ActualResolvedMinutes AS BIGINT), -9223372036854775808) END DESC,
            CASE WHEN @sortKey = 'breachsla' AND UPPER(@p_sortOrder) = 'ASC'
                 THEN COALESCE(CAST(BreachMinutes AS BIGINT), 9223372036854775807) END ASC,
            CASE WHEN @sortKey = 'breachsla' AND UPPER(@p_sortOrder) = 'DESC'
                 THEN COALESCE(CAST(BreachMinutes AS BIGINT), -9223372036854775808) END DESC
        OFFSET @Offset ROWS FETCH NEXT @p_pageSize ROWS ONLY;
    END

    DROP TABLE #Filtered;
    DROP TABLE #FinalTemp;
END;

GO


