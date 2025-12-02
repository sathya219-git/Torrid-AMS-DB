USE [TorridAMS]
GO

/****** Object:  StoredProcedure [dbo].[sp_NameAndIncidentCountByPriority]    Script Date: 26-11-2025 11:45:40 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [dbo].[sp_NameAndIncidentCountByPriority] 
    @p_fromDate       DATETIME     = NULL,
    @p_toDate         DATETIME     = NULL,
    @p_category       NVARCHAR(MAX)= NULL,
    @p_assignmentGroup NVARCHAR(MAX)= NULL,
    @p_priority       NVARCHAR(MAX)= NULL,
    @p_assignedToName NVARCHAR(MAX)= NULL,
    @p_state          NVARCHAR(MAX)= NULL,
    @p_pageNumber     INT          = 1,
    @p_pageSize       INT          = 4,
    @p_sortBy         NVARCHAR(50) = 'Name',  -- default sort column
    @p_sortOrder      NVARCHAR(4)  = 'ASC'    -- ASC or DESC
AS
BEGIN
    SET NOCOUNT ON;

    -- Default safety
    IF @p_pageNumber IS NULL OR @p_pageNumber < 1 SET @p_pageNumber = 1;
    IF @p_pageSize IS NULL OR @p_pageSize < 0 SET @p_pageSize = 4;

    DECLARE @Offset INT = (@p_pageNumber - 1) * CASE WHEN @p_pageSize = 0 THEN 1 ELSE @p_pageSize END;

    ----------------------------------------------------------------------
    -- Compute per-incident business minutes (one pass), then aggregate
    ----------------------------------------------------------------------
    ;WITH Filtered AS
    (
        SELECT i.*
        FROM dbo.incidents i
        WHERE
            -- apply bounds independently (small fix)
            (@p_fromDate IS NULL OR i.Opened >= @p_fromDate)
            AND (@p_toDate   IS NULL OR i.Opened <= @p_toDate)
            AND (@p_assignmentGroup IS NULL OR UPPER(i.Assignment_group) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_assignmentGroup, ',')))
            AND (@p_category IS NULL OR UPPER(i.Category) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_category, ',')))
            AND (@p_priority IS NULL OR UPPER(i.Priority) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_priority, ',')))
            AND (@p_assignedToName IS NULL OR UPPER(i.Assigned_to) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_assignedToName, ',')))
            AND (@p_state IS NULL OR UPPER(i.State) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_state, ',')))
    ),
    PerIncident AS
    (
        SELECT
            COALESCE(NULLIF(LTRIM(RTRIM(f.Assigned_to)), ''), N'Unassigned') AS AssignedName,
            f.Priority,
            f.Updated,
            -- compute SLA-aware minutes once per incident via the inline TVF (explicit cast to DATETIME2)
            -- IncludeWeekends = 1 for priorities starting with 1 or 2, else 0 (match breachlist logic)
            br.BusinessMinutes AS BusinessMinutesResolved,
            -- UPDATED: use TVF minutes directly (match BreachList formatting logic)
            -- MinutesTaken now equals the TVF result (may be NULL or 0); no fall back to wall-clock minutes.
            br.BusinessMinutes AS MinutesTaken
        FROM Filtered f
        CROSS APPLY dbo.fn_SLAMinutes_iTVF(
            CAST(f.Opened AS DATETIME2),
            CAST(f.Resolved AS DATETIME2),
            CASE WHEN LEFT(LTRIM(RTRIM(ISNULL(f.Priority, ''))),1) IN ('1','2') THEN 1 ELSE 0 END
        ) AS br
    )

    ----------------------------------------------------------------------
    -- Aggregate per assignee (same columns as your original)
    ----------------------------------------------------------------------
    SELECT
        p.AssignedName AS Name,
        SUM(CASE WHEN p.Priority LIKE '1%' THEN 1 ELSE 0 END) AS P1,
        SUM(CASE WHEN p.Priority LIKE '2%' THEN 1 ELSE 0 END) AS P2,
        SUM(CASE WHEN p.Priority LIKE '3%' THEN 1 ELSE 0 END) AS P3,
        SUM(CASE WHEN p.Priority LIKE '4%' THEN 1 ELSE 0 END) AS P4,
        COUNT(*) AS TotalCount,
        MAX(p.Updated) AS LastUpdated,
        -- average minutes taken (use DECIMAL for fractional averages)
        AVG(CAST(p.MinutesTaken AS DECIMAL(18,2))) AS ActualResolvedTime_Min
    INTO #Results
    FROM PerIncident p
    GROUP BY p.AssignedName;

    DECLARE @TotalCount INT = (SELECT COUNT(*) FROM #Results);

    ----------------------------------------------------------------------
    -- Format ActualResolvedTime (minutes ? days/hours/mins)
    ----------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#FormattedResults') IS NOT NULL DROP TABLE #FormattedResults;

    SELECT * ,
        CASE 
            WHEN ActualResolvedTime_Min IS NULL THEN N'N/A'
            WHEN ActualResolvedTime_Min < 60 THEN CONCAT(CAST(FLOOR(ActualResolvedTime_Min) AS INT), N' mins')
            WHEN ActualResolvedTime_Min < 1440 THEN CONCAT(CAST(FLOOR(ActualResolvedTime_Min/60) AS INT), N' hours ', CAST(FLOOR(ActualResolvedTime_Min % 60) AS INT), N' mins')
            ELSE CONCAT(CAST(FLOOR(ActualResolvedTime_Min/1440) AS INT), N' days ', CAST(FLOOR((ActualResolvedTime_Min % 1440)/60) AS INT), N' hours ', CAST(FLOOR(ActualResolvedTime_Min % 60) AS INT), N' mins')
        END AS [ActualResolvedTime]
    INTO #FormattedResults
    FROM #Results;

    ----------------------------------------------------------------------
    -- Static CASE-based ORDER BY / Paging (no dynamic SQL)
    ----------------------------------------------------------------------
    IF @p_pageSize = 0
    BEGIN
        SELECT
            1 AS CurrentPage,
            0 AS PageSize,
            1 AS TotalPages,
            @TotalCount AS TotalRecords,
            Name, P1, P2, P3, P4, TotalCount, [ActualResolvedTime], LastUpdated
        FROM #FormattedResults
        ORDER BY 
            CASE WHEN LOWER(@p_sortBy) = 'name' AND UPPER(@p_sortOrder) = 'ASC'  THEN Name END ASC,
            CASE WHEN LOWER(@p_sortBy) = 'name' AND UPPER(@p_sortOrder) = 'DESC' THEN Name END DESC,
            CASE WHEN LOWER(@p_sortBy) = 'p1' AND UPPER(@p_sortOrder) = 'ASC'  THEN P1 END ASC,
            CASE WHEN LOWER(@p_sortBy) = 'p1' AND UPPER(@p_sortOrder) = 'DESC' THEN P1 END DESC,
            CASE WHEN LOWER(@p_sortBy) = 'p2' AND UPPER(@p_sortOrder) = 'ASC'  THEN P2 END ASC,
            CASE WHEN LOWER(@p_sortBy) = 'p2' AND UPPER(@p_sortOrder) = 'DESC' THEN P2 END DESC,
            CASE WHEN LOWER(@p_sortBy) = 'p3' AND UPPER(@p_sortOrder) = 'ASC'  THEN P3 END ASC,
            CASE WHEN LOWER(@p_sortBy) = 'p3' AND UPPER(@p_sortOrder) = 'DESC' THEN P3 END DESC,
            CASE WHEN LOWER(@p_sortBy) = 'p4' AND UPPER(@p_sortOrder) = 'ASC'  THEN P4 END ASC,
            CASE WHEN LOWER(@p_sortBy) = 'p4' AND UPPER(@p_sortOrder) = 'DESC' THEN P4 END DESC,
            CASE WHEN LOWER(@p_sortBy) = 'totalcount' AND UPPER(@p_sortOrder) = 'ASC'  THEN TotalCount END ASC,
            CASE WHEN LOWER(@p_sortBy) = 'totalcount' AND UPPER(@p_sortOrder) = 'DESC' THEN TotalCount END DESC,
            CASE WHEN LOWER(@p_sortBy) = 'lastupdated' AND UPPER(@p_sortOrder) = 'ASC'  THEN LastUpdated END ASC,
            CASE WHEN LOWER(@p_sortBy) = 'lastupdated' AND UPPER(@p_sortOrder) = 'DESC' THEN LastUpdated END DESC,
            CASE WHEN LOWER(@p_sortBy) = 'actualresolvedtime' AND UPPER(@p_sortOrder) = 'ASC'
                 THEN COALESCE(CAST(ActualResolvedTime_Min AS BIGINT), 9223372036854775807) END ASC,
            CASE WHEN LOWER(@p_sortBy) = 'actualresolvedtime' AND UPPER(@p_sortOrder) = 'DESC'
                 THEN COALESCE(CAST(ActualResolvedTime_Min AS BIGINT), -9223372036854775808) END DESC;
    END
    ELSE
    BEGIN
        SELECT
            @p_pageNumber AS CurrentPage,
            @p_pageSize   AS PageSize,
            CEILING(CAST(@TotalCount AS FLOAT) / NULLIF(@p_pageSize,0)) AS TotalPages,
            @TotalCount   AS TotalRecords,
            Name, P1, P2, P3, P4, TotalCount, [ActualResolvedTime], LastUpdated
        FROM #FormattedResults
        ORDER BY 
            CASE WHEN LOWER(@p_sortBy) = 'name' AND UPPER(@p_sortOrder) = 'ASC'  THEN Name END ASC,
            CASE WHEN LOWER(@p_sortBy) = 'name' AND UPPER(@p_sortOrder) = 'DESC' THEN Name END DESC,
            CASE WHEN LOWER(@p_sortBy) = 'p1' AND UPPER(@p_sortOrder) = 'ASC'  THEN P1 END ASC,
            CASE WHEN LOWER(@p_sortBy) = 'p1' AND UPPER(@p_sortOrder) = 'DESC' THEN P1 END DESC,
            CASE WHEN LOWER(@p_sortBy) = 'p2' AND UPPER(@p_sortOrder) = 'ASC'  THEN P2 END ASC,
            CASE WHEN LOWER(@p_sortBy) = 'p2' AND UPPER(@p_sortOrder) = 'DESC' THEN P2 END DESC,
            CASE WHEN LOWER(@p_sortBy) = 'p3' AND UPPER(@p_sortOrder) = 'ASC'  THEN P3 END ASC,
            CASE WHEN LOWER(@p_sortBy) = 'p3' AND UPPER(@p_sortOrder) = 'DESC' THEN P3 END DESC,
            CASE WHEN LOWER(@p_sortBy) = 'p4' AND UPPER(@p_sortOrder) = 'ASC'  THEN P4 END ASC,
            CASE WHEN LOWER(@p_sortBy) = 'p4' AND UPPER(@p_sortOrder) = 'DESC' THEN P4 END DESC,
            CASE WHEN LOWER(@p_sortBy) = 'totalcount' AND UPPER(@p_sortOrder) = 'ASC'  THEN TotalCount END ASC,
            CASE WHEN LOWER(@p_sortBy) = 'totalcount' AND UPPER(@p_sortOrder) = 'DESC' THEN TotalCount END DESC,
            CASE WHEN LOWER(@p_sortBy) = 'lastupdated' AND UPPER(@p_sortOrder) = 'ASC'  THEN LastUpdated END ASC,
            CASE WHEN LOWER(@p_sortBy) = 'lastupdated' AND UPPER(@p_sortOrder) = 'DESC' THEN LastUpdated END DESC,
            CASE WHEN LOWER(@p_sortBy) = 'actualresolvedtime' AND UPPER(@p_sortOrder) = 'ASC'
                 THEN COALESCE(CAST(ActualResolvedTime_Min AS BIGINT), 9223372036854775807) END ASC,
            CASE WHEN LOWER(@p_sortBy) = 'actualresolvedtime' AND UPPER(@p_sortOrder) = 'DESC'
                 THEN COALESCE(CAST(ActualResolvedTime_Min AS BIGINT), -9223372036854775808) END DESC
        OFFSET @Offset ROWS FETCH NEXT @p_pageSize ROWS ONLY;
    END;

    DROP TABLE #FormattedResults;
    DROP TABLE #Results;
END;

GO


