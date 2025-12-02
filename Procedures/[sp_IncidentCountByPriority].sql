USE [TorridAMS]
GO

/****** Object:  StoredProcedure [dbo].[sp_IncidentCountByPriority]    Script Date: 26-11-2025 11:44:56 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [dbo].[sp_IncidentCountByPriority]
    @p_fromDate DATETIME = NULL,
    @p_toDate DATETIME = NULL,
    @p_category NVARCHAR(MAX) = NULL,
    @p_assignmentGroup NVARCHAR(MAX) = NULL,
    @p_priority NVARCHAR(MAX) = NULL,
    @p_assignedToName NVARCHAR(MAX) = NULL,
    @p_state NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    ----------------------------------------------------------------------
    -- Filter incidents first
    ----------------------------------------------------------------------
    ;WITH FilteredIncidents AS (
        SELECT *
        FROM incidents
        WHERE 
            -- apply bounds independently so a single NULL doesn't disable the filter
            (@p_fromDate IS NULL OR Opened >= @p_fromDate)
            AND (@p_toDate   IS NULL OR Opened <= @p_toDate)
            AND (@p_assignmentGroup IS NULL OR UPPER(Assignment_group) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_assignmentGroup, ',')))
            AND (@p_category IS NULL OR UPPER(Category) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_category, ',')))
            AND (@p_priority IS NULL OR UPPER(Priority) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_priority, ',')))
            AND (@p_assignedToName IS NULL OR UPPER(Assigned_To) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_assignedToName, ',')))
            AND (@p_state IS NULL OR UPPER(State) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_state, ',')))
    )

    ----------------------------------------------------------------------
    -- BusinessCalc: compute SLA-aware minutes per incident (using SLA TVF)
    ----------------------------------------------------------------------
    , BusinessCalc AS (
        SELECT
            fi.*,
            SLABM.BusinessMinutes AS BusinessMinutesResolved
        FROM FilteredIncidents fi
        CROSS APPLY
            dbo.fn_SLAMinutes_iTVF(
                CAST(fi.Opened AS DATETIME2),
                CAST(fi.Resolved AS DATETIME2),
                CASE WHEN LEFT(LTRIM(RTRIM(ISNULL(fi.Priority, '4'))),1) IN ('1','2') THEN 1 ELSE 0 END
            ) AS SLABM
    )

    ----------------------------------------------------------------------
    -- ResolvedData: only closed incidents (use TVF minutes as MinutesTaken)
    -- Also compute SLA and breached flag (based on TVF minutes > SLA)
    ----------------------------------------------------------------------
    , ResolvedData AS (
        SELECT 
            Priority,
            State,
            BusinessMinutesResolved,
            -- use TVF minutes directly
            BusinessMinutesResolved AS MinutesTaken,
            -- SLA mapping by priority (first digit) -> UPDATED SLA values
            CASE LEFT(LTRIM(RTRIM(ISNULL(Priority,'4'))),1)
                WHEN '1' THEN 120    -- Critical: 2 hours
                WHEN '2' THEN 240    -- High:     4 hours
                WHEN '3' THEN 1440   -- Moderate: 24 hours (1 day)
                WHEN '4' THEN 7200   -- Low:      120 hours (5 days)
                ELSE 7200
            END AS SLA_Minutes,
            -- breached flag: 1 if TVF minutes (Opened->Resolved) > SLA, else 0
            CASE 
                WHEN Resolved IS NULL THEN 0
                WHEN BusinessMinutesResolved > 
                    CASE LEFT(LTRIM(RTRIM(ISNULL(Priority,'4'))),1)
                        WHEN '1' THEN 120 WHEN '2' THEN 240 WHEN '3' THEN 1440 WHEN '4' THEN 7200 ELSE 7200
                    END
                THEN 1 ELSE 0
            END AS IsBreached
        FROM BusinessCalc
        WHERE Resolved IS NOT NULL
    )

    ----------------------------------------------------------------------
    -- Totals and averages aggregated by Priority, State
    ----------------------------------------------------------------------
    , TotalAndAvg AS (
        SELECT 
            Priority,
            State,
            COUNT(*) AS ClosedCount,
            SUM(MinutesTaken) AS TotalMinutes,
            AVG(CAST(MinutesTaken AS FLOAT)) AS AvgMinutes,
            -- count of breached incidents (TVF minutes > SLA)
            SUM(CASE WHEN IsBreached = 1 THEN 1 ELSE 0 END) AS BreachedCount
        FROM ResolvedData
        GROUP BY Priority, State
    )

    ----------------------------------------------------------------------
    -- Incident counts per priority/state
    ----------------------------------------------------------------------
    , IncidentCount AS (
        SELECT Priority, State, COUNT(*) AS IncidentCount
        FROM FilteredIncidents
        GROUP BY Priority, State
    )

    , TotalPerPriority AS (
        SELECT Priority, SUM(IncidentCount) AS TotalCount
        FROM IncidentCount
        GROUP BY Priority
    )

    ----------------------------------------------------------------------
    -- Final output: join counts with totals/averages and format minutes -> days/hours/mins
    ----------------------------------------------------------------------
    SELECT 
        ic.Priority,
        ic.State,
        ic.IncidentCount,
        tp.TotalCount,  -- Total incidents per priority across states

        -- Average Time formatted as days, hours, minutes (based on TVF minutes)
        CASE 
            WHEN ta.AvgMinutes IS NULL THEN NULL
            WHEN ta.AvgMinutes >= 1440 THEN 
                CONCAT(
                    CAST(FLOOR(ta.AvgMinutes / 1440) AS BIGINT), ' days ',
                    CAST( (CAST(FLOOR(ta.AvgMinutes) AS BIGINT) % 1440) / 60 AS BIGINT), ' hours ',
                    CAST( CAST(FLOOR(ta.AvgMinutes) AS BIGINT) % 60 AS BIGINT), ' minutes'
                )
            WHEN ta.AvgMinutes >= 60 THEN 
                CONCAT(
                    CAST(FLOOR(ta.AvgMinutes / 60) AS BIGINT), ' hours ',
                    CAST(CAST(FLOOR(ta.AvgMinutes) AS BIGINT) % 60 AS BIGINT), ' minutes'
                )
            ELSE 
                CONCAT(CAST(FLOOR(ta.AvgMinutes) AS BIGINT), ' minutes')
        END AS AvgResolvedTime,

        -- Total Time formatted as days, hours, minutes (TVF minutes)
        CASE 
            WHEN ta.TotalMinutes IS NULL THEN NULL
            WHEN ta.TotalMinutes >= 1440 THEN 
                CONCAT(
                    CAST(FLOOR(ta.TotalMinutes / 1440) AS BIGINT), ' days ',
                    CAST( (CAST(FLOOR(ta.TotalMinutes) AS BIGINT) % 1440) / 60 AS BIGINT), ' hours ',
                    CAST( CAST(FLOOR(ta.TotalMinutes) AS BIGINT) % 60 AS BIGINT), ' minutes'
                )
            WHEN ta.TotalMinutes >= 60 THEN 
                CONCAT(
                    CAST(FLOOR(ta.TotalMinutes / 60) AS BIGINT), ' hours ',
                    CAST(CAST(FLOOR(ta.TotalMinutes) AS BIGINT) % 60 AS BIGINT), ' minutes'
                )
            ELSE 
                CONCAT(CAST(FLOOR(ta.TotalMinutes) AS BIGINT), ' minutes')
        END AS TotalResolvedTime,

        -- count of breached incidents (TVF-minute breaches vs SLA)
        ISNULL(ta.BreachedCount, 0) AS BreachedCount

    FROM IncidentCount ic
    LEFT JOIN TotalAndAvg ta 
        ON ic.Priority = ta.Priority AND ic.State = ta.State
    LEFT JOIN TotalPerPriority tp
        ON ic.Priority = tp.Priority
    ORDER BY ic.Priority, ic.State;
END;

GO


