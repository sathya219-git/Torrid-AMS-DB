USE [TorridAMS]
GO

/****** Object:  StoredProcedure [dbo].[sp_GetDashboardKpis]    Script Date: 26-11-2025 11:39:27 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [dbo].[sp_GetDashboardKpis]
(
    @p_fromDate DATETIME = NULL,
    @p_toDate   DATETIME = NULL,
    @p_assignmentGroup NVARCHAR(MAX) = NULL,
    @p_category NVARCHAR(MAX) = NULL,
    @p_priority NVARCHAR(MAX) = NULL,
    @p_assignedToName NVARCHAR(MAX) = NULL,
    @p_state NVARCHAR(MAX) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    ;WITH FilteredIncidents AS
    (
        SELECT
            i.Number,
            i.Assigned_to,
            i.State,
            i.Opened,
            i.Resolved,
            i.Priority,
            i.Assignment_group,
            i.Category
        FROM dbo.incidents AS i
        WHERE
            (@p_fromDate IS NULL OR i.Opened >= @p_fromDate)
            AND (@p_toDate   IS NULL OR i.Opened <= @p_toDate)
            AND (
                @p_assignmentGroup IS NULL
                OR UPPER(i.Assignment_group) IN (
                    SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_assignmentGroup, ',')
                )
            )
            AND (
                @p_category IS NULL
                OR UPPER(i.Category) IN (
                    SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_category, ',')
                )
            )
            AND (
                @p_priority IS NULL
                OR UPPER(i.Priority) IN (
                    SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_priority, ',')
                )
            )
            AND (
                @p_assignedToName IS NULL
                OR UPPER(i.Assigned_to) IN (
                    SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_assignedToName, ',')
                )
            )
            AND (
                @p_state IS NULL
                OR UPPER(i.State) IN (
                    SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_state, ',')
                )
            )
    )

    SELECT
        COUNT(*) AS totalIncidents,
        SUM(CASE WHEN UPPER(State) = 'OPEN' THEN 1 ELSE 0 END) AS openIncidents,
        SUM(CASE WHEN UPPER(State) = 'IN PROGRESS' THEN 1 ELSE 0 END) AS inProgressIncidents,
        SUM(CASE WHEN UPPER(State) = 'CLOSED' THEN 1 ELSE 0 END) AS closedIncidents,
        -- breached: Resolved not null AND computed minutes > SLA_Minutes
        SUM(
            CASE
                WHEN fi.Resolved IS NOT NULL
                     AND COALESCE(bm.BusinessMinutes, 0) > sla.SLA_Minutes
                THEN 1 ELSE 0
            END
        ) AS breached
    FROM FilteredIncidents AS fi
    CROSS APPLY
        dbo.fn_SLAMinutes_iTVF(CAST(fi.Opened AS DATETIME2), CAST(fi.Resolved AS DATETIME2),
            CASE WHEN LEFT(LTRIM(RTRIM(ISNULL(fi.Priority,'4'))),1) IN ('1','2') THEN 1 ELSE 0 END
        ) AS bm
    CROSS APPLY
        ( SELECT
            CASE LEFT(LTRIM(RTRIM(ISNULL(fi.Priority, '4'))),1)
                WHEN '1' THEN 120   -- Critical: 2 hours
                WHEN '2' THEN 240   -- High: 4 hours
                WHEN '3' THEN 1440  -- Moderate: 24 hours
                WHEN '4' THEN 7200  -- Low: 120 hours (5 days)
                ELSE 7200
            END AS SLA_Minutes
        ) AS sla;
END;

GO


