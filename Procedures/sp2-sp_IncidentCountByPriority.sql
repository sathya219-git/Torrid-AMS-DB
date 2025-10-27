USE Torrid_AMS;
GO

IF OBJECT_ID('sp_IncidentCountByPriority', 'P') IS NOT NULL
    DROP PROCEDURE sp_IncidentCountByPriority;
GO

CREATE PROCEDURE sp_IncidentCountByPriority
    @p_fromDate DATETIME = NULL,
    @p_toDate DATETIME = NULL,
    @p_category VARCHAR(100) = NULL,
    @p_assignmentGroup VARCHAR(100) = NULL,
    @p_priority VARCHAR(50) = NULL,
    @p_assignedToName VARCHAR(100) = NULL,
    @p_state VARCHAR(50) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    SELECT 
        Priority,
        COUNT(*) AS IncidentCount,
        AVG(CASE 
                WHEN State = 'Closed' AND Resolved IS NOT NULL 
                THEN DATEDIFF(HOUR, Opened, Resolved) 
                ELSE NULL 
            END) AS AvgResolutionTime_Hours
    FROM incidents
    WHERE
        (@p_fromDate IS NULL OR @p_toDate IS NULL OR Opened BETWEEN @p_fromDate AND @p_toDate)
        AND (@p_category IS NULL OR UPPER(Category) = UPPER(@p_category))
        AND (@p_assignmentGroup IS NULL OR UPPER(Assignment_group) = UPPER(@p_assignmentGroup))
        AND (@p_priority IS NULL OR UPPER(Priority) = UPPER(@p_priority))
        AND (@p_assignedToName IS NULL OR UPPER(Assigned_to) = UPPER(@p_assignedToName))
        AND (@p_state IS NULL OR UPPER(State) = UPPER(@p_state))
    GROUP BY Priority
    ORDER BY IncidentCount DESC;
END;
GO
