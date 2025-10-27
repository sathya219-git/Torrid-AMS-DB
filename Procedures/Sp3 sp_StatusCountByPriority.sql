USE  Torrid_AMS;;
GO

IF OBJECT_ID('sp_StatusCountByPriority', 'P') IS NOT NULL
    DROP PROCEDURE sp_StatusCountByPriority;
GO

CREATE PROCEDURE sp_StatusCountByPriority
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
        State AS Status,
        COUNT(*) AS IncidentCount
    FROM incidents
    WHERE 
        (@p_fromDate IS NULL OR @p_toDate IS NULL OR Opened BETWEEN @p_fromDate AND @p_toDate)
        AND (@p_category IS NULL OR UPPER(Category) = UPPER(@p_category))
        AND (@p_assignmentGroup IS NULL OR UPPER(Assignment_group) = UPPER(@p_assignmentGroup))
        AND (@p_priority IS NULL OR UPPER(Priority) = UPPER(@p_priority))
        AND (@p_assignedToName IS NULL OR UPPER(Assigned_to) = UPPER(@p_assignedToName))
        AND (@p_state IS NULL OR UPPER(State) = UPPER(@p_state))
    GROUP BY Priority, State
    ORDER BY Priority, State;
END;
GO
