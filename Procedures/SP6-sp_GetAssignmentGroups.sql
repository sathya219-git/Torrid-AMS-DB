USE IncidentDB;
GO

IF OBJECT_ID('sp_GetAssignmentGroups', 'P') IS NOT NULL
    DROP PROCEDURE sp_GetAssignmentGroups;
GO

CREATE PROCEDURE sp_GetAssignmentGroups
    @p_fromDate DATETIME = NULL,
    @p_toDate DATETIME = NULL,
    @p_category VARCHAR(100) = NULL,
    @p_priority VARCHAR(50) = NULL,
    @p_assignedToName VARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    SELECT DISTINCT Assignment_group AS AssignmentGroup
    FROM incidents
    WHERE
        (@p_fromDate IS NULL OR @p_toDate IS NULL OR Opened BETWEEN @p_fromDate AND @p_toDate)
        AND (@p_category IS NULL OR UPPER(Category) = UPPER(@p_category))
        AND (@p_priority IS NULL OR UPPER(Priority) = UPPER(@p_priority))
        AND (@p_assignedToName IS NULL OR UPPER(Assigned_to) = UPPER(@p_assignedToName))
    ORDER BY Assignment_group;
END
GO
