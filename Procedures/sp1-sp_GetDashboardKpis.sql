USE Torrid_AMS;
GO
IF OBJECT_ID('sp_GetDashboardKpis', 'P') IS NOT NULL
    DROP PROCEDURE sp_GetDashboardKpis;
GO


create PROCEDURE sp_GetDashboardKpis
    @p_fromDate DATETIME = NULL,
    @p_toDate DATETIME = NULL,
    @p_assignmentGroup VARCHAR(100) = NULL,
    @p_category VARCHAR(100) = NULL,
    @p_priority VARCHAR(50) = NULL,
    @p_assignedToName VARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    SELECT 
        COUNT(*) AS totalIncidents,

        SUM(CASE WHEN State = 'New' THEN 1 ELSE 0 END) AS newIncidents,
        SUM(CASE WHEN State = 'Open' THEN 1 ELSE 0 END) AS openIncidents,
        SUM(CASE WHEN State = 'In Progress' THEN 1 ELSE 0 END) AS inProgressIncidents,
        SUM(CASE WHEN State = 'On Hold' THEN 1 ELSE 0 END) AS onHoldIncidents,
        SUM(CASE WHEN State = 'Resolved' THEN 1 ELSE 0 END) AS resolvedIncidents,
        SUM(CASE WHEN State = 'Closed' THEN 1 ELSE 0 END) AS closedIncidents
    FROM incidents
    WHERE 
        (@p_fromDate IS NULL OR @p_toDate IS NULL OR Opened BETWEEN @p_fromDate AND @p_toDate)
        AND (@p_assignmentGroup IS NULL OR Assignment_group = @p_assignmentGroup)
        AND (@p_category IS NULL OR Category = @p_category)
        AND (@p_priority IS NULL OR Priority = @p_priority)
        AND (@p_assignedToName IS NULL OR Assigned_To = @p_assignedToName);
END
GO