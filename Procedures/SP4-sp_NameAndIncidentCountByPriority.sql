USE Torrid_AMS;;
GO
 
IF OBJECT_ID('sp_NameAndIncidentCountByPriority', 'P') IS NOT NULL
    DROP PROCEDURE sp_NameAndIncidentCountByPriority;
GO
 
CREATE PROCEDURE sp_NameAndIncidentCountByPriority
    @p_fromDate DATETIME = NULL,
    @p_toDate DATETIME = NULL,
    @p_category VARCHAR(100) = NULL,
    @p_assignmentGroup VARCHAR(100) = NULL,
    @p_priority VARCHAR(50) = NULL,
    @p_assignedToName VARCHAR(100) = NULL,
    @p_state VARCHAR(50) = NULL,
	@p_search VARCHAR(100) = NULL,
    @p_pageNumber INT = 1,       -- Default page number
    @p_pageSize INT = 4          -- Matches design (4 records per page)
AS
BEGIN
    SET NOCOUNT ON;
 
    DECLARE @Offset INT = (@p_pageNumber - 1) * @p_pageSize;
 
    --  Get total count for pagination footer
    DECLARE @TotalCount INT;
    SELECT
        @TotalCount = COUNT(DISTINCT Assigned_to)
    FROM incidents
    WHERE
        (@p_fromDate IS NULL OR @p_toDate IS NULL OR Opened BETWEEN @p_fromDate AND @p_toDate)
        AND (@p_category IS NULL OR UPPER(Category) = UPPER(@p_category))
        AND (@p_assignmentGroup IS NULL OR UPPER(Assignment_group) = UPPER(@p_assignmentGroup))
        AND (@p_priority IS NULL OR UPPER(Priority) = UPPER(@p_priority))
        AND (@p_assignedToName IS NULL OR UPPER(Assigned_to) = UPPER(@p_assignedToName))
        AND (@p_state IS NULL OR UPPER(State) = UPPER(@p_state))
		AND (@p_search IS NULL OR UPPER(Assigned_to) LIKE '%' + UPPER(@p_search) + '%');
 
    -- Get paginated result set
    ;WITH CTE_Incidents AS (
        SELECT
            Assigned_to AS AssignedToName,
            Priority,
            COUNT(*) AS IncidentCount,
            AVG(CASE WHEN State = 'Closed' AND Resolved IS NOT NULL
                     THEN DATEDIFF(HOUR, Opened, Resolved) ELSE NULL END) AS AvgResolutionTime_Hours
        FROM incidents
        WHERE
            (@p_fromDate IS NULL OR @p_toDate IS NULL OR Opened BETWEEN @p_fromDate AND @p_toDate)
            AND (@p_category IS NULL OR UPPER(Category) = UPPER(@p_category))
            AND (@p_assignmentGroup IS NULL OR UPPER(Assignment_group) = UPPER(@p_assignmentGroup))
            AND (@p_priority IS NULL OR UPPER(Priority) = UPPER(@p_priority))
            AND (@p_assignedToName IS NULL OR UPPER(Assigned_to) = UPPER(@p_assignedToName))
            AND (@p_state IS NULL OR UPPER(State) = UPPER(@p_state))
			AND (@p_search IS NULL OR UPPER(Assigned_to) LIKE '%' + UPPER(@p_search) + '%')
        GROUP BY Assigned_to, Priority
    )
    SELECT
        *,
        @TotalCount AS TotalCount    -- Return total count along with each row
    FROM CTE_Incidents
    ORDER BY AssignedToName, Priority
    OFFSET @Offset ROWS FETCH NEXT @p_pageSize ROWS ONLY;
END;
GO