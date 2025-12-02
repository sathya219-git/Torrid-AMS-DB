USE Torrid_AMS;
GO
 
IF OBJECT_ID('sp_GetIncidentDetailsByPriority', 'P') IS NOT NULL
    DROP PROCEDURE sp_GetIncidentDetailsByPriority;
GO
 
CREATE PROCEDURE sp_GetIncidentDetailsByPriority
    @p_fromDate DATETIME = NULL,
    @p_toDate DATETIME = NULL,
    @p_category VARCHAR(100) = NULL,
    @p_assignmentGroup VARCHAR(100) = NULL,
    @p_priority VARCHAR(50) = NULL,
    @p_assignedToName VARCHAR(100) = NULL,
    @p_searchIncidentNumber VARCHAR(100) = NULL,
    @p_state VARCHAR(50) = NULL,
    @p_sortBy VARCHAR(50) = 'State',   -- Default sort column
    @p_sortOrder VARCHAR(4) = 'ASC',   -- Default sort order
    @p_pageNumber INT = 1,
    @p_pageSize INT = 8
AS
BEGIN
    SET NOCOUNT ON;
 
    DECLARE @Offset INT = (@p_pageNumber - 1) * @p_pageSize;
    DECLARE @TotalCount INT;
 
    -- Count total records for pagination display
    SELECT @TotalCount = COUNT(*)
    FROM incidents
    WHERE
        (@p_fromDate IS NULL OR @p_toDate IS NULL OR Opened BETWEEN @p_fromDate AND @p_toDate)
        AND (@p_category IS NULL OR UPPER(Category) = UPPER(@p_category))
        AND (@p_assignmentGroup IS NULL OR UPPER(Assignment_group) = UPPER(@p_assignmentGroup))
        AND (@p_priority IS NULL OR UPPER(Priority) = UPPER(@p_priority))
        AND (@p_assignedToName IS NULL OR UPPER(Assigned_to) = UPPER(@p_assignedToName))
        AND (@p_searchIncidentNumber IS NULL OR UPPER(Number) LIKE '%' + UPPER(@p_searchIncidentNumber) + '%')
        AND (@p_state IS NULL OR UPPER(State) = UPPER(@p_state));
 
    -- Dynamic ORDER BY handling
    DECLARE @SQL NVARCHAR(MAX);
 
    SET @SQL = N'
        SELECT
            Number AS IncidentNumber,
            Opened AS OpenedDate,
            Short_description AS Description,
            Caller AS CallerName,
            Priority AS PriorityLevel,
            State AS CurrentState,
            Category AS CategoryName,
            Assignment_group AS AssignmentGroup,
            Assigned_to AS AssignedTo,
            Resolved AS ResolutionDate,
            Updated AS LastUpdated,
            Updated_by AS UpdatedBy,
            Child_Incidents AS ChildIncidents,
            SLA_due AS SLADueDate,
            Severity AS SeverityLevel,
            Subcategory AS SubcategoryName,
            ' + CAST(@TotalCount AS NVARCHAR(20)) + ' AS TotalCount
        FROM incidents
        WHERE
            (' + CASE WHEN @p_fromDate IS NULL OR @p_toDate IS NULL
                      THEN '1=1'
                      ELSE 'Opened BETWEEN @p_fromDate AND @p_toDate' END + ')
            AND (@p_category IS NULL OR UPPER(Category) = UPPER(@p_category))
            AND (@p_assignmentGroup IS NULL OR UPPER(Assignment_group) = UPPER(@p_assignmentGroup))
            AND (@p_priority IS NULL OR UPPER(Priority) = UPPER(@p_priority))
            AND (@p_assignedToName IS NULL OR UPPER(Assigned_to) = UPPER(@p_assignedToName))
            AND (@p_searchIncidentNumber IS NULL OR UPPER(Number) LIKE ''%'' + UPPER(@p_searchIncidentNumber) + ''%'')
            AND (@p_state IS NULL OR UPPER(State) = UPPER(@p_state))
        ORDER BY ' +
            CASE
                WHEN LOWER(@p_sortBy) = 'category' THEN 'Category'
                WHEN LOWER(@p_sortBy) = 'state' THEN 'State'
                WHEN LOWER(@p_sortBy) = 'resolutiondatetime' THEN 'Resolved'
                ELSE 'Opened'  -- Default sort
            END + ' ' +
            CASE
                WHEN UPPER(@p_sortOrder) = 'DESC' THEN 'DESC'
                ELSE 'ASC'
            END + '
        OFFSET ' + CAST(@Offset AS NVARCHAR(10)) + ' ROWS
        FETCH NEXT ' + CAST(@p_pageSize AS NVARCHAR(10)) + ' ROWS ONLY;
    ';
 
    -- Execute dynamic SQL safely with parameters
    EXEC sp_executesql
        @SQL,
        N'@p_fromDate DATETIME, @p_toDate DATETIME, @p_category VARCHAR(100),
          @p_assignmentGroup VARCHAR(100), @p_priority VARCHAR(50),
          @p_assignedToName VARCHAR(100), @p_searchIncidentNumber VARCHAR(100), @p_state VARCHAR(50)',
        @p_fromDate, @p_toDate, @p_category, @p_assignmentGroup, @p_priority,
        @p_assignedToName, @p_searchIncidentNumber, @p_state;
END;
GO