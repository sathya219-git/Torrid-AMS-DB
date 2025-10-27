USE IncidentDB;
GO

ALTER PROCEDURE sp_ExportIncidents
    @p_fromDate DATETIME = NULL,
    @p_toDate DATETIME = NULL,
    @p_category VARCHAR(100) = NULL,
    @p_assignmentGroup VARCHAR(100) = NULL,
    @p_priority VARCHAR(50) = NULL,
    @p_assignedToName VARCHAR(100) = NULL,
    @p_status VARCHAR(50) = NULL,
    @p_state VARCHAR(50) = NULL     -- Added new parameter
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        Number,
        Opened,
        Short_description,
        Caller,
        Priority,
        State,
        Category,
        Assignment_group,
        Assigned_to,
        Updated,
        Updated_by,
        Child_Incidents,
        SLA_due,
        Severity,
        Subcategory,
        Resolution_notes,
        Resolved,
        SLA_Calculation,
        Parent_Incident,
        Parent,
        Task_type
    FROM incidents
    WHERE
        (@p_fromDate IS NULL OR @p_toDate IS NULL OR Opened BETWEEN @p_fromDate AND @p_toDate)
        AND (@p_category IS NULL OR UPPER(Category) = UPPER(@p_category))
        AND (@p_assignmentGroup IS NULL OR UPPER(Assignment_group) = UPPER(@p_assignmentGroup))
        AND (@p_priority IS NULL OR UPPER(Priority) = UPPER(@p_priority))
        AND (@p_assignedToName IS NULL OR UPPER(Assigned_to) LIKE '%' + UPPER(@p_assignedToName) + '%')
        AND (@p_status IS NULL OR UPPER(State) = UPPER(@p_status))
        AND (@p_state IS NULL OR UPPER(State) = UPPER(@p_state))   -- Added state filter
    ORDER BY 
        UPPER(State) ASC,     -- Sort by State
        Opened DESC;          -- Then by Opened date (newest first)
END;
GO
