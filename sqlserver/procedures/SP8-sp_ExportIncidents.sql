USE [TorridAMS]
GO

/****** Object:  StoredProcedure [dbo].[sp_ExportIncidents]    Script Date: 26-11-2025 11:41:05 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [dbo].[sp_ExportIncidents]
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
    FROM dbo.incidents
    WHERE
        ((@p_fromDate IS NULL AND @p_toDate IS NULL) OR (Opened BETWEEN @p_fromDate AND @p_toDate))
        AND (@p_assignmentGroup IS NULL OR UPPER(Assignment_group) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_assignmentGroup, ',')))
        AND (@p_category IS NULL OR UPPER(Category) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_category, ',')))
        AND (@p_priority IS NULL OR UPPER(Priority) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_priority, ',')))
        AND (@p_assignedToName IS NULL OR UPPER(Assigned_To) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_assignedToName, ',')))
        AND (@p_state IS NULL OR UPPER(State) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_state, ',')))
    ORDER BY Opened DESC;
END;
GO


