USE [TorridAMS]
GO

/****** Object:  StoredProcedure [dbo].[sp_GetAssignmentGroups]    Script Date: 26-11-2025 11:42:30 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [dbo].[sp_GetAssignmentGroups]
    @p_fromDate DATETIME = NULL,
    @p_toDate DATETIME = NULL,
    @p_category NVARCHAR(MAX) = NULL,
    @p_priority NVARCHAR(MAX) = NULL,
    @p_assignedToName NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    SELECT DISTINCT 
        Assignment_group AS AssignmentGroupName
    FROM incidents
    WHERE
        ((@p_fromDate IS NULL AND @p_toDate IS NULL) OR (Opened BETWEEN @p_fromDate AND @p_toDate))
        AND (@p_category IS NULL OR UPPER(Category) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_category, ',')))
        AND (@p_priority IS NULL OR UPPER(Priority) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_priority, ',')))
        AND (@p_assignedToName IS NULL OR UPPER(Assigned_To) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_assignedToName, ',')))
    ORDER BY Assignment_group;
END;
GO


