USE [TorridAMS]
GO

/****** Object:  StoredProcedure [dbo].[sp_StatusCountByPriority]    Script Date: 26-11-2025 11:46:25 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [dbo].[sp_StatusCountByPriority]
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
        State AS Status,
        COUNT(*) AS IncidentCount
    FROM incidents
    WHERE 
        ((@p_fromDate IS NULL AND @p_toDate IS NULL) OR (Opened BETWEEN @p_fromDate AND @p_toDate))
        AND (@p_assignmentGroup IS NULL OR UPPER(Assignment_group) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_assignmentGroup, ',')))
        AND (@p_category IS NULL OR UPPER(Category) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_category, ',')))
        AND (@p_priority IS NULL OR UPPER(Priority) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_priority, ',')))
        AND (@p_assignedToName IS NULL OR UPPER(Assigned_To) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_assignedToName, ',')))
        AND (@p_state IS NULL OR UPPER(State) IN (SELECT TRIM(UPPER(value)) FROM STRING_SPLIT(@p_state, ',')))
    GROUP BY State
    ORDER BY State;
END;
GO


