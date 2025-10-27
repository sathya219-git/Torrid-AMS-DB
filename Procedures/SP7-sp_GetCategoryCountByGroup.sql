USE Torrid_AMS;
GO

IF OBJECT_ID('sp_GetCategoryCountByGroup', 'P') IS NOT NULL
    DROP PROCEDURE sp_GetCategoryCountByGroup;
GO

CREATE PROCEDURE sp_GetCategoryCountByGroup
    @p_fromDate DATETIME = NULL,
    @p_toDate DATETIME = NULL,
    @p_assignmentGroup VARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    SELECT 
        Category AS CategoryName,
        COUNT(*) AS IncidentCount
    FROM incidents
    WHERE
        (@p_fromDate IS NULL OR @p_toDate IS NULL OR Opened BETWEEN @p_fromDate AND @p_toDate)
        AND (@p_assignmentGroup IS NULL OR UPPER(Assignment_group) = UPPER(@p_assignmentGroup))
    GROUP BY Category
    ORDER BY IncidentCount DESC;
END
GO
