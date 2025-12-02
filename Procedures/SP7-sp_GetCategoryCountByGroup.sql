USE [TorridAMS]
GO

/****** Object:  StoredProcedure [dbo].[sp_GetCategoryCountByGroup]    Script Date: 26-11-2025 11:43:01 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [dbo].[sp_GetCategoryCountByGroup]
    @p_fromDate DATETIME = NULL,
    @p_toDate DATETIME = NULL,
    @p_assignmentGroup NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    SELECT 
        Category AS CategoryName,
        COUNT(*) AS IncidentCount
    FROM dbo.incidents
    WHERE
        ((@p_fromDate IS NULL AND @p_toDate IS NULL) OR (Opened BETWEEN @p_fromDate AND @p_toDate))
        AND (@p_assignmentGroup IS NULL 
            OR UPPER(Assignment_group) IN (
                SELECT TRIM(UPPER(value)) 
                FROM STRING_SPLIT(@p_assignmentGroup, ',')
            )
        )
    GROUP BY Category
    ORDER BY IncidentCount DESC;
END;
GO


