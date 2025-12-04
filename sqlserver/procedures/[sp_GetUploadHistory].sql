USE [TorridAMS]
GO

/****** Object:  StoredProcedure [dbo].[sp_GetUploadHistory]    Script Date: 26-11-2025 11:44:03 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [dbo].[sp_GetUploadHistory]
    @SearchText   NVARCHAR(4000) = NULL,        -- one search box for all fields
    @SortBy       NVARCHAR(20)   = N'UploadedDate',  -- ID | FileName | FileSize | UploadedDate
    @SortDir      NVARCHAR(4)    = N'DESC',          -- ASC | DESC
    @PageNumber   INT            = 1,                -- 1-based
    @PageSize     INT            = 4                 -- default page size = 4
AS
BEGIN
    SET NOCOUNT ON;

    -- Normalize paging
    IF @PageNumber IS NULL OR @PageNumber < 1 SET @PageNumber = 1;
    IF @PageSize   IS NULL OR @PageSize   < 1 SET @PageSize   = 4;

    -- Normalize sort inputs
    SET @SortBy  = UPPER(ISNULL(@SortBy, N'UPLOADEDDATE'));
    SET @SortDir = CASE WHEN UPPER(@SortDir) = N'ASC' THEN N'ASC' ELSE N'DESC' END;

    -- Attempt to interpret the search text as number/date for exact matches
    DECLARE @SearchID    INT       = TRY_CONVERT(INT,       @SearchText);
    DECLARE @SearchSize  BIGINT    = TRY_CONVERT(BIGINT,    @SearchText);
    DECLARE @SearchDate  DATETIME  = TRY_CONVERT(DATETIME,  @SearchText);

    ;WITH F AS
    (
        SELECT
            uh.ID,
            uh.FileName,
            uh.FileSize,
            uh.UploadedDate,
            COUNT(*) OVER() AS TotalCount
        FROM dbo.UploadHistory AS uh
        WHERE
            @SearchText IS NULL
            OR
            (
                 uh.FileName LIKE N'%' + @SearchText + N'%'
              OR ( @SearchID   IS NOT NULL  AND uh.ID = @SearchID )
              OR ( @SearchSize IS NOT NULL  AND uh.FileSize = @SearchSize )
              -- If user types a date (e.g., 2025-11-11), match by date part
              OR ( @SearchDate IS NOT NULL AND CONVERT(date, uh.UploadedDate) = CONVERT(date, @SearchDate) )
            )
    )
    SELECT
        ID,
        FileName,
        FileSize,
        UploadedDate,
        TotalCount
    FROM F
    ORDER BY
        CASE WHEN @SortBy = N'ID'            AND @SortDir = N'ASC'  THEN ID           END ASC,
        CASE WHEN @SortBy = N'ID'            AND @SortDir = N'DESC' THEN ID           END DESC,
        CASE WHEN @SortBy = N'FILENAME'      AND @SortDir = N'ASC'  THEN FileName     END ASC,
        CASE WHEN @SortBy = N'FILENAME'      AND @SortDir = N'DESC' THEN FileName     END DESC,
        CASE WHEN @SortBy = N'FILESIZE'      AND @SortDir = N'ASC'  THEN FileSize     END ASC,
        CASE WHEN @SortBy = N'FILESIZE'      AND @SortDir = N'DESC' THEN FileSize     END DESC,
        CASE WHEN @SortBy = N'UPLOADEDDATE'  AND @SortDir = N'ASC'  THEN UploadedDate END ASC,
        CASE WHEN @SortBy = N'UPLOADEDDATE'  AND @SortDir = N'DESC' THEN UploadedDate END DESC
    OFFSET (@PageNumber - 1) * @PageSize ROWS
    FETCH NEXT @PageSize ROWS ONLY;
END;
GO


