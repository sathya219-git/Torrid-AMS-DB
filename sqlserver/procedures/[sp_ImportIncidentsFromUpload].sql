USE [TorridAMS]
GO

/****** Object:  StoredProcedure [dbo].[sp_ImportIncidentsFromUpload]    Script Date: 26-11-2025 11:44:30 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [dbo].[sp_ImportIncidentsFromUpload]
    @UploadHistoryId INT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @FilePath NVARCHAR(4000);

    -- 1) Get file path
    SELECT @FilePath = FilePath FROM dbo.UploadHistory WHERE ID = @UploadHistoryId;

    IF @FilePath IS NULL
    BEGIN
        RAISERROR('UploadHistory row not found for ID = %d', 16, 1, @UploadHistoryId);
        RETURN;
    END

    -------------------------------------------------------------------------
    -- 2) Ensure staging exists (assumes the staging table is already created as in prior scripts)
    -------------------------------------------------------------------------
    IF OBJECT_ID('dbo.StagingIncidents', 'U') IS NULL
    BEGIN
        RAISERROR('Staging table dbo.StagingIncidents does not exist. Create it before running this proc.', 16, 1);
        RETURN;
    END

    -- 3) Clear staging
    TRUNCATE TABLE dbo.StagingIncidents;

    -- 4) BULK INSERT (dynamic SQL)
    DECLARE @bulkSql NVARCHAR(MAX) = N'
    BULK INSERT dbo.StagingIncidents
    FROM ''' + REPLACE(@FilePath,'''','''''') + N'''
    WITH
    (
        FORMAT = ''CSV'',
        FIRSTROW = 2,
        CODEPAGE = ''1252'',  -- file encoding detected
        TABLOCK,
        MAXERRORS = 0
    );';

    BEGIN TRY
        EXEC sp_executesql @bulkSql;
    END TRY
    BEGIN CATCH
        DECLARE @err NVARCHAR(4000) = ERROR_MESSAGE();
        RAISERROR('BULK INSERT failed: %s', 16, 1, @err);
        RETURN;
    END CATCH

    -------------------------------------------------------------------------
    -- 5) MERGE (UPSERT) into dbo.Incidents with conditional update:
    --    Update only when incoming Updated_dt is newer than target.Updated
    -------------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#MergeOutput') IS NOT NULL DROP TABLE #MergeOutput;
    CREATE TABLE #MergeOutput (ActionPerformed NVARCHAR(10)); -- 'INSERT' or 'UPDATE'

    BEGIN TRY
        BEGIN TRAN;

        MERGE dbo.Incidents AS tgt
        USING
        (
            SELECT
                LTRIM(RTRIM([Number])) AS Number,
                TRY_CONVERT(datetime, TRY_PARSE(NULLIF([Updated], '') AS datetime USING 'en-US')) AS Updated_dt,
                TRY_CONVERT(datetime, TRY_PARSE(NULLIF([Opened], '') AS datetime USING 'en-US')) AS Opened_dt,
                LEFT([Short_description], 2147483647) AS Short_description,
                LEFT(NULLIF([Caller], ''), 100) AS Caller_trunc,
                LEFT(NULLIF([Priority], ''), 50) AS Priority_trunc,
                LEFT(NULLIF([State], ''), 50) AS State_trunc,
                LEFT(NULLIF([Category], ''), 100) AS Category_trunc,
                LEFT(NULLIF([Assignment_group], ''), 100) AS Assignment_group_trunc,
                LEFT(NULLIF([Assigned_to], ''), 100) AS Assigned_to_trunc,
                LEFT(NULLIF([Updated_by], ''), 100) AS Updated_by_trunc,
                CASE WHEN NULLIF([Child_Incidents], '') IS NULL THEN NULL ELSE TRY_CONVERT(INT, REPLACE([Child_Incidents], ',', '')) END AS Child_Incidents_int,
                CASE WHEN NULLIF([SLA_due], '') IS NULL THEN NULL ELSE TRY_CONVERT(DECIMAL(10,2), REPLACE(REPLACE([SLA_due], ',', ''), '$', '')) END AS SLA_due_dec,
                LEFT(NULLIF([Severity], ''), 50) AS Severity_trunc,
                LEFT(NULLIF([Subcategory], ''), 100) AS Subcategory_trunc,
                [Resolution_notes] AS Resolution_notes,
                TRY_CONVERT(datetime, TRY_PARSE(NULLIF([Resolved], '') AS datetime USING 'en-US')) AS Resolved_dt,
                CASE WHEN NULLIF([SLA_Calculation], '') IS NULL THEN NULL ELSE TRY_CONVERT(DECIMAL(10,2), REPLACE(REPLACE([SLA_Calculation], ',', ''), '$', '')) END AS SLA_Calc_dec,
                CASE WHEN NULLIF([Parent_Incident], '') IS NULL THEN NULL ELSE TRY_CONVERT(DECIMAL(18,2), REPLACE(REPLACE([Parent_Incident], ',', ''), '$', '')) END AS Parent_Incident_dec,
                LEFT(NULLIF([Parent], ''), 100) AS Parent_trunc,
                LEFT(NULLIF([Task_type], ''), 50) AS Task_type_trunc
            FROM dbo.StagingIncidents
            WHERE NULLIF(LTRIM(RTRIM([Number])), '') IS NOT NULL
        ) AS src
        ON tgt.Number = src.Number

        -- Update only if incoming Updated_dt is newer than existing Updated (or target Updated is NULL and source Updated is not NULL)
        WHEN MATCHED AND
            (
                (src.Updated_dt IS NOT NULL AND tgt.Updated IS NULL)
                OR (src.Updated_dt IS NOT NULL AND tgt.Updated IS NOT NULL AND src.Updated_dt > tgt.Updated)
            )
        THEN
            UPDATE SET
                tgt.Opened            = COALESCE(src.Opened_dt, tgt.Opened),
                tgt.Short_description = COALESCE(src.Short_description, tgt.Short_description),
                tgt.Caller            = COALESCE(src.Caller_trunc, tgt.Caller),
                tgt.Priority          = COALESCE(src.Priority_trunc, tgt.Priority),
                tgt.State             = COALESCE(src.State_trunc, tgt.State),
                tgt.Category          = COALESCE(src.Category_trunc, tgt.Category),
                tgt.Assignment_group  = COALESCE(src.Assignment_group_trunc, tgt.Assignment_group),
                tgt.Assigned_to       = COALESCE(src.Assigned_to_trunc, tgt.Assigned_to),
                tgt.Updated           = src.Updated_dt,
                tgt.Updated_by        = COALESCE(src.Updated_by_trunc, tgt.Updated_by),
                tgt.Child_Incidents   = COALESCE(src.Child_Incidents_int, tgt.Child_Incidents),
                tgt.SLA_due           = COALESCE(src.SLA_due_dec, tgt.SLA_due),
                tgt.Severity          = COALESCE(src.Severity_trunc, tgt.Severity),
                tgt.Subcategory       = COALESCE(src.Subcategory_trunc, tgt.Subcategory),
                tgt.Resolution_notes  = COALESCE(src.Resolution_notes, tgt.Resolution_notes),
                tgt.Resolved          = COALESCE(src.Resolved_dt, tgt.Resolved),
                tgt.SLA_Calculation   = COALESCE(src.SLA_Calc_dec, tgt.SLA_Calculation),
                tgt.Parent_Incident   = COALESCE(src.Parent_Incident_dec, tgt.Parent_Incident),
                tgt.Parent            = COALESCE(src.Parent_trunc, tgt.Parent),
                tgt.Task_type         = COALESCE(src.Task_type_trunc, tgt.Task_type)

        WHEN NOT MATCHED BY TARGET THEN
            INSERT
            (
                Number, Opened, Short_description, Caller, Priority, State, Category,
                Assignment_group, Assigned_to, Updated, Updated_by, Child_Incidents,
                SLA_due, Severity, Subcategory, Resolution_notes, Resolved,
                SLA_Calculation, Parent_Incident, Parent, Task_type
            )
            VALUES
            (
                src.Number, src.Opened_dt, src.Short_description, src.Caller_trunc, src.Priority_trunc, src.State_trunc, src.Category_trunc,
                src.Assignment_group_trunc, src.Assigned_to_trunc, src.Updated_dt, src.Updated_by_trunc, src.Child_Incidents_int,
                src.SLA_due_dec, src.Severity_trunc, src.Subcategory_trunc, src.Resolution_notes, src.Resolved_dt,
                src.SLA_Calc_dec, src.Parent_Incident_dec, src.Parent_trunc, src.Task_type_trunc
            )
        OUTPUT $action INTO #MergeOutput(ActionPerformed);

        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        DECLARE @mergeErr NVARCHAR(4000) = ERROR_MESSAGE();
        RAISERROR('MERGE failed: %s', 16, 1, @mergeErr);
        RETURN;
    END CATCH

    -------------------------------------------------------------------------
    -- 6) Compute counts and matched-but-not-updated
    -------------------------------------------------------------------------
    DECLARE @StagingCount INT = (SELECT COUNT(*) FROM dbo.StagingIncidents);
    DECLARE @InsertedCount INT = (SELECT COUNT(*) FROM #MergeOutput WHERE ActionPerformed = 'INSERT');
    DECLARE @UpdatedCount INT  = (SELECT COUNT(*) FROM #MergeOutput WHERE ActionPerformed = 'UPDATE');
    DECLARE @SkippedCount INT  = (SELECT COUNT(*) FROM dbo.StagingIncidents WHERE NULLIF(LTRIM(RTRIM([Number])), '') IS NULL);

    -- Matched but not updated = number of staging rows whose Number exists in target but did not satisfy update condition
    DECLARE @MatchedButNotUpdated INT =
    (
        SELECT COUNT(*)
        FROM dbo.StagingIncidents s
        WHERE NULLIF(LTRIM(RTRIM(s.[Number])), '') IS NOT NULL
          AND EXISTS (SELECT 1 FROM dbo.Incidents t WHERE t.Number = LTRIM(RTRIM(s.[Number])))
          AND NOT EXISTS
          (
              -- exists update-qualifying condition for this row
              SELECT 1
              FROM dbo.Incidents t2
              WHERE t2.Number = LTRIM(RTRIM(s.[Number]))
                AND (
                    (TRY_CONVERT(datetime, TRY_PARSE(NULLIF(s.[Updated], '') AS datetime USING 'en-US')) IS NOT NULL AND t2.Updated IS NULL)
                    OR (TRY_CONVERT(datetime, TRY_PARSE(NULLIF(s.[Updated], '') AS datetime USING 'en-US')) IS NOT NULL AND t2.Updated IS NOT NULL AND TRY_CONVERT(datetime, TRY_PARSE(NULLIF(s.[Updated], '') AS datetime USING 'en-US')) > t2.Updated)
                )
          )
    );

    -- 7) Return summary
    SELECT
        @StagingCount                   AS StagingRowCount,
        @InsertedCount                  AS InsertedCount,
        @UpdatedCount                   AS UpdatedCount,
        @MatchedButNotUpdated           AS MatchedButNotUpdatedCount,
        @SkippedCount                   AS SkippedDueToMissingNumber;

    DROP TABLE #MergeOutput;
END;
GO


