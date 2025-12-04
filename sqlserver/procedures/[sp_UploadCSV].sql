USE [TorridAMS]
GO

/****** Object:  StoredProcedure [dbo].[sp_UploadCSV]    Script Date: 26-11-2025 11:47:16 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [dbo].[sp_UploadCSV]
    @FilePath NVARCHAR(1024),
    @FileName NVARCHAR(255),
    @FileSize BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO dbo.UploadHistory (FileName, FileSize, FilePath)
    VALUES (@FileName, @FileSize, @FilePath);

    SELECT SCOPE_IDENTITY() AS UploadID;
END;
GO


