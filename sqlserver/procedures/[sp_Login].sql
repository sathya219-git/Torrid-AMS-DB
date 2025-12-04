USE [TorridAMS]
GO

/****** Object:  StoredProcedure [dbo].[sp_Login]    Script Date: 26-11-2025 11:45:18 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [dbo].[sp_Login]
    @Email NVARCHAR(255),
    @Password NVARCHAR(100)  -- Change from VARCHAR to NVARCHAR
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (
        SELECT 1
        FROM Users
        WHERE Email = @Email
          AND PasswordHash = HASHBYTES('SHA2_256', @Password)
    )
    BEGIN
        SELECT 
            'Login successful' AS Message,
            UserID,
            Username,
            Email,
            Role
        FROM Users
        WHERE Email = @Email;
    END
    ELSE
    BEGIN
        SELECT 'Invalid username or password' AS Message;
    END
END;
GO


