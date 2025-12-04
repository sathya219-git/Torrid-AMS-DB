USE [TorridAMS]
GO

/****** Object:  StoredProcedure [dbo].[SP_UpdatePasswordByDefault]    Script Date: 26-11-2025 11:46:52 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [dbo].[SP_UpdatePasswordByDefault]
    @DefaultPassword NVARCHAR(255),
    @NewPassword NVARCHAR(255),
    @ConfirmNewPassword NVARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;

    -- Step 2: Check if default password matches
    IF NOT EXISTS (
        SELECT 1 
        FROM Users 
        WHERE DefaultPassword = @DefaultPassword
    )
    BEGIN
        RAISERROR('Default password is incorrect.', 16, 1);
        RETURN;
    END

    -- Step 3: Check if new password and confirm password match
    IF @NewPassword <> @ConfirmNewPassword
    BEGIN
        RAISERROR('New password and confirmed password do not match.', 16, 1);
        RETURN;
    END

    -- Step 4: Update the new password (hashing it for security)
    UPDATE Users
    SET 
        PasswordHash = HASHBYTES('SHA2_256', @NewPassword),
        CreatedDate = GETDATE()                 -- Optional: update timestamp
    WHERE DefaultPassword = @DefaultPassword;

    PRINT 'Password updated successfully.';
END
GO


