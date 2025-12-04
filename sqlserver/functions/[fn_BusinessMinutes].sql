USE [TorridAMS]
GO

/****** Object:  UserDefinedFunction [dbo].[fn_BusinessMinutes]    Script Date: 26-11-2025 11:48:19 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

 
CREATE     FUNCTION [dbo].[fn_BusinessMinutes]  
(
    @Start DATETIME,  
    @End DATETIME
)  
RETURNS INT  
AS  
BEGIN
    DECLARE @Minutes INT = 0;
 
    -- Return NULL if either date is NULL
    IF @Start IS NULL OR @End IS NULL
        RETURN NULL;
 
    -- Calculate difference in minutes (for now, simple difference, add business hours logic if needed)
    SET @Minutes = DATEDIFF(MINUTE, @Start, @End);
 
    RETURN @Minutes;
END
GO


