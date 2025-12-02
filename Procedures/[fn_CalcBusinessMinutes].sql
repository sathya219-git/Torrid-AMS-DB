USE [TorridAMS]
GO

/****** Object:  UserDefinedFunction [dbo].[fn_CalcBusinessMinutes]    Script Date: 26-11-2025 11:47:48 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE   FUNCTION [dbo].[fn_CalcBusinessMinutes]
(
    @Start DATETIME,
    @End DATETIME,
    @Priority NVARCHAR(50)
)
RETURNS TABLE
AS
RETURN
(
    SELECT
        -- Actual minutes only if resolved
        CASE WHEN @End IS NULL THEN NULL ELSE dbo.fn_BusinessMinutes(@Start, @End) END AS ActualResolvedMinutes,

        -- SLA in minutes based on priority
        CASE
            WHEN @Priority LIKE '1%' THEN 120   -- 2 hours
            WHEN @Priority LIKE '2%' THEN 240   -- 4 hours
            WHEN @Priority LIKE '3%' THEN 540   -- 9 hours
            WHEN @Priority LIKE '4%' THEN 5*540 -- 5 days
            ELSE NULL
        END AS SLAMinutes,

        -- Breach = Actual - SLA only if resolved
        CASE
            WHEN @End IS NULL THEN NULL
            ELSE dbo.fn_BusinessMinutes(@Start, @End) -
                CASE
                    WHEN @Priority LIKE '1%' THEN 120
                    WHEN @Priority LIKE '2%' THEN 240
                    WHEN @Priority LIKE '3%' THEN 540
                    WHEN @Priority LIKE '4%' THEN 5*540
                    ELSE NULL
                END
        END AS BreachMinutes
);
GO


