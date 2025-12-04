USE [TorridAMS]
GO

/****** Object:  UserDefinedFunction [dbo].[fn_SLAMinutes_iTVF]    Script Date: 26-11-2025 12:20:01 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE   FUNCTION [dbo].[fn_SLAMinutes_iTVF]
(
    @FromDateTime DATETIME2,
    @ToDateTime   DATETIME2,
    @IncludeWeekends BIT  -- 1 => include weekends (calendar minutes). 0 => exclude weekends (count Mon-Fri full 24h days)
)
RETURNS TABLE
AS
RETURN
(
    SELECT
        CASE
            WHEN @FromDateTime IS NULL OR @ToDateTime IS NULL OR @ToDateTime <= @FromDateTime THEN 0

            WHEN @IncludeWeekends = 1 THEN DATEDIFF(MINUTE, @FromDateTime, @ToDateTime)

            ELSE
                /* exclude weekends: count minutes only for Mon-Fri across the interval (full 24h per included day) */
                CASE
                    -- same calendar date: only count if that date is weekday
                    WHEN CONVERT(date,@FromDateTime) = CONVERT(date,@ToDateTime)
                    THEN
                        CASE WHEN ((DATEDIFF(DAY,'1900-01-01', CONVERT(date,@FromDateTime)) % 7) + 1) BETWEEN 1 AND 5
                             THEN DATEDIFF(MINUTE, @FromDateTime, @ToDateTime)
                             ELSE 0 END

                    ELSE
                        (
                            -- first day partial (if weekday): minutes from FromDateTime -> midnight(next day)
                            CASE
                                WHEN ((DATEDIFF(DAY,'1900-01-01', CONVERT(date,@FromDateTime)) % 7) + 1) BETWEEN 1 AND 5
                                THEN DATEDIFF(MINUTE, @FromDateTime, DATEADD(DAY, 1, CAST(CONVERT(date,@FromDateTime) AS DATETIME2)))
                                ELSE 0
                            END

                            +

                            -- last day partial (if weekday): minutes from start of that day -> ToDateTime
                            CASE
                                WHEN ((DATEDIFF(DAY,'1900-01-01', CONVERT(date,@ToDateTime)) % 7) + 1) BETWEEN 1 AND 5
                                THEN DATEDIFF(MINUTE, CAST(CONVERT(date,@ToDateTime) AS DATETIME2), @ToDateTime)
                                ELSE 0
                            END

                            +

                            -- middle full days (FromDate+1 .. ToDate-1): count weekdays * 1440
                            (
                                CASE
                                    WHEN DATEADD(DAY,1, CONVERT(date,@FromDateTime)) > DATEADD(DAY,-1, CONVERT(date,@ToDateTime)) THEN 0
                                    ELSE
                                        (
                                            -- total days in middle part
                                            (
                                                (DATEDIFF(DAY, DATEADD(DAY,1, CONVERT(date,@FromDateTime)), DATEADD(DAY,-1, CONVERT(date,@ToDateTime))) + 1) / 7
                                            ) * 5

                                            +

                                            -- remainder days (0..6): count weekdays among them
                                            (
                                                SELECT COUNT(1)
                                                FROM (VALUES (0),(1),(2),(3),(4),(5),(6)) AS nums(n)
                                                WHERE nums.n < ((DATEDIFF(DAY, DATEADD(DAY,1, CONVERT(date,@FromDateTime)), DATEADD(DAY,-1, CONVERT(date,@ToDateTime))) + 1) % 7)
                                                  AND (
                                                    (
                                                      ( (DATEDIFF(DAY,'1900-01-01', DATEADD(DAY,1, CONVERT(date,@FromDateTime))) % 7) + 1 )
                                                      + nums.n - 1
                                                    ) % 7
                                                    ) + 1 BETWEEN 1 AND 5
                                            )
                                        )
                                END
                            ) * 1440
                        )
                END
        END AS BusinessMinutes
);

GO


