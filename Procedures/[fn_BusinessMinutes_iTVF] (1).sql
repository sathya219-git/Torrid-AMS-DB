USE [TorridAMS]
GO

/****** Object:  UserDefinedFunction [dbo].[fn_BusinessMinutes_iTVF]    Script Date: 26-11-2025 12:19:26 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE   FUNCTION [dbo].[fn_BusinessMinutes_iTVF]
(
    @FromDateTime DATETIME2,
    @ToDateTime   DATETIME2
)
RETURNS TABLE
AS
RETURN
(
    SELECT
        CASE
            -- invalid input or non-positive interval
            WHEN base.FromDateTime IS NULL OR base.ToDateTime IS NULL OR base.ToDateTime <= base.FromDateTime THEN 0

            -- same calendar date
            WHEN base.FromDate = base.ToDate THEN
                CASE
                    WHEN ((DATEDIFF(DAY,'1900-01-01', base.FromDate) % 7) + 1) NOT BETWEEN 1 AND 5 THEN 0
                    ELSE
                        CASE
                            WHEN DATEDIFF(MINUTE,
                                CASE WHEN base.FromDateTime > DATEADD(HOUR,9,CAST(base.FromDate AS DATETIME2))
                                     THEN base.FromDateTime
                                     ELSE DATEADD(HOUR,9,CAST(base.FromDate AS DATETIME2)) END,
                                CASE WHEN base.ToDateTime < DATEADD(HOUR,18,CAST(base.FromDate AS DATETIME2))
                                     THEN base.ToDateTime
                                     ELSE DATEADD(HOUR,18,CAST(base.FromDate AS DATETIME2)) END
                            ) > 0
                            THEN DATEDIFF(MINUTE,
                                CASE WHEN base.FromDateTime > DATEADD(HOUR,9,CAST(base.FromDate AS DATETIME2))
                                     THEN base.FromDateTime
                                     ELSE DATEADD(HOUR,9,CAST(base.FromDate AS DATETIME2)) END,
                                CASE WHEN base.ToDateTime < DATEADD(HOUR,18,CAST(base.FromDate AS DATETIME2))
                                     THEN base.ToDateTime
                                     ELSE DATEADD(HOUR,18,CAST(base.FromDate AS DATETIME2)) END
                            )
                            ELSE 0
                        END
                END

            -- different calendar dates
            ELSE
                (
                    -- first partial day (if weekday)
                    CASE
                        WHEN ((DATEDIFF(DAY,'1900-01-01', base.FromDate) % 7) + 1) NOT BETWEEN 1 AND 5 THEN 0
                        ELSE
                            CASE WHEN DATEDIFF(MINUTE,
                                    CASE WHEN base.FromDateTime > DATEADD(HOUR,9,CAST(base.FromDate AS DATETIME2))
                                         THEN base.FromDateTime
                                         ELSE DATEADD(HOUR,9,CAST(base.FromDate AS DATETIME2)) END,
                                    DATEADD(HOUR,18,CAST(base.FromDate AS DATETIME2))
                                 ) > 0
                                 THEN DATEDIFF(MINUTE,
                                    CASE WHEN base.FromDateTime > DATEADD(HOUR,9,CAST(base.FromDate AS DATETIME2))
                                         THEN base.FromDateTime
                                         ELSE DATEADD(HOUR,9,CAST(base.FromDate AS DATETIME2)) END,
                                    DATEADD(HOUR,18,CAST(base.FromDate AS DATETIME2))
                                 )
                                 ELSE 0
                            END
                    END

                    +

                    -- last partial day (if weekday)
                    CASE
                        WHEN ((DATEDIFF(DAY,'1900-01-01', base.ToDate) % 7) + 1) NOT BETWEEN 1 AND 5 THEN 0
                        ELSE
                            CASE WHEN DATEDIFF(MINUTE,
                                    DATEADD(HOUR,9,CAST(base.ToDate AS DATETIME2)),
                                    CASE WHEN base.ToDateTime < DATEADD(HOUR,18,CAST(base.ToDate AS DATETIME2))
                                         THEN base.ToDateTime
                                         ELSE DATEADD(HOUR,18,CAST(base.ToDate AS DATETIME2)) END
                                 ) > 0
                                 THEN DATEDIFF(MINUTE,
                                    DATEADD(HOUR,9,CAST(base.ToDate AS DATETIME2)),
                                    CASE WHEN base.ToDateTime < DATEADD(HOUR,18,CAST(base.ToDate AS DATETIME2))
                                         THEN base.ToDateTime
                                         ELSE DATEADD(HOUR,18,CAST(base.ToDate AS DATETIME2)) END
                                 )
                                 ELSE 0
                            END
                    END

                    +

                    -- middle full business days (FromDate+1 .. ToDate-1) * 540
                    (
                        CASE
                            WHEN mid.TotalMiddleDays <= 0 THEN 0
                            ELSE
                                (
                                    (mid.TotalMiddleDays / 7) * 5
                                    +
                                    (
                                        -- count weekdays among the remainder days (correlated subquery)
                                        SELECT COUNT(1)
                                        FROM (VALUES (0),(1),(2),(3),(4),(5),(6)) AS v(n)
                                        WHERE v.n < (mid.TotalMiddleDays % 7)
                                          AND (((mid.StartDOW + v.n - 1) % 7) + 1) BETWEEN 1 AND 5
                                    )
                                )
                        END
                    ) * 540
                )
        END AS BusinessMinutes

    FROM
    (
        SELECT
            @FromDateTime AS FromDateTime,
            @ToDateTime   AS ToDateTime,
            CONVERT(date, @FromDateTime) AS FromDate,
            CONVERT(date, @ToDateTime)   AS ToDate
    ) AS base

    CROSS APPLY
    (
        SELECT
            CASE 
                WHEN DATEADD(DAY,1,base.FromDate) > DATEADD(DAY,-1,base.ToDate) THEN 0
                ELSE DATEDIFF(DAY, DATEADD(DAY,1,base.FromDate), DATEADD(DAY,-1,base.ToDate)) + 1
            END AS TotalMiddleDays,
            ((DATEDIFF(DAY,'1900-01-01', DATEADD(DAY,1,base.FromDate)) % 7) + 1) AS StartDOW
    ) AS mid
);

GO


