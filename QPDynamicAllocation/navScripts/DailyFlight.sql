USE [REZ0KWB01]
GO

/****** Object:  StoredProcedure [dbo].[SP_DailyFlights]    Script Date: 24/04/2025 15:02:05 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[SP_DailyFlights]
    @StartDate DATE,
    @EndDate DATE
AS
BEGIN
    SET NOCOUNT ON;

    WITH FlightData AS (
        SELECT
            IL.DepartureStation AS Origin,
            IL.ArrivalStation AS Destination,
            IL.FlightNumber,
            IL.DepartureDate,
            LEAD(IL.FlightNumber) OVER (
                PARTITION BY IL.DepartureStation, IL.ArrivalStation, IL.DepartureDate
                ORDER BY IL.STD
            ) AS NextFlightNumber
        FROM [rez].InventoryLeg IL
        WHERE
            IL.Lid > 0
            AND IL.Status NOT IN (2)
            AND IL.DepartureDate BETWEEN @StartDate AND @EndDate
    )
    SELECT
        Origin,
        Destination,
        FlightNumber,
        DepartureDate,
        CASE
            WHEN NextFlightNumber IS NOT NULL THEN 1
            ELSE 0
        END AS NextFlight
    FROM FlightData
    ORDER BY DepartureDate, Origin, Destination, FlightNumber;
END;
GO


