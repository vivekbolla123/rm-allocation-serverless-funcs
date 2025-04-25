USE [REZ0KWB01]
GO

/****** Object:  StoredProcedure [dbo].[SP_LastBookingsCount]    Script Date: 24/04/2025 15:01:11 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[SP_LastBookingsCount]
    @DepartureStartDate DATE,
    @DepartureEndDate DATE,
    @ComparisonDate DATE,
    @ComparisonDateTime DATETIME
AS
BEGIN
    WITH CommonCTE AS (
    SELECT BP.PassengerID, 
           IL.DepartureDate,
           IL.FlightNumber,
           IL.DepartureStation,
           IL.ArrivalStation,
           CAST(SWITCHOFFSET(B.BookingUTC, DATEPART(TZOFFSET, B.BookingUTC AT TIME ZONE 'India Standard Time')) AS DATETIME) AS BookingDateTime
    FROM rez.Booking B 
    INNER JOIN rez.BookingPassenger BP ON BP.BookingID = B.BookingID 
    INNER JOIN rez.PassengerJourneySegment PJS ON PJS.PassengerID = BP.PassengerID AND (PJS.FareBasis != 'SO7NBSOL' OR PJS.FareBasis IS NULL)
    INNER JOIN rez.PassengerJourneyLeg PJL ON PJL.PassengerID = PJS.PassengerID AND PJL.SegmentID = PJS.SegmentID 
    INNER JOIN rez.InventoryLeg IL ON IL.InventoryLegId = PJL.InventoryLegID 
    WHERE  IL.DepartureDate BETWEEN @DepartureStartDate AND @DepartureEndDate
)
SELECT 
    IL.FlightNumber,
    IL.DepartureStation,
    IL.ArrivalStation,
    IL.DepartureDate,
    (SELECT COUNT(DISTINCT PassengerID) 
     FROM CommonCTE 
     WHERE FlightNumber = IL.FlightNumber 
       AND DepartureStation = IL.DepartureStation 
       AND ArrivalStation = IL.ArrivalStation 
       AND DepartureDate = IL.DepartureDate
       AND BookingDateTime >= DATEADD(HOUR, -1, @ComparisonDateTime)) AS BookingsInHour,
    (SELECT COUNT(DISTINCT PassengerID) 
     FROM CommonCTE 
     WHERE FlightNumber = IL.FlightNumber 
       AND DepartureStation = IL.DepartureStation 
       AND ArrivalStation = IL.ArrivalStation 
       AND DepartureDate = IL.DepartureDate
       AND BookingDateTime <= @ComparisonDate) AS BookingsCount,
    (SELECT COUNT(DISTINCT PassengerID) 
     FROM CommonCTE 
     WHERE FlightNumber = IL.FlightNumber 
       AND DepartureStation = IL.DepartureStation 
       AND ArrivalStation = IL.ArrivalStation 
       AND DepartureDate = IL.DepartureDate
       AND BookingDateTime >= CAST(GETDATE() AS DATE) 
       AND NOT EXISTS (
           SELECT 1 
           FROM rez.PassengerJourneySegment PJS 
           WHERE PJS.PassengerID = CommonCTE.PassengerID 
           AND PJS.ClassOfService = 'S'
       )) AS BookingToday
FROM (
    SELECT DISTINCT 
        FlightNumber, 
        DepartureStation, 
        ArrivalStation, 
        DepartureDate 
    FROM CommonCTE
) IL
ORDER BY IL.DepartureDate, IL.FlightNumber, IL.DepartureStation, IL.ArrivalStation;
     
END;
GO


