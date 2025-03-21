USE [REZ0KWB01]
GO
/****** Object:  StoredProcedure [dbo].[LastBookingsCount]    Script Date: 20-03-2024 4.52.39 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[SP_LastBookingsCount]
    @DepartureDate DATE,
    @FlightNumber NVARCHAR(255),
    @DepartureStation NVARCHAR(255),
    @ArrivalStation NVARCHAR(255),
    @ComparisonDate DATE,
    @ComparisonDateTime DATETIME
AS
BEGIN
    WITH CommonCTE AS (
    SELECT BP.PassengerID, 
           IL.DepartureDate,
           CAST(SWITCHOFFSET(B.BookingUTC, DATEPART(TZOFFSET, B.BookingUTC AT TIME ZONE 'India Standard Time')) AS DATETIME) AS BookingDateTime
    FROM rez.Booking B 
    INNER JOIN rez.BookingPassenger BP ON BP.BookingID = B.BookingID 
    INNER JOIN rez.PassengerJourneySegment PJS ON PJS.PassengerID = BP.PassengerID AND (PJS.FareBasis != 'SO7NBSOL' OR PJS.FareBasis IS NULL)
    INNER JOIN rez.PassengerJourneyLeg PJL ON PJL.PassengerID = PJS.PassengerID AND PJL.SegmentID = PJS.SegmentID 
    INNER JOIN rez.InventoryLeg IL ON IL.InventoryLegId = PJL.InventoryLegID 
    WHERE  IL.DepartureDate = @DepartureDate
        AND IL.FlightNumber = @FlightNumber
        AND IL.DepartureStation = @DepartureStation
        AND IL.ArrivalStation = @ArrivalStation
)
SELECT 
    (SELECT COUNT(DISTINCT PassengerID) 
     FROM CommonCTE 
     WHERE BookingDateTime >= @ComparisonDateTime) AS BookingsInHour,
    (SELECT COUNT(DISTINCT PassengerID) 
     FROM CommonCTE 
     WHERE BookingDateTime <= @ComparisonDate) AS BookingsCount,
    (SELECT COUNT(DISTINCT PassengerID) 
     FROM CommonCTE 
     WHERE BookingDateTime >= CAST(GETDATE() AS DATE) 
     AND NOT EXISTS (
         SELECT 1 
         FROM rez.PassengerJourneySegment PJS 
         WHERE PJS.PassengerID = CommonCTE.PassengerID 
         AND PJS.ClassOfService = 'S'
     )) AS BookingToday
     
END;
