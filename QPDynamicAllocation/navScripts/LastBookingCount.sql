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
    @ComparisonDate DATE
AS
BEGIN
    SELECT COUNT(*) as BookingsCount
    FROM (
        SELECT BP.PassengerID, IL.DepartureDate,
            CAST(SWITCHOFFSET(B.BookingUTC, DATEPART(TZOFFSET, B.BookingUTC AT TIME ZONE 'India Standard Time')) AS DATE) AS BookingDate, 
            CAST(SWITCHOFFSET(B.BookingUTC, DATEPART(TZOFFSET, B.BookingUTC AT TIME ZONE 'India Standard Time')) AS TIME) AS BookingTime
        FROM rez.Booking B 
        INNER JOIN rez.BookingPassenger BP ON BP.BookingID = B.BookingID 
        INNER JOIN rez.PassengerJourneySegment PJS ON PJS.PassengerID = BP.PassengerID AND (PJS.FareBasis != 'SO7NBSOL' OR PJS.FareBasis IS NULL)  AND PJS.ClassOfService NOT IN ('S') 
        INNER JOIN rez.PassengerJourneyLeg PJL ON PJL.PassengerID = PJS.PassengerID AND PJL.SegmentID = PJS.SegmentID 
        INNER JOIN rez.InventoryLeg IL ON IL.InventoryLegId = PJL.InventoryLegID 
        WHERE  IL.DepartureDate = @DepartureDate
            AND IL.FlightNumber = @FlightNumber
            AND IL.DepartureStation = @DepartureStation
            AND IL.ArrivalStation = @ArrivalStation
        GROUP BY BP.PassengerID, B.BookingUTC, IL.DepartureDate, PJL.UnitDesignator
    ) A
    WHERE CONCAT(A.BookingDate, ' ', A.BookingTime) <= CONVERT(VARCHAR, DATEADD(DAY, 0, @ComparisonDate), 23)
END;
