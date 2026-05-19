USE [REZ0KWB01]
GO

/****** Object:  StoredProcedure [dbo].[SP_LastBookingsCount]    Script Date: 24/04/2025 15:01:11 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[SP_LastBookingsCount]
    @DepartureStartDate DATE,
    @DepartureEndDate DATE
AS
BEGIN

    DECLARE @curr_datetime_utc AS DATETIME = GETUTCDATE();
    DECLARE @todaydate_ist AS DATE;
    DECLARE @todaydatetime_utc AS DATETIME;

    SET @todaydate_ist = CAST(@curr_datetime_utc AT TIME ZONE 'UTC' AT TIME ZONE 'India Standard Time' AS DATE);
    SET @todaydatetime_utc = CAST(CAST(@todaydate_ist AS DATETIME) AT TIME ZONE 'India Standard Time' AT TIME ZONE 'UTC' AS DATETIME);

    SELECT 
           IL.DepartureDate,
           IL.FlightNumber,
           IL.DepartureStation,
           IL.ArrivalStation,
           COUNT(DISTINCT BP.PassengerID) BookingToday
    FROM rez.Booking B WITH(NOLOCK)
    INNER JOIN rez.BookingPassenger BP WITH(NOLOCK) ON BP.BookingID = B.BookingID 
    INNER JOIN rez.PassengerJourneySegment PJS WITH(NOLOCK) ON PJS.PassengerID = BP.PassengerID 
    INNER JOIN rez.PassengerJourneyLeg PJL WITH(NOLOCK) ON PJL.PassengerID = PJS.PassengerID AND PJL.SegmentID = PJS.SegmentID 
    INNER JOIN rez.InventoryLeg IL WITH(NOLOCK) ON IL.InventoryLegId = PJL.InventoryLegID 
    WHERE IL.DepartureDate BETWEEN @DepartureStartDate AND @DepartureEndDate
      AND B.BookingUTC >= @todaydatetime_utc
      AND
      (
          ClassOfService != 'S' 
        OR FareBasis != 'SO7NBSOL' 
        OR FareBasis IS NULL
      )
    GROUP BY 
           IL.DepartureDate,
           IL.FlightNumber,
           IL.DepartureStation,
           IL.ArrivalStation;

END;
GO


