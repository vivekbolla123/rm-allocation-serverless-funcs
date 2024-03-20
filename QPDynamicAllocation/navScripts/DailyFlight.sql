USE [REZ0KWB01]
GO
/****** Object:  StoredProcedure [dbo].[SP_DailyFlights]    Script Date: 20-03-2024 4.54.47 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:    <Author,,Name>
-- Create date: <Create Date,,>
-- Description:  <Description,,>
-- =============================================

ALTER PROCEDURE [dbo].[SP_DailyFlights]
    @DepartureDate DATE,
    @DepartureStation CHAR(3),
    @ArrivalStation CHAR(3)
AS
BEGIN

SELECT
    IL.DepartureStation as Origin,
    IL.ArrivalStation as Destination,
    IL.FlightNumber,
    FORMAT(IL.STD, 'HH:mm') as DepartureTime
FROM
    [rez].InventoryLeg IL
    LEFT OUTER JOIN [rez].PassengerJourneyLeg PJL ON PJL.InventoryLegID = IL.InventoryLegID
    LEFT OUTER JOIN [rez].PassengerJourneySegment PJS ON PJS.PassengerID = PJL.PassengerID AND PJS.SegmentID = PJL.SegmentID AND (PJS.FareBasis != 'SO7NBSOL' OR PJS.FareBasis IS NULL)
    LEFT OUTER JOIN [rez].BookingPassenger BP ON BP.PassengerID = PJS.PassengerID
WHERE
    IL.Lid > 0
    AND IL.Status NOT IN (2)
    AND IL.DepartureDate = @DepartureDate
    AND IL.DepartureStation = @DepartureStation
    AND IL.ArrivalStation = @ArrivalStation
GROUP BY
    IL.DepartureStation, IL.ArrivalStation, IL.FlightNumber, IL.STD
ORDER BY
    IL.STD, IL.DepartureStation, IL.ArrivalStation, IL.FlightNumber
END;