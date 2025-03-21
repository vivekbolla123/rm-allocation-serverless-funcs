USE [REZ0KWB01]
GO
/****** Object:  StoredProcedure [dbo].[SP_LastSoldFare]    Script Date: 20-03-2024 4.53.36 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:    <Author,,Name>
-- Create date: <Create Date,,>
-- Description:  <Description,,>
-- Execution: exec SP_LastSoldFare '2022-07-21','1363','bom','blr';
-- ============================================= 
ALTER PROCEDURE [dbo].[SP_LastSoldFare]
@DepartureDate Date,
@FlightNumber CHAR(4),
@DepartureStation CHAR(3),
@ArrivalStation CHAR(3) 
AS 
BEGIN

SET NOCOUNT ON;

SELECT
    ISNULL(MAX(CASE WHEN SUBSTRING(FareBasis, 5, 1) = 'M' THEN Fare ELSE 0 END),0) AS B2Bfare,
    ISNULL(MAX(CASE WHEN SUBSTRING(FareBasis, 5, 1) = 'R' THEN Fare ELSE 0 END),0) AS B2Cfare
FROM
    (
        SELECT
            PJS.FareBasis,
            SUM(CASE WHEN PJC.ChargeType IN (0, 1, 2, 4, 5) AND PJL.LegNumber = PJS.SegmentNumber THEN (PJC.ChargeAmount * CTM.PositiveNegativeFlag) ELSE 0 END) AS Fare,
            ROW_NUMBER() OVER (PARTITION BY SUBSTRING(FareBasis, 5, 1) ORDER BY BookingUTC DESC ) AS Sno
        FROM
            rez.Booking B
            INNER JOIN rez.BookingPassenger BP ON BP.BookingID = B.BookingID
            INNER JOIN rez.PassengerJourneySegment PJS ON PJS.PassengerID = BP.PassengerID AND PJS.ClassOfService NOT IN ('G', 'S') AND SUBSTRING(PJS.FareBasis, 5, 1) IN ('M', 'R')
            INNER JOIN rez.PassengerJourneyLeg PJL ON PJL.PassengerID = PJS.PassengerID AND PJL.SegmentID = PJS.SegmentID
            INNER JOIN rez.InventoryLeg IL ON IL.InventoryLegId = PJL.InventoryLegID
            LEFT JOIN rez.PassengerJourneyCharge PJC ON PJC.PassengerID = PJL.PassengerID AND PJC.SegmentID = PJL.SegmentID
            LEFT JOIN dw.ChargeTypeMatrix CTM ON CTM.ChargeTypeID = PJC.ChargeType
        WHERE
            IL.Lid > 0
            AND IL.Status NOT IN (2)
            AND IL.DepartureDate = @DepartureDate
            AND IL.FlightNumber = @FlightNumber
            AND IL.DepartureStation = @DepartureStation
            AND IL.ArrivalStation = @ArrivalStation
            AND BookingUTC >= DATEADD(HOUR, -48, GETDATE())
        GROUP BY
            B.BookingUTC, BP.PassengerID, PJS.FareBasis
    ) AS A
WHERE
    Sno = 1;
    
END;