USE [REZ0KWB01]
GO
/****** Object:  StoredProcedure [dbo].[SP_LastSoldFare]    Script Date: 25-01-2024 11.31.28 AM ******/
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
        GROUP BY
            B.BookingUTC, BP.PassengerID, PJS.FareBasis
    ) AS A
WHERE
    Sno = 1;

END;



USE [REZ0KWB01]
GO
/****** Object:  StoredProcedure [dbo].[SP_Bookeddata]    Script Date: 25-01-2024 10.10.44 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:    <Author,,Name>
-- Create date: <Create Date,,>
-- Description:  <Description,,>
-- Execution: EXEC SP_Bookeddata '2024-01-02', '2024-01-03', 'BOM','BLR';
-- =============================================

ALTER PROCEDURE [dbo].[SP_Bookeddata]
    @DepartureDate_from DATE,
	@DepartureDate_to DATE,
    @DepartureStation CHAR(3),
    @ArrivalStation CHAR(3)
AS
BEGIN

select
    Origin,
    Destination,
    FlightNumber,
    DepartureDate,
    FORMAT(STDUTC, 'HH:mm') as DepartureTime,
    count(PassengerID) as bookedPax,
    max(Lid) as capacity,
    AdjustedCapacity as adjustedCapacity
from
    (
        select
            BP.PassengerID,
            IL.DepartureStation as Origin,
            IL.ArrivalStation as Destination,
            IL.DepartureDate,
            IL.FlightNumber,
            IL.AdjustedCapacity,
            IL.Lid,
            IL.STDUTC
        from
            [rez].InventoryLeg IL
            left outer join [rez].PassengerJourneyLeg PJL on PJL.InventoryLegID = IL.InventoryLegID
            left outer join [rez].PassengerJourneySegment PJS on PJS.PassengerID = PJL.PassengerID and PJS.SegmentID = PJL.SegmentID and (PJS.FareBasis != 'SO7NBSOL' or PJS.FareBasis IS NULL)
            left outer join [rez].BookingPassenger BP on BP.PassengerID = PJS.PassengerID
        where
            IL.Lid > 0
            and IL.Status NOT IN (2)
            and IL.DepartureDate >= @DepartureDate_from
            and IL.DepartureDate <= @DepartureDate_to
            and IL.DepartureStation = @DepartureStation
            and IL.ArrivalStation = @ArrivalStation
    ) A
group by
    Origin, Destination, FlightNumber, DepartureDate, STDUTC, AdjustedCapacity
order by
    Origin, Destination, DepartureDate

END;