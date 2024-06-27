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
-- Execution: exec SP_LastSoldFare '2022-07-21';
-- ============================================= 
ALTER PROCEDURE [dbo].[SP_LastSoldFare]
@DepartureDate varchar(11)
AS 
BEGIN

SET NOCOUNT ON;

Select 
BookingID, BookingUTC, PassengerID, SegmentID, InventoryLegID, DepartureStation, ArrivalStation, DepartureDate, FlightNumber, 
SUBSTRING(FareBasis, 5, 1) as FareBasis, RecordLocator, Fare
from 
(
  Select
    BookingID, BookingUTC, PassengerID, SegmentID, InventoryLegID, DepartureStation, ArrivalStation, DepartureDate, FlightNumber, FareBasis, RecordLocator,
    CAST(sum(BaseFare+AirportCharges+Taxes) over (Partition by PassengerID,TripNumber,JourneyNumber,ModSegmentNumber)*(SQRT(Distance))/SUM(SQRT(Distance)) OVER (Partition by PassengerID,TripNumber,JourneyNumber,ModSegmentNumber) as decimal(19,4)) as Fare
    from 
    (
      SELECT
        B.RecordLocator, B.BookingID, B.BookingUTC, BP.PassengerID, PJS.SegmentID, IL.InventoryLegID, 
        IL.DepartureStation, IL.ArrivalStation, IL.DepartureDate, IL.FlightNumber,
        PJS.TripNumber, PJS.JourneyNumber, PJS.SegmentNumber, PJS.FareComponentNumber, PJS.FareJourneyType, PJS.FareBasis,
        Case when PJS.FareJourneyType in (1,4) then SUM(Case when PJC.ChargeType in (0,1,2,3) then (PJC.ChargeAmount*CTM.PositiveNegativeFlag*CCV.ConversionRate) else 0 end) 
          when PJS.FareJourneyType=2 then SUM(Case when PJC.ChargeType in (0,1,2,3) and PJS.SegmentNumber=PJL.LegNumber then (PJC.ChargeAmount*CTM.PositiveNegativeFlag*CCV.ConversionRate) else 0 end) else 0 end as BaseFare,
        Case when PJS.FareJourneyType in (1,4) then SUM(Case when PJC.ChargeType=4 then (PJC.ChargeAmount*CTM.PositiveNegativeFlag*CCV.ConversionRate) else 0 end) 
          when PJS.FareJourneyType=2 then SUM(Case when PJC.ChargeType=4 and PJS.SegmentNumber=PJL.LegNumber then (PJC.ChargeAmount*CTM.PositiveNegativeFlag*CCV.ConversionRate) else 0 end) else 0 end as AirportCharges,
        Case when PJS.FareJourneyType in (1,4) then SUM(Case when PJC.ChargeType=5 then (PJC.ChargeAmount*CTM.PositiveNegativeFlag*CCV.ConversionRate) else 0 end) 
        + SUM(Case when PJC.ChargeType=3 and CTM.IncludedTaxFlag=1  then (PJC.ChargeAmount*CCV.ConversionRate) else 0 end)
        when PJS.FareJourneyType=2 then SUM(Case when PJC.ChargeType=5 and PJS.SegmentNumber=PJL.LegNumber then (PJC.ChargeAmount*CTM.PositiveNegativeFlag*CCV.ConversionRate) else 0 end) 
        + SUM(Case when PJC.ChargeType=3 and CTM.IncludedTaxFlag=1  then (PJC.ChargeAmount*CCV.ConversionRate) else 0 end)
        else 0 end as Taxes,
        case when PJS.FareBasis='' then 1 else SegmentNumber end as ModSegmentNumber,
        avg(CP.ActualDistance) as Distance
      FROM
        rez.Booking B
        INNER JOIN rez.BookingPassenger BP ON BP.BookingID = B.BookingID
        INNER JOIN rez.PassengerJourneySegment PJS ON PJS.PassengerID = BP.PassengerID AND PJS.ClassOfService NOT IN ('G', 'S') 
        INNER JOIN rez.PassengerJourneyLeg PJL ON PJL.PassengerID = PJS.PassengerID AND PJL.SegmentID = PJS.SegmentID
        INNER JOIN rez.InventoryLeg IL ON IL.InventoryLegId = PJL.InventoryLegID AND IL.Status NOT IN (2) AND IL.Lid > 0
        LEFT JOIN rez.PassengerJourneyCharge PJC ON PJC.PassengerID = PJL.PassengerID AND PJC.SegmentID = PJL.SegmentID
        LEFT JOIN dw.ChargeTypeMatrix CTM ON CTM.ChargeTypeID = PJC.ChargeType
        Left join dw.CurrencyConversionVersion CCV ON CCV.FromCurrencyCode=PJC.CurrencyCode and CCV.ToCurrencyCode='INR' and CCV.ClientCode='0K' and CCV.VersionStartUTC<=PJC.CreatedUTC and CCV.VersionEndUTC>PJC.CreatedUTC
        inner join dw.CityPair CP ON CP.DepartureStation = IL.DepartureStation and CP.ArrivalStation = IL.ArrivalStation 
      WHERE
        BookingUTC >= DATEADD(HOUR, -48, GETDATE())
      GROUP BY
        B.BookingID, BP.PassengerID, PJS.SegmentID, IL.InventoryLegID, IL.DepartureStation, IL.ArrivalStation, IL.DepartureDate, IL.FlightNumber,
        PJS.TripNumber, PJS.JourneyNumber, PJS.SegmentNumber, PJS.FareComponentNumber, PJS.FareJourneyType, PJS.FareBasis, B.BookingUTC, B.RecordLocator
    ) X
  ) Y
  WHERE
	DepartureDate between DATEADD(HOUR, -48, @DepartureDate) and DATEADD(HOUR, 48, @DepartureDate)
	AND (SUBSTRING(FareBasis, 5, 1) IN ('M', 'R') OR FareBasis='')

END;
