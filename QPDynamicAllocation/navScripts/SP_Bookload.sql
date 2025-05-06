USE [REZ0KWB01]
GO

/****** Object:  StoredProcedure [dbo].[SP_Bookeddata2]    Script Date: 24/04/2025 15:00:10 ******/
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
	@DepartureDate_to DATE
AS
BEGIN

select
    Origin,
    Destination,
    TRIM(FlightNumber) as FlightNumber,
    DepartureDate,
    FORMAT(STD, 'HH:mm') as DepartureTime,
    count(PassengerID) as bookedPax,
    max(Lid) as capacity,
    AdjustedCapacity as adjustedCapacity,
	FORMAT(STDUTC, 'HH:mm') as DepartureTimeUTC,
	SUM(CASE WHEN ClassOfService IN ('TF', 'TE', 'TD', 'TC', 'TB', 'TY') THEN 1 ELSE 0 END) AS tbf_bookings,
	 SUM(CASE WHEN ClassOfService IN ('G0') THEN 1 ELSE 0 END) AS G0_bookings,
	 SUM(CASE WHEN ClassOfService IN ('G1') THEN 1 ELSE 0 END) AS G1_bookings,
	 SUM(CASE WHEN ClassOfService IN ('G2') THEN 1 ELSE 0 END) AS G2_bookings,
	 SUM(CASE WHEN ClassOfService IN ('G3') THEN 1 ELSE 0 END) AS G3_bookings,
	 SUM(CASE WHEN ClassOfService IN ('G4') THEN 1 ELSE 0 END) AS G4_bookings
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
            IL.STD,
			IL.STDUTC,
			PJS.ClassOfService
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
    ) A
group by
    Origin, Destination, FlightNumber, DepartureDate, STD, STDUTC, AdjustedCapacity
order by
    Origin, Destination, DepartureDate

END;
GO


