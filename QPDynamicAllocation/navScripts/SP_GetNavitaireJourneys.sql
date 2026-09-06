USE [REZ0KWB01]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =======================================================================================
-- Script: Navitaire_Connecting_Through_Extract
-- Description: Filters strictly by actual flight date (DepartureDate1).
-- Execution: EXEC dbo.SP_GetNavitaireJourneys '2026-04-28', '2027-04-01'
-- =======================================================================================

ALTER PROCEDURE [dbo].[SP_GetNavitaireJourneys]
    @StartDate DATE,
    @EndDate DATE
AS
BEGIN
    SET NOCOUNT ON;

    -- =====================================================================
    -- STEP 1: Dump active schedules into a Temp Table
    -- =====================================================================
    IF OBJECT_ID('tempdb..#BaseSchedule') IS NOT NULL DROP TABLE #BaseSchedule;

    SELECT 
        ScheduleJourneyID,
        JourneyDepartureStation,
        JourneyArrivalStation,
        CAST(JourneyBeginDate AS DATE) AS JourneyBeginDate,
        CAST(JourneyEndDate AS DATE) AS JourneyEndDate,
        JourneyType,
        SegmentNumber,
        LegNumber,
        LTRIM(RTRIM(SegmentCarrierCode)) AS CarrierCode,
        LTRIM(RTRIM(CAST(SegmentFlightNumber AS VARCHAR(20)))) AS FlightNumber,
        DepartureStation,
        ArrivalStation,
        PaxSTD,
        PaxSTA,
        ISNULL(OnwardDays, 0) AS OnwardDays
    INTO #BaseSchedule
    FROM Schedule.ActiveJourneyView
    WHERE JourneyType IN (2, 4)
      -- Removed strict Begin/End filters as requested. 
      -- Keeping this minimal safety net so we don't pull 10 years of expired historical schedules:
      AND JourneyEndDate >= @StartDate; 

    -- Give the Temp Table lightning-fast indexes
    CREATE CLUSTERED INDEX IX_ScheduleID ON #BaseSchedule(ScheduleJourneyID, SegmentNumber, LegNumber);
    CREATE NONCLUSTERED INDEX IX_ScheduleFlight ON #BaseSchedule(CarrierCode, FlightNumber, DepartureStation, ArrivalStation);


    -- =====================================================================
    -- STEP 2: Dump active Inventory into a Temp Table
    -- =====================================================================
    IF OBJECT_ID('tempdb..#ActiveInv') IS NOT NULL DROP TABLE #ActiveInv;

    SELECT 
        LTRIM(RTRIM(CarrierCode)) AS CarrierCode,
        LTRIM(RTRIM(CAST(FlightNumber AS VARCHAR(20)))) AS FlightNumber,
        DepartureStation,
        ArrivalStation,
        CAST(DepartureDate AS DATE) AS FlightDate
    INTO #ActiveInv
    FROM Rez.InventoryLeg
    WHERE Status = 0
      AND DepartureDate >= @StartDate 
      -- Added 2 buffer days to the end to catch Leg 2s that cross midnight
      AND DepartureDate <= DATEADD(DAY, 2, @EndDate);

    -- Index the exact columns we will join on
    CREATE CLUSTERED INDEX IX_Inv_Flight ON #ActiveInv(CarrierCode, FlightNumber, DepartureStation, ArrivalStation, FlightDate);


    -- =====================================================================
    -- STEP 3: The Main Query (Now running off the lightning-fast Temp Tables)
    -- =====================================================================
    WITH JourneyData AS (

        -- CONNECTING flights (Type 4: 2 segments)
        SELECT
            a.ScheduleJourneyID,
            a.JourneyDepartureStation,
            a.JourneyArrivalStation,
            'Connecting' AS JourneyType,
            s1.FlightNumber AS FlightNumber1,
            s1.DepartureStation + s1.ArrivalStation AS Sector1,
            il1.FlightDate AS DepartureDate1, 
            RIGHT('0' + CAST(s1.PaxSTD / 60 AS VARCHAR), 2) + ':' + RIGHT('0' + CAST(s1.PaxSTD % 60 AS VARCHAR), 2) AS STD1,
            RIGHT('0' + CAST(s1.PaxSTA / 60 AS VARCHAR), 2) + ':' + RIGHT('0' + CAST(s1.PaxSTA % 60 AS VARCHAR), 2) AS STA1,
            s2.FlightNumber AS FlightNumber2,
            s2.DepartureStation + s2.ArrivalStation AS Sector2,
            il2.FlightDate AS DepartureDate2, 
            RIGHT('0' + CAST(s2.PaxSTD / 60 AS VARCHAR), 2) + ':' + RIGHT('0' + CAST(s2.PaxSTD % 60 AS VARCHAR), 2) AS STD2,
            RIGHT('0' + CAST(s2.PaxSTA / 60 AS VARCHAR), 2) + ':' + RIGHT('0' + CAST(s2.PaxSTA % 60 AS VARCHAR), 2) AS STA2,
            ROUND(((DATEDIFF(DAY, il1.FlightDate, il2.FlightDate) + CASE WHEN s2.PaxSTA < s2.PaxSTD THEN 1 ELSE 0 END) * 1440.0 + s2.PaxSTA - s1.PaxSTD) / 60.0, 2) AS ConnectionDuration
        FROM (
            SELECT DISTINCT ScheduleJourneyID, JourneyDepartureStation, JourneyArrivalStation, JourneyBeginDate, JourneyEndDate 
            FROM #BaseSchedule WHERE JourneyType = 4
        ) a
        INNER JOIN #BaseSchedule s1
            ON s1.ScheduleJourneyID = a.ScheduleJourneyID AND s1.SegmentNumber = 1 AND s1.LegNumber = 1
        INNER JOIN #BaseSchedule s2
            ON s2.ScheduleJourneyID = a.ScheduleJourneyID AND s2.SegmentNumber = 2 AND s2.LegNumber IN (1, 2)
        INNER JOIN #ActiveInv il1
            ON il1.CarrierCode = s1.CarrierCode
            AND il1.FlightNumber = s1.FlightNumber
            AND il1.DepartureStation = s1.DepartureStation
            AND il1.ArrivalStation = s1.ArrivalStation
            AND il1.FlightDate >= a.JourneyBeginDate 
            AND il1.FlightDate <= a.JourneyEndDate
        INNER JOIN #ActiveInv il2
            ON il2.CarrierCode = s2.CarrierCode
            AND il2.FlightNumber = s2.FlightNumber
            AND il2.DepartureStation = s2.DepartureStation
            AND il2.ArrivalStation = s2.ArrivalStation
            AND il2.FlightDate = DATEADD(DAY, s2.OnwardDays, il1.FlightDate)

        UNION ALL

        -- THROUGH flights (Type 2: 1 segment, 2 legs)
        SELECT
            a.ScheduleJourneyID,
            a.JourneyDepartureStation,
            a.JourneyArrivalStation,
            'Through' AS JourneyType,
            l1.FlightNumber AS FlightNumber1,
            l1.DepartureStation + l1.ArrivalStation AS Sector1,
            il1.FlightDate AS DepartureDate1,
            RIGHT('0' + CAST(l1.PaxSTD / 60 AS VARCHAR), 2) + ':' + RIGHT('0' + CAST(l1.PaxSTD % 60 AS VARCHAR), 2) AS STD1,
            RIGHT('0' + CAST(l1.PaxSTA / 60 AS VARCHAR), 2) + ':' + RIGHT('0' + CAST(l1.PaxSTA % 60 AS VARCHAR), 2) AS STA1,
            l2.FlightNumber AS FlightNumber2,
            l2.DepartureStation + l2.ArrivalStation AS Sector2,
            il2.FlightDate AS DepartureDate2,
            RIGHT('0' + CAST(l2.PaxSTD / 60 AS VARCHAR), 2) + ':' + RIGHT('0' + CAST(l2.PaxSTD % 60 AS VARCHAR), 2) AS STD2,
            RIGHT('0' + CAST(l2.PaxSTA / 60 AS VARCHAR), 2) + ':' + RIGHT('0' + CAST(l2.PaxSTA % 60 AS VARCHAR), 2) AS STA2,
            ROUND(((DATEDIFF(DAY, il1.FlightDate, il2.FlightDate) + CASE WHEN l2.PaxSTA < l2.PaxSTD THEN 1 ELSE 0 END) * 1440.0 + l2.PaxSTA - l1.PaxSTD) / 60.0, 2) AS ConnectionDuration
        FROM (
            SELECT DISTINCT ScheduleJourneyID, JourneyDepartureStation, JourneyArrivalStation, JourneyBeginDate, JourneyEndDate 
            FROM #BaseSchedule WHERE JourneyType = 2
        ) a
        INNER JOIN #BaseSchedule l1
            ON l1.ScheduleJourneyID = a.ScheduleJourneyID AND l1.SegmentNumber = 1 AND l1.LegNumber = 1
        INNER JOIN #BaseSchedule l2
            ON l2.ScheduleJourneyID = a.ScheduleJourneyID AND l2.SegmentNumber = 1 AND l2.LegNumber = 2
        INNER JOIN #ActiveInv il1
            ON il1.CarrierCode = l1.CarrierCode
            AND il1.FlightNumber = l1.FlightNumber
            AND il1.DepartureStation = l1.DepartureStation
            AND il1.ArrivalStation = l1.ArrivalStation
            AND il1.FlightDate >= a.JourneyBeginDate 
            AND il1.FlightDate <= a.JourneyEndDate
        INNER JOIN #ActiveInv il2
            ON il2.CarrierCode = l2.CarrierCode
            AND il2.FlightNumber = l2.FlightNumber
            AND il2.DepartureStation = l2.DepartureStation
            AND il2.ArrivalStation = l2.ArrivalStation
            AND il2.FlightDate = DATEADD(DAY, l2.OnwardDays, il1.FlightDate)
    )

    -- =====================================================================
    -- STEP 4: Final Deduplication and Strict Filter on DepartureDate1
    -- =====================================================================
    SELECT 
        ScheduleJourneyID,
        JourneyDepartureStation,
        JourneyArrivalStation,
        JourneyType,
        FlightNumber1,
        Sector1,
        DepartureDate1,
        STD1,
        STA1,
        FlightNumber2,
        Sector2,
        DepartureDate2,
        STD2,
        STA2,
        ConnectionDuration
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY FlightNumber1, FlightNumber2, Sector1, Sector2, DepartureDate1, DepartureDate2
                   ORDER BY ScheduleJourneyID
               ) AS rn
        FROM JourneyData
    ) FinalData
    WHERE rn = 1
      -- EXACT FILTERS applied strictly to DepartureDate1 as requested
      AND DepartureDate1 >= @StartDate
      AND DepartureDate1 <= @EndDate
    ORDER BY DepartureDate1 ASC, JourneyDepartureStation, JourneyArrivalStation;

    -- =====================================================================
    -- STEP 5: Cleanup Temp Tables
    -- =====================================================================
    IF OBJECT_ID('tempdb..#BaseSchedule') IS NOT NULL DROP TABLE #BaseSchedule;
    IF OBJECT_ID('tempdb..#ActiveInv') IS NOT NULL DROP TABLE #ActiveInv;

END;
GO