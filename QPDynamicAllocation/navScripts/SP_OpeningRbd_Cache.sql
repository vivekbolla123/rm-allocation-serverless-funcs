/*
    Combined bookload + OPENING-RBD query used by the rm-allocation-scheduled-jobs
    `navitaire_cache` module (navitaireCache.py).

    Opening-RBD logic (via ILC.ClassRank):
      - A class is sellable when  ClassAU > 0  AND  ClassAU - bookedPax > 0
        (bookedPax = current booked load on the flight).
      - The opening RBD is the sellable class with the LOWEST ILC.ClassRank.
        ClassRank is Navitaire's own inventory class ordering, so we rank by it
        directly instead of a hard-coded class list.

    Timing reference: full 0-365 window ~20-40s. The module runs this in 30-day
    batches; set the two dates below to one 30-day window to mirror a batch.

    Restricts open classes to the configured B2C RBD set (@B2C_RBDS), matching
    constants B2C_RBD_VALUES.
*/

USE [REZ0KWB01]
GO

DECLARE @DepartureDate_from DATE = CAST(GETDATE() AS DATE);
DECLARE @DepartureDate_to   DATE = DATEADD(DAY, 30, CAST(GETDATE() AS DATE));

DECLARE @B2C_RBDS TABLE (ClassOfService VARCHAR(3) PRIMARY KEY);
INSERT INTO @B2C_RBDS (ClassOfService) VALUES
    ('V4'),('V3'),('V2'),('V1'),('V0'),
    ('U4'),('U3'),('U2'),('U1'),('U0'),
    ('T4'),('T3'),('T2'),('T1'),('T0'),
    ('R4'),('R3'),('R2'),('R1'),('R0'),
    ('Q4'),('Q3'),('Q2'),('Q1'),('Q0'),
    ('P4'),('P3'),('P2'),('P1'),('P0'),
    ('O4'),('O3'),('O2'),('O1'),('O'),
    ('N4'),('N3'),('N2'),('N1'),('N'),
    ('M'),('K'),('J'),('I'),('H'),('F'),('E'),('D'),('C'),('B'),('Y');

;WITH Bookload AS (
    SELECT
        A.Origin,
        A.Destination,
        TRIM(A.FlightNumber) AS FlightNumber,
        A.DepartureDate,
        A.InventoryLegID,
        FORMAT(A.STD, 'HH:mm') AS DepartureTime,
        COUNT(A.PassengerID) AS bookedPax,
        A.AdjustedCapacity AS adjustedCapacity
    FROM
        (
            SELECT
                BP.PassengerID,
                IL.InventoryLegID,
                IL.DepartureStation AS Origin,
                IL.ArrivalStation AS Destination,
                IL.DepartureDate,
                IL.FlightNumber,
                IL.AdjustedCapacity,
                IL.Lid,
                IL.STD
            FROM
                [rez].InventoryLeg IL
                LEFT OUTER JOIN [rez].PassengerJourneyLeg PJL
                    ON PJL.InventoryLegID = IL.InventoryLegID
                LEFT OUTER JOIN [rez].PassengerJourneySegment PJS
                    ON PJS.PassengerID = PJL.PassengerID
                    AND PJS.SegmentID = PJL.SegmentID
                    AND (PJS.FareBasis != 'SO7NBSOL' OR PJS.FareBasis IS NULL)
                LEFT OUTER JOIN [rez].BookingPassenger BP
                    ON BP.PassengerID = PJS.PassengerID
            WHERE
                IL.Lid > 0
                AND IL.Status NOT IN (2)
                AND IL.DepartureDate >= @DepartureDate_from
                AND IL.DepartureDate <= @DepartureDate_to
        ) A
    GROUP BY
        A.Origin, A.Destination, A.FlightNumber, A.DepartureDate,
        A.InventoryLegID, A.STD, A.AdjustedCapacity
),
-- Open classes per leg with both a sellable ranking and a highest-rank
-- fallback (for overbooked flights where no class is sellable).
RankedRbd AS (
    SELECT
        B.InventoryLegID,
        ILC.ClassOfService,
        ILC.ClassAU,
        ILC.ClassRank,
        CASE WHEN (ILC.ClassAU - B.bookedPax) > 0 THEN 1 ELSE 0 END AS is_sellable,
        -- Lowest-rank sellable class (opening RBD on the happy path).
        ROW_NUMBER() OVER (
            PARTITION BY B.InventoryLegID
            ORDER BY
                CASE WHEN (ILC.ClassAU - B.bookedPax) > 0 THEN 0 ELSE 1 END ASC,
                ILC.ClassRank ASC
        ) AS sellable_rn,
        -- Highest-rank open class (fallback when overbooked).
        ROW_NUMBER() OVER (
            PARTITION BY B.InventoryLegID
            ORDER BY ILC.ClassRank DESC
        ) AS highest_rn
    FROM Bookload B
        JOIN [rez].InventoryLegClass ILC
            ON ILC.InventoryLegID = B.InventoryLegID
        JOIN @B2C_RBDS R
            ON R.ClassOfService = ILC.ClassOfService
    WHERE ILC.ClassAU > 0
),
-- One opening RBD per leg: lowest-rank sellable class if any is sellable,
-- otherwise (overbooked => none sellable) the highest-rank open class (e.g. Y).
OpeningRbd AS (
    SELECT
        r.InventoryLegID,
        r.ClassOfService,
        r.ClassAU,
        r.ClassRank
    FROM RankedRbd r
    WHERE
        (EXISTS (SELECT 1 FROM RankedRbd s
                 WHERE s.InventoryLegID = r.InventoryLegID AND s.is_sellable = 1)
         AND r.is_sellable = 1 AND r.sellable_rn = 1)
        OR
        (NOT EXISTS (SELECT 1 FROM RankedRbd s
                     WHERE s.InventoryLegID = r.InventoryLegID AND s.is_sellable = 1)
         AND r.highest_rn = 1)
)
SELECT
    B.Origin,
    B.Destination,
    B.FlightNumber,
    B.DepartureDate,
    B.DepartureTime,
    B.bookedPax,
    B.adjustedCapacity,
    O.ClassOfService AS OpeningRBD,
    O.ClassAU        AS OpeningRBD_ClassAU,
    O.ClassRank      AS OpeningRBD_ClassRank
FROM Bookload B
    LEFT OUTER JOIN OpeningRbd O
        ON O.InventoryLegID = B.InventoryLegID
ORDER BY
    B.Origin, B.Destination, B.DepartureDate;
GO
