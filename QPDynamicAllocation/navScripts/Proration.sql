WITH QP_DL_BOOKEDDATAPAX_CTE AS
        ( SELECT 
            BookingID, PNR, BookingUTC, 
            CAST(SWITCHOFFSET(BookingUTC, DATEPART(TZOFFSET, BookingUTC AT TIME ZONE 'India Standard Time')) as date) as BookingDate, 
            CONVERT(VARCHAR(20), CAST(SWITCHOFFSET(BookingUTC, DATEPART(TZOFFSET, BookingUTC AT TIME ZONE 'India Standard Time')) as time), 108) as BookingTime,     

            PassengerID,   

            SegmentID,TripNumber,JourneyNumber,SegmentNumber,FareBasis,
            D.DepartureDate, Origin, Destination, D.FlightNumber, D.InventoryLegID,     
            RevenuePax,

            CAST(BaseFare as DECIMAL(19,4)) as BaseFare, 
            CAST(BaseFare+OverrideAmount+DiscountAmount as DECIMAL(19,4)) as DiscountedBaseFare,
            CAST(DiscountAmount as DECIMAL(19,4)) as DiscountAmount,
            CAST(RCS+CUTE as DECIMAL(19,4)) as RCSCUTE, 
            CAST(RCS as DECIMAL(19,4)) as RCS, 
            CAST(CUTE as DECIMAL(19,4)) as CUTE,

            D.AdjustedCapacity, Seat, D.LegNumber, 
            CAST(BaseFare_Leg as DECIMAL(19,4)) as BaseFare_Pro, 
            CAST(BaseFare_Leg+DiscountAmount_New+OverrideAmount_Leg as DECIMAL(19,4)) as DiscountedBaseFare_Pro,
            CAST(DiscountAmount_New as DECIMAL(19,4)) as DiscountAmount_Pro, 
            CAST(YQ as DECIMAL(19,4)) as YQ, 
            CAST(YQ_Leg as DECIMAL(19,4)) as YQ_Pro

        FROM 
        (
            Select 

                BookingID, PNR, BookingPromoCode, BookingUTC, BookingCurrency,

                PassengerID, DiscountCode,

                SegmentID, TripNumber,JourneyNumber,SegmentNumber, FareComponentNumber,FareJourneyType, FareBasis, TripTypeCode,
                FareStatusCode, ChannelTypeCode, Journey_Origin, Journey_Destination,
                Journey_Distance,
                GoverningFare, GoverningFare_Journey,

                LegNumber, Seat,

                InventoryLegID, DepartureDate, Origin, Destination, FlightNumber, AdjustedCapacity, 

                Route, Market, Distance, DomesticFlag, RevenuePax,OrganizationCode,
                OPassengerID,
                BaseFare_G,
                Override_G,         

                BaseFare,
                BaseFare_Journey,
                BaseFare_Leg,

                OverrideAmount,
                OverrideAmount_Journey,
                OverrideAmount_Leg,	

                DiscountAmount,
                DiscountAmount_Journey,
                DiscountAmount_Leg,

                CASE
                    When OPassengerID is not null and FareJourneyType<>1 Then 0 
                    /*When FareJourneyType is 1, then OPassengerID is not applied in BaseFare, so it should not be applied in discountamount*/
                    When  SUM(BaseFare_Leg) OVER (PARTITION BY PassengerID,TripNumber,JourneyNumber)=0 then 0
                    /*To remove Division by 0*/
                    WHEN SUM(GoverningFare) OVER (PARTITION BY PassengerID,TripNumber,JourneyNumber) = 0 THEN DiscountAmount_Leg
                    /*No further pro rata required if Governing Fare is 0*/
                    else SUM(DiscountAmount) OVER (PARTITION BY PassengerID,TripNumber,JourneyNumber) * (BaseFare_Leg)/SUM(BaseFare_Leg) OVER (PARTITION BY PassengerID,TripNumber,JourneyNumber)
                    /*Discount is pro rata distributed in the ratio of basefare*/
                END as DiscountAmount_New,

                RCS, CUTE, YQ, YQ_Journey,

                Case
                    When FareJourneyType=4 and Sum(GoverningFare) Over (PARTITION BY PassengerID,TripNumber,JourneyNumber,SegmentNumber)=0 Then YQ
                    Else YQ_Leg
                End YQ_Leg
            from 
            (
                /* Leg Query
                It is required to distribute the amounts on leg level
                */
                Select 

                    BookingID, PNR, BookingPromoCode, BookingUTC, BookingCurrency,

                    PassengerID,DiscountCode,

                    SegmentID, TripNumber,JourneyNumber,SegmentNumber, FareComponentNumber,FareJourneyType, FareBasis, TripTypeCode,
                    FareStatusCode, ChannelTypeCode, Journey_Origin, Journey_Destination,
                    Journey_Distance,
                    GoverningFare, GoverningFare_Journey,

                    LegNumber, Seat,

                    InventoryLegID, DepartureDate, Origin, Destination, FlightNumber, AdjustedCapacity, 

                    Route, Market, Distance, DomesticFlag, RevenuePax,OrganizationCode,
                    OPassengerID,
                    BaseFare_G,
                    Override_G,   

                    BaseFare,
                    BaseFare_Journey,
                    SUM(BaseFare_Journey) OVER (PARTITION BY PassengerID,TripNumber,JourneyNumber,SegmentNumber) * (SQRT(Distance))/SUM(SQRT(Distance)) OVER (PARTITION BY PassengerID,TripNumber,JourneyNumber,SegmentNumber) as BaseFare_Leg,

                    OverrideAmount,
                    OverrideAmount_Journey,
                    SUM(OverrideAmount_Journey) OVER (PARTITION BY PassengerID,TripNumber,JourneyNumber,SegmentNumber) * (SQRT(Distance))/SUM(SQRT(Distance)) OVER (PARTITION BY PassengerID,TripNumber,JourneyNumber,SegmentNumber) as OverrideAmount_Leg,

                    DiscountAmount,
                    DiscountAmount_Journey,
                    SUM(DiscountAmount_Journey) OVER (PARTITION BY PassengerID,TripNumber,JourneyNumber,SegmentNumber) * (SQRT(Distance))/SUM(SQRT(Distance)) OVER (PARTITION BY PassengerID,TripNumber,JourneyNumber,SegmentNumber) as DiscountAmount_Leg,

                    RCS,
                    CUTE,
                    YQ,
                    YQ_Journey,
                    SUM(YQ_Journey) OVER (PARTITION BY PassengerID,TripNumber,JourneyNumber,SegmentNumber) * (SQRT(Distance))/SUM(SQRT(Distance)) OVER (PARTITION BY PassengerID,TripNumber,JourneyNumber,SegmentNumber)  as YQ_Leg

                from 
                (
                    /* Journey Query 
                    First the amounts are found on Journey Level
                    */
                    Select 

                        BookingID, PNR, BookingPromoCode, BookingUTC, BookingCurrency,

                        PassengerID, DiscountCode,

                        SegmentID, TripNumber,JourneyNumber,SegmentNumber, FareComponentNumber,FareJourneyType, FareBasis, TripTypeCode,
                        FareStatusCode, ChannelTypeCode,Journey_Origin, Journey_Destination,
                        Journey_Distance,
                        GoverningFare, GoverningFare_Journey,

                        LegNumber, Seat,

                        InventoryLegID, DepartureDate, Origin, Destination, FlightNumber, AdjustedCapacity, 

                        Route, Market, Distance, DomesticFlag, RevenuePax,

                        OrganizationCode,
                        OPassengerID,
                        BaseFare_G,
                        Override_G,                

                        BaseFare,
                        Lag(BaseFare) Over(PARTITION BY PassengerID Order by PassengerID) LagFare,

                        Case 
                            when FareJourneyType = 1 then BaseFare /*Pro rata not required in case of direct flights*/
                            else 
                                CASE 
                                    When OPassengerID is not null /*If OPassengerID is not null means that published fare (Big numbers with lots of 9s) has been filed*/
                                    and Override_G>0 /*In case of positive overrides in base query, this case statement is not to be used*/
                                    and BaseFare*ISNULL(Lead(BaseFare) Over(PARTITION BY PassengerID, TripNumber, JourneyNumber Order by PassengerID,TripNumber, JourneyNumber, SegmentNumber),Lag(BaseFare) Over(PARTITION BY PassengerID, TripNumber, JourneyNumber Order by PassengerID,TripNumber, JourneyNumber, SegmentNumber))<>0 then BaseFare_G * BaseFare / SUM(BaseFare) Over (PARTITION BY PassengerID,TripNumber,JourneyNumber)
                                    /*The above statement checks if one of the published fares is 0. If it is then the ratio is 1:0, and the fare will not be 
                                    distributed on BaseFare, but on distance*/
                                    WHEN SUM(GoverningFare) OVER (PARTITION BY PassengerID,TripNumber,JourneyNumber,GoverningFare) = 0 THEN 
                                    SUM(BaseFare) OVER (PARTITION BY PassengerID,TripNumber,JourneyNumber,GoverningFare) * (SQRT(Journey_Distance))/SUM(SQRT(Journey_Distance)) OVER (PARTITION BY PassengerID,TripNumber,JourneyNumber,GoverningFare) 
                                    ELSE (BaseFare) 
                                END 
                            END
                        as BaseFare_Journey,		

                        OverrideAmount, 

                        Case when FareJourneyType = 1 then OverrideAmount
                            else 
                            CASE
                                When OPassengerID is not null 
                                and Override_G>0
                                and BaseFare*ISNULL(Lead(BaseFare) Over(PARTITION BY PassengerID,TripNumber, JourneyNumber Order by PassengerID,TripNumber, JourneyNumber, SegmentNumber),Lag(BaseFare) Over(PARTITION BY PassengerID,TripNumber, JourneyNumber Order by PassengerID,TripNumber, JourneyNumber, SegmentNumber))<>0 then 0
                                WHEN SUM(GoverningFare) OVER (PARTITION BY PassengerID,TripNumber,JourneyNumber,GoverningFare) = 0 THEN 
                                SUM(OverrideAmount) OVER (PARTITION BY PassengerID,TripNumber,JourneyNumber,GoverningFare) * (SQRT(Journey_Distance))/SUM(SQRT(Journey_Distance)) OVER (PARTITION BY PassengerID,TripNumber,JourneyNumber,GoverningFare) 
                                    ELSE (OverrideAmount) END 
                            END 
                            as OverrideAmount_Journey,	

                        DiscountAmount,
                        SUM(DiscountAmount) OVER (PARTITION BY PassengerID,TripNumber,JourneyNumber) * (SQRT(Journey_Distance))/SUM(SQRT(Journey_Distance)) OVER (PARTITION BY PassengerID,TripNumber,JourneyNumber) 
                                as DiscountAmount_Journey,

                        RCS,
                        CUTE,
                        YQ,

                        CASE WHEN SUM(GoverningFare) OVER (PARTITION BY PassengerID,TripNumber,JourneyNumber) = 0 THEN 
                            SUM(YQ) OVER (PARTITION BY PassengerID,TripNumber,JourneyNumber) * (SQRT(Journey_Distance))/SUM(SQRT(Journey_Distance)) OVER (PARTITION BY PassengerID,TripNumber,JourneyNumber) 
                            ELSE (YQ) 
                        END as YQ_Journey


                    from 
                        (
                            SELECT 
                                B.BookingID, B.RecordLocator as PNR, B.BookingPromoCode, B.BookingUTC, B.CurrencyCode as BookingCurrency,	

                                BP.PassengerID, BP.DiscountCode,

                                PJS.SegmentID,PJS.TripNumber, PJS.JourneyNumber, PJS.SegmentNumber,
                                PJS.FareComponentNumber, PJS.FareJourneyType, PJS.FareBasis, PJS.TripType as TripTypeCode,
                                PJS.FareStatus as FareStatusCode,
                                PJS.ChannelType as ChannelTypeCode,

                                PJS.DepartureStation as Journey_Origin, PJS.ArrivalStation as Journey_Destination,

                                PJL.LegNumber, PJL.UnitDesignator as Seat, 

                                IL.InventoryLegID, IL.DepartureDate, IL.DepartureStation as Origin, IL.ArrivalStation as Destination, 
                                CONCAT(IL.FlightNumber,IL.OpSuffix) as FlightNumber, IL.AdjustedCapacity,

                                CP.CityPair as Route, CP.CityPairGroup as Market, CP.ActualDistance as Distance, CP.DomesticFlag, 

                                1 as RevenuePax,

                                SUM(CASE 
                                        WHEN PJC.ChargeType IN (0,2,3,8) AND PJL.LegNumber=1 THEN (PJC.ChargeAmount*CTM.PositiveNegativeFlag*CCV.ConversionRate) 
                                        ELSE 0
                                    END) as BaseFare,

                                SUM(CASE 
                                        WHEN PJC.ChargeType IN (1) AND B.BookingPromoCode='' AND BP.DiscountCode='' AND PJL.LegNumber=1 THEN (PJC.ChargeAmount*CTM.PositiveNegativeFlag*CCV.ConversionRate)
                                        ELSE 0 
                                    END) as OverrideAmount,
                                    /*Sometimes override is applied as chargetype 1. So, if there is a promocode/discount present,
                                    then chargetype 1 is considered a discount, else it is considered an override*/
                                SUM(CASE 
                                        WHEN PJC.ChargeType IN (7) AND PJL.LegNumber=1 THEN (PJC.ChargeAmount*CTM.PositiveNegativeFlag*CCV.ConversionRate)
                                        WHEN PJC.ChargeType IN (1) AND B.BookingPromoCode<>'' AND PJL.LegNumber=1 THEN (PJC.ChargeAmount*CTM.PositiveNegativeFlag*CCV.ConversionRate)
                                        WHEN PJC.ChargeType IN (1) AND BP.DiscountCode<>'' AND PJL.LegNumber=1  THEN (PJC.ChargeAmount*CTM.PositiveNegativeFlag*CCV.ConversionRate)
                                        ELSE 0 
                                    END) as DiscountAmount,

                                SUM(CASE WHEN PJC.ChargeType=4 AND PJL.LegNumber=1 AND PJC.ChargeCode IN ('RCS') THEN (PJC.ChargeAmount*CTM.PositiveNegativeFlag*CCV.ConversionRate) ELSE 0 END) as RCS,

                                SUM(CASE WHEN PJC.ChargeType=4 AND PJL.LegNumber=1 AND PJC.ChargeCode IN ('CUTE') THEN (PJC.ChargeAmount*CTM.PositiveNegativeFlag*CCV.ConversionRate) ELSE 0 END) as CUTE,

                                                    SUM(CASE WHEN PJC.ChargeType=4 AND PJL.LegNumber=1 AND PJC.ChargeCode IN ('YQ') THEN (PJC.ChargeAmount*CTM.PositiveNegativeFlag*CCV.ConversionRate) ELSE 0 END) as YQ,

            


                                B.SourceOrganizationCode as OrganizationCode, 

                                case when PJL.LegNumber=1 then sum(CP.ActualDistance) over (Partition by BP.PassengerID,PJS.TripNumber,PJS.JourneyNumber, PJS.SegmentNumber) else 0 end as Journey_Distance,

                                CAST(PJS.GoverningFare as INT) as GoverningFare, 
                                SUM(CAST(PJS.GoverningFare as INT)) OVER (PARTITION BY BP.PassengerID,PJS.TripNumber,PJS.JourneyNumber) as GoverningFare_Journey,


                                OV.PassengerID as OPassengerID,
                                BaseFare_G,
                                Override_G
                            FROM 
                                {NAV_DB_NAME}rez.Booking B 
                                INNER JOIN {NAV_DB_NAME}rez.BookingPassenger BP ON BP.BookingID=B.BookingID 
                                INNER JOIN {NAV_DB_NAME}rez.PassengerJourneySegment PJS ON PJS.PassengerID=BP.PassengerID --AND PJS.BookingStatus='HK' 
                                INNER JOIN {NAV_DB_NAME}rez.PassengerJourneyLeg PJL ON PJL.PassengerID=PJS.PassengerID AND PJL.SegmentID=PJS.SegmentID 
                                INNER JOIN {NAV_DB_NAME}rez.InventoryLeg IL ON IL.InventoryLegId=PJL.InventoryLegID and IL.CarrierCode='QP'
                                INNER JOIN {NAV_DB_DW_NAME}dw.CityPair CP ON CP.DepartureStation = IL.DepartureStation AND CP.ArrivalStation = IL.ArrivalStation 
                                LEFT JOIN {NAV_DB_NAME}rez.PassengerJourneyCharge PJC ON PJC.PassengerID=PJL.PassengerID AND PJC.SegmentID=PJL.SegmentID 
                                LEFT JOIN {NAV_DB_DW_NAME}dw.ChargeTypeMatrix CTM ON CTM.ChargeTypeID=PJC.ChargeType   
                                LEFT JOIN {NAV_DB_DW_NAME}dw.CurrencyConversionVersion CCV ON CCV.FromCurrencyCode=PJC.CurrencyCode AND CCV.ToCurrencyCode='INR' AND CCV.ClientCode='0K' AND CCV.VersionStartUTC<=PJC.CreatedUTC AND CCV.VersionEndUTC>PJC.CreatedUTC 


                                left join 
                                (
                                    Select 
                                        PassengerID, 
                                        Sum(Case When ChargeType=0 Then ChargeAmount*CCV.ConversionRate Else 0 End)  - Sum(Case when Chargetype=1 Then ChargeAmount*CCV.ConversionRate Else 0 End) BaseFare_G,
                                        SUM(Case when Chargetype=1 Then ChargeAmount*CCV.ConversionRate Else 0 End) as Override_G
                                    From 
                                    {NAV_DB_NAME}rez.PassengerJourneyCharge PJC
                                    LEFT JOIN {NAV_DB_DW_NAME}dw.CurrencyConversionVersion CCV ON CCV.FromCurrencyCode=PJC.CurrencyCode
                                        AND CCV.ToCurrencyCode='INR' AND CCV.ClientCode='0K' AND CCV.VersionStartUTC<=PJC.CreatedUTC AND CCV.VersionEndUTC>PJC.CreatedUTC 
                                    Where PassengerID in (Select PassengerID from {NAV_DB_NAME}rez.PassengerJourneySegment where FareStatus in (3,5) )
                                    Group by PassengerID
                                ) OV on OV.PassengerID=BP.PassengerID
                            /*The subquery up ahead is to handle the published fares. It calculates the final total amount (BaseFare+Discount) as BaseFare_G
                            and if called in the outer query (when OPassengerID is not null), this value replaces the BaseFare, which is then distributed over sectors*/

                            WHERE 
                            B.BookingID in 
                            ( 
                                Select Distinct B.BookingID from {NAV_DB_NAME}rez.Booking B 
                                    inner join {NAV_DB_NAME}rez.BookingPassenger BP on BP.BookingID=B.BookingID
                                    inner join {NAV_DB_NAME}rez.PassengerJourneySegment PJS on PJS.PassengerId=BP.PassengerID --and PJS.BookingStatus='HK'
                                    inner join {NAV_DB_NAME}rez.PassengerJourneyLeg PJL on PJS.PassengerId=PJL.PassengerID and PJS.SegmentID=PJL.SegmentID
                                    inner join {NAV_DB_NAME}rez.InventoryLeg IL on IL.InventoryLegID=PJL.InventoryLegID and IL.CarrierCode='QP'
                                /*Where statement goes here*/
                                Where -- B.RecordLocator='TD4QXK' AND 
                                IL.STD >= '{start_time_str}' AND IL.STD <= '{end_time_str}'
                            )

                            GROUP BY 
                            B.BookingID, B.RecordLocator, B.BookingPromoCode, B.BookingUTC,
                            BP.PassengerID, BP.FirstName, BP.MiddleName, BP.LastName, BP.Nationality, BP.ResidentCountry, BP.DiscountCode, BP.Infant, 
                            BP.PaxType,
                            PJS.SegmentID, PJS.ClassOfService, PJS.TripNumber, PJS.JourneyNumber, PJS.SegmentNumber, PJS.FareComponentNumber,
                            PJS.FareJourneyType, PJS.FareBasis, 
                            PJS.TripType,
                            PJS.DepartureStation, PJS.ArrivalStation, 
                            PJS.FareStatus,  PJS.ChannelType,
                            PJS.ProductClassCode,
                            IL.DepartureDate, IL.DepartureStation, IL.ArrivalStation, IL.FlightNumber,IL.OpSuffix, IL.InventoryLegID,
                            CP.CityPair, CP.CityPairGroup, PJL.LiftStatus,
                            B.SourceOrganizationCode, IL.AdjustedCapacity, IL.Capacity, PJL.UnitDesignator, PJL.LegNumber, CP.DomesticFlag,
                            B.CurrencyCode,CP.ActualDistance,PJL.LegNumber,PJS.GoverningFare,
                            OV.PassengerID,OV.BaseFare_G, OV.Override_G
                        ) A
                    ) B
                ) C
            /*Where statement goes here*/
            ) D
        INNER JOIN {NAV_DB_NAME}rez.InventoryLeg IL 
            on IL.InventoryLegID = D.InventoryLegID 
            AND IL.Status<>2
            AND IL.STD >= '{start_time_str}' AND IL.STD <= '{end_time_str}'
            AND IL.InventoryLegID NOT IN 
                (
                    Select distinct IL.InventoryLegID
                    From {NAV_DB_NAME}rez.Inventoryleg IL 
                    Join {NAV_DB_NAME}rez.PassengerJourneyLeg PJL on PJL.InventoryLegID=IL.InventoryLegID
                    Join {NAV_DB_NAME}rez.PassengerJourneySegment PJS on PJS.SegmentID=PJL.SegmentID
                        and PJS.PassengerID=PJL.PassengerID
                        and PJS.ClassOfService in ('QC','QD')
                )
        )


        SELECT
            CONCAT(PX.Origin, PX.Destination) AS Sector,
            PX.FlightNumber,
            PX.DepartureDate,
            CASE 
                WHEN CAST(IL.STD AS TIME) BETWEEN '00:00:00' AND '02:30:00' THEN '21:00-02:30'
                WHEN CAST(IL.STD AS TIME) BETWEEN '02:30:01' AND '07:30:00' THEN '02:30-07:30'
                WHEN CAST(IL.STD AS TIME) BETWEEN '07:30:01' AND '11:00:00' THEN '07:30-11:00'
                WHEN CAST(IL.STD AS TIME) BETWEEN '11:00:01' AND '14:30:00' THEN '11:00-14:30'
                WHEN CAST(IL.STD AS TIME) BETWEEN '14:30:01' AND '18:00:00' THEN '14:30-18:00'
                WHEN CAST(IL.STD AS TIME) BETWEEN '18:00:01' AND '21:00:00' THEN '18:00-21:00'
                WHEN CAST(IL.STD AS TIME) BETWEEN '21:00:01' AND '23:59:59' THEN '21:00-02:30'
            END AS TimeCluster,

            
            -- Customer segmentation
            CASE
                WHEN SUBSTRING(PJS.FareBasis, 5, 1) = 'C' THEN 'C'
                WHEN SUBSTRING(PJS.FareBasis, 5, 1) = 'F' THEN 'F'
                WHEN LEFT(PJS.FareBasis, 1) = 'G' THEN 'GROUP'
                WHEN SUBSTRING(PJS.FareBasis, 5, 1) = 'K' THEN 'K'
                WHEN SUBSTRING(PJS.FareBasis, 5, 1) = 'M' THEN 'M'
                WHEN SUBSTRING(PJS.FareBasis, 5, 1) = 'R' THEN 'R'
                WHEN LEFT(PJS.FareBasis, 1) = 'S' THEN 'STAFF'
                WHEN SUBSTRING(PJS.FareBasis, 5, 1) = 'V' THEN 'V'
                WHEN SUBSTRING(PJS.FareBasis, 5, 1) IN ('A', 'B', 'D', 'J', 'N', 'O', 'S') THEN 'Others'
                ELSE 'Unknown'
            END AS CustomerSegment,
            
            CASE
                WHEN DATEDIFF(DAY, CONVERT(DATE, B.BookingUTC), IL.DepartureDate) = 0 THEN '1. 0'
                WHEN DATEDIFF(DAY, CONVERT(DATE, B.BookingUTC), IL.DepartureDate) BETWEEN 1 AND 2 THEN '2. 1-2'
                WHEN DATEDIFF(DAY, CONVERT(DATE, B.BookingUTC), IL.DepartureDate) BETWEEN 3 AND 7 THEN '3. 3-7'
                WHEN DATEDIFF(DAY, CONVERT(DATE, B.BookingUTC), IL.DepartureDate) BETWEEN 8 AND 14 THEN '4. 8-14'
                WHEN DATEDIFF(DAY, CONVERT(DATE, B.BookingUTC), IL.DepartureDate) BETWEEN 15 AND 30 THEN '5. 15-30'
                WHEN DATEDIFF(DAY, CONVERT(DATE, B.BookingUTC), IL.DepartureDate) BETWEEN 31 AND 60 THEN '6. 31-60'
                WHEN DATEDIFF(DAY, CONVERT(DATE, B.BookingUTC), IL.DepartureDate) > 60 THEN '7. 60+'
                ELSE 'Unknown'
            END AS NDOBand,
            
            -- Calculate average fare per booking and classify into fare ranges
            CASE
                WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) = 0 THEN '1. 0-1000'
                WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) BETWEEN 0.001 AND 1000 THEN '1. 0-1000'
                WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) BETWEEN 1000.001 AND 3000 THEN '2. 1000-3000'
                WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) BETWEEN 3000.001 AND 4000 THEN '3. 3000-4000'
                WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) BETWEEN 4000.001 AND 5000 THEN '4. 4000-5000'
                WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) BETWEEN 5000.001 AND 8000 THEN '5. 5000-8000'
                WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) BETWEEN 8000.001 AND 10000 THEN '6. 8000-10000'
                WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) BETWEEN 10000.001 AND 15000 THEN '7. 10000-15000'
                WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) > 15000 THEN '8. 15000+'
                ELSE 'Unknown'
            END AS FareRange,
            
            COUNT(PX.PassengerID) AS BookedPax
        FROM
            {NAV_DB_NAME}[rez].InventoryLeg IL
            LEFT OUTER JOIN {NAV_DB_NAME}[rez].PassengerJourneyLeg PJL ON PJL.InventoryLegID = IL.InventoryLegID
            LEFT OUTER JOIN {NAV_DB_NAME}[rez].PassengerJourneySegment PJS ON PJS.PassengerID = PJL.PassengerID AND PJS.SegmentID = PJL.SegmentID
            LEFT OUTER JOIN {NAV_DB_NAME}[rez].BookingPassenger BP ON BP.PassengerID = PJS.PassengerID
            LEFT OUTER JOIN {NAV_DB_NAME}[rez].Booking B ON BP.BookingID = B.BookingID and IL.CarrierCode='QP'
            LEFT OUTER JOIN QP_DL_BOOKEDDATAPAX_CTE PX ON PX.InventoryLegID = IL.InventoryLegID AND PX.PassengerID = PJL.PassengerID AND PX.SegmentID = PJL.SegmentID
        WHERE
            IL.Lid > 0
            AND IL.STD >= '{start_time_str}' AND IL.STD <= '{end_time_str}'
            AND BP.BookingID IS NOT NULL
            AND CONCAT(PX.Origin, PX.Destination) IS NOT NULL
            AND PX.DepartureDate IS NOT NULL
            AND PX.FlightNumber IS NOT NULL

        GROUP BY
            CONCAT(PX.Origin, PX.Destination),
                PX.FlightNumber,
                PX.DepartureDate,
            CASE 
                WHEN CAST(IL.STD AS TIME) BETWEEN '00:00:00' AND '02:30:00' THEN '21:00-02:30'
                WHEN CAST(IL.STD AS TIME) BETWEEN '02:30:01' AND '07:30:00' THEN '02:30-07:30'
                WHEN CAST(IL.STD AS TIME) BETWEEN '07:30:01' AND '11:00:00' THEN '07:30-11:00'
                WHEN CAST(IL.STD AS TIME) BETWEEN '11:00:01' AND '14:30:00' THEN '11:00-14:30'
                WHEN CAST(IL.STD AS TIME) BETWEEN '14:30:01' AND '18:00:00' THEN '14:30-18:00'
                WHEN CAST(IL.STD AS TIME) BETWEEN '18:00:01' AND '21:00:00' THEN '18:00-21:00'
                WHEN CAST(IL.STD AS TIME) BETWEEN '21:00:01' AND '23:59:59' THEN '21:00-02:30'
            END,
            CASE
                WHEN SUBSTRING(PJS.FareBasis, 5, 1) = 'C' THEN 'C'
                WHEN SUBSTRING(PJS.FareBasis, 5, 1) = 'F' THEN 'F'
                WHEN LEFT(PJS.FareBasis, 1) = 'G' THEN 'GROUP'
                WHEN SUBSTRING(PJS.FareBasis, 5, 1) = 'K' THEN 'K'
                WHEN SUBSTRING(PJS.FareBasis, 5, 1) = 'M' THEN 'M'
                WHEN SUBSTRING(PJS.FareBasis, 5, 1) = 'R' THEN 'R'
                WHEN LEFT(PJS.FareBasis, 1) = 'S' THEN 'STAFF'
                WHEN SUBSTRING(PJS.FareBasis, 5, 1) = 'V' THEN 'V'
                WHEN SUBSTRING(PJS.FareBasis, 5, 1) IN ('A', 'B', 'D', 'J', 'N', 'O', 'S') THEN 'Others'
                ELSE 'Unknown'
            END,
            CASE
                WHEN DATEDIFF(DAY, CONVERT(DATE, B.BookingUTC), IL.DepartureDate) = 0 THEN '1. 0'
                WHEN DATEDIFF(DAY, CONVERT(DATE, B.BookingUTC), IL.DepartureDate) BETWEEN 1 AND 2 THEN '2. 1-2'
                WHEN DATEDIFF(DAY, CONVERT(DATE, B.BookingUTC), IL.DepartureDate) BETWEEN 3 AND 7 THEN '3. 3-7'
                WHEN DATEDIFF(DAY, CONVERT(DATE, B.BookingUTC), IL.DepartureDate) BETWEEN 8 AND 14 THEN '4. 8-14'
                WHEN DATEDIFF(DAY, CONVERT(DATE, B.BookingUTC), IL.DepartureDate) BETWEEN 15 AND 30 THEN '5. 15-30'
                WHEN DATEDIFF(DAY, CONVERT(DATE, B.BookingUTC), IL.DepartureDate) BETWEEN 31 AND 60 THEN '6. 31-60'
                WHEN DATEDIFF(DAY, CONVERT(DATE, B.BookingUTC), IL.DepartureDate) > 60 THEN '7. 60+'
                ELSE 'Unknown'
            END,
            CASE
                WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) = 0 THEN '1. 0-1000'
                WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) BETWEEN 0.001 AND 1000 THEN '1. 0-1000'
                WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) BETWEEN 1000.001 AND 3000 THEN '2. 1000-3000'
                WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) BETWEEN 3000.001 AND 4000 THEN '3. 3000-4000'
                WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) BETWEEN 4000.001 AND 5000 THEN '4. 4000-5000'
                WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) BETWEEN 5000.001 AND 8000 THEN '5. 5000-8000'
                WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) BETWEEN 8000.001 AND 10000 THEN '6. 8000-10000'
                WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) BETWEEN 10000.001 AND 15000 THEN '7. 10000-15000'
                WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) > 15000 THEN '8. 15000+'
                ELSE 'Unknown'
            END