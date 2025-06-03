import pandas as pd
import csv
import logging
import time
import os
import sys
import boto3
import json
import warnings
import pymssql
import pytz
import numpy as np
import time
import pandas as pd
import configsettings
from uuid import uuid4
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, timezone


class ExpectedNoShows:
    
    def __init__(self, logger, params, overrides, context):
        self.logger = logging.getLogger()
        self.rm_rd_conn = params['wrconn']
        self.nav_conn = params['navconn']
        self.dw_conn = params['dwconn']
        self.logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")
        self.nav_db_name = configsettings.NAV_DB_NAME
        self.nav_db_dw_name = configsettings.NAV_DB_DW_NAME

    def load_noshow_rates_from_dw(self):
        """
        Load historical no-show rates from the DW database
        """
        self.logger.info("Loading no-show rates from RM_DW_DATA.no_show_rate table")
        
        try:
            # Query to fetch no-show rates from the database
            query = """
            SELECT
                TimeCluster,
                CustomerSegment,
                NDOBand,
                FareRange,
                NoShowRate
            FROM
                RM_DW_DATA.no_show_rate
            """
            
            # Execute the query and load into a DataFrame
            noshow_df = pd.read_sql(query, self.dw_conn)
            
            # Ensure required columns exist
            required_columns = ['TimeCluster', 'CustomerSegment', 'NDOBand', 'FareRange', 'NoShowRate']
            missing_columns = [col for col in required_columns if col not in noshow_df.columns]
            
            if missing_columns:
                raise ValueError(f"Missing required columns in database table: {', '.join(missing_columns)}")
            
            self.logger.info(f"Successfully loaded {len(noshow_df)} no-show rate records from database")
            return noshow_df
        
        except Exception as e:
            self.logger.error(f"Error loading no-show rates from database: {str(e)}")
            raise

    def fetch_current_bookings_from_esb(self):
        """
        Fetch bookings from ESB database from 2:00 AM today to 4:00 AM tomorrow in IST
        """
        self.logger.info("Fetching bookings from 2:00 AM today to 4:00 AM tomorrow from ESB")
        
        # Define IST timezone
        ist = pytz.timezone('Asia/Kolkata')
        
        # Get current date and timestamps for the query in IST
        current_date = datetime.now(ist)
        start_time = current_date.replace(hour=2, minute=0, second=0, microsecond=0)
        next_day = current_date + timedelta(days=1)
        end_time = next_day.replace(hour=4, minute=0, second=0, microsecond=0)
        
        # Format the timestamps for SQL query
        start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
        end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
        
        self.logger.info(f"Querying bookings from {start_time_str} to {end_time_str}")
        
        query = f"""
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
                                    {self.nav_db_name}rez.Booking B 
                                    INNER JOIN {self.nav_db_name}rez.BookingPassenger BP ON BP.BookingID=B.BookingID 
                                    INNER JOIN {self.nav_db_name}rez.PassengerJourneySegment PJS ON PJS.PassengerID=BP.PassengerID AND PJS.BookingStatus='HK' 
                                    INNER JOIN {self.nav_db_name}rez.PassengerJourneyLeg PJL ON PJL.PassengerID=PJS.PassengerID AND PJL.SegmentID=PJS.SegmentID 
                                    INNER JOIN {self.nav_db_name}rez.InventoryLeg IL ON IL.InventoryLegId=PJL.InventoryLegID and IL.CarrierCode='QP'
                                    INNER JOIN {self.nav_db_dw_name}dw.CityPair CP ON CP.DepartureStation = IL.DepartureStation AND CP.ArrivalStation = IL.ArrivalStation 
                                    LEFT JOIN {self.nav_db_name}rez.PassengerJourneyCharge PJC ON PJC.PassengerID=PJL.PassengerID AND PJC.SegmentID=PJL.SegmentID 
                                    LEFT JOIN {self.nav_db_dw_name}dw.ChargeTypeMatrix CTM ON CTM.ChargeTypeID=PJC.ChargeType   
                                    LEFT JOIN {self.nav_db_dw_name}dw.CurrencyConversionVersion CCV ON CCV.FromCurrencyCode=PJC.CurrencyCode AND CCV.ToCurrencyCode='INR' AND CCV.ClientCode='0K' AND CCV.VersionStartUTC<=PJC.CreatedUTC AND CCV.VersionEndUTC>PJC.CreatedUTC 


                                    left join 
                                    (
                                        Select 
                                            PassengerID, 
                                            Sum(Case When ChargeType=0 Then ChargeAmount*CCV.ConversionRate Else 0 End)  - Sum(Case when Chargetype=1 Then ChargeAmount*CCV.ConversionRate Else 0 End) BaseFare_G,
                                            SUM(Case when Chargetype=1 Then ChargeAmount*CCV.ConversionRate Else 0 End) as Override_G
                                        From 
                                        {self.nav_db_name}rez.PassengerJourneyCharge PJC
                                        LEFT JOIN {self.nav_db_dw_name}dw.CurrencyConversionVersion CCV ON CCV.FromCurrencyCode=PJC.CurrencyCode
                                            AND CCV.ToCurrencyCode='INR' AND CCV.ClientCode='0K' AND CCV.VersionStartUTC<=PJC.CreatedUTC AND CCV.VersionEndUTC>PJC.CreatedUTC 
                                        Where PassengerID in (Select PassengerID from {self.nav_db_name}rez.PassengerJourneySegment where FareStatus in (3,5) )
                                        Group by PassengerID
                                    ) OV on OV.PassengerID=BP.PassengerID
                                /*The subquery up ahead is to handle the published fares. It calculates the final total amount (BaseFare+Discount) as BaseFare_G
                                and if called in the outer query (when OPassengerID is not null), this value replaces the BaseFare, which is then distributed over sectors*/

                                WHERE 
                                B.BookingID in 
                                ( 
                                    Select Distinct B.BookingID from {self.nav_db_name}rez.Booking B 
                                        inner join {self.nav_db_name}rez.BookingPassenger BP on BP.BookingID=B.BookingID
                                        inner join {self.nav_db_name}rez.PassengerJourneySegment PJS on PJS.PassengerId=BP.PassengerID and PJS.BookingStatus='HK'
                                        inner join {self.nav_db_name}rez.PassengerJourneyLeg PJL on PJS.PassengerId=PJL.PassengerID and PJS.SegmentID=PJL.SegmentID
                                        inner join {self.nav_db_name}rez.InventoryLeg IL on IL.InventoryLegID=PJL.InventoryLegID and IL.CarrierCode='QP'
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
            INNER JOIN {self.nav_db_name}rez.InventoryLeg IL 
                on IL.InventoryLegID = D.InventoryLegID 
                AND IL.Status<>2
                AND IL.STD >= '{start_time_str}' AND IL.STD <= '{end_time_str}'
                AND IL.InventoryLegID NOT IN 
                    (
                        Select distinct IL.InventoryLegID
                        From {self.nav_db_name}rez.Inventoryleg IL 
                        Join {self.nav_db_name}rez.PassengerJourneyLeg PJL on PJL.InventoryLegID=IL.InventoryLegID
                        Join {self.nav_db_name}rez.PassengerJourneySegment PJS on PJS.SegmentID=PJL.SegmentID
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
                    WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) BETWEEN 0.0001 AND 1000 THEN '1. 0-1000'
                    WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) BETWEEN 1000.0001 AND 3000 THEN '2. 1000-3000'
                    WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) BETWEEN 3000.0001 AND 4000 THEN '3. 3000-4000'
                    WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) BETWEEN 4000.0001 AND 5000 THEN '4. 4000-5000'
                    WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) BETWEEN 5000.0001 AND 8000 THEN '5. 5000-8000'
                    WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) BETWEEN 8000.0001 AND 10000 THEN '6. 8000-10000'
                    WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) BETWEEN 10000.0001 AND 15000 THEN '7. 10000-15000'
                    WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) > 15000 THEN '8. 15000+'
                    ELSE 'Unknown'
                END AS FareRange,
                
                COUNT(PX.PassengerID) AS BookedPax
            FROM
                {self.nav_db_name}[rez].InventoryLeg IL
                LEFT OUTER JOIN {self.nav_db_name}[rez].PassengerJourneyLeg PJL ON PJL.InventoryLegID = IL.InventoryLegID
                LEFT OUTER JOIN {self.nav_db_name}[rez].PassengerJourneySegment PJS ON PJS.PassengerID = PJL.PassengerID AND PJS.SegmentID = PJL.SegmentID
                LEFT OUTER JOIN {self.nav_db_name}[rez].BookingPassenger BP ON BP.PassengerID = PJS.PassengerID
                LEFT OUTER JOIN {self.nav_db_name}[rez].Booking B ON BP.BookingID = B.BookingID and IL.CarrierCode='QP'
                LEFT OUTER JOIN QP_DL_BOOKEDDATAPAX_CTE PX ON PX.InventoryLegID = IL.InventoryLegID AND PX.PassengerID = PJL.PassengerID AND PX.SegmentID = PJL.SegmentID
            WHERE
                IL.Lid > 0
                AND IL.STD >= '{start_time_str}' AND IL.STD <= '{end_time_str}'
                AND BP.BookingID IS NOT NULL
                AND PX.FareBasis NOT LIKE '%%SOL'
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
                    WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) BETWEEN 0.0001 AND 1000 THEN '1. 0-1000'
                    WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) BETWEEN 1000.0001 AND 3000 THEN '2. 1000-3000'
                    WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) BETWEEN 3000.0001 AND 4000 THEN '3. 3000-4000'
                    WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) BETWEEN 4000.0001 AND 5000 THEN '4. 4000-5000'
                    WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) BETWEEN 5000.0001 AND 8000 THEN '5. 5000-8000'
                    WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) BETWEEN 8000.0001 AND 10000 THEN '6. 8000-10000'
                    WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) BETWEEN 10000.0001 AND 15000 THEN '7. 10000-15000'
                    WHEN (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) > 15000 THEN '8. 15000+'
                    ELSE 'Unknown'
                END
        """
        
        try:
            # Just call read_sql and it directly returns a DataFrame
            bookings_df = pd.read_sql(query, self.nav_conn)
            
            self.logger.info(f"Successfully fetched {len(bookings_df)} booking records from ESB")
            return bookings_df
        
        except Exception as e:
            self.logger.error(f"Error fetching current bookings from NAV for no show table: {str(e)}")
            raise
    
    def calculate_expected_noshows(self, bookings_df, noshow_df):
        """
        Join bookings with no-show rates and calculate expected no-shows
        """
        self.logger.info("Calculating expected no-shows")
        
        try:
            # Merge the booking data with no-show rates on the four common dimensions
            merged_df = pd.merge(
                bookings_df, 
                noshow_df,
                on=['TimeCluster', 'CustomerSegment', 'NDOBand', 'FareRange'],
                how='left'
            )
            
            # Replace NULL no-show rates with 0
            merged_df['NoShowRate'] = merged_df['NoShowRate'].fillna(0.0)
            
            # Calculate expected no-shows for each row
            merged_df['ExpectedNoShows'] = (merged_df['BookedPax'] * merged_df['NoShowRate']).round(4)
            
            self.logger.info("Successfully calculated expected no-shows")
            return merged_df
        
        except Exception as e:
            self.logger.error(f"Error calculating expected no-shows: {str(e)}")
            raise

    def generate_flight_summary(self, merged_df):
        """
        Aggregate the data to create the flight summary
        """
        self.logger.info("Generating flight summary")
        
        try:
            # Group by flight details and aggregate the required metrics
            flight_summary = merged_df.groupby(['DepartureDate', 'Sector', 'FlightNumber', 'TimeCluster']).agg({
                'BookedPax': 'sum',
                'ExpectedNoShows': 'sum'
            }).reset_index()
            
            # Round the expected no-shows to whole numbers
            flight_summary['ExpectedNoShows'] = np.floor(flight_summary['ExpectedNoShows']).astype(int)
            
            # Format the departure date as MM/DD/YYYY
            flight_summary['DepartureDate'] = pd.to_datetime(flight_summary['DepartureDate']).dt.strftime('%m/%d/%Y')
            
            # Add timestamp
            flight_summary['GeneratedAt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            self.logger.info(f"Generated summary for {len(flight_summary)} flights")
            return flight_summary
        
        except Exception as e:
            self.logger.error(f"Error generating flight summary: {str(e)}")
            raise

    def save_flight_summary_to_db(self, flight_summary):
        """
        Save the flight summary to the database table using upsert
        """
        self.logger.info("Upserting flight summary to QP_DW_RMALLOC.expected_no_shows table")
        
        try:
            # Convert DataFrame to a list of dictionaries for SQL execution
            records = flight_summary.to_dict('records')
            
            # Use SQLAlchemy to execute an upsert query
            with self.rm_rd_conn.connect() as connection:
                # Begin a transaction
                transaction = connection.begin()
                try:
                    # First delete any existing records that match the primary key
                    for record in records:
                        delete_query = text("""
                        DELETE FROM QP_DW_RMALLOC.expected_no_shows 
                        WHERE DepartureDate = :departure_date 
                        AND FlightNumber = :flight_number
                        AND Sector = :sector
                        """)
                        
                        connection.execute(delete_query, {
                            "departure_date": record['DepartureDate'],
                            "flight_number": record['FlightNumber'],
                            "sector": record['Sector']
                        })
                    
                    # Then insert all new records
                    columns = ', '.join(flight_summary.columns)
                    placeholders = ', '.join([f":{col}" for col in flight_summary.columns])
                    
                    # Build the INSERT query using text() for parameterization
                    insert_query = text(f"""
                    INSERT INTO QP_DW_RMALLOC.expected_no_shows 
                    ({columns}) 
                    VALUES ({placeholders})
                    """)
                    
                    # Execute by passing each record as a parameter dictionary
                    for record in records:
                        connection.execute(insert_query, record)
                    
                    # Commit the transaction
                    transaction.commit()
                    
                    self.logger.info(f"Successfully upserted {len(flight_summary)} flight records to database")
                    
                except Exception as e:
                    # If an error occurs, rollback the transaction
                    transaction.rollback()
                    raise e
            
        except Exception as e:
            self.logger.error(f"Error upserting flight summary to database: {str(e)}")
            raise
        
    def run(self):
        """
        Main function to orchestrate the no-show prediction process
        """
        try:
            self.logger.info("Starting no-show prediction process")
            
            # Step 1: Load no-show rates from the database
            noshow_rates = self.load_noshow_rates_from_dw()
            
            # Step 2: Fetch current and next day bookings from ESB
            current_bookings = self.fetch_current_bookings_from_esb()
            
            # Step 3: Calculate expected no-shows by joining the datasets
            merged_data = self.calculate_expected_noshows(current_bookings, noshow_rates)
            
            # Step 4: Generate the flight summary
            flight_summary = self.generate_flight_summary(merged_data)
            
            # Step 5: Save the flight summary to database
            self.save_flight_summary_to_db(flight_summary)
            
            # Print a sample of the results
            self.logger.info("\nSample of flight no-show predictions:")
            self.logger.info(flight_summary.head().to_string())
            
            self.logger.info("No-show prediction process completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error in run process: {str(e)}")
            return False