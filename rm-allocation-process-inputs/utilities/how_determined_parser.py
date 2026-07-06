import re

class HowDeterminedParser:
    @staticmethod
    def parse_and_format(raw_str: str) -> str:
        """
        Parses the raw HowDetermined string and formats it into the 5 requested categories:
        1. Which logic is used (F/V/AF/AllocationY/etc)
        2. How potential fare is used (Computation using last sold fare, strategy fare, profile fare)
        3. End conditions (Floor, Cap, Tolerance)
        4. QP fare (extreme cases)
        5. Forecast / Variance / AF Forecast
        """
        if not raw_str:
            return "N/A | N/A | N/A | N/A | N/A"
            
        # If it's a simple legacy constant, just return it
        if raw_str.startswith("HD_") or "Sales Inventory Only" in raw_str:
            return raw_str
            
        logic = []
        fare = []
        cond = []
        qp = []
        forecast = []
        
        # 1. Logic
        if "AF_Forecast" in raw_str:
            logic.append("AF")
        elif "_F_" in raw_str or raw_str.startswith("F_"):
            logic.append("F")
        elif "_V_" in raw_str or raw_str.startswith("V_"):
            logic.append("V")
        
        if not logic:
            if "AllocationY" in raw_str:
                logic.append("AllocationY")
            elif "Strategy" in raw_str:
                logic.append("Strategy")
            elif "Connection" in raw_str or "Matched and allocated" in raw_str:
                logic.append("Connection Allocation")
            else:
                logic.append("Standard")
                
        # 2. Fare Computation
        if "AvgFare" in raw_str or "Average" in raw_str or "mean" in raw_str.lower() or "median" in raw_str.lower():
            if "LastSoldFare" in raw_str and "StrategyFare" in raw_str:
                fare.append("Average of last sold and Strategy fare")
            elif "StrategyFare" in raw_str and "ProfileFare" in raw_str:
                fare.append("Average of Strategy and Profile fare")
            else:
                fare.append("Average of fares")
        elif "Maxof_Startegy_Profile" in raw_str or "Startegy_Profile_Max" in raw_str:
            fare.append("Computation using max of strategy fare and profile fare")
        elif "interpolated_lastSold_Profile" in raw_str or "Interpollated_Max_own_fare" in raw_str:
            fare.append("Interpolated fare")
        elif "last_sold_fare" in raw_str or "LastSoldFare" in raw_str:
            fare.append("Computation using last sold fare")
        elif "Startegy_fare" in raw_str or "StrategyFare" in raw_str or "Strategy" in raw_str:
            fare.append("Computation using strategy fare")
        elif "ProfileFare" in raw_str or "profileFare" in raw_str:
            fare.append("Computation using profile fare")
        
        if "UpsellY" in raw_str or "Upsold" in raw_str:
            if fare:
                fare[0] = fare[0].replace("Computation using", "Computation using").replace("fare", "fare(if upsell)")
            else:
                fare.append("Computation using last sold fare(if upsell)")
                
        if "Matched and allocated" in raw_str or "Connection" in raw_str:
            parts = raw_str.split("-")
            if len(parts) > 1:
                fare = [parts[1].strip()]
                
        # 3. End Conditions (Floor, Cap, Tolerance)
        if "ToleranceHit" in raw_str or "TolerenceHit" in raw_str:
            cond.append("Tolerance applied")
        if "harAnchorfare" in raw_str or "hardanchorfare" in raw_str:
            m = re.search(r'har(?:d)?(?:A|a)nchorfare_([0-9.]+)', raw_str)
            if m:
                cond.append(f"Hard anchor hit at PLF threshold Value {m.group(1)}")
            else:
                cond.append("Hard anchor hit at PLF threshold Value")
        if "Backstop" in raw_str:
            m = re.search(r'Backstop Hit \((.*?)\)', raw_str)
            if m:
                cond.append(f"Backstop hit - {m.group(1)}")
            else:
                cond.append("Intended opening fare is below backstop")
        if "LowerBound" in raw_str:
            cond.append("Lower bound hit")
            
        # 4. QP Fare
        if "extreme value" in raw_str.lower() or "extreme published Fares" in raw_str:
            if "B2C" in raw_str:
                qp.append("B2C fare anchor at extreme levels due to QP fares")
            elif "B2B" in raw_str:
                qp.append("Unable to open B2B fare at extreme levels due to QP fares")
            else:
                qp.append("Fare anchor at extreme levels due to QP fares")
        if "Y/V4" in raw_str or "extreme value allocating at Y/V4" in raw_str:
            qp.append("B2C fare anchor is higher/lower than extreme value allocating at Y/V4")
            
        # 5. Forecast / Variance
        match_f_v = re.search(r'^[FV]_(\d+)_(\d+)', raw_str)
        if match_f_v:
            forecast.append(f"Imp Band {match_f_v.group(1)} DPLF Band {match_f_v.group(2)}")
            
        match_band = re.search(r'Band(\d+)', raw_str)
        if match_band:
            forecast.append(f"AF Band {match_band.group(1)}")
            
        if "PLFGainNeg" in raw_str:
            forecast.append("PLF Gain Negative")
        elif "PLFGainPos" in raw_str:
            forecast.append("PLF Gain Positive")
        
        # Build the final string
        logic_str = "/".join(logic) if logic else "N/A"
        fare_str = ", ".join(fare) if fare else "N/A"
        cond_str = ", ".join(cond) if cond else "N/A"
        qp_str = ", ".join(qp) if qp else "N/A"
        forecast_str = ", ".join(forecast) if forecast else "N/A"
        
        parts = [logic_str, fare_str, cond_str, qp_str, forecast_str]
        valid_parts = [p for p in parts if p != "N/A"]
        
        return " | ".join(valid_parts) if valid_parts else "N/A"
