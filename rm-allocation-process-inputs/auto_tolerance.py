import json
import pandas as pd


def apply_auto_tolerance(redis_client, auto_config, origin, destin, flight_number, per_start, b2c_tolerance_param, strategy_fare, pre_tolerance_anchor, dplf_band, ndo_counter):
    if not auto_config:
        return None
        
    redis_key = f"auto_tolerance_{origin}_{destin}_{flight_number}_{per_start}"
    state_str = redis_client.get(redis_key)
    
    # 1. Parse current Redis state
    state = {}
    if state_str:
        try:
            state = json.loads(state_str)
        except Exception:
            pass
            
    saved_b2c_tolerance = state.get("b2c_tolerance")
    t_prime = state.get("T_prime")
    
    # 2. If Analyst changed B2C Tolerance, clear T' and bypass calculation for this run
    analyst_changed = False
    if saved_b2c_tolerance is not None and saved_b2c_tolerance != b2c_tolerance_param:
        t_prime = None
        analyst_changed = True
        
    # T is B2CTolerance which is Tolerance numeric value itself
    T = float(b2c_tolerance_param) if pd.notna(b2c_tolerance_param) else 0.0
    
    # 3. Use T' if available, otherwise calculate it if x > 0
    tolerance_value = float(strategy_fare) - T
    x = tolerance_value - float(pre_tolerance_anchor)
    
    if t_prime is not None:
        final_tolerance_limit = t_prime
    else:
        if x > 0 and not analyst_changed:
            # Hit! Calculate new T'
            
            # Find G from Grid Values
            # We need Delta Variance band which requires variance. 
            # Variance = x. We need to match x to delta_bands
            
            # Find N from NDO config
            # N value based on ndo_counter matching ndo_min, ndo_max
            ndo_df = auto_config.get("ndo_config")
            n_value = 0
            if ndo_df is not None:
                match = ndo_df[(ndo_df['ndo_min'] <= ndo_counter) & (ndo_df['ndo_max'] >= ndo_counter)]
                if not match.empty:
                    n_value = match.iloc[0]['n_value']
            
            delta_df = auto_config.get("delta_bands")
            delta_band_id = None
            if delta_df is not None:
                match = delta_df[(delta_df['min_value'] <= x) & (delta_df['max_value'] >= x)]
                if not match.empty:
                    delta_band_id = match.iloc[0]['id']
            
            grid_df = auto_config.get("grid_values")
            g_value = 0
            if grid_df is not None and delta_band_id is not None:
                match = grid_df[(grid_df['dplf_band_name'] == str(dplf_band)) & (grid_df['delta_band_id'] == delta_band_id)]
                if not match.empty:
                    g_value = match.iloc[0]['g_value']
                    
            t_prime = x * g_value + T
            if t_prime > 100:
                t_prime = 100.0
            final_tolerance_limit = t_prime
        else:
            final_tolerance_limit = T
            
    # 4. Save state
    new_state = {
        "b2c_tolerance": b2c_tolerance_param,
        "T_prime": t_prime,
        "T_V": final_tolerance_limit,
        "variance": x
    }
    redis_client.set(redis_key, json.dumps(new_state))
    
    return final_tolerance_limit

