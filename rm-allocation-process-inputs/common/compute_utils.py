from Constants import *
import math
import pandas as pd

def getParsedForecast(self):
  # Check if CanAdjustedForecast flag is set to 1, use adjustedPLF instead
  can_adjusted = getattr(self.currParams, 'CanAdjustedForecast', 0)
  if can_adjusted == 1 or can_adjusted == '1':
    value = getattr(self.currParams, 'adjustedPLF', CONST_NA)
  else:
    value = getattr(self.currParams, 'forecastPLF', CONST_NA)
  try:
    value = float(value)
    if not math.isnan(value):
      return value
    return CONST_NA
  except (ValueError, TypeError):
    return CONST_NA

def getParsedImputedForecast(self, forecast_plf, CB, CT, capacity):
  """
  Phase 2 BRD imputed forecast (PLF, 0-100 scale).

    Forecast_Load = (forecastPLF/100) * capacity         [refresh-time]
    CB_r          = (forecast_run_plf/100) * capacity     [refresh-time bookload]
    CT_r          = (Expected_PLF/100) * capacity         [refresh-time target]
    adj_gain      = Adjustment_Factor * (CT - CT_r)
    imp_load      = Forecast_Load + (CB - CB_r) - adj_gain
    imp_forecast  = round(imp_load)/capacity*100  - deduction(LB/ML)

  Guards (return forecast_plf as the imputed value):
    - Flown_Target_Load == CT_r  (adjustment undefined)
    - missing refresh params
    - adjusted gain NaN
  """
  try:
    capacity = float(capacity)
    if capacity <= 0:
      return CONST_NA

    # Refresh-time stored params -> seats
    raw_fc_plf = getattr(self.currParams, 'forecastPLF', CONST_NA)
    raw_cb_r   = getattr(self.currParams, 'forecast_run_plf', CONST_NA)
    raw_ct_r   = getattr(self.currParams, 'Expected_PLF', CONST_NA)
    flown_tgt_load = getattr(self.currParams, 'Flown_Target_Load', CONST_NA)

    forecast_load = (float(raw_fc_plf) / 100.0) * capacity
    cb_r = round((float(raw_cb_r) / 100.0) * capacity)
    ct_r = round((float(raw_ct_r) / 100.0) * capacity)
    flown_tgt_load = float(flown_tgt_load)
    CB = float(CB)
    CT = float(CT)
    forecast_plf = float(forecast_plf)

    # --- Compute base imputed forecast (pre-deduction) ---
    if math.isnan(forecast_load) or math.isnan(cb_r):
      base_imp = forecast_plf
    elif (not math.isnan(flown_tgt_load) and not math.isnan(ct_r)
          and flown_tgt_load == ct_r):
      # BRD: Flown Target Load == CT_r -> use Forecast itself
      base_imp = forecast_plf
    else:
      adjusted_target_gain = getAdjustedTargetGain(self, CT, ct_r)
      if adjusted_target_gain == CONST_NA or (
          isinstance(adjusted_target_gain, float) and math.isnan(adjusted_target_gain)):
        base_imp = forecast_plf
      else:
        imp_load = forecast_load + (CB - cb_r) - adjusted_target_gain
        imp_load = round(imp_load)               # round LOAD first
        base_imp = (imp_load / capacity) * 100.0

    if base_imp == CONST_NA or math.isnan(base_imp):
      return CONST_NA

    # --- Point 1: configurable LB/ML deduction (default 0, output-side, points) ---
    model = getattr(self.currParams, 'forecast_model', 'LB')
    is_ml = str(model).strip().upper() == 'ML'
    if is_ml:
        deduction = getattr(self.constantsUtils, 'IMP_FORECAST_DEDUCTION_ML', 0)
    else:
        deduction = getattr(self.constantsUtils, 'IMP_FORECAST_DEDUCTION_LB', 0)
    try:
        deduction = float(deduction)
    except (ValueError, TypeError):
        deduction = 0.0

    return base_imp - deduction

  except (ValueError, TypeError):
    return CONST_NA

def getAdjustedTargetGain(self, tlf, ctr):
  #Using adjustment factor, an adjusted target Gain is to be calculated each time imputed forecast is to be calculated. 
  # Adjusted target gain = Adjustment factor* (CT – CT_r) where CT is current target load of the flight. 
  adjustment_factor = getattr(self.currParams, 'Adjustment_Factor', CONST_NA)
  current_target_load = float(ctr)
  run_tlf = tlf
  try:
    adjustment_factor = float(adjustment_factor)
    current_target_load = float(current_target_load)
    run_tlf = float(run_tlf)
    if not math.isnan(adjustment_factor) and not math.isnan(current_target_load) and not math.isnan(run_tlf):
      return adjustment_factor * (run_tlf - current_target_load)
    return CONST_NA
  except (ValueError, TypeError):
    return CONST_NA

def calculateAfNbAllin(self, ndocounter, curr_imputed_plf, curr_plf, CT=CONST_NA, capacity=CONST_NA):
  Af_allin = getattr(self.currParams, 'ALLIN_AF', CONST_NA)
  ForecastAF_Allin = getattr(self.currParams, 'ForecastAF_Allin', CONST_NA)

  if ndocounter == 0:
    forecast_plf = getParsedForecast(self)
    plf = getattr(self.currParams, 'forecast_run_plf', CONST_NA)
    CB = plf  # at NDO 0, current bookload basis — see note
    imputed_plf = getParsedImputedForecast(self, forecast_plf, CB, CT, capacity)
  else:
    imputed_plf = curr_imputed_plf
    plf = curr_plf

  try:
    imputed_plf = float(imputed_plf)
    plf = float(plf)
    if not math.isnan(Af_allin) and not math.isnan(ForecastAF_Allin):
      return ((ForecastAF_Allin * imputed_plf)-(Af_allin * plf))/(imputed_plf-plf)
    return CONST_NA
  except (ValueError, TypeError, ZeroDivisionError):
    return CONST_NA

def calculate_profilefare_quartile(final_totalCapacity, currBookedLoad, mathutils):
  # Profile Fare Quartile calculation
  actualPercentile_full = final_totalCapacity / PROFILE_FARES_QUARTILE if PROFILE_FARES_QUARTILE != 0 else final_totalCapacity # Avoid DivByZero
  bookloadPercentile_full = mathutils.getCeilNumber(float(currBookedLoad) / actualPercentile_full) if actualPercentile_full != 0 else PROFILE_FARES_QUARTILE # Avoid DivByZero
  
  if bookloadPercentile_full > PROFILE_FARES_QUARTILE: bookloadPercentile_full = PROFILE_FARES_QUARTILE
  if bookloadPercentile_full < PROFILE_FARES_25PERCENT_QUARTILE: bookloadPercentile_full = PROFILE_FARES_25PERCENT_QUARTILE
  return bookloadPercentile_full

def calculateVariance(currBookedLoad, totalCapacity, ndocounter, tgtAtCurrentHour, startOfDayBookLoad,startOfDayTgtBookedLoad):
  variance = ((currBookedLoad / totalCapacity) - (tgtAtCurrentHour / totalCapacity)) * 100
  if ndocounter >= 22:
    variance = ((currBookedLoad - (startOfDayTgtBookedLoad)) / totalCapacity) * 100
  return variance

def calculate_variance_for_load(simulated_booked_load, total_capacity, ndocounter, tgt_at_current_hour, sod_tgt_booked_load):
  """
  Standalone variance calculation that can be called without self.
  Used in iterative group allocation where booked load changes each iteration.
  """
  if total_capacity <= 0:
    return 0
  variance = ((simulated_booked_load / total_capacity) - (tgt_at_current_hour / total_capacity)) * 100
  if ndocounter >= 22:
    variance = ((simulated_booked_load - sod_tgt_booked_load) / total_capacity) * 100
  return variance

def calculate_group_adjusted_load(simulated_booked_load, total_capacity, adjusted_capacity, ndocounter, 
                                   tgt_at_current_hour, sod_tgt_booked_load,
                                   imp_forecast, use_forecast, constantsutils):
  """
  Calculate adjusted PLF for group allocation.
  Converts simulated booked load to PLF (percentage), then adds adjustment.
  Returns adjusted PLF as a percentage.
  """
  simulated_plf = (simulated_booked_load / adjusted_capacity) * 100 if adjusted_capacity > 0 else 0

  if use_forecast:
    adjustment = (imp_forecast - constantsutils.GROUP_IMP_FORECAST_REDUCTION) * constantsutils.GROUP_SCALING_FACTOR
  else:
    current_variance = calculate_variance_for_load(
      simulated_booked_load, total_capacity, ndocounter, tgt_at_current_hour, sod_tgt_booked_load
    )
    adjustment = current_variance * constantsutils.GROUP_SCALING_FACTOR
  return simulated_plf + adjustment

def calculate_group_fare_anchor(adjusted_profile_fare, discounted_b2c_fare, fare_anchor_strategy):
  """
  Determine the fare anchor for group allocation based on fare_anchor_strategy.
  Strategy 'min': MIN(adjusted profile fare, discounted B2C fare)
  Strategy 'max': MAX(adjusted profile fare, discounted B2C fare)
  Returns None if both fares are <= 0.
  """
  if adjusted_profile_fare <= 0 and discounted_b2c_fare <= 0:
    return None
  if adjusted_profile_fare <= 0:
    return discounted_b2c_fare
  if discounted_b2c_fare <= 0:
    return adjusted_profile_fare
  if fare_anchor_strategy == 'max':
    return max(adjusted_profile_fare, discounted_b2c_fare)
  return min(adjusted_profile_fare, discounted_b2c_fare)

def safe_get_from_dict(d, key, default=CONST_NA, index=0):
  val_list = d.get(key)
  if isinstance(val_list, list) and len(val_list) > index:
    return val_list[index]
  return default

def parse_numeric(val, default=0):
  try:
    if val == CONST_NA or val == "" or val is None:
      return default
    return float(val)
  except (ValueError, TypeError):
    return default

def parse_int_flag(val, default=0):
  try:
    if val == CONST_NA or val == "" or val is None:
      return default
    val_float = float(val)
    if math.isnan(val_float):
      return default
    return int(val_float)
  except (ValueError, TypeError):
    return default

def safe_float(val, default=0.0):
  """Safely cast a payload value to float, handling NAs and empty strings."""
  if not pd.notna(val):
    return default
  try:
    val_str = str(val).strip()
    if val_str in ('', 'NA', 'None'):
      return default
    return float(val)
  except (ValueError, TypeError):
    return default
