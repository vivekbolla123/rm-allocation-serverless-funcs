from Constants import *
import math
import json
import logging
import pandas as pd
from datetime import datetime


def parse_spoilage_trigger_bands(json_str):
  """Parse the D-0 spoilage-reduction trigger bands.

  Input format: [{"startTime": "00:01", "endTime": "11:00", "triggerHours": 16}, ...]
  Output: [(time(0, 1), time(11, 0), 16.0), (time(11, 1), time(23, 59), 10.0)]

  Returns an empty list when the input is missing/unparseable; callers then
  fall back to the configured default trigger hours.
  """
  try:
    raw_data = json.loads(json_str) if isinstance(json_str, str) else json_str
    parsed = []
    for band in raw_data:
      start = datetime.strptime(str(band['startTime']).strip(), TIME_FORMAT).time()
      end = datetime.strptime(str(band['endTime']).strip(), TIME_FORMAT).time()
      parsed.append((start, end, float(band['triggerHours'])))
    return parsed
  except Exception as e:
    logging.getLogger().error(f"Error parsing spoilage reduction trigger bands: {e}")
    return []


def get_spoilage_trigger_hours(bands, departure_time, default_hours):
  """Hours-before-departure at which spoilage reduction activates for a given
  departure time.

  Args:
    bands: Parsed bands as returned by ``parse_spoilage_trigger_bands`` — a list
      of ``(start_time, end_time, trigger_hours)`` tuples.
    departure_time: A ``datetime.time``, a ``datetime``, or an ``HH:MM`` string.
    default_hours: Returned when no band matches (e.g. a 00:00 departure against
      bands that start at 00:01, or an empty band list).

  Bands are inclusive on both ends and may wrap past midnight
  (startTime > endTime).
  """
  if isinstance(departure_time, str):
    departure_time = datetime.strptime(departure_time.strip(), TIME_FORMAT).time()
  elif isinstance(departure_time, datetime):
    departure_time = departure_time.time()

  for start, end, trigger_hours in bands:
    if start <= end:
      in_band = start <= departure_time <= end
    else:
      # Band wraps past midnight, e.g. 23:01 -> 02:00
      in_band = departure_time >= start or departure_time <= end
    if in_band:
      return trigger_hours

  return float(default_hours)


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

    Forecast_Load = (forecast_plf/100) * capacity         [model-resolved, LB-switched]
    CB_r          = (forecast_run_plf/100) * capacity     [refresh-time bookload]
    CT_r          = (Expected_PLF/100) * capacity         [refresh-time target]
    Flown_Target_Load = curve_lf_at_ndo0 * capacity       [recomputed from curve]
    Adjustment_Factor = (Forecast_Load - CB_r) / (Flown_Target_Load - CT_r)
    adj_gain      = Adjustment_Factor * (CT - CT_r)
    imp_load      = Forecast_Load + (CB - CB_r) - adj_gain
    imp_forecast  = round(imp_load)/capacity*100  - deduction(LB/ML)

  Both Flown_Target_Load and Adjustment_Factor are recomputed here using the
  model-resolved forecast_plf (which may be ML or LB-floored) rather than relying
  on pre-computed values from forecast_results that used the raw LB forecastPLF.

  Guards (return forecast_plf as the imputed value):
    - Flown_Target_Load == CT_r  (adjustment undefined / denominator zero)
    - missing refresh params
    - adjusted gain NaN
  """
  try:
    capacity = float(capacity)
    if capacity <= 0:
      return CONST_NA

    # Refresh-time stored params -> seats
    raw_cb_r   = getattr(self.currParams, 'forecast_run_plf', CONST_NA)
    raw_ct_r   = getattr(self.currParams, 'Expected_PLF', CONST_NA)

    # Use the passed-in forecast_plf (already model-resolved and LB-switched)
    forecast_plf = float(forecast_plf)
    forecast_load = round((forecast_plf / 100.0) * capacity)
    cb_r = round((float(raw_cb_r) / 100.0) * capacity)
    ct_r = round((float(raw_ct_r) / 100.0) * capacity)
    CB = round(float(CB))
    CT = round(float(CT))
    flown_tgt_load = _compute_flown_target_load(self, capacity)
    if not math.isnan(flown_tgt_load):
      flown_tgt_load = round(flown_tgt_load)
    else:
      return CONST_NA
    adjustment_factor = _compute_adjustment_factor(forecast_load, cb_r, flown_tgt_load, ct_r)

    # --- Compute base imputed forecast (pre-deduction) ---
    if math.isnan(forecast_load) or math.isnan(cb_r):
      base_imp = forecast_plf
    elif (not math.isnan(flown_tgt_load) and not math.isnan(ct_r)
          and flown_tgt_load == ct_r):
      # BRD: Flown Target Load == CT_r -> use Forecast itself
      base_imp = forecast_plf
    else:
      if math.isnan(adjustment_factor):
        base_imp = forecast_plf
      else:
        adjusted_target_gain = adjustment_factor * (CT - ct_r)
        if math.isnan(adjusted_target_gain):
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

def _compute_flown_target_load(self, capacity):
  """Compute Flown_Target_Load = curve_lf_at_ndo0 * capacity.
  
  Looks up the curve's LoadFactor at NDO 0 using the flight's CurveID.
  Falls back to the pre-stored Flown_Target_Load if curve lookup fails.
  """
  try:
    curve_id = getattr(self.currParams, 'curve_id', None)
    if curve_id is None or str(curve_id).strip() in ('', 'nan', 'None', 'NA'):
      # Fallback to pre-stored value if curve_id unavailable
      stored = getattr(self.currParams, 'Flown_Target_Load', CONST_NA)
      return float(stored) if stored != CONST_NA else float('nan')

    # Lookup curve LF at NDO 0
    tgt_list = self.rmdatabasehelper.getTargetBookedLoadFromBenchmarkCurve(0, curve_id)
    if tgt_list is not None and not tgt_list.empty and 'LF' in tgt_list.columns:
      lf_ndo0 = float(tgt_list['LF'].iloc[0])
      if not math.isnan(lf_ndo0):
        return lf_ndo0 * capacity

    # Fallback to pre-stored value
    stored = getattr(self.currParams, 'Flown_Target_Load', CONST_NA)
    return float(stored) if stored != CONST_NA else float('nan')
  except (ValueError, TypeError, AttributeError):
    # If rmdatabasehelper not available, fall back to stored value
    try:
      stored = getattr(self.currParams, 'Flown_Target_Load', CONST_NA)
      return float(stored) if stored != CONST_NA else float('nan')
    except (ValueError, TypeError):
      return float('nan')


def _compute_adjustment_factor(forecast_load, cb_r, flown_tgt_load, ct_r):
  """Compute Adjustment_Factor = (Forecast_Load - CB_r) / (Flown_Target_Load - CT_r).
  
  Uses the model-resolved Forecast_Load (not the raw refresh-time value).
  Returns NaN if denominator is zero or inputs are NaN.
  """
  try:
    if (math.isnan(forecast_load) or math.isnan(cb_r) or
        math.isnan(flown_tgt_load) or math.isnan(ct_r)):
      return float('nan')

    denom = flown_tgt_load - ct_r
    if denom == 0:
      return float('nan')

    return (forecast_load - cb_r) / denom
  except (ValueError, TypeError):
    return float('nan')


def compute_imputed_forecast_connections(forecast_plf, capacity, cb_r_plf, ct_r_plf,
                                  CB, CT, flown_tgt_load, deduction=0.0):
  """
  Shared BRD imputed forecast calculation usable by both direct and connections.

  All inputs are raw values — no self/currParams dependency.

  Args:
    forecast_plf:   Model-resolved forecast PLF (0-100 scale)
    capacity:       Flight capacity (seats)
    cb_r_plf:       Refresh-time bookload as PLF (forecast_run_plf, 0-100)
    ct_r_plf:       Refresh-time target as PLF (Expected_PLF, 0-100)
    CB:             Current bookload (seats)
    CT:             Current target load (seats, intraday-adjusted)
    flown_tgt_load: NDO-0 target load (seats) from curve lookup
    deduction:      PLF-points deduction to apply (default 0)

  Returns:
    Imputed forecast PLF (float) or CONST_NA if computation fails.
  """
  try:
    forecast_plf = float(forecast_plf)
    capacity = float(capacity)
    if capacity <= 0 or math.isnan(forecast_plf):
      return CONST_NA

    forecast_load = round((forecast_plf / 100.0) * capacity)
    cb_r = round((float(cb_r_plf) / 100.0) * capacity)
    ct_r = round((float(ct_r_plf) / 100.0) * capacity)
    CB = round(float(CB))
    CT = round(float(CT))
    flown_tgt_load = float(flown_tgt_load)
    if not math.isnan(flown_tgt_load):
      flown_tgt_load = round(flown_tgt_load)
    else:
      return CONST_NA

    # Compute adjustment factor
    adjustment_factor = _compute_adjustment_factor(forecast_load, cb_r, flown_tgt_load, ct_r)

    # Compute base imputed forecast
    if math.isnan(forecast_load) or math.isnan(cb_r):
      base_imp = forecast_plf
    elif flown_tgt_load == ct_r:
      base_imp = forecast_plf
    elif math.isnan(adjustment_factor):
      base_imp = forecast_plf
    else:
      adjusted_target_gain = adjustment_factor * (CT - ct_r)
      if math.isnan(adjusted_target_gain):
        base_imp = forecast_plf
      else:
        imp_load = forecast_load + (CB - cb_r) - adjusted_target_gain
        imp_load = round(imp_load)
        base_imp = (imp_load / capacity) * 100.0

    if base_imp == CONST_NA or math.isnan(base_imp):
      return CONST_NA

    return base_imp - deduction

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
  if totalCapacity <= 0:
    return 0.0
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
    eff_capacity = adjusted_capacity if (adjusted_capacity is not None and adjusted_capacity > 0) else total_capacity
    current_variance = calculate_variance_for_load(
      simulated_booked_load, eff_capacity, ndocounter, tgt_at_current_hour, sod_tgt_booked_load
    )
    adjustment = current_variance * constantsutils.GROUP_SCALING_FACTOR
  return simulated_plf + adjustment

def calculate_group_fare_anchor(adjusted_profile_fare, discounted_b2c_fare, fare_anchor_strategy, b2c_fare_weight=0, profile_fare_weight=1):
  """
  Determine the fare anchor for group allocation based on fare_anchor_strategy
  and weighted fare calculation.

  The weighted fare is: discounted_b2c_fare * b2c_fare_weight + adjusted_profile_fare * profile_fare_weight
  The final fare anchor is: fare_anchor_strategy(discounted_b2c_fare, weighted_fare)

  Returns None if both fares are <= 0.
  """
  if adjusted_profile_fare <= 0 and discounted_b2c_fare <= 0:
    return None
  if adjusted_profile_fare <= 0:
    return discounted_b2c_fare
  if discounted_b2c_fare <= 0:
    return adjusted_profile_fare

  weighted_fare = discounted_b2c_fare * b2c_fare_weight + adjusted_profile_fare * profile_fare_weight

  if fare_anchor_strategy == 'max':
    return max(discounted_b2c_fare, weighted_fare)
  return min(discounted_b2c_fare, weighted_fare)

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


def safe_round_fare(val, default=CONST_NA):
  """Safely round a fare value to the nearest integer, preserving NA/None/alphanumeric strings."""
  if val is None or pd.isna(val):
    return default
  s = str(val).strip()
  if s.upper() in ('', 'NA', 'NONE', 'NAN', 'NULL', 'CONST_NA'):
    return default
  if any(c.isalpha() for c in s):
    return val
  try:
    f = float(s)
    return int(round(f))
  except (ValueError, TypeError):
    return val


SUMMARY_FARE_COLUMNS = [
  'Backstop', 'hardAnchorFare', 'strategyFare', 'lastSellingFare', 'profileFare',
  'lastSevenAvgFare', 'anchorFareBeforeTolerance', 'toleranceValue', 'anchorFare',
  'SellingFare', 'ForecastAF_Allin', 'AF_Allin', 'AF_NB_Allin', 'ALLIN_AF_NB_LM',
  'ALLIN_AF_NB_LY', 'grpAdjProfileFare', 'Portfolio_Anchor', 'grpFareAnchor',
  'Portfolio88_Anchor', 'upsell', 'NG_AF_Forecast', 'NG_Opening_fare',
  'NG_Strategy_fare', 'NG_Anchor_fare', 'MktFare_Min', 'MktFare_Max', 'autoBackstop'
]

NEXTGEN_SUMMARY_FARE_COLUMNS = [
  'MarketFare_Selected', 'MarketFare_Min', 'MarketFare_Max',
  'NG_AF_Forecast', 'NG_Opening_fare', 'NG_Strategy_fare', 'NG_Anchor_fare'
]

