from Constants import *
import math

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

def getParsedImputedForecast(self,variance):
  # Check if CanAdjustedForecast flag is set to 1, use adjustedPLF instead
  can_adjusted = getattr(self.currParams, 'CanAdjustedForecast', 0)
  if can_adjusted == 1 or can_adjusted == '1':
    forecastPLF = getattr(self.currParams, 'adjustedPLF', CONST_NA)
  else:
    forecastPLF = getattr(self.currParams, 'forecastPLF', CONST_NA)
  forecast_variance = getattr(self.currParams, 'forecast_variance', CONST_NA)
  IMP_FORECAST_DEDUCTION = self.constantsUtils.IMP_FORECAST_DEDUCTION

  try:
    forecastPLF = float(forecastPLF)
    forecast_variance = float(forecast_variance)
    IMP_FORECAST_DEDUCTION = float(IMP_FORECAST_DEDUCTION)
    if not math.isnan(forecastPLF) and not math.isnan(forecast_variance):
      return forecastPLF+variance-forecast_variance-IMP_FORECAST_DEDUCTION
    return CONST_NA
  except (ValueError, TypeError):
    return CONST_NA

def calculateAfNbAllin(self, ndocounter, curr_imputed_plf, curr_plf):
  Af_allin = getattr(self.currParams, 'ALLIN_AF', CONST_NA)
  ForecastAF_Allin = getattr(self.currParams, 'ForecastAF_Allin', CONST_NA)
  
  if ndocounter == 0:
    forecast_variance = getattr(self.currParams, 'forecast_variance', CONST_NA)
    try:
      forecast_variance = float(forecast_variance)
    except (ValueError, TypeError):
      forecast_variance = CONST_NA

    imputed_plf = getParsedImputedForecast(self, forecast_variance)
    plf = getattr(self.currParams, 'forecast_run_plf', CONST_NA)
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