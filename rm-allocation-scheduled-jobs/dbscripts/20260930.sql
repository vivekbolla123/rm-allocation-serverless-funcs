-- =====================================================================
-- One-time migration: normalize historical audit_reports rows
--   1) Rename ML colName (Java bean names) -> real market_list names
--   2) Normalize boolean-flag values  Y/N -> 1/0  (WB vs ML encoding)
-- Table: RM_DW_DATA.audit_reports
-- Generated from Constants.ML_AUDIT_COLNAME_TO_MARKET_LIST_FIELD and
-- Constants.ML_FLAG_COLUMNS. Run inside a transaction.
-- =====================================================================

-- Safety: preview counts BEFORE running the updates (optional).
-- SELECT colName, COUNT(*) FROM RM_DW_DATA.audit_reports
--   WHERE colName IN ('OBFare', 'OBSeats', 'b2bOutbound_Stop', 'carrExclusionB2B', 'carrExclusionB2C', 'curveId', 'startTime', 'endTime', 'firstAllocation', 'otherAllocation', 'skippingFactor', 'b2bBackstop', 'b2cBackstop', 'b2bFactor', 'b2bTolerance', 'b2cTolerance', 'tbfEnhFlag', 'marketfare_Upsell', 'spoilage_Flag', 'canAdjustedForecast', 'profileFareUpsell', 'daySpan', 'autoTimeRangeFlag', 'overBooking', 'cugFlag', 'mktFareFlag')
--   GROUP BY colName;

START TRANSACTION;

-- ---------------------------------------------------------------------
-- STEP 1: colName rename (ML bean name -> market_list name)
-- Only rows whose colName is an OLD ML bean name are touched. Rows that
-- already carry the market_list name (WB rows, post-fix ML rows) and
-- any unrelated colName are left unchanged by the ELSE branch.
-- ---------------------------------------------------------------------
UPDATE RM_DW_DATA.audit_reports
SET colName = CASE colName
    WHEN 'OBFare' THEN 'obFare'
    WHEN 'OBSeats' THEN 'obSeats'
    WHEN 'b2bOutbound_Stop' THEN 'Outbound_Stop'
    WHEN 'carrExclusionB2B' THEN 'CarrExlusionB2B'
    WHEN 'carrExclusionB2C' THEN 'CarrExlusionB2C'
    WHEN 'curveId' THEN 'CurveID'
    WHEN 'startTime' THEN 'TimeWindowStart'
    WHEN 'endTime' THEN 'TimeWindowEnd'
    WHEN 'firstAllocation' THEN 'FirstRBDAlloc'
    WHEN 'otherAllocation' THEN 'OtherRBDAlloc'
    WHEN 'skippingFactor' THEN 'SkippingFactor'
    WHEN 'b2bBackstop' THEN 'B2BBackstop'
    WHEN 'b2cBackstop' THEN 'B2CBackstop'
    WHEN 'b2bFactor' THEN 'B2BFactor'
    WHEN 'b2bTolerance' THEN 'B2BTolerance'
    WHEN 'b2cTolerance' THEN 'B2CTolerance'
    WHEN 'tbfEnhFlag' THEN 'TBFEnhFlag'
    WHEN 'marketfare_Upsell' THEN 'Marketfare_Upsell'
    WHEN 'spoilage_Flag' THEN 'Spoilage_Flag'
    WHEN 'canAdjustedForecast' THEN 'CanAdjustedForecast'
    WHEN 'profileFareUpsell' THEN 'ProfileFareUpsell'
    WHEN 'daySpan' THEN 'DaySpan'
    WHEN 'autoTimeRangeFlag' THEN 'AutoTimeRangeFlag'
    WHEN 'overBooking' THEN 'OverBooking'
    WHEN 'cugFlag' THEN 'CugFlag'
    WHEN 'mktFareFlag' THEN 'MktFareFlag'
    ELSE colName
END
WHERE colName IN (
    'OBFare', 'OBSeats', 'b2bOutbound_Stop', 'carrExclusionB2B', 'carrExclusionB2C', 'curveId', 'startTime', 'endTime', 'firstAllocation', 'otherAllocation', 'skippingFactor', 'b2bBackstop', 'b2cBackstop', 'b2bFactor', 'b2bTolerance', 'b2cTolerance', 'tbfEnhFlag', 'marketfare_Upsell', 'spoilage_Flag', 'canAdjustedForecast', 'profileFareUpsell', 'daySpan', 'autoTimeRangeFlag', 'overBooking', 'cugFlag', 'mktFareFlag'
);

-- ---------------------------------------------------------------------
-- STEP 2: boolean-flag value normalization  Y/N -> 1/0
-- Runs AFTER Step 1 so colName is already the market_list name, which
-- is how the flag set is keyed (matches native WB header too).
-- Only the known flag columns with a literal Y/N value are affected.
-- ---------------------------------------------------------------------
UPDATE RM_DW_DATA.audit_reports
SET value = CASE UPPER(value) WHEN 'Y' THEN '1' WHEN 'N' THEN '0' ELSE value END
WHERE colName IN (
    'AutoTimeRangeFlag', 'CanAdjustedForecast', 'CugFlag', 'MktFareFlag', 'OverBooking', 'Spoilage_Flag', 'TBFEnhFlag', 'afForecastAllocFlag', 'autoBackstopFlag', 'autoGroup', 'auto_tolerance_flag', 'defaultMlFlag', 'directFlag', 'distressInventoryFlag', 'forecastAllocFlag', 'incrementedProfileFareFlag', 'market_station_mapping', 'openingFares', 'profileFares', 'rbdPushFlag', 'seriesBlock', 'tbfFlag'
)
AND UPPER(value) IN ('Y', 'N');

-- Review, then COMMIT;  (or ROLLBACK; to abort)
COMMIT;
