-- =====================================================================
-- One-time migration: normalize historical audit_reports rows
--   1) Rename ML colName (Java bean names) -> real market_list names
--   2) Normalize boolean-flag values  Y/N -> 1/0  (WB vs ML encoding)
-- Table: RM_DW_DATA.audit_reports
-- Generated from Constants.ML_AUDIT_COLNAME_TO_MARKET_LIST_FIELD and
-- Constants.ML_AUDIT_BOOLEAN_FLAG_COLUMNS. Run inside a transaction.
-- =====================================================================

-- Safety: preview counts BEFORE running the updates (optional).
-- SELECT colName, COUNT(*) FROM RM_DW_DATA.audit_reports
--   WHERE colName IN ('OBFare', 'OBSeats', 'b2bOutbound_Stop', 'carrExclusionB2B', 'carrExclusionB2C', 'curveId', 'startTime', 'endTime', 'firstAllocation', 'otherAllocation', 'skippingFactor', 'b2bBackstop', 'b2cBackstop', 'b2bFactor', 'b2bTolerance', 'b2cTolerance', 'tbfEnhFlag', 'marketfare_Upsell', 'spoilage_Flag', 'canAdjustedForecast', 'profileFareUpsell', 'daySpan', 'autoTimeRangeFlag', 'overBooking', 'cugFlag', 'mktFareFlag')
--   GROUP BY colName;

START TRANSACTION;

-- ---------------------------------------------------------------------
-- STEP 0: remove collisions before renaming.
-- The unique key uk_audit_record is
--   (sector, flightNo, departureDate, source, changedAtDate, colName, analystName).
-- Because the code fix was already live for part of the window, some audit
-- events have BOTH an old-name row (e.g. colName='firstAllocation') and the
-- correct new-name row (colName='FirstRBDAlloc') for the same key tuple.
-- Renaming the old row would duplicate the existing one and violate the key.
--
-- These old-name rows are redundant captures of the same change, so delete the
-- OLD-name row whenever a NEW-name row already exists for the same key. A
-- self-join maps each old colName to its target via the same (old,new) pairs
-- used in STEP 1. The correct new-name row is always kept.
-- ---------------------------------------------------------------------
DELETE old_ar
FROM RM_DW_DATA.audit_reports AS old_ar
JOIN (
    SELECT 'OBFare' AS old_name, 'obFare' AS new_name
    UNION ALL SELECT 'OBSeats', 'obSeats'
    UNION ALL SELECT 'b2bOutbound_Stop', 'Outbound_Stop'
    UNION ALL SELECT 'carrExclusionB2B', 'CarrExlusionB2B'
    UNION ALL SELECT 'carrExclusionB2C', 'CarrExlusionB2C'
    UNION ALL SELECT 'curveId', 'CurveID'
    UNION ALL SELECT 'startTime', 'TimeWindowStart'
    UNION ALL SELECT 'endTime', 'TimeWindowEnd'
    UNION ALL SELECT 'firstAllocation', 'FirstRBDAlloc'
    UNION ALL SELECT 'otherAllocation', 'OtherRBDAlloc'
    UNION ALL SELECT 'skippingFactor', 'SkippingFactor'
    UNION ALL SELECT 'b2bBackstop', 'B2BBackstop'
    UNION ALL SELECT 'b2cBackstop', 'B2CBackstop'
    UNION ALL SELECT 'b2bFactor', 'B2BFactor'
    UNION ALL SELECT 'b2bTolerance', 'B2BTolerance'
    UNION ALL SELECT 'b2cTolerance', 'B2CTolerance'
    UNION ALL SELECT 'tbfEnhFlag', 'TBFEnhFlag'
    UNION ALL SELECT 'marketfare_Upsell', 'Marketfare_Upsell'
    UNION ALL SELECT 'spoilage_Flag', 'Spoilage_Flag'
    UNION ALL SELECT 'canAdjustedForecast', 'CanAdjustedForecast'
    UNION ALL SELECT 'profileFareUpsell', 'ProfileFareUpsell'
    UNION ALL SELECT 'daySpan', 'DaySpan'
    UNION ALL SELECT 'autoTimeRangeFlag', 'AutoTimeRangeFlag'
    UNION ALL SELECT 'overBooking', 'OverBooking'
    UNION ALL SELECT 'cugFlag', 'CugFlag'
    UNION ALL SELECT 'mktFareFlag', 'MktFareFlag'
    UNION ALL SELECT 'competitionResponseGrid_NG', 'CompetitionResponseGrid_NG'
    UNION ALL SELECT 'promoFlag_NG', 'PromoFlag_NG'
) AS m ON old_ar.colName = m.old_name
JOIN RM_DW_DATA.audit_reports AS new_ar
    ON  new_ar.colName       = m.new_name
    AND new_ar.sector        = old_ar.sector
    AND new_ar.flightNo      = old_ar.flightNo
    AND new_ar.departureDate = old_ar.departureDate
    AND new_ar.source        = old_ar.source
    AND new_ar.changedAtDate = old_ar.changedAtDate
    AND new_ar.analystName   = old_ar.analystName;

-- ---------------------------------------------------------------------
-- STEP 1: colName rename (ML bean name -> market_list name)
-- Only rows whose colName is an OLD ML bean name are touched. Rows that
-- already carry the market_list name (WB rows, post-fix ML rows) and
-- any unrelated colName are left unchanged by the ELSE branch.
-- After STEP 0, no surviving old-name row collides with an existing new-name
-- row, so this rename cannot violate uk_audit_record.
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
    WHEN 'competitionResponseGrid_NG' THEN 'CompetitionResponseGrid_NG'
    WHEN 'promoFlag_NG' THEN 'PromoFlag_NG'
    ELSE colName
END
WHERE colName IN (
    'OBFare', 'OBSeats', 'b2bOutbound_Stop', 'carrExclusionB2B', 'carrExclusionB2C', 'curveId', 'startTime', 'endTime', 'firstAllocation', 'otherAllocation', 'skippingFactor', 'b2bBackstop', 'b2cBackstop', 'b2bFactor', 'b2bTolerance', 'b2cTolerance', 'tbfEnhFlag', 'marketfare_Upsell', 'spoilage_Flag', 'canAdjustedForecast', 'profileFareUpsell', 'daySpan', 'autoTimeRangeFlag', 'overBooking', 'cugFlag', 'mktFareFlag', 'competitionResponseGrid_NG', 'promoFlag_NG'
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
