package com.akasaair.queues.common.Validations;

import java.util.*;
import java.util.regex.Pattern;

public class CompetitionResponseValidator {

    private static final List<String> EXPECTED_PLF_BANDS_HEADERS = Arrays.asList("band_name", "start", "end");
    private static final List<String> EXPECTED_GRID_HEADERS = Arrays.asList("grid_name", "ndo_band", "plf_band", "time_range", "criteria", "response_rule");

    private static final Pattern FORMULA_CHAR_PATTERN = Pattern.compile("^[a-zA-Z0-9\\s\\+\\-\\*/%\\(\\),.]+$");
    private static final Pattern SECTOR_PATTERN = Pattern.compile("^[A-Za-z]{6}$");

    public static class ExtractedPlfBandRow {
        public int lineNum;
        public int id;
        public int plfBand;
        public String bandName;
        public double start;
        public double end;
        public double thresholdFactor = 0.0;
    }

    public static class ExtractedGridRow {
        public int lineNum;
        public String gridName;
        public int ndoBand;
        public int plfBand;
        public String timeRange;
        public String criteria;
        public String responseRule;
        public Double tolerance = null;
    }

    public static class ExtractedFareRecalibrationRow {
        public int lineNum;
        public String sector;
        public Map<String, Double> airlineOffsets = new LinkedHashMap<>();
    }

    // ==========================================
    // Helper Methods
    // ==========================================

    public static int getParenthesesBalance(String s) {
        if (s == null) return 0;
        int balance = 0;
        for (char c : s.toCharArray()) {
            if (c == '(') balance++;
            else if (c == ')') balance--;
        }
        return balance;
    }

    public static String cleanHeader(String h) {
        return h != null ? h.replace("\uFEFF", "").trim().toLowerCase() : "";
    }

    public static int findHeaderIndex(List<String> headers, String... aliases) {
        for (int i = 0; i < headers.size(); i++) {
            String clean = headers.get(i);
            for (String alias : aliases) {
                if (clean.equalsIgnoreCase(alias)) {
                    return i;
                }
            }
        }
        return -1;
    }

    public static String canonicalCriteria(String raw) {
        if (raw == null) return null;
        String trimmed = raw.trim().toLowerCase();
        if (trimmed.equals("min") || trimmed.equals("minimum")) return "Min";
        if (trimmed.equals("med") || trimmed.equals("median") || trimmed.equals("middle")) return "Med";
        if (trimmed.equals("max") || trimmed.equals("maximum")) return "Max";
        return null;
    }

    public static String cleanSector(String raw) {
        if (raw == null) return "";
        return raw.replaceAll("[^a-zA-Z]", "").toUpperCase();
    }

    // ==========================================
    // 1. PLF Bands Validation & Extraction
    // ==========================================

    public static boolean validatePlfBandsFile(List<List<String>> csvData, List<String> errors) {
        if (csvData == null || csvData.isEmpty()) {
            errors.add("Competition Response PLF Bands CSV file is empty.");
            return false;
        }

        if (csvData.size() < 2) {
            errors.add("Competition Response PLF Bands CSV file has headers but lacks data rows.");
            return false;
        }

        List<String> rawHeaders = csvData.get(0);
        List<String> headers = new ArrayList<>();
        for (String h : rawHeaders) {
            headers.add(cleanHeader(h));
        }

        int bandNameIdx = findHeaderIndex(headers, "band_name", "bandname", "band", "name");
        int startIdx = findHeaderIndex(headers, "start", "start_plf", "startplf");
        int endIdx = findHeaderIndex(headers, "end", "end_plf", "endplf");
        int idIdx = findHeaderIndex(headers, "id", "plf_band", "plfband");
        int tfIdx = findHeaderIndex(headers, "threshold_factor", "thresholdfactor", "threshold_ratio", "threshold");

        if (bandNameIdx == -1 || startIdx == -1 || endIdx == -1) {
            List<String> missing = new ArrayList<>();
            if (bandNameIdx == -1) missing.add("'band_name'");
            if (startIdx == -1) missing.add("'start'");
            if (endIdx == -1) missing.add("'end'");
            errors.add("Missing required columns for Competition Response PLF Bands file. Expected: band_name, start, end. Missing: "
                    + String.join(", ", missing) + ". Found: " + rawHeaders);
            return false;
        }

        Set<String> seenBandNames = new HashSet<>();
        Set<Integer> seenIds = new HashSet<>();
        List<ExtractedPlfBandRow> validRows = new ArrayList<>();
        boolean hasRowLevelError = false;

        for (int i = 1; i < csvData.size(); i++) {
            List<String> row = csvData.get(i);
            int lineNum = i + 1;
            String linePrefix = "Line " + lineNum + ": ";

            if (row == null || row.isEmpty() || (row.size() == 1 && row.get(0).trim().isEmpty())) {
                continue; // Skip blank lines
            }

            int maxReqIdx = Math.max(bandNameIdx, Math.max(startIdx, endIdx));
            if (row.size() <= maxReqIdx) {
                errors.add(linePrefix + "Insufficient columns in row. Found " + row.size() + ", expected at least " + (maxReqIdx + 1) + ".");
                hasRowLevelError = true;
                continue;
            }

            String bandNameStr = row.get(bandNameIdx).trim();
            String startStr = row.get(startIdx).trim();
            String endStr = row.get(endIdx).trim();

            if (bandNameStr.isEmpty()) {
                errors.add(linePrefix + "Band name cannot be empty.");
                hasRowLevelError = true;
            } else {
                String lowerName = bandNameStr.toLowerCase();
                if (seenBandNames.contains(lowerName)) {
                    errors.add(linePrefix + "Duplicate band_name '" + bandNameStr + "' found.");
                    hasRowLevelError = true;
                } else {
                    seenBandNames.add(lowerName);
                }
            }

            double startVal = 0;
            boolean startValid = false;
            try {
                startVal = Double.parseDouble(startStr);
                startValid = true;
            } catch (NumberFormatException e) {
                errors.add(linePrefix + "Start value must be a valid number, found '" + startStr + "'.");
                hasRowLevelError = true;
            }

            double endVal = 0;
            boolean endValid = false;
            try {
                endVal = Double.parseDouble(endStr);
                endValid = true;
            } catch (NumberFormatException e) {
                errors.add(linePrefix + "End value must be a valid number, found '" + endStr + "'.");
                hasRowLevelError = true;
            }

            if (startValid && endValid) {
                if (startVal >= endVal) {
                    errors.add(linePrefix + "Start value (" + startVal + ") must be strictly less than end value (" + endVal + ").");
                    hasRowLevelError = true;
                }
            }

            Integer explicitId = null;
            if (idIdx != -1 && idIdx < row.size()) {
                String idStr = row.get(idIdx).trim();
                if (!idStr.isEmpty()) {
                    try {
                        int parsedId = Integer.parseInt(idStr);
                        if (parsedId < 0) {
                            errors.add(linePrefix + "ID must be a non-negative integer, found '" + idStr + "'.");
                            hasRowLevelError = true;
                        } else if (seenIds.contains(parsedId)) {
                            errors.add(linePrefix + "Duplicate ID '" + parsedId + "' found.");
                            hasRowLevelError = true;
                        } else {
                            seenIds.add(parsedId);
                            explicitId = parsedId;
                        }
                    } catch (NumberFormatException e) {
                        errors.add(linePrefix + "ID must be a valid integer, found '" + idStr + "'.");
                        hasRowLevelError = true;
                    }
                }
            }

            double tfVal = 0.0;
            if (tfIdx != -1 && tfIdx < row.size()) {
                String tfStr = row.get(tfIdx).trim();
                if (!tfStr.isEmpty()) {
                    try {
                        tfVal = Double.parseDouble(tfStr);
                        if (tfVal < 0) {
                            errors.add(linePrefix + "Threshold factor must be non-negative, found '" + tfStr + "'.");
                            hasRowLevelError = true;
                        }
                    } catch (NumberFormatException e) {
                        errors.add(linePrefix + "Threshold factor must be a valid number, found '" + tfStr + "'.");
                        hasRowLevelError = true;
                    }
                }
            }

            if (startValid && endValid && startVal < endVal && !bandNameStr.isEmpty()) {
                ExtractedPlfBandRow bandRow = new ExtractedPlfBandRow();
                bandRow.lineNum = lineNum;
                bandRow.bandName = bandNameStr;
                bandRow.start = startVal;
                bandRow.end = endVal;
                bandRow.thresholdFactor = tfVal;
                if (explicitId != null) {
                    bandRow.id = explicitId;
                    bandRow.plfBand = explicitId;
                }
                validRows.add(bandRow);
            }
        }

        // Interval Overlap Validation across all rows with valid start and end
        if (validRows.size() > 1) {
            List<ExtractedPlfBandRow> sortedBands = new ArrayList<>(validRows);
            sortedBands.sort(Comparator.comparingDouble((ExtractedPlfBandRow r) -> r.start)
                    .thenComparingDouble(r -> r.end));

            for (int k = 0; k < sortedBands.size() - 1; k++) {
                ExtractedPlfBandRow curr = sortedBands.get(k);
                ExtractedPlfBandRow next = sortedBands.get(k + 1);

                if (curr.end > next.start) {
                    errors.add("Overlapping PLF bands detected: Band '" + curr.bandName + "' [" + curr.start + ", " + curr.end
                            + ") overlaps with Band '" + next.bandName + "' [" + next.start + ", " + next.end
                            + ") on Lines " + curr.lineNum + " and " + next.lineNum + ".");
                    hasRowLevelError = true;
                }
            }
        }

        return !hasRowLevelError && errors.isEmpty();
    }

    public static List<ExtractedPlfBandRow> extractPlfBands(List<List<String>> csvData) {
        List<ExtractedPlfBandRow> result = new ArrayList<>();
        if (csvData == null || csvData.size() < 2) return result;

        List<String> rawHeaders = csvData.get(0);
        List<String> headers = new ArrayList<>();
        for (String h : rawHeaders) {
            headers.add(cleanHeader(h));
        }

        int bandNameIdx = findHeaderIndex(headers, "band_name", "bandname", "band", "name");
        int startIdx = findHeaderIndex(headers, "start", "start_plf", "startplf");
        int endIdx = findHeaderIndex(headers, "end", "end_plf", "endplf");
        int idIdx = findHeaderIndex(headers, "id", "plf_band", "plfband");
        int tfIdx = findHeaderIndex(headers, "threshold_factor", "thresholdfactor", "threshold_ratio", "threshold");

        boolean hasExplicitIds = true;
        for (int i = 1; i < csvData.size(); i++) {
            List<String> row = csvData.get(i);
            if (row == null || row.isEmpty() || (row.size() == 1 && row.get(0).trim().isEmpty())) continue;
            int maxReqIdx = Math.max(bandNameIdx, Math.max(startIdx, endIdx));
            if (row.size() <= maxReqIdx) continue;

            ExtractedPlfBandRow r = new ExtractedPlfBandRow();
            r.lineNum = i + 1;
            r.bandName = row.get(bandNameIdx).trim();
            r.start = Double.parseDouble(row.get(startIdx).trim());
            r.end = Double.parseDouble(row.get(endIdx).trim());

            if (idIdx != -1 && idIdx < row.size() && !row.get(idIdx).trim().isEmpty()) {
                r.id = Integer.parseInt(row.get(idIdx).trim());
                r.plfBand = r.id;
            } else {
                hasExplicitIds = false;
            }

            if (tfIdx != -1 && tfIdx < row.size() && !row.get(tfIdx).trim().isEmpty()) {
                try {
                    r.thresholdFactor = Double.parseDouble(row.get(tfIdx).trim());
                } catch (NumberFormatException e) {
                    r.thresholdFactor = 0.0;
                }
            }

            result.add(r);
        }

        // Always sort by start ascending
        result.sort(Comparator.comparingDouble(r -> r.start));

        // If any IDs were missing or omitted, auto-assign sequential 0, 1, 2... based on sorted start
        if (!hasExplicitIds || idIdx == -1) {
            for (int idx = 0; idx < result.size(); idx++) {
                result.get(idx).id = idx;
                result.get(idx).plfBand = idx;
            }
        }

        return result;
    }

    // ==========================================
    // 2. Grid Validation & Extraction
    // ==========================================

    public static ExtractedGridRow extractGridRow(List<String> row, int gridIdx, int ndoIdx, int plfIdx, int trIdx, int critIdx, int ruleIdx) {
        return extractGridRow(row, gridIdx, ndoIdx, plfIdx, trIdx, critIdx, ruleIdx, -1);
    }

    public static ExtractedGridRow extractGridRow(List<String> row, int gridIdx, int ndoIdx, int plfIdx, int trIdx, int critIdx, int ruleIdx, int tolIdx) {
        ExtractedGridRow res = new ExtractedGridRow();
        res.gridName = gridIdx >= 0 && gridIdx < row.size() ? row.get(gridIdx).trim() : "";
        res.timeRange = trIdx >= 0 && trIdx < row.size() ? row.get(trIdx).trim() : "";
        res.criteria = critIdx >= 0 && critIdx < row.size() ? row.get(critIdx).trim() : "";

        String rawNdo = ndoIdx >= 0 && ndoIdx < row.size() ? row.get(ndoIdx).trim() : "";
        try {
            res.ndoBand = Integer.parseInt(rawNdo);
        } catch (Exception e) {
            res.ndoBand = -1;
        }

        String rawPlf = plfIdx >= 0 && plfIdx < row.size() ? row.get(plfIdx).trim() : "";
        try {
            res.plfBand = Integer.parseInt(rawPlf);
        } catch (Exception e) {
            res.plfBand = -1;
        }

        String rawRule = ruleIdx >= 0 && ruleIdx < row.size() ? row.get(ruleIdx).trim() : "";
        int balance = getParenthesesBalance(rawRule);

        if (balance > 0 && row.size() > ruleIdx + 1) {
            // Unquoted formula containing commas was split across multiple columns by CSV reader
            StringBuilder ruleBuilder = new StringBuilder(rawRule);
            int nextIdx = ruleIdx + 1;
            while (nextIdx < row.size() && balance > 0) {
                String part = row.get(nextIdx).trim();
                ruleBuilder.append(", ").append(part);
                balance += getParenthesesBalance(part);
                nextIdx++;
            }
            res.responseRule = ruleBuilder.toString();
        } else {
            res.responseRule = rawRule;
        }

        if (tolIdx >= 0 && tolIdx < row.size()) {
            String tolStr = row.get(tolIdx).trim();
            if (!tolStr.isEmpty()) {
                try {
                    res.tolerance = Double.parseDouble(tolStr);
                } catch (NumberFormatException ignored) {}
            }
        }

        return res;
    }

    public static boolean validateGridFile(List<List<String>> csvData, List<String> errors) {
        if (csvData == null || csvData.isEmpty()) {
            errors.add("Competition Response Grid CSV file is empty.");
            return false;
        }

        if (csvData.size() < 2) {
            errors.add("Competition Response Grid CSV file has headers but lacks data rows.");
            return false;
        }

        List<String> rawHeaders = csvData.get(0);
        List<String> headers = new ArrayList<>();
        for (String h : rawHeaders) {
            headers.add(cleanHeader(h));
        }

        int gridIdx = findHeaderIndex(headers, "grid_name", "gridname", "grid");
        int ndoIdx = findHeaderIndex(headers, "ndo_band", "ndoband", "ndo");
        int plfIdx = findHeaderIndex(headers, "plf_band", "plfband", "plf");
        int trIdx = findHeaderIndex(headers, "time_range", "timerange", "time");
        int critIdx = findHeaderIndex(headers, "criteria", "criterion");
        int ruleIdx = findHeaderIndex(headers, "response_rule", "responserule", "rule", "strategy", "response");

        if (gridIdx == -1 || ndoIdx == -1 || plfIdx == -1 || trIdx == -1 || critIdx == -1 || ruleIdx == -1) {
            List<String> missing = new ArrayList<>();
            if (gridIdx == -1) missing.add("'grid_name'");
            if (ndoIdx == -1) missing.add("'ndo_band'");
            if (plfIdx == -1) missing.add("'plf_band'");
            if (trIdx == -1) missing.add("'time_range'");
            if (critIdx == -1) missing.add("'criteria'");
            if (ruleIdx == -1) missing.add("'response_rule'");

            errors.add("Missing required columns for Competition Response Grid file. Expected: grid_name, ndo_band, plf_band, time_range, criteria, response_rule. Missing: "
                    + String.join(", ", missing) + ". Found: " + rawHeaders);
            return false;
        }

        Set<String> seenKeys = new HashSet<>();
        boolean isValid = true;

        for (int i = 1; i < csvData.size(); i++) {
            List<String> row = csvData.get(i);
            int lineNum = i + 1;
            String linePrefix = "Line " + lineNum + ": ";

            if (row == null || row.isEmpty() || (row.size() == 1 && row.get(0).trim().isEmpty())) {
                continue;
            }

            int maxReqIdx = Math.max(Math.max(gridIdx, ndoIdx), Math.max(Math.max(plfIdx, trIdx), Math.max(critIdx, ruleIdx)));
            if (row.size() <= maxReqIdx) {
                errors.add(linePrefix + "Insufficient columns in row. Found " + row.size() + ", expected at least " + (maxReqIdx + 1) + ".");
                isValid = false;
                continue;
            }

            ExtractedGridRow extracted = extractGridRow(row, gridIdx, ndoIdx, plfIdx, trIdx, critIdx, ruleIdx);
            extracted.lineNum = lineNum;

            // 1. Validate grid_name
            if (extracted.gridName.isEmpty()) {
                errors.add(linePrefix + "Grid name cannot be empty.");
                isValid = false;
            }

            // 2. Validate ndo_band
            String rawNdo = row.get(ndoIdx).trim();
            int ndoVal = -1;
            try {
                ndoVal = Integer.parseInt(rawNdo);
                if (ndoVal < 0) {
                    errors.add(linePrefix + "NDO band must be a non-negative integer, found '" + rawNdo + "'.");
                    isValid = false;
                }
            } catch (NumberFormatException e) {
                errors.add(linePrefix + "NDO band must be a valid integer, found '" + rawNdo + "'.");
                isValid = false;
            }

            // 3. Validate plf_band
            String rawPlf = row.get(plfIdx).trim();
            int plfVal = -1;
            try {
                plfVal = Integer.parseInt(rawPlf);
                if (plfVal < 0) {
                    errors.add(linePrefix + "PLF band must be a non-negative integer, found '" + rawPlf + "'.");
                    isValid = false;
                }
            } catch (NumberFormatException e) {
                errors.add(linePrefix + "PLF band must be a valid integer, found '" + rawPlf + "'.");
                isValid = false;
            }

            // 4. Validate time_range
            if (extracted.timeRange.isEmpty()) {
                errors.add(linePrefix + "Time range cannot be empty.");
                isValid = false;
            }

            // 5. Validate criteria
            String canonCrit = canonicalCriteria(extracted.criteria);
            if (canonCrit == null) {
                errors.add(linePrefix + "Invalid criteria '" + extracted.criteria + "'. Allowed values: Min, Med, Max.");
                isValid = false;
            }

            // 6. Validate response_rule
            String rule = extracted.responseRule;
            if (rule == null || rule.trim().isEmpty()) {
                errors.add(linePrefix + "Response rule cannot be empty.");
                isValid = false;
            } else {
                rule = rule.trim();
                if (!FORMULA_CHAR_PATTERN.matcher(rule).matches()) {
                    errors.add(linePrefix + "Response rule contains invalid characters: '" + rule + "'.");
                    isValid = false;
                } else if (getParenthesesBalance(rule) != 0) {
                    errors.add(linePrefix + "Response rule has unbalanced parentheses: '" + rule + "'.");
                    isValid = false;
                } else {
                    char firstChar = rule.charAt(0);
                    char lastChar = rule.charAt(rule.length() - 1);
                    if (firstChar == '*' || firstChar == '/' || firstChar == '+' || firstChar == ',') {
                        errors.add(linePrefix + "Response rule cannot start with operator '" + firstChar + "': '" + rule + "'.");
                        isValid = false;
                    }
                    if (lastChar == '*' || lastChar == '/' || lastChar == '+' || lastChar == '-' || lastChar == ',') {
                        errors.add(linePrefix + "Response rule cannot end with operator '" + lastChar + "': '" + rule + "'.");
                        isValid = false;
                    }
                }
            }

            // 7. Composite Uniqueness
            if (!extracted.gridName.isEmpty() && ndoVal >= 0 && plfVal >= 0 && !extracted.timeRange.isEmpty() && canonCrit != null) {
                String compositeKey = extracted.gridName.toUpperCase() + "_" + ndoVal + "_" + plfVal + "_"
                        + extracted.timeRange.toUpperCase() + "_" + canonCrit;
                if (seenKeys.contains(compositeKey)) {
                    errors.add(linePrefix + "Duplicate entry for Grid '" + extracted.gridName + "', NDO Band " + ndoVal
                            + ", PLF Band " + plfVal + ", Time Range '" + extracted.timeRange + "', Criteria '" + canonCrit + "'.");
                    isValid = false;
                } else {
                    seenKeys.add(compositeKey);
                }
            }
        }

        return isValid && errors.isEmpty();
    }

    public static List<ExtractedGridRow> extractGridRows(List<List<String>> csvData) {
        List<ExtractedGridRow> result = new ArrayList<>();
        if (csvData == null || csvData.size() < 2) return result;

        List<String> rawHeaders = csvData.get(0);
        List<String> headers = new ArrayList<>();
        for (String h : rawHeaders) {
            headers.add(cleanHeader(h));
        }

        int gridIdx = findHeaderIndex(headers, "grid_name", "gridname", "grid");
        int ndoIdx = findHeaderIndex(headers, "ndo_band", "ndoband", "ndo");
        int plfIdx = findHeaderIndex(headers, "plf_band", "plfband", "plf");
        int trIdx = findHeaderIndex(headers, "time_range", "timerange", "time");
        int critIdx = findHeaderIndex(headers, "criteria", "criterion");
        int ruleIdx = findHeaderIndex(headers, "response_rule", "responserule", "rule", "strategy", "response");
        int tolIdx = findHeaderIndex(headers, "tolerance", "tolernace", "threshold_factor");

        for (int i = 1; i < csvData.size(); i++) {
            List<String> row = csvData.get(i);
            if (row == null || row.isEmpty() || (row.size() == 1 && row.get(0).trim().isEmpty())) continue;
            int maxReqIdx = Math.max(Math.max(gridIdx, ndoIdx), Math.max(Math.max(plfIdx, trIdx), Math.max(critIdx, ruleIdx)));
            if (row.size() <= maxReqIdx) continue;

            ExtractedGridRow r = extractGridRow(row, gridIdx, ndoIdx, plfIdx, trIdx, critIdx, ruleIdx, tolIdx);
            r.lineNum = i + 1;
            r.criteria = canonicalCriteria(r.criteria);
            result.add(r);
        }

        return result;
    }

    // ==========================================
    // 3. Competition Fare Recalibration Validation & Extraction
    // ==========================================

    public static boolean validateFareRecalibrationFile(List<List<String>> csvData, List<String> errors) {
        if (csvData == null || csvData.isEmpty()) {
            errors.add("Competition Fare Recalibration CSV file is empty.");
            return false;
        }

        if (csvData.size() < 2) {
            errors.add("Competition Fare Recalibration CSV file has headers but lacks data rows.");
            return false;
        }

        List<String> rawHeaders = csvData.get(0);
        List<String> headers = new ArrayList<>();
        for (String h : rawHeaders) {
            headers.add(cleanHeader(h));
        }

        int sectorIdx = findHeaderIndex(headers, "sector", "sectorname", "sector_name", "sector_code");
        if (sectorIdx == -1) {
            errors.add("Missing required 'Sector' column for Competition Fare Recalibration file. Found headers: " + rawHeaders);
            return false;
        }

        List<Integer> airlineIndices = new ArrayList<>();
        List<String> airlineNames = new ArrayList<>();
        for (int c = 0; c < rawHeaders.size(); c++) {
            if (c != sectorIdx) {
                String col = rawHeaders.get(c).trim();
                if (!col.isEmpty()) {
                    airlineIndices.add(c);
                    airlineNames.add(col);
                }
            }
        }

        if (airlineIndices.isEmpty()) {
            errors.add("No competitor airline columns found in Competition Fare Recalibration file. Expected at least one airline code column (e.g. IX, 6E, SG, AI).");
            return false;
        }

        Set<String> seenSectors = new HashSet<>();
        boolean isValid = true;

        for (int i = 1; i < csvData.size(); i++) {
            List<String> row = csvData.get(i);
            int lineNum = i + 1;
            String linePrefix = "Line " + lineNum + ": ";

            if (row == null || row.isEmpty() || (row.size() == 1 && row.get(0).trim().isEmpty())) {
                continue;
            }

            if (row.size() <= sectorIdx) {
                errors.add(linePrefix + "Missing Sector value in row.");
                isValid = false;
                continue;
            }

            String rawSector = row.get(sectorIdx).trim();
            String cleanSec = cleanSector(rawSector);

            if (cleanSec.isEmpty()) {
                errors.add(linePrefix + "Sector cannot be empty.");
                isValid = false;
            } else if (cleanSec.length() != 6 || !SECTOR_PATTERN.matcher(cleanSec).matches()) {
                errors.add(linePrefix + "Invalid Sector format '" + rawSector + "'. Expected 6-letter sector code (e.g., BOMDEL).");
                isValid = false;
            } else {
                String origin = cleanSec.substring(0, 3);
                String dest = cleanSec.substring(3, 6);
                if (origin.equals(dest)) {
                    errors.add(linePrefix + "Origin and Destination in Sector '" + cleanSec + "' cannot be the same.");
                    isValid = false;
                } else if (seenSectors.contains(cleanSec)) {
                    errors.add(linePrefix + "Duplicate Sector '" + cleanSec + "' found.");
                    isValid = false;
                } else {
                    seenSectors.add(cleanSec);
                }
            }

            // Validate airline numeric offsets
            for (int k = 0; k < airlineIndices.size(); k++) {
                int colIdx = airlineIndices.get(k);
                String airlineName = airlineNames.get(k);
                if (colIdx < row.size()) {
                    String val = row.get(colIdx).trim();
                    if (!val.isEmpty()) {
                        try {
                            Double.parseDouble(val);
                        } catch (NumberFormatException e) {
                            errors.add(linePrefix + "Invalid numeric offset '" + val + "' for airline '" + airlineName + "' in Sector '" + rawSector + "'.");
                            isValid = false;
                        }
                    }
                }
            }
        }

        return isValid && errors.isEmpty();
    }

    public static List<ExtractedFareRecalibrationRow> extractFareRecalibrationRows(List<List<String>> csvData) {
        List<ExtractedFareRecalibrationRow> result = new ArrayList<>();
        if (csvData == null || csvData.size() < 2) return result;

        List<String> rawHeaders = csvData.get(0);
        List<String> headers = new ArrayList<>();
        for (String h : rawHeaders) {
            headers.add(cleanHeader(h));
        }

        int sectorIdx = findHeaderIndex(headers, "sector", "sectorname", "sector_name", "sector_code");
        if (sectorIdx == -1) return result;

        List<Integer> airlineIndices = new ArrayList<>();
        List<String> airlineNames = new ArrayList<>();
        for (int c = 0; c < rawHeaders.size(); c++) {
            if (c != sectorIdx) {
                String col = rawHeaders.get(c).trim();
                if (!col.isEmpty()) {
                    airlineIndices.add(c);
                    airlineNames.add(col);
                }
            }
        }

        for (int i = 1; i < csvData.size(); i++) {
            List<String> row = csvData.get(i);
            if (row == null || row.isEmpty() || (row.size() == 1 && row.get(0).trim().isEmpty())) continue;
            if (row.size() <= sectorIdx) continue;

            String cleanSec = cleanSector(row.get(sectorIdx).trim());
            if (cleanSec.length() != 6) continue;

            ExtractedFareRecalibrationRow r = new ExtractedFareRecalibrationRow();
            r.lineNum = i + 1;
            r.sector = cleanSec;

            for (int k = 0; k < airlineIndices.size(); k++) {
                int colIdx = airlineIndices.get(k);
                String airlineName = airlineNames.get(k);
                double offsetVal = 0.0;
                if (colIdx < row.size()) {
                    String val = row.get(colIdx).trim();
                    if (!val.isEmpty()) {
                        try {
                            offsetVal = Double.parseDouble(val);
                        } catch (NumberFormatException ignored) {}
                    }
                }
                r.airlineOffsets.put(airlineName, offsetVal);
            }
            result.add(r);
        }

        return result;
    }
}
