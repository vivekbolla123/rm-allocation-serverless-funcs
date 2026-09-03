package com.akasaair.queues.common.Validations;

import java.util.*;
import java.util.regex.Pattern;

public class ForecastAllocationValidator {

    private static final Set<String> ALLOWED_TRENDS = new HashSet<>(Arrays.asList(
            "increasing", "stable", "decreasing"
    ));

    private static final Pattern FORMULA_CHAR_PATTERN = Pattern.compile("^[a-zA-Z0-9\\s\\+\\-\\*/%\\(\\),.]+$");

    public static class ExtractedGridRow {
        public String bandStr;
        public String trendStr;
        public String formulaStr;
        public String descriptionStr;
    }

    /**
     * Extracts and safely re-stitches formula parts if an unquoted CSV contained commas inside a formula (e.g. Min(LSF, Strategy)).
     */
    public static ExtractedGridRow extractGridRow(List<String> row, int bandIdx, int trendIdx, int formulaIdx, int descIdx) {
        ExtractedGridRow res = new ExtractedGridRow();
        res.bandStr = bandIdx >= 0 && bandIdx < row.size() ? row.get(bandIdx).trim() : "";
        res.trendStr = trendIdx >= 0 && trendIdx < row.size() ? row.get(trendIdx).trim() : "";

        String rawFormula = formulaIdx >= 0 && formulaIdx < row.size() ? row.get(formulaIdx).trim() : "";
        int balance = getParenthesesBalance(rawFormula);

        if (balance > 0 && row.size() > formulaIdx + 1) {
            // Unquoted formula containing commas was split across multiple columns by CSV parser
            StringBuilder formulaBuilder = new StringBuilder(rawFormula);
            int nextIdx = formulaIdx + 1;
            while (nextIdx < row.size() && balance > 0) {
                String part = row.get(nextIdx).trim();
                formulaBuilder.append(", ").append(part);
                balance += getParenthesesBalance(part);
                nextIdx++;
            }
            res.formulaStr = formulaBuilder.toString();

            // Any remaining columns after the formula are the description
            if (nextIdx < row.size()) {
                StringBuilder descBuilder = new StringBuilder();
                for (int d = nextIdx; d < row.size(); d++) {
                    if (descBuilder.length() > 0) {
                        descBuilder.append(", ");
                    }
                    descBuilder.append(row.get(d).trim());
                }
                res.descriptionStr = descBuilder.toString();
            } else {
                res.descriptionStr = "";
            }
        } else {
            res.formulaStr = rawFormula;
            res.descriptionStr = descIdx != -1 && descIdx < row.size() ? row.get(descIdx).trim() : "";
        }

        return res;
    }

    public static int getParenthesesBalance(String s) {
        if (s == null) return 0;
        int balance = 0;
        for (char c : s.toCharArray()) {
            if (c == '(') balance++;
            else if (c == ')') balance--;
        }
        return balance;
    }

    /**
     * Validates the config_forecast_allocation_grid CSV data.
     * Expected columns: band, trend, formula, description
     */
    public static boolean validateGridFile(List<List<String>> csvData, List<String> errors) {
        if (csvData == null || csvData.isEmpty()) {
            errors.add("Forecast Allocation Grid CSV file is empty.");
            return false;
        }

        if (csvData.size() < 2) {
            errors.add("Forecast Allocation Grid CSV file has headers but lacks data rows.");
            return false;
        }

        List<String> rawHeaders = csvData.get(0);
        List<String> headers = new ArrayList<>();
        for (String h : rawHeaders) {
            headers.add(h != null ? h.replace("\uFEFF", "").trim().toLowerCase() : "");
        }

        int bandIdx = headers.indexOf("band");
        int trendIdx = headers.indexOf("trend");
        int formulaIdx = headers.indexOf("formula");
        int descIdx = headers.indexOf("description");

        if (bandIdx == -1 || trendIdx == -1 || formulaIdx == -1) {
            errors.add("Missing required columns for Grid file. Expected: band, trend, formula, description. Found: " + rawHeaders);
            return false;
        }

        Set<String> seenCombinations = new HashSet<>();
        boolean isValid = true;

        for (int i = 1; i < csvData.size(); i++) {
            List<String> row = csvData.get(i);
            int lineNum = i + 1;
            String linePrefix = "Line " + lineNum + ": ";

            if (row.size() <= Math.max(bandIdx, Math.max(trendIdx, formulaIdx))) {
                errors.add(linePrefix + "Insufficient columns in row. Found " + row.size());
                isValid = false;
                continue;
            }

            ExtractedGridRow extracted = extractGridRow(row, bandIdx, trendIdx, formulaIdx, descIdx);
            String bandStr = extracted.bandStr;
            String trendStr = extracted.trendStr;
            String formulaStr = extracted.formulaStr;

            // 1. Validate band
            int band;
            try {
                band = Integer.parseInt(bandStr);
                if (band < 1 || band > 4) {
                    errors.add(linePrefix + "Invalid band '" + bandStr + "'. Must be an integer between 1 and 4.");
                    isValid = false;
                }
            } catch (NumberFormatException e) {
                errors.add(linePrefix + "Band must be a valid integer, found '" + bandStr + "'.");
                isValid = false;
                band = -1;
            }

            // 2. Validate trend
            if (trendStr.isEmpty() || !ALLOWED_TRENDS.contains(trendStr.toLowerCase())) {
                errors.add(linePrefix + "Invalid trend '" + trendStr + "'. Allowed values: Increasing, Stable, Decreasing.");
                isValid = false;
            }

            // 3. Validate duplicate (band, trend)
            if (band >= 1 && band <= 4 && ALLOWED_TRENDS.contains(trendStr.toLowerCase())) {
                String key = band + "_" + trendStr.toLowerCase();
                if (seenCombinations.contains(key)) {
                    errors.add(linePrefix + "Duplicate combination for Band " + band + " and Trend " + trendStr + ".");
                    isValid = false;
                } else {
                    seenCombinations.add(key);
                }
            }

            // 4. Validate formula
            if (formulaStr.isEmpty()) {
                errors.add(linePrefix + "Formula cannot be empty.");
                isValid = false;
            } else {
                if (!FORMULA_CHAR_PATTERN.matcher(formulaStr).matches()) {
                    errors.add(linePrefix + "Formula contains invalid characters: '" + formulaStr + "'.");
                    isValid = false;
                } else if (!hasBalancedParentheses(formulaStr)) {
                    errors.add(linePrefix + "Formula has unbalanced parentheses: '" + formulaStr + "'.");
                    isValid = false;
                }
            }
        }

        return isValid;
    }

    /**
     * Validates the config_forecast_allocation_bands CSV data.
     * Expected columns: band, start, end, tolerance, b2b_factor
     */
    public static boolean validateBandsFile(List<List<String>> csvData, List<String> errors) {
        if (csvData == null || csvData.isEmpty()) {
            errors.add("Forecast Allocation Bands CSV file is empty.");
            return false;
        }

        if (csvData.size() < 2) {
            errors.add("Forecast Allocation Bands CSV file has headers but lacks data rows.");
            return false;
        }

        List<String> rawHeaders = csvData.get(0);
        List<String> headers = new ArrayList<>();
        for (String h : rawHeaders) {
            headers.add(h != null ? h.replace("\uFEFF", "").trim().toLowerCase() : "");
        }

        int bandIdx = headers.indexOf("band");
        int startIdx = headers.indexOf("start");
        int endIdx = headers.indexOf("end");
        int tolIdx = headers.indexOf("tolerance");
        int b2bIdx = headers.indexOf("b2b_factor");

        if (bandIdx == -1 || startIdx == -1 || endIdx == -1 || tolIdx == -1 || b2bIdx == -1) {
            errors.add("Missing required columns for Bands file. Expected: band, start, end, tolerance, b2b_factor. Found: " + rawHeaders);
            return false;
        }

        Set<Integer> seenBands = new HashSet<>();
        boolean isValid = true;

        for (int i = 1; i < csvData.size(); i++) {
            List<String> row = csvData.get(i);
            int lineNum = i + 1;
            String linePrefix = "Line " + lineNum + ": ";

            int maxIdx = Math.max(bandIdx, Math.max(startIdx, Math.max(endIdx, Math.max(tolIdx, b2bIdx))));
            if (row.size() <= maxIdx) {
                errors.add(linePrefix + "Insufficient columns in row. Found " + row.size());
                isValid = false;
                continue;
            }

            String bandStr = row.get(bandIdx).trim();
            String startStr = row.get(startIdx).trim();
            String endStr = row.get(endIdx).trim();
            String tolStr = row.get(tolIdx).trim();
            String b2bStr = row.get(b2bIdx).trim();

            // 1. Validate band
            int band;
            try {
                band = Integer.parseInt(bandStr);
                if (band < 1 || band > 4) {
                    errors.add(linePrefix + "Invalid band '" + bandStr + "'. Must be an integer between 1 and 4.");
                    isValid = false;
                } else if (seenBands.contains(band)) {
                    errors.add(linePrefix + "Duplicate band definition for Band " + band + ".");
                    isValid = false;
                } else {
                    seenBands.add(band);
                }
            } catch (NumberFormatException e) {
                errors.add(linePrefix + "Band must be a valid integer, found '" + bandStr + "'.");
                isValid = false;
            }

            // 2. Validate start & end
            double start = 0, end = 0;
            boolean startValid = false, endValid = false;
            try {
                start = Double.parseDouble(startStr);
                startValid = true;
            } catch (NumberFormatException e) {
                errors.add(linePrefix + "Start value must be a valid number, found '" + startStr + "'.");
                isValid = false;
            }

            try {
                end = Double.parseDouble(endStr);
                endValid = true;
            } catch (NumberFormatException e) {
                errors.add(linePrefix + "End value must be a valid number, found '" + endStr + "'.");
                isValid = false;
            }

            if (startValid && endValid && start > end) {
                errors.add(linePrefix + "Start value (" + start + ") cannot be greater than end value (" + end + ").");
                isValid = false;
            }

            // 3. Validate tolerance
            try {
                double tol = Double.parseDouble(tolStr);
                if (tol < 0.0) {
                    errors.add(linePrefix + "Tolerance must be a non-negative number, found '" + tolStr + "'.");
                    isValid = false;
                }
            } catch (NumberFormatException e) {
                errors.add(linePrefix + "Tolerance must be a valid number, found '" + tolStr + "'.");
                isValid = false;
            }

            // 4. Validate b2b_factor
            try {
                double b2b = Double.parseDouble(b2bStr);
                if (b2b < 0.0) {
                    errors.add(linePrefix + "B2B factor must be a non-negative number, found '" + b2bStr + "'.");
                    isValid = false;
                }
            } catch (NumberFormatException e) {
                errors.add(linePrefix + "B2B factor must be a valid number, found '" + b2bStr + "'.");
                isValid = false;
            }
        }

        return isValid;
    }

    private static boolean hasBalancedParentheses(String s) {
        return getParenthesesBalance(s) == 0;
    }
}
