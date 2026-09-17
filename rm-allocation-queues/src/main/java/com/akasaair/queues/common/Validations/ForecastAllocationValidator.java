package com.akasaair.queues.common.Validations;

import java.util.*;
import java.util.regex.Pattern;

public class ForecastAllocationValidator {

    private static final List<String> EXPECTED_GRID_HEADERS = Arrays.asList("band", "trend", "formula", "description");
    private static final List<String> EXPECTED_BANDS_HEADERS = Arrays.asList("band", "start", "end");

    private static final Set<String> ALLOWED_TRENDS = new HashSet<>(Arrays.asList(
            "increasing", "stable", "decreasing"
    ));
    private static final List<String> CANONICAL_TRENDS = Arrays.asList("Increasing", "Stable", "Decreasing");

    private static final Set<String> ALLOWED_FORMULA_WORDS = new HashSet<>(Arrays.asList(
            "max", "min", "average", "avg", "mean", "median", "middle", "highest", "lowest",
            "strategy", "profile", "lsf", "fare", "last", "sold", "selling", "x"
    ));
    private static final List<String> CANONICAL_FORMULA_TOKENS = Arrays.asList(
            "Strategy", "Profile", "LSF", "Highest", "Middle", "Lowest", "Max", "Min", "Average", "Median"
    );

    private static final Pattern FORMULA_CHAR_PATTERN = Pattern.compile("^[a-zA-Z0-9\\s\\+\\-\\*/%\\(\\),.]+$");
    private static final Pattern WORD_TOKEN_PATTERN = Pattern.compile("[a-zA-Z_]+");
    private static final Pattern EMPTY_PAREN_PATTERN = Pattern.compile("\\(\\s*\\)");
    private static final Pattern CONSECUTIVE_OP_PATTERN = Pattern.compile("([\\+\\-\\*/,])\\s*\\1");

    public static class ExtractedGridRow {
        public String bandStr;
        public String trendStr;
        public String formulaStr;
        public String descriptionStr;
    }

    public static int levenshteinDistance(String a, String b) {
        if (a == null && b == null) return 0;
        if (a == null) return b.length();
        if (b == null) return a.length();
        int[][] dp = new int[a.length() + 1][b.length() + 1];
        for (int i = 0; i <= a.length(); i++) dp[i][0] = i;
        for (int j = 0; j <= b.length(); j++) dp[0][j] = j;
        for (int i = 1; i <= a.length(); i++) {
            for (int j = 1; j <= b.length(); j++) {
                int cost = Character.toLowerCase(a.charAt(i - 1)) == Character.toLowerCase(b.charAt(j - 1)) ? 0 : 1;
                dp[i][j] = Math.min(Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1), dp[i - 1][j - 1] + cost);
            }
        }
        return dp[a.length()][b.length()];
    }

    public static String findClosest(String token, Collection<String> candidates, int maxDistance) {
        if (token == null || candidates == null) return null;
        String best = null;
        int bestDist = Integer.MAX_VALUE;
        for (String c : candidates) {
            int dist = levenshteinDistance(token.trim(), c);
            if (dist < bestDist && dist <= maxDistance) {
                bestDist = dist;
                best = c;
            }
        }
        return best;
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

        if (bandIdx == -1 || trendIdx == -1 || formulaIdx == -1 || descIdx == -1) {
            List<String> missing = new ArrayList<>();
            for (String expected : EXPECTED_GRID_HEADERS) {
                if (!headers.contains(expected)) {
                    // Check if any raw header is a misspelled version
                    String suggestion = null;
                    for (String actual : headers) {
                        if (levenshteinDistance(actual, expected) <= 2) {
                            suggestion = actual;
                            break;
                        }
                    }
                    if (suggestion != null) {
                        missing.add("'" + expected + "' (found misspelled column '" + suggestion + "')");
                    } else {
                        missing.add("'" + expected + "'");
                    }
                }
            }
            errors.add("Missing required columns for Grid file. Expected: band, trend, formula, description. Missing: " + String.join(", ", missing) + ". Found: " + rawHeaders);
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

            // 2. Validate trend & check spellings
            if (trendStr.isEmpty()) {
                errors.add(linePrefix + "Trend cannot be empty.");
                isValid = false;
            } else if (!ALLOWED_TRENDS.contains(trendStr.toLowerCase())) {
                String suggestion = findClosest(trendStr, CANONICAL_TRENDS, 3);
                if (suggestion != null) {
                    errors.add(linePrefix + "Invalid or misspelled trend '" + trendStr + "'. Did you mean '" + suggestion + "'? Allowed values: Increasing, Stable, Decreasing.");
                } else {
                    errors.add(linePrefix + "Invalid trend '" + trendStr + "'. Allowed values: Increasing, Stable, Decreasing.");
                }
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

            // 4. Validate formula & check spellings of tokens
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
                } else if (EMPTY_PAREN_PATTERN.matcher(formulaStr).find()) {
                    errors.add(linePrefix + "Formula contains empty parentheses: '" + formulaStr + "'.");
                    isValid = false;
                } else if (CONSECUTIVE_OP_PATTERN.matcher(formulaStr).find()) {
                    errors.add(linePrefix + "Formula contains invalid consecutive operators: '" + formulaStr + "'.");
                    isValid = false;
                } else {
                    String trimmedFormula = formulaStr.trim();
                    char firstChar = trimmedFormula.charAt(0);
                    char lastChar = trimmedFormula.charAt(trimmedFormula.length() - 1);
                    if (firstChar == '*' || firstChar == '/' || firstChar == '+' || firstChar == ',') {
                        errors.add(linePrefix + "Formula cannot start with operator '" + firstChar + "': '" + formulaStr + "'.");
                        isValid = false;
                    }
                    if (lastChar == '*' || lastChar == '/' || lastChar == '+' || lastChar == '-' || lastChar == ',') {
                        errors.add(linePrefix + "Formula cannot end with operator '" + lastChar + "': '" + formulaStr + "'.");
                        isValid = false;
                    }

                    // Spell check all word tokens in the formula
                    java.util.regex.Matcher matcher = WORD_TOKEN_PATTERN.matcher(formulaStr);
                    while (matcher.find()) {
                        String token = matcher.group();
                        String lowerToken = token.toLowerCase();
                        if (!ALLOWED_FORMULA_WORDS.contains(lowerToken)) {
                            String suggestion = findClosest(token, CANONICAL_FORMULA_TOKENS, 3);
                            if (suggestion != null) {
                                errors.add(linePrefix + "Invalid or misspelled token '" + token + "' in formula '" + formulaStr + "'. Did you mean '" + suggestion + "'?");
                            } else {
                                errors.add(linePrefix + "Invalid token '" + token + "' in formula '" + formulaStr + "'. Allowed tokens: Strategy, Profile, LSF, Highest, Middle, Lowest, Max, Min, Average, Median.");
                            }
                            isValid = false;
                        }
                    }
                }
            }
        }

        // 5. Validate completeness: all 12 combinations required
        List<String> missingCombinations = new ArrayList<>();
        for (int b = 1; b <= 4; b++) {
            for (String t : CANONICAL_TRENDS) {
                String key = b + "_" + t.toLowerCase();
                if (!seenCombinations.contains(key)) {
                    missingCombinations.add("Band " + b + " " + t);
                }
            }
        }
        if (!missingCombinations.isEmpty()) {
            errors.add("Incomplete Forecast Allocation Grid. Missing combinations: " + String.join(", ", missingCombinations) + ".");
            isValid = false;
        }

        return isValid;
    }

    /**
     * Validates the config_forecast_allocation_bands CSV data.
     * Expected columns: band, start, end
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

        if (bandIdx == -1 || startIdx == -1 || endIdx == -1) {
            List<String> missing = new ArrayList<>();
            for (String expected : EXPECTED_BANDS_HEADERS) {
                if (!headers.contains(expected)) {
                    String suggestion = null;
                    for (String actual : headers) {
                        if (levenshteinDistance(actual, expected) <= 2) {
                            suggestion = actual;
                            break;
                        }
                    }
                    if (suggestion != null) {
                        missing.add("'" + expected + "' (found misspelled column '" + suggestion + "')");
                    } else {
                        missing.add("'" + expected + "'");
                    }
                }
            }
            errors.add("Missing required columns for Bands file. Expected: band, start, end. Missing: " + String.join(", ", missing) + ". Found: " + rawHeaders);
            return false;
        }

        Set<Integer> seenBands = new HashSet<>();
        boolean isValid = true;

        for (int i = 1; i < csvData.size(); i++) {
            List<String> row = csvData.get(i);
            int lineNum = i + 1;
            String linePrefix = "Line " + lineNum + ": ";

            int maxIdx = Math.max(bandIdx, Math.max(startIdx, endIdx));
            if (row.size() <= maxIdx) {
                errors.add(linePrefix + "Insufficient columns in row. Found " + row.size());
                isValid = false;
                continue;
            }

            String bandStr = row.get(bandIdx).trim();
            String startStr = row.get(startIdx).trim();
            String endStr = row.get(endIdx).trim();

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
        }

        return isValid;
    }

    private static boolean hasBalancedParentheses(String s) {
        return getParenthesesBalance(s) == 0;
    }
}
