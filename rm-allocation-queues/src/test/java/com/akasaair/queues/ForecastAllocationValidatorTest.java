package com.akasaair.queues;

import com.akasaair.queues.common.Validations.ForecastAllocationValidator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ForecastAllocationValidatorTest {

    @Test
    public void testValidGridFile() {
        List<List<String>> csvData = new ArrayList<>();
        csvData.add(Arrays.asList("band", "trend", "formula", "description"));
        csvData.add(Arrays.asList("1", "Increasing", "Profile", "Profile fare"));
        csvData.add(Arrays.asList("1", "Stable", "Profile", "Profile fare"));
        csvData.add(Arrays.asList("1", "Decreasing", "Min(LSF, Strategy)", "Min of LSF and Strategy"));
        csvData.add(Arrays.asList("4", "Increasing", "Max(Strategy, 0.95*LSF)", "Upsell"));

        List<String> errors = new ArrayList<>();
        boolean valid = ForecastAllocationValidator.validateGridFile(csvData, errors);
        assertTrue(valid, "Grid should be valid");
        assertTrue(errors.isEmpty(), "There should be no errors");
    }

    @Test
    public void testGridMissingHeaders() {
        List<List<String>> csvData = new ArrayList<>();
        csvData.add(Arrays.asList("band", "trend", "wrong_column"));
        csvData.add(Arrays.asList("1", "Increasing", "Profile"));

        List<String> errors = new ArrayList<>();
        boolean valid = ForecastAllocationValidator.validateGridFile(csvData, errors);
        assertFalse(valid);
        assertFalse(errors.isEmpty());
        assertTrue(errors.get(0).contains("Missing required columns"));
    }

    @Test
    public void testGridInvalidBandAndTrend() {
        List<List<String>> csvData = new ArrayList<>();
        csvData.add(Arrays.asList("band", "trend", "formula", "description"));
        csvData.add(Arrays.asList("5", "Upward", "Profile", "Invalid band and trend"));

        List<String> errors = new ArrayList<>();
        boolean valid = ForecastAllocationValidator.validateGridFile(csvData, errors);
        assertFalse(valid);
        assertTrue(errors.stream().anyMatch(e -> e.contains("Invalid band")));
        assertTrue(errors.stream().anyMatch(e -> e.contains("Invalid trend")));
    }

    @Test
    public void testGridDuplicateCombination() {
        List<List<String>> csvData = new ArrayList<>();
        csvData.add(Arrays.asList("band", "trend", "formula", "description"));
        csvData.add(Arrays.asList("2", "Increasing", "Strategy", "First"));
        csvData.add(Arrays.asList("2", "increasing", "Profile", "Duplicate"));

        List<String> errors = new ArrayList<>();
        boolean valid = ForecastAllocationValidator.validateGridFile(csvData, errors);
        assertFalse(valid);
        assertTrue(errors.stream().anyMatch(e -> e.contains("Duplicate combination")));
    }

    @Test
    public void testGridUnbalancedFormula() {
        List<List<String>> csvData = new ArrayList<>();
        csvData.add(Arrays.asList("band", "trend", "formula", "description"));
        csvData.add(Arrays.asList("3", "Stable", "Min(LSF, Strategy", "Missing paren"));

        List<String> errors = new ArrayList<>();
        boolean valid = ForecastAllocationValidator.validateGridFile(csvData, errors);
        assertFalse(valid);
        assertTrue(errors.stream().anyMatch(e -> e.contains("unbalanced parentheses")));
    }

    @Test
    public void testValidBandsFile() {
        List<List<String>> csvData = new ArrayList<>();
        csvData.add(Arrays.asList("band", "start", "end", "tolerance", "b2b_factor"));
        csvData.add(Arrays.asList("1", "-9999", "79.99", "0.0", "0.0"));
        csvData.add(Arrays.asList("2", "80.00", "84.99", "150.0", "0.0"));
        csvData.add(Arrays.asList("3", "85.00", "89.99", "150.0", "0.5"));
        csvData.add(Arrays.asList("4", "90.00", "9999", "150.0", "0.5"));

        List<String> errors = new ArrayList<>();
        boolean valid = ForecastAllocationValidator.validateBandsFile(csvData, errors);
        assertTrue(valid, "Bands should be valid");
        assertTrue(errors.isEmpty(), "Errors list should be empty");
    }

    @Test
    public void testBandsStartGreaterThanEnd() {
        List<List<String>> csvData = new ArrayList<>();
        csvData.add(Arrays.asList("band", "start", "end", "tolerance", "b2b_factor"));
        csvData.add(Arrays.asList("1", "80.0", "70.0", "0.0", "0.0"));

        List<String> errors = new ArrayList<>();
        boolean valid = ForecastAllocationValidator.validateBandsFile(csvData, errors);
        assertFalse(valid);
        assertTrue(errors.stream().anyMatch(e -> e.contains("cannot be greater than end value")));
    }

    @Test
    public void testGridUnquotedFormulaWithCommasReconstructed() {
        // Simulates unquoted CSV where formula containing commas was split across multiple array elements
        List<List<String>> csvData = new ArrayList<>();
        csvData.add(Arrays.asList("band", "trend", "formula", "description"));
        // Line 4: Min(LSF, Strategy) split into "Min(LSF", " Strategy)"
        csvData.add(Arrays.asList("1", "Decreasing", "Min(LSF", " Strategy)", "Min of LSF and Strategy"));
        // Line 7: Min(LSF, 0.80*LSF + 0.20*Lowest) split into "Min(LSF", " 0.80*LSF + 0.20*Lowest)"
        csvData.add(Arrays.asList("2", "Decreasing", "Min(LSF", " 0.80*LSF + 0.20*Lowest)", "Weighted LSF/Lowest"));
        // Line 11: Max(Strategy, 0.95*LSF) split into "Max(Strategy", " 0.95*LSF)"
        csvData.add(Arrays.asList("4", "Increasing", "Max(Strategy", " 0.95*LSF)", "Upsell capped formula"));

        List<String> errors = new ArrayList<>();
        boolean valid = ForecastAllocationValidator.validateGridFile(csvData, errors);
        assertTrue(valid, "Should successfully re-stitch unquoted formulas with commas: " + errors);
        assertTrue(errors.isEmpty(), "Should have no errors");
    }

    @Test
    public void testBandsNegativeTolerance() {
        List<List<String>> csvData = new ArrayList<>();
        csvData.add(Arrays.asList("band", "start", "end", "tolerance", "b2b_factor"));
        csvData.add(Arrays.asList("1", "0.0", "70.0", "-50.0", "0.0"));

        List<String> errors = new ArrayList<>();
        boolean valid = ForecastAllocationValidator.validateBandsFile(csvData, errors);
        assertFalse(valid);
        assertTrue(errors.stream().anyMatch(e -> e.contains("Tolerance must be a non-negative number")));
    }
}

