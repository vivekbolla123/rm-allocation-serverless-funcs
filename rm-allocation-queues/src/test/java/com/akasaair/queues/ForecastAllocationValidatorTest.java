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
        List<List<String>> csvData = createAll12GridCombinations();

        List<String> errors = new ArrayList<>();
        boolean valid = ForecastAllocationValidator.validateGridFile(csvData, errors);
        assertTrue(valid, "Grid should be valid: " + errors);
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
    public void testGridMisspelledHeaderSuggestion() {
        List<List<String>> csvData = new ArrayList<>();
        csvData.add(Arrays.asList("band", "trand", "formla", "description"));
        csvData.add(Arrays.asList("1", "Increasing", "Profile", "Profile desc"));

        List<String> errors = new ArrayList<>();
        boolean valid = ForecastAllocationValidator.validateGridFile(csvData, errors);
        assertFalse(valid);
        assertTrue(errors.stream().anyMatch(e -> e.contains("misspelled column 'trand'")));
        assertTrue(errors.stream().anyMatch(e -> e.contains("misspelled column 'formla'")));
    }

    @Test
    public void testGridInvalidBandAndMisspelledTrend() {
        List<List<String>> csvData = new ArrayList<>();
        csvData.add(Arrays.asList("band", "trend", "formula", "description"));
        csvData.add(Arrays.asList("8", "Incresing", "Profile", "Invalid band and misspelled trend"));

        List<String> errors = new ArrayList<>();
        boolean valid = ForecastAllocationValidator.validateGridFile(csvData, errors);
        assertFalse(valid);
        assertTrue(errors.stream().anyMatch(e -> e.contains("Invalid band")));
        assertTrue(errors.stream().anyMatch(e -> e.contains("Did you mean 'Increasing'")));
    }

    @Test
    public void testGridMisspelledFormulaTokens() {
        List<List<String>> csvData = createAll12GridCombinations();
        // Modify row 1 to contain misspelled token "Stratgy" and "Profl"
        csvData.get(1).set(2, "Max(Stratgy, Profl)");

        List<String> errors = new ArrayList<>();
        boolean valid = ForecastAllocationValidator.validateGridFile(csvData, errors);
        assertFalse(valid);
        assertTrue(errors.stream().anyMatch(e -> e.contains("Stratgy") && e.contains("Did you mean 'Strategy'")));
        assertTrue(errors.stream().anyMatch(e -> e.contains("Profl") && e.contains("Did you mean 'Profile'")));
    }

    @Test
    public void testGridFormulaSyntaxErrors() {
        List<List<String>> csvData = createAll12GridCombinations();
        // Row with empty parentheses
        csvData.get(1).set(2, "Max()");
        // Row with trailing operator
        csvData.get(2).set(2, "Strategy +");

        List<String> errors = new ArrayList<>();
        boolean valid = ForecastAllocationValidator.validateGridFile(csvData, errors);
        assertFalse(valid);
        assertTrue(errors.stream().anyMatch(e -> e.contains("empty parentheses")));
        assertTrue(errors.stream().anyMatch(e -> e.contains("Formula cannot end with operator")));
    }

    @Test
    public void testGridIncompleteCombinations() {
        List<List<String>> csvData = new ArrayList<>();
        csvData.add(Arrays.asList("band", "trend", "formula", "description"));
        csvData.add(Arrays.asList("1", "Increasing", "Profile", "Only one"));

        List<String> errors = new ArrayList<>();
        boolean valid = ForecastAllocationValidator.validateGridFile(csvData, errors);
        assertFalse(valid);
        assertTrue(errors.stream().anyMatch(e -> e.contains("Incomplete Forecast Allocation Grid")));
    }

    @Test
    public void testGridDuplicateCombination() {
        List<List<String>> csvData = createAll12GridCombinations();
        csvData.add(Arrays.asList("2", "increasing", "Profile", "Duplicate"));

        List<String> errors = new ArrayList<>();
        boolean valid = ForecastAllocationValidator.validateGridFile(csvData, errors);
        assertFalse(valid);
        assertTrue(errors.stream().anyMatch(e -> e.contains("Duplicate combination")));
    }

    @Test
    public void testGridUnbalancedFormula() {
        List<List<String>> csvData = createAll12GridCombinations();
        csvData.get(1).set(2, "Min(LSF, Strategy");

        List<String> errors = new ArrayList<>();
        boolean valid = ForecastAllocationValidator.validateGridFile(csvData, errors);
        assertFalse(valid);
        assertTrue(errors.stream().anyMatch(e -> e.contains("unbalanced parentheses")));
    }

    @Test
    public void testValidBandsFile() {
        List<List<String>> csvData = new ArrayList<>();
        csvData.add(Arrays.asList("band", "start", "end"));
        csvData.add(Arrays.asList("1", "-9999", "79.99"));
        csvData.add(Arrays.asList("2", "80.00", "84.99"));
        csvData.add(Arrays.asList("3", "85.00", "87.99"));
        csvData.add(Arrays.asList("4", "88.00", "91.99"));
        csvData.add(Arrays.asList("5", "92.00", "94.99"));
        csvData.add(Arrays.asList("6", "95.00", "100.00"));
        csvData.add(Arrays.asList("7", "100.01", "9999"));

        List<String> errors = new ArrayList<>();
        boolean valid = ForecastAllocationValidator.validateBandsFile(csvData, errors);
        assertTrue(valid, "Bands should be valid");
        assertTrue(errors.isEmpty(), "Errors list should be empty");
    }

    @Test
    public void testBandsStartGreaterThanEnd() {
        List<List<String>> csvData = new ArrayList<>();
        csvData.add(Arrays.asList("band", "start", "end"));
        csvData.add(Arrays.asList("1", "80.0", "70.0"));

        List<String> errors = new ArrayList<>();
        boolean valid = ForecastAllocationValidator.validateBandsFile(csvData, errors);
        assertFalse(valid);
        assertTrue(errors.stream().anyMatch(e -> e.contains("cannot be greater than end value")));
    }

    @Test
    public void testBandsMissingRequiredHeaders() {
        List<List<String>> csvData = new ArrayList<>();
        csvData.add(Arrays.asList("band", "strt", "wrong"));
        csvData.add(Arrays.asList("1", "0.0", "70.0"));

        List<String> errors = new ArrayList<>();
        boolean valid = ForecastAllocationValidator.validateBandsFile(csvData, errors);
        assertFalse(valid);
        assertTrue(errors.stream().anyMatch(e -> e.contains("Missing required columns for Bands file")));
    }

    @Test
    public void testGridUnquotedFormulaWithCommasReconstructed() {
        List<List<String>> csvData = createAll12GridCombinations();
        // Line 4: Min(LSF, Strategy) split into "Min(LSF", " Strategy)"
        csvData.set(3, new ArrayList<>(Arrays.asList("1", "Decreasing", "Min(LSF", " Strategy)", "Min of LSF and Strategy")));

        List<String> errors = new ArrayList<>();
        boolean valid = ForecastAllocationValidator.validateGridFile(csvData, errors);
        assertTrue(valid, "Should successfully re-stitch unquoted formulas with commas: " + errors);
        assertTrue(errors.isEmpty(), "Should have no errors: " + errors);
    }

    private List<List<String>> createAll12GridCombinations() {
        // Retained name for existing callers; now builds the full 21-cell (7-band) grid.
        List<List<String>> csvData = new ArrayList<>();
        csvData.add(new ArrayList<>(Arrays.asList("band", "trend", "formula", "description")));

        csvData.add(new ArrayList<>(Arrays.asList("1", "Increasing", "Min(Strategy, MaxOwn)", "Band 1 Increasing")));
        csvData.add(new ArrayList<>(Arrays.asList("1", "Stable", "Min(Strategy, MaxOwn)", "Band 1 Stable")));
        csvData.add(new ArrayList<>(Arrays.asList("1", "Decreasing", "Min(Strategy, MaxOwn)", "Band 1 Decreasing")));

        csvData.add(new ArrayList<>(Arrays.asList("2", "Increasing", "Interpolate_2(MaxOwn, Strategy)", "Band 2 Increasing")));
        csvData.add(new ArrayList<>(Arrays.asList("2", "Stable", "Interpolate_2(MaxOwn, Strategy)", "Band 2 Stable")));
        csvData.add(new ArrayList<>(Arrays.asList("2", "Decreasing", "Interpolate_2(MaxOwn, Strategy)", "Band 2 Decreasing")));

        csvData.add(new ArrayList<>(Arrays.asList("3", "Increasing", "Interpolate_1(MaxOwn, Strategy)", "Band 3 Increasing")));
        csvData.add(new ArrayList<>(Arrays.asList("3", "Stable", "Interpolate_1(MaxOwn, Strategy)", "Band 3 Stable")));
        csvData.add(new ArrayList<>(Arrays.asList("3", "Decreasing", "Interpolate_1(MaxOwn, Strategy)", "Band 3 Decreasing")));

        csvData.add(new ArrayList<>(Arrays.asList("4", "Increasing", "Max(LSF, 0.70*LSF + 0.30*Highest)", "Band 4 Increasing")));
        csvData.add(new ArrayList<>(Arrays.asList("4", "Stable", "Min(LSF, 0.30*Highest + 0.70*Middle)", "Band 4 Stable")));
        csvData.add(new ArrayList<>(Arrays.asList("4", "Decreasing", "Min(LSF, 0.60*LSF + 0.40*Strategy)", "Band 4 Decreasing")));

        csvData.add(new ArrayList<>(Arrays.asList("5", "Increasing", "Max(LSF, 0.70*Highest + 0.30*Middle)", "Band 5 Increasing")));
        csvData.add(new ArrayList<>(Arrays.asList("5", "Stable", "Max(LSF, 0.30*Highest + 0.70*Middle)", "Band 5 Stable")));
        csvData.add(new ArrayList<>(Arrays.asList("5", "Decreasing", "Min(LSF, 0.30*Highest + 0.70*Middle)", "Band 5 Decreasing")));

        csvData.add(new ArrayList<>(Arrays.asList("6", "Increasing", "1.05*Max(LSF, Average(Highest, Middle))", "Band 6 Increasing")));
        csvData.add(new ArrayList<>(Arrays.asList("6", "Stable", "Max(LSF, Average(Highest, Middle))", "Band 6 Stable")));
        csvData.add(new ArrayList<>(Arrays.asList("6", "Decreasing", "Max(LSF, Average(Highest, Middle))", "Band 6 Decreasing")));

        csvData.add(new ArrayList<>(Arrays.asList("7", "Increasing", "1.05*Max(LSF, Average(Highest, Middle))", "Band 7 Increasing")));
        csvData.add(new ArrayList<>(Arrays.asList("7", "Stable", "Max(LSF, Average(Highest, Middle))", "Band 7 Stable")));
        csvData.add(new ArrayList<>(Arrays.asList("7", "Decreasing", "Max(LSF, Average(Highest, Middle))", "Band 7 Decreasing")));
        return csvData;
    }
}

