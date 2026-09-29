package com.akasaair.queues;

import com.akasaair.queues.common.Validations.CompetitionResponseValidator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class CompetitionResponseValidatorTest {

    // ==========================================
    // 1. PLF Bands Tests
    // ==========================================

    @Test
    public void testValidPlfBandsFile() {
        List<List<String>> csvData = new ArrayList<>();
        csvData.add(Arrays.asList("id", "band_name", "start", "end"));
        csvData.add(Arrays.asList("0", "<80%", "-9999", "80"));
        csvData.add(Arrays.asList("1", "80-85%", "80", "85"));
        csvData.add(Arrays.asList("2", "85-90%", "85", "90"));
        csvData.add(Arrays.asList("3", "90-95%", "90", "95"));
        csvData.add(Arrays.asList("4", "95-100%", "95", "100"));
        csvData.add(Arrays.asList("5", "100+%", "100", "9999"));

        List<String> errors = new ArrayList<>();
        boolean valid = CompetitionResponseValidator.validatePlfBandsFile(csvData, errors);
        assertTrue(valid, "Should be valid: " + errors);
        assertTrue(errors.isEmpty());

        List<CompetitionResponseValidator.ExtractedPlfBandRow> extracted = CompetitionResponseValidator.extractPlfBands(csvData);
        assertEquals(6, extracted.size());
        assertEquals("<80%", extracted.get(0).bandName);
        assertEquals(-9999.0, extracted.get(0).start);
        assertEquals(80.0, extracted.get(0).end);
        assertEquals(0.0, extracted.get(0).thresholdFactor);
    }

    @Test
    public void testValidPlfBandsWithThresholdFactor() {
        List<List<String>> csvData = new ArrayList<>();
        csvData.add(Arrays.asList("id", "band_name", "start", "end", "threshold_factor"));
        csvData.add(Arrays.asList("0", "<80%", "-9999", "80", "0.85"));
        csvData.add(Arrays.asList("1", "80-85%", "80", "85", "0.90"));

        List<String> errors = new ArrayList<>();
        boolean valid = CompetitionResponseValidator.validatePlfBandsFile(csvData, errors);
        assertTrue(valid, "Should be valid: " + errors);

        List<CompetitionResponseValidator.ExtractedPlfBandRow> extracted = CompetitionResponseValidator.extractPlfBands(csvData);
        assertEquals(2, extracted.size());
        assertEquals(0.85, extracted.get(0).thresholdFactor);
        assertEquals(0.90, extracted.get(1).thresholdFactor);
    }

    @Test
    public void testPlfBandsNegativeThresholdFactor() {
        List<List<String>> csvData = new ArrayList<>();
        csvData.add(Arrays.asList("id", "band_name", "start", "end", "threshold_factor"));
        csvData.add(Arrays.asList("0", "<80%", "-9999", "80", "-0.5"));

        List<String> errors = new ArrayList<>();
        boolean valid = CompetitionResponseValidator.validatePlfBandsFile(csvData, errors);
        assertFalse(valid, "Should fail due to negative threshold factor");
        assertTrue(errors.stream().anyMatch(e -> e.contains("Threshold factor must be non-negative")));
    }

    @Test
    public void testPlfBandsStartGreaterThanEnd_UserScenario() {
        // User scenario: row ">45%","45","2" where start=45 and end=2
        List<List<String>> csvData = new ArrayList<>();
        csvData.add(Arrays.asList("band_name", "start", "end"));
        csvData.add(Arrays.asList("<45%", "0", "45"));
        csvData.add(Arrays.asList(">45%", "45", "2"));

        List<String> errors = new ArrayList<>();
        boolean valid = CompetitionResponseValidator.validatePlfBandsFile(csvData, errors);
        assertFalse(valid, "Should fail due to start >= end");
        assertFalse(errors.isEmpty());
        assertTrue(errors.stream().anyMatch(e -> e.contains("Start value (45.0) must be strictly less than end value (2.0)")),
                "Error should specifically mention start value must be less than end value. Errors: " + errors);
    }

    @Test
    public void testPlfBandsIntervalOverlap() {
        List<List<String>> csvData = new ArrayList<>();
        csvData.add(Arrays.asList("band_name", "start", "end"));
        csvData.add(Arrays.asList("Band A", "0", "50"));
        csvData.add(Arrays.asList("Band B", "40", "80")); // Overlaps with Band A between 40 and 50

        List<String> errors = new ArrayList<>();
        boolean valid = CompetitionResponseValidator.validatePlfBandsFile(csvData, errors);
        assertFalse(valid, "Should fail due to interval overlap");
        assertTrue(errors.stream().anyMatch(e -> e.contains("Overlapping PLF bands detected")),
                "Error should report interval overlap: " + errors);
    }

    @Test
    public void testPlfBandsDuplicateBandName() {
        List<List<String>> csvData = new ArrayList<>();
        csvData.add(Arrays.asList("band_name", "start", "end"));
        csvData.add(Arrays.asList("Band A", "0", "50"));
        csvData.add(Arrays.asList("Band A", "50", "100"));

        List<String> errors = new ArrayList<>();
        boolean valid = CompetitionResponseValidator.validatePlfBandsFile(csvData, errors);
        assertFalse(valid, "Should fail due to duplicate band name");
        assertTrue(errors.stream().anyMatch(e -> e.contains("Duplicate band_name 'Band A' found.")));
    }

    @Test
    public void testPlfBandsAutoAssignIdsWhenOmitted() {
        List<List<String>> csvData = new ArrayList<>();
        csvData.add(Arrays.asList("band_name", "start", "end"));
        csvData.add(Arrays.asList("Band 2", "50", "100"));
        csvData.add(Arrays.asList("Band 1", "0", "50"));

        List<String> errors = new ArrayList<>();
        boolean valid = CompetitionResponseValidator.validatePlfBandsFile(csvData, errors);
        assertTrue(valid);

        List<CompetitionResponseValidator.ExtractedPlfBandRow> extracted = CompetitionResponseValidator.extractPlfBands(csvData);
        assertEquals(2, extracted.size());
        assertEquals("Band 1", extracted.get(0).bandName);
        assertEquals(0, extracted.get(0).id);
        assertEquals(0, extracted.get(0).plfBand);
        assertEquals("Band 2", extracted.get(1).bandName);
        assertEquals(1, extracted.get(1).id);
        assertEquals(1, extracted.get(1).plfBand);
    }

    // ==========================================
    // 2. Grid Tests
    // ==========================================

    @Test
    public void testValidGridFileWithUnquotedFormula() {
        List<List<String>> csvData = new ArrayList<>();
        csvData.add(Arrays.asList("grid_name", "ndo_band", "plf_band", "time_range", "criteria", "response_rule"));
        // Notice: unquoted CSV parser splits "Min (2A, 0.9C)" into 2 cells:
        csvData.add(Arrays.asList("DEFAULT", "0", "0", "TR", "Min", "Min (2A", "0.9C)"));
        csvData.add(Arrays.asList("DEFAULT", "0", "1", "TR", "Min", "Min (2A, 0.95C)"));

        List<String> errors = new ArrayList<>();
        boolean valid = CompetitionResponseValidator.validateGridFile(csvData, errors);
        assertTrue(valid, "Grid should be valid: " + errors);

        List<CompetitionResponseValidator.ExtractedGridRow> rows = CompetitionResponseValidator.extractGridRows(csvData);
        assertEquals(2, rows.size());
        assertEquals("Min (2A, 0.9C)", rows.get(0).responseRule);
        assertEquals("Min (2A, 0.95C)", rows.get(1).responseRule);
    }

    @Test
    public void testGridInvalidCriteriaAndUnbalancedFormula() {
        List<List<String>> csvData = new ArrayList<>();
        csvData.add(Arrays.asList("grid_name", "ndo_band", "plf_band", "time_range", "criteria", "response_rule"));
        csvData.add(Arrays.asList("DEFAULT", "0", "0", "TR", "InvalidCrit", "Min (2A, 0.9C"));

        List<String> errors = new ArrayList<>();
        boolean valid = CompetitionResponseValidator.validateGridFile(csvData, errors);
        assertFalse(valid);
        assertTrue(errors.stream().anyMatch(e -> e.contains("Invalid criteria 'InvalidCrit'")));
        assertTrue(errors.stream().anyMatch(e -> e.contains("unbalanced parentheses")));
    }

    @Test
    public void testGridDuplicateKey() {
        List<List<String>> csvData = new ArrayList<>();
        csvData.add(Arrays.asList("grid_name", "ndo_band", "plf_band", "time_range", "criteria", "response_rule"));
        csvData.add(Arrays.asList("DEFAULT", "0", "0", "TR", "Min", "Min (2A, 0.9C)"));
        csvData.add(Arrays.asList("DEFAULT", "0", "0", "TR", "Min", "Min (2A, 0.95C)"));

        List<String> errors = new ArrayList<>();
        boolean valid = CompetitionResponseValidator.validateGridFile(csvData, errors);
        assertFalse(valid);
        assertTrue(errors.stream().anyMatch(e -> e.contains("Duplicate entry for Grid 'DEFAULT'")));
    }

    @Test
    public void testValidGridFileWithTolerance() {
        List<List<String>> csvData = new ArrayList<>();
        csvData.add(Arrays.asList("grid_name", "ndo_band", "plf_band", "time_range", "criteria", "response_rule", "tolerance"));
        csvData.add(Arrays.asList("DEFAULT", "0", "0", "TR", "Min", "Min (2A, 0.9C)", "0.7"));
        csvData.add(Arrays.asList("DEFAULT", "0", "1", "TR", "Min", "Min (2A, 0.95C)", "0.8"));

        List<String> errors = new ArrayList<>();
        boolean valid = CompetitionResponseValidator.validateGridFile(csvData, errors);
        assertTrue(valid, "Grid should be valid: " + errors);

        List<CompetitionResponseValidator.ExtractedGridRow> rows = CompetitionResponseValidator.extractGridRows(csvData);
        assertEquals(2, rows.size());
        assertEquals("Min (2A, 0.9C)", rows.get(0).responseRule);
        assertEquals(0.7, rows.get(0).tolerance);
        assertEquals(0.8, rows.get(1).tolerance);
    }

    // ==========================================
    // 3. Fare Recalibration Tests
    // ==========================================

    @Test
    public void testValidFareRecalibration() {
        List<List<String>> csvData = new ArrayList<>();
        csvData.add(Arrays.asList("Sector", "IX", "6E", "SG", "AI"));
        csvData.add(Arrays.asList("BOMDEL", "2000", "0", "0", "0"));
        csvData.add(Arrays.asList("DEL/GOX", "0", "-500", "0", "0"));

        List<String> errors = new ArrayList<>();
        boolean valid = CompetitionResponseValidator.validateFareRecalibrationFile(csvData, errors);
        assertTrue(valid, "Fare recalibration should be valid: " + errors);

        List<CompetitionResponseValidator.ExtractedFareRecalibrationRow> rows =
                CompetitionResponseValidator.extractFareRecalibrationRows(csvData);
        assertEquals(2, rows.size());
        assertEquals("BOMDEL", rows.get(0).sector);
        assertEquals(2000.0, rows.get(0).airlineOffsets.get("IX"));
        assertEquals("DELGOX", rows.get(1).sector);
        assertEquals(-500.0, rows.get(1).airlineOffsets.get("6E"));
    }

    @Test
    public void testFareRecalibrationInvalidSectorAndSameOriginDest() {
        List<List<String>> csvData = new ArrayList<>();
        csvData.add(Arrays.asList("Sector", "IX", "6E"));
        csvData.add(Arrays.asList("BOM", "100", "200")); // Too short
        csvData.add(Arrays.asList("BOMBOM", "100", "200")); // Same origin and dest
        csvData.add(Arrays.asList("DELBOM", "invalid_num", "200")); // Non-numeric

        List<String> errors = new ArrayList<>();
        boolean valid = CompetitionResponseValidator.validateFareRecalibrationFile(csvData, errors);
        assertFalse(valid);
        assertTrue(errors.stream().anyMatch(e -> e.contains("Expected 6-letter sector code")));
        assertTrue(errors.stream().anyMatch(e -> e.contains("Origin and Destination in Sector 'BOMBOM' cannot be the same")));
        assertTrue(errors.stream().anyMatch(e -> e.contains("Invalid numeric offset 'invalid_num'")));
    }
}
