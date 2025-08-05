package com.akasaair.queues.common.helpers;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class QpFaresFileConverter {

    private static final Map<String, String> COLUMN_MAPPING = Map.ofEntries(
            Map.entry("Carrier Code", "carrier_code"),
            Map.entry("Action Code", "action_code"),
            Map.entry("Origin", "origin"),
            Map.entry("Destination", "destination"),
            Map.entry("Fare Basis Code", "fare_basis_code"),
            Map.entry("OW/RT", "ow_rt"),
            Map.entry("Routing", "routing"),
            Map.entry("Footnote", "footnote"),
            Map.entry("Currency", "currency"),
            Map.entry("Fare Amount", "fare_amount"),
            Map.entry("Point Price", "point_price"),
            Map.entry("Effective Date", "effective_date"),
            Map.entry("Tariff No", "tariff_no"),
            Map.entry("Rule Nbr", "rule_nbr"),
            Map.entry("Link", "link"),
            Map.entry("Sequence", "sequence"),
            Map.entry("SOS Charge", "sos_charge"),
            Map.entry("Sort Price", "sort_price"),
            Map.entry("Fare Direction", "fare_direction"),
            Map.entry("Trip Direction", "trip_direction"),
            Map.entry("Inbound/Outbound", "inbound_outbound"),
            Map.entry("Discontinue Date", "discontinue_date"),
            Map.entry("In Batch", "in_batch"),
            Map.entry("OpenJaw", "openjaw")
    );

    private static final List<String> DB_COLUMNS = Arrays.asList(
            "carrier_code", "action_code", "origin", "destination", "fare_basis_code",
            "ow_rt", "routing", "footnote", "currency", "fare_amount", "point_price",
            "effective_date", "tariff_no", "rule_nbr", "link", "sequence", "sos_charge",
            "sort_price", "fare_direction", "trip_direction", "inbound_outbound",
            "discontinue_date", "in_batch", "openjaw"
    );

    public static StringReader convertToCSVReader(String inputFilePath) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(inputFilePath));

        if (lines.isEmpty()) {
            throw new IllegalArgumentException("Input file is empty");
        }

        int headerLineIndex = findHeaderLine(lines);
        if (headerLineIndex == -1) {
            throw new IllegalArgumentException("Could not find header line starting with 'Carrier Code'");
        }

        String headerLine = lines.get(headerLineIndex);
        List<String> fileHeaders = parseTabDelimitedLine(headerLine);

        if (fileHeaders.isEmpty()) {
            throw new IllegalArgumentException("No headers found in header line");
        }

        Map<String, Integer> headerIndexMap = createHeaderIndexMap(fileHeaders);

        StringBuilder csvContent = new StringBuilder();

        csvContent.append(DB_COLUMNS.stream()
                        .map(col -> escapeCSVValue(col))
                        .collect(Collectors.joining(",")))
                .append("\n");

        int processedRows = 0;
        for (int i = headerLineIndex + 1; i < lines.size(); i++) {
            String line = lines.get(i).trim();

            if (line.isEmpty()) {
                continue;
            }

            List<String> dataValues = parseTabDelimitedLine(line);
            if (dataValues.isEmpty()) {
                continue;
            }

            String csvRow = convertToCSVRow(headerIndexMap, dataValues);
            if (csvRow != null && !csvRow.trim().isEmpty()) {
                csvContent.append(csvRow).append("\n");
                processedRows++;
            }
        }

        if (processedRows == 0) {
            throw new IllegalArgumentException("No valid data rows found in file");
        }

        return new StringReader(csvContent.toString());
    }

    private static int findHeaderLine(List<String> lines) {
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i).trim();
            if (line.startsWith("Carrier Code")) {
                return i;
            }
        }
        return -1;
    }

    private static List<String> parseTabDelimitedLine(String line) {
        if (line == null || line.trim().isEmpty()) {
            return new ArrayList<>();
        }

        String[] parts = line.split("\t", -1);
        List<String> values = new ArrayList<>();

        for (String part : parts) {
            values.add(part != null ? part.trim() : "");
        }

        return values;
    }

    private static Map<String, Integer> createHeaderIndexMap(List<String> fileHeaders) {
        Map<String, Integer> headerIndexMap = new HashMap<>();
        for (int i = 0; i < fileHeaders.size(); i++) {
            String header = fileHeaders.get(i).trim();
            if (!header.isEmpty()) {
                headerIndexMap.put(header, i);
            }
        }
        return headerIndexMap;
    }

    private static String convertToCSVRow(Map<String, Integer> headerIndexMap, List<String> dataValues) {
        List<String> csvValues = new ArrayList<>();

        for (String dbColumn : DB_COLUMNS) {
            String fileHeader = getFileHeaderForDBColumn(dbColumn);
            String value = "";

            if (fileHeader != null && headerIndexMap.containsKey(fileHeader)) {
                int index = headerIndexMap.get(fileHeader);
                if (index >= 0 && index < dataValues.size()) {
                    value = cleanValue(dataValues.get(index));
                }
            }

            csvValues.add(escapeCSVValue(value));
        }

        return String.join(",", csvValues);
    }

    private static String getFileHeaderForDBColumn(String dbColumn) {
        return COLUMN_MAPPING.entrySet().stream()
                .filter(entry -> entry.getValue().equals(dbColumn))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse(null);
    }

    private static String cleanValue(String value) {
        if (value == null) {
            return "";
        }

        value = value.trim();

        if (value.matches("\\d{1,2}/\\d{1,2}/\\d{4}\\s+\\d{1,2}:\\d{2}:\\d{2}\\s+[AP]M")) {
            try {
                SimpleDateFormat inputFormat = new SimpleDateFormat("M/d/yyyy h:mm:ss a");
                SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date date = inputFormat.parse(value);
                value = outputFormat.format(date);
            } catch (Exception e) {
                // Keep original value if parsing fails
            }
        }

        return value;
    }

    private static String escapeCSVValue(String value) {
        if (value == null) {
            value = "";
        }

        String escaped = value.replace("\"", "\"\"");
        return "\"" + escaped + "\"";
    }
}