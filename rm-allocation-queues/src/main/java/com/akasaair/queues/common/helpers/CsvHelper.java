package com.akasaair.queues.common.helpers;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.akasaair.queues.common.constants.Constants.*;

public class CsvHelper {

    public static ByteArrayInputStream convertToCSV(List<Map<String, Object>> values, List<String> columnHeader) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
             CSVWriter csvWriter = new CSVWriter(new PrintWriter(out))) {
            csvWriter.writeNext(columnHeader.toArray(new String[0]));
            for (Map<String, Object> value : values) {
                List<String> row = new ArrayList<>();
                columnHeader.forEach(header -> {
                    if (value.get(header) != null) {
                        row.add(value.get(header).toString());
                    } else {
                        row.add("");
                    }
                });
                String[] stringArray = row.toArray(new String[0]);
                List<String[]> listOfStringArray = new ArrayList<>();
                listOfStringArray.add(stringArray);
                csvWriter.writeAll(listOfStringArray);
            }
            csvWriter.flush();
            return new ByteArrayInputStream(out.toByteArray());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static List<List<String>> getDataFromCSVFile(Reader reader) {
        List<List<String>> csvData = new ArrayList<>();
        try (CSVReader csvReader = new CSVReaderBuilder(reader).build()) {
            String[] nextLine;
            while ((nextLine = csvReader.readNext()) != null) {
                List<String> row = new ArrayList<>();
                Collections.addAll(row, nextLine);
                csvData.add(row);
            }
        } catch (Exception e) {
            // Replace with appropriate logging
            e.printStackTrace();
        }
        return csvData;
    }

}
