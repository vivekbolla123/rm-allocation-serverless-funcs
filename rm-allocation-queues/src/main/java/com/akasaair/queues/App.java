package com.akasaair.queues;

import com.akasaair.queues.common.Validations.*;
import com.akasaair.queues.common.aws.DependencyFactory;
import com.akasaair.queues.common.aws.S3FileStorageDao;
import com.akasaair.queues.common.entity.MarketListAuditEntity;
import com.akasaair.queues.common.entity.MarketListConnectionsEntity;
import com.akasaair.queues.common.entity.MarketListEntity;
import com.akasaair.queues.common.helpers.AuditHelper;
import com.akasaair.queues.common.helpers.JdbcConnection;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.nimbusds.jose.shaded.json.JSONArray;
import com.akasaair.queues.common.entity.MarketListSalesInvEntity;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.*;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.time.format.DateTimeFormatter;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import java.io.*;
import java.math.BigInteger;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.ParseException;
import java.util.Date;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.text.SimpleDateFormat;
import static com.akasaair.queues.common.Validations.MarketListValidator.*;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.*;
import static com.akasaair.queues.common.constants.Constants.*;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.*;
import static com.akasaair.queues.common.constants.DateTimeConstants.*;
import static com.akasaair.queues.common.constants.ErrorMessagesConstants.*;
import static com.akasaair.queues.common.constants.PermissionsConstants.*;
import static com.akasaair.queues.common.constants.QueryConstants.*;
import static com.akasaair.queues.common.constants.TableNamesConstants.*;
import static com.akasaair.queues.common.helpers.CsvHelper.convertToCSV;
import static com.akasaair.queues.common.helpers.CsvHelper.getDataFromCSVFile;
import static com.akasaair.queues.common.helpers.QpFaresFileConverter.convertToCSVReader;

public class App implements RequestHandler<S3Event, Object> {
    final Connection connection = new JdbcConnection().connection();
    final Connection connection2 = new JdbcConnection().connection();
    List<String> listOfErrors = new ArrayList<>();
    private final S3AsyncClient s3Client;
    final S3FileStorageDao s3FileStorageDao = new S3FileStorageDao();
    public static int error_occurred = 0;
    public static int partial_upload_occurred = 0;
    public static final String UPLOAD_QUEUE_STATUS_PARTIALLY_UPLOADED = "PARTIALLY UPLOADED";

    private static final Set<String> ALLOWED_SI_RBD_TYPES = Set.of("closer", "farther");
    public enum UploadFileType {
        UPSELL,
        DISTRESS
    }
    public App() throws SQLException {
        s3Client = DependencyFactory.s3Client();
    }

    public static void main(String[] args) throws SQLException {
        App app = new App();
        app.uploadFile(FILE_NAME, S3_BUCKET_NAME);
    }

    @Override
    public Object handleRequest(S3Event event, Context context) {
        event.getRecords().forEach(record -> {
            String bucketName = record.getS3().getBucket().getName();
            String key;
            try {
                key = URLDecoder.decode(record.getS3().getObject().getKey(), StandardCharsets.UTF_8.name());
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }

            uploadFile(key, bucketName);

        });
        error_occurred = 0;
        partial_upload_occurred = 0;
        return null;
    }

    private void uploadFile(String key, String bucketName) {
        final String finalKey = key;
        int lengthOfKey = finalKey.length();
        String obj_id = finalKey.substring(S3_FILE_PATH_NAME.length() + 7, lengthOfKey - 4);

        String tableName = null;
        Object role = null;
        String usern = null;
        String routeStr = null;
        String nameForadhoc = null;
        LocalDateTime startTime = null;
        AtomicLong numOfRows = new AtomicLong();

        try (PreparedStatement pstmt = connection.prepareStatement(INPUT_FILE_UPLOAD_INSERT())) {
            pstmt.setString(1, obj_id);
            try (ResultSet resultSet = pstmt.executeQuery()) {
                if (resultSet.next()) {
                    tableName = resultSet.getString("tableName");
                    role = resultSet.getObject("role");
                    usern = resultSet.getString("username");
                    nameForadhoc = resultSet.getString("name");
                    startTime = resultSet.getTimestamp("init_timestamp").toLocalDateTime();
                    routeStr = resultSet.getString("route_for_adhoc");
                }
            }
        } catch (SQLException e) {
            error_occurred = 1;
            listOfErrors.add(DATABASE_ACCESS_ERROR_MESSAGE);
            addErrorsToTable(listOfErrors, obj_id);
            System.out.println(DATABASE_ACCESS_ERROR_MESSAGE);
            e.printStackTrace();
        }
        final String table = tableName;
        final Object role_obj = role;
        final String userName = usern;
        final String adhocUsername = nameForadhoc;
        final String routeString = routeStr;
        String status = UPLOAD_QUEUE_STATUS_IN_PROGRESS;

        System.out.println("The table name is -> " + table);
        try {
            insertAndValidatedBasedOnTableName(bucketName, key, obj_id, tableName, numOfRows, table, role_obj, userName,
                    adhocUsername, routeString, status);
        } catch (Exception e) {
            error_occurred = 1;
            if (listOfErrors.isEmpty()) {
                listOfErrors.add("Validation has failed while inserting data in table");
                addErrorsToTable(listOfErrors, obj_id);
            }
            System.out.println(e);
            e.printStackTrace();
        }
        listOfErrors.clear();
        if (error_occurred == 0) {
            status = UPLOAD_QUEUE_STATUS_DONE;
        } else if (partial_upload_occurred == 1) {
            status = UPLOAD_QUEUE_STATUS_PARTIALLY_UPLOADED;
        } else {
            status = UPLOAD_QUEUE_STATUS_FAILED;
        }
        int rNum = (int) numOfRows.get();
        LocalDateTime endTime = LocalDateTime.now();
        Duration duration = Duration.between(startTime, endTime);
        long seconds = duration.getSeconds();
        long hours = seconds / 3600;
        long minutes = (seconds % 3600) / 60;
        long secs = seconds % 60;

        String formattedDuration = String.format("%02d:%02d:%02d", hours, minutes, secs);
        System.out.println("Status--->" + status);

        try (PreparedStatement pstmt = connection.prepareStatement(INPUT_FILE_UPLOAD_TIME_TAKEN_UPDATE())) {
            pstmt.setString(1, status);
            pstmt.setObject(2, endTime);
            pstmt.setInt(3, rNum);
            pstmt.setString(4, formattedDuration);
            pstmt.setString(5, obj_id);
            pstmt.executeUpdate();
            System.out.println("Status Updated");
        } catch (SQLException e) {
            listOfErrors.add(DATABASE_ACCESS_ERROR_MESSAGE);
            System.out.println(DATABASE_ACCESS_ERROR_MESSAGE);
            e.printStackTrace();
        }

        if (!finalKey.substring(S3_FILE_PATH_NAME.length(), 20).equalsIgnoreCase("locals")) {
            s3Client.deleteObject(DeleteObjectRequest.builder()
                            .bucket(bucketName)
                            .key(finalKey)
                            .build())
                    .join();
        }

    }

    private void insertAndValidatedBasedOnTableName(String bucketName, String key, String obj_id, String tableName,
                                                    AtomicLong numOfRows, String table, Object role_obj, String userName, String adhocUsername,
                                                    String routeString, String status) throws Exception {
        try {
            switch (tableName) {
                case MARKET_LIST_TABLE_NAME, MARKET_LIST_INTERNATIONAL_TABLE_NAME, MARKET_LIST_ADHOC_TABLE_NAME,
                     MARKET_LIST_CONNECTIONS_TABLE_NAME, MARKET_LIST_ADHOC_CONNECTIONS_TABLE_NAME -> {
                    String route;
                    if (tableName.equals(MARKET_LIST_ADHOC_CONNECTIONS_TABLE_NAME)) {
                        route = DOMESTIC;
                        tableName = MARKET_LIST_ADHOC_TABLE_NAME + UNDERSCORE + CONNECTIONS + UNDERSCORE
                                + adhocUsername;
                    } else if (tableName.startsWith(MARKET_LIST_ADHOC_TABLE_NAME)) {
                        if (routeString.equals(INTERNATIONAL)) {
                            route = INTERNATIONAL;
                            tableName = MARKET_LIST_ADHOC_TABLE_NAME + UNDERSCORE + INTERNATIONAL + UNDERSCORE
                                    + adhocUsername;
                        } else {
                            route = DOMESTIC;
                            tableName = MARKET_LIST_ADHOC_TABLE_NAME + UNDERSCORE + adhocUsername;
                        }
                    } else if (tableName.equals(MARKET_LIST_INTERNATIONAL_TABLE_NAME)) {
                        route = INTERNATIONAL;
                    } else {
                        route = DOMESTIC;
                    }
                    System.out.println("Route is ->" + route);
                    try (PreparedStatement pstmt = connection.prepareStatement(INPUT_FILE_UPLOAD_UPDATE())) {
                        pstmt.setString(1, status);
                        pstmt.setObject(2, LocalDateTime.now());
                        pstmt.setString(3, obj_id);
                        pstmt.executeUpdate();
                    }

                    String finalRoute = route;
                    String finalTableName = tableName;
                    s3Client.getObject(GetObjectRequest.builder()
                                            .bucket(bucketName)
                                            .key(key)
                                            .build(),
                                    AsyncResponseTransformer.toBytes())
                            .thenAccept(response -> {
                                String fileContent = new String(response.asByteArray(), StandardCharsets.UTF_8);
                                try (Reader reader = new StringReader(fileContent)) {
                                    if (finalTableName.equalsIgnoreCase(MARKET_LIST_CONNECTIONS_TABLE_NAME)) {
                                        uploadMarketListConnectionsFile(reader, table, userName, role_obj, obj_id);
                                    } else if (finalTableName.toLowerCase()
                                            .startsWith(MARKET_LIST_ADHOC_CONNECTIONS_TABLE_NAME)) {
                                        uploadMarketListConnectionsFile(reader, finalTableName, userName, role_obj,
                                                obj_id);
                                    } else {
                                        uploadMarketListFile(reader, finalTableName, userName, role_obj, finalRoute,
                                                obj_id, numOfRows);
                                    }
                                } catch (Exception e) {
                                    listOfErrors.add("Error Uploading MarketList to DB. Please check File content.");
                                    throw new RuntimeException("Error processing file content: " + e.getMessage(), e);
                                }
                            })
                            .exceptionally(ex -> {
                                listOfErrors.add("Error during file processing for S3");
                                throw new RuntimeException("Error during file processing: " + ex.getMessage(), ex);
                            })
                            .join();
                }
                case STRATEGY_TABLE_NAME -> {
                    try (PreparedStatement pstmt = connection.prepareStatement(INPUT_FILE_UPLOAD_UPDATE())) {
                        pstmt.setString(1, status);
                        pstmt.setObject(2, LocalDateTime.now());
                        pstmt.setString(3, obj_id);
                        pstmt.executeUpdate();
                    }

                    s3Client.getObject(GetObjectRequest.builder()
                                            .bucket(bucketName)
                                            .key(key)
                                            .build(),
                                    AsyncResponseTransformer.toBytes())
                            .thenAccept(response -> {
                                String fileContent = new String(response.asByteArray(), StandardCharsets.UTF_8);
                                long rowCount = new BufferedReader(new StringReader(fileContent)).lines().skip(1)
                                        .count();
                                numOfRows.set(rowCount);
                                try (Reader reader = new StringReader(fileContent)) {
                                    uploadStrategyGridFile(reader, userName, obj_id);
                                } catch (Exception e) {
                                    listOfErrors.add("Error Uploading Strategy File to DB. Please check file content");
                                    throw new RuntimeException(
                                            "Error processing strategy file content: " + e.getMessage(), e);
                                }
                            })
                            .join();
                }
                case AUTO_B2B_TABLE_NAME -> {
                    try (PreparedStatement pstmt = connection.prepareStatement(INPUT_FILE_UPLOAD_UPDATE())) {
                        pstmt.setString(1, status);
                        pstmt.setObject(2, LocalDateTime.now());
                        pstmt.setString(3, obj_id);
                        pstmt.executeUpdate();
                    }

                    s3Client.getObject(GetObjectRequest.builder()
                                            .bucket(bucketName)
                                            .key(key)
                                            .build(),
                                    AsyncResponseTransformer.toBytes())
                            .thenAccept(response -> {
                                String fileContent = new String(response.asByteArray(), StandardCharsets.UTF_8);
                                long rowCount = new BufferedReader(new StringReader(fileContent)).lines().skip(1)
                                        .count();
                                numOfRows.set(rowCount);
                                try (Reader reader = new StringReader(fileContent)) {
                                    uploadAutoB2BGridFile(reader, userName, obj_id);
                                } catch (Exception e) {
                                    throw new RuntimeException(
                                            "Error processing autoB2BFactor file content: " + e.getMessage(), e);
                                }
                            })
                            .join();
                }
                case DATE_EVENT_TABLE_NAME -> {
                    try (PreparedStatement pstmt = connection.prepareStatement(INPUT_FILE_UPLOAD_UPDATE())) {
                        pstmt.setString(1, status);
                        pstmt.setObject(2, LocalDateTime.now());
                        pstmt.setString(3, obj_id);
                        pstmt.executeUpdate();
                    }

                    s3Client.getObject(GetObjectRequest.builder()
                                            .bucket(bucketName)
                                            .key(key)
                                            .build(),
                                    AsyncResponseTransformer.toBytes())
                            .thenAccept(response -> {
                                String fileContent = new String(response.asByteArray(), StandardCharsets.UTF_8);
                                long rowCount = new BufferedReader(new StringReader(fileContent)).lines().skip(1)
                                        .count();
                                numOfRows.set(rowCount);
                                try (Reader reader = new StringReader(fileContent)) {
                                    uploadPartialDateEvent(reader, userName, obj_id, listOfErrors);
                                } catch (Exception e) {
                                    listOfErrors.add("Error Uploading Calendar File to DB. Please check file content");
                                    throw new RuntimeException(
                                            "Error processing date event file content: " + e.getMessage(), e);
                                }
                            })
                            .join();
                }
                case DISTRESS_CLAENDAR -> {
                    try (PreparedStatement pstmt = connection.prepareStatement(INPUT_FILE_UPLOAD_UPDATE())) {
                        pstmt.setString(1, status);
                        pstmt.setObject(2, LocalDateTime.now());
                        pstmt.setString(3, obj_id);
                        pstmt.executeUpdate();
                    }

                    s3Client.getObject(GetObjectRequest.builder()
                                            .bucket(bucketName)
                                            .key(key)
                                            .build(),
                                    AsyncResponseTransformer.toBytes())
                            .thenAccept(response -> {
                                String fileContent = new String(response.asByteArray(), StandardCharsets.UTF_8);
                                long rowCount = new BufferedReader(new StringReader(fileContent)).lines().skip(1)
                                        .count();
                                numOfRows.set(rowCount);
                                try (Reader reader = new StringReader(fileContent)) {
                                    uploadDateEventFile(reader, userName, obj_id,UploadFileType.DISTRESS);
                                } catch (Exception e) {
                                    listOfErrors.add("Error Distress Calendar File to DB. Please check file content");
                                    throw new RuntimeException(
                                            "Error processing date event file content: " + e.getMessage(), e);
                                }
                            })
                            .join();
                }
                case SALES_INVENTORY_TABLE_NAME-> {
                    try (PreparedStatement pstmt = connection.prepareStatement(INPUT_FILE_UPLOAD_UPDATE())) {
                        pstmt.setString(1, status);
                        pstmt.setObject(2, LocalDateTime.now());
                        pstmt.setString(3, obj_id);
                        pstmt.executeUpdate();
                    }

                    String finalTableName1 = tableName;
                    s3Client.getObject(GetObjectRequest.builder()
                                            .bucket(bucketName)
                                            .key(key)
                                            .build(),
                                    AsyncResponseTransformer.toBytes())
                            .thenAccept(response -> {
                                String fileContent = new String(response.asByteArray(), StandardCharsets.UTF_8);
                                try (Reader reader = new StringReader(fileContent)) {
                                    uploadSalesInventoryFile(reader, finalTableName1, userName, obj_id, numOfRows);
                                } catch (Exception e) {
                                    error_occurred = 1;
                                    if (listOfErrors.isEmpty()) {
                                        listOfErrors.add("Error processing sales inventory file: ");
                                        addErrorsToTable(listOfErrors, obj_id);
                                    }
                                    throw new RuntimeException(
                                            "Error processing sales inventory file content: " + e.getMessage(), e);
                                }
                            })
                            .exceptionally(ex -> {
                                error_occurred = 1;
                                System.err.println("Error during S3 getObject or processing: " + ex.getMessage());
                                if (listOfErrors.isEmpty()) {
                                    listOfErrors.add("Failed to retrieve or process file from S3: " + ex.getMessage());
                                    addErrorsToTable(listOfErrors, obj_id);
                                }
                                return null;
                            })
                            .join();
                    break;
                }
                case SALESINV_RBD_DISTRIBUTION_TABLE_NAME -> {
                    try (PreparedStatement pstmt = connection.prepareStatement(INPUT_FILE_UPLOAD_UPDATE())) {
                        pstmt.setString(1, status);
                        pstmt.setObject(2, LocalDateTime.now());
                        pstmt.setString(3, obj_id);
                        pstmt.executeUpdate();
                    }
                    String finalTableNameRbd = tableName;
                    s3Client.getObject(GetObjectRequest.builder()
                                            .bucket(bucketName)
                                            .key(key)
                                            .build(),
                                    AsyncResponseTransformer.toBytes())
                            .thenAccept(response -> {
                                String fileContent = new String(response.asByteArray(), StandardCharsets.UTF_8);
                                try (Reader reader = new StringReader(fileContent)) {
                                    uploadSalesInvRbdDistributionFile(reader, finalTableNameRbd, userName, obj_id, numOfRows);
                                } catch (Exception e) {
                                    error_occurred = 1;
                                    if (listOfErrors.isEmpty()) {
                                        listOfErrors.add("Error processing Sales RBD Distribution file");
                                        addErrorsToTable(listOfErrors, obj_id);
                                    }
                                    throw new RuntimeException(
                                            "Error processing Sales RBD Distribution file content: " + e.getMessage(), e);
                                }
                            })
                            .exceptionally(ex -> {
                                error_occurred = 1;
                                System.err.println("Error during S3 getObject or processing for Sales RBD Distribution: " + ex.getMessage());
                                if (listOfErrors.isEmpty()) {
                                    listOfErrors.add("Failed to retrieve or process Sales RBD Distribution file from S3");
                                    addErrorsToTable(listOfErrors, obj_id);
                                }
                                return null;
                            })
                            .join();
                    break;
                }
                case GROUP_POTENTIAL_SEATS_TABLE_NAME -> {
                    try (PreparedStatement pstmt = connection.prepareStatement(INPUT_FILE_UPLOAD_UPDATE())) {
                        pstmt.setString(1, status);
                        pstmt.setObject(2, LocalDateTime.now());
                        pstmt.setString(3, obj_id);
                        pstmt.executeUpdate();
                    }

                    String finalTableName2 = tableName;
                    s3Client.getObject(GetObjectRequest.builder()
                                            .bucket(bucketName)
                                            .key(key)
                                            .build(),
                                    AsyncResponseTransformer.toBytes())
                            .thenAccept(response -> {
                                String fileContent = new String(response.asByteArray(), StandardCharsets.UTF_8);
                                try (Reader reader = new StringReader(fileContent)) {
                                    uploadGroupPotentialSeatsFile(reader, finalTableName2, userName, obj_id, numOfRows);
                                } catch (Exception e) {
                                    error_occurred = 1;
                                    if (listOfErrors.isEmpty()) {
                                        listOfErrors.add("Error processing Group Potential Seats file");
                                        addErrorsToTable(listOfErrors, obj_id);
                                    }
                                    throw new RuntimeException(
                                            "Error processing Group Potential Seats file content: " + e.getMessage(), e);
                                }
                            })
                            .exceptionally(ex -> {
                                error_occurred = 1;
                                System.err.println("Error during S3 getObject or processing for Group Potential Seats: " + ex.getMessage());
                                if (listOfErrors.isEmpty()) {
                                    listOfErrors.add("Failed to retrieve or process Group Potential Seats file from S3");
                                    addErrorsToTable(listOfErrors, obj_id);
                                }
                                return null;
                            })
                            .join();
                    break;
                }
                case MARKET_LIST_SALESINV_TABLE_NAME,MARKET_LIST_SALESINV_ADHOC_TABLE_NAME -> {
                    try (PreparedStatement pstmt = connection.prepareStatement(INPUT_FILE_UPLOAD_UPDATE())) {
                        pstmt.setString(1, status);
                        pstmt.setObject(2, LocalDateTime.now());
                        pstmt.setString(3, obj_id);
                        pstmt.executeUpdate();
                    }

                    if (tableName.startsWith(MARKET_LIST_SALESINV_ADHOC_TABLE_NAME)) {
                        tableName = MARKET_LIST_SALESINV_ADHOC_TABLE_NAME + UNDERSCORE + adhocUsername;
                    }
                    String finalTableName = tableName;
                    s3Client.getObject(GetObjectRequest.builder()
                                            .bucket(bucketName)
                                            .key(key)
                                            .build(),
                                    AsyncResponseTransformer.toBytes())
                            .thenAccept(response -> {
                                String fileContent = new String(response.asByteArray(), StandardCharsets.UTF_8);
                                try (Reader reader = new StringReader(fileContent)) {
                                    uploadMarketListSalesInvFile(reader, finalTableName, userName, role_obj, obj_id, numOfRows);
                                } catch (Exception e) {
                                    listOfErrors.add("Error processing market_list_salesinv file content");
                                    throw new RuntimeException("Error processing market_list_salesinv file content: " + e.getMessage(), e);
                                }
                            })
                            .exceptionally(ex -> {
                                listOfErrors.add("Error processing market_list_salesinv file content");
                                System.out.println("Error during file processing: " + ex.getMessage());
                                throw new RuntimeException("Error during file processing: " + ex.getMessage(), ex);
                            })
                            .join();
                }
                default -> {
                    try (PreparedStatement pstmt = connection.prepareStatement(INPUT_FILE_UPLOAD_UPDATE())) {
                        pstmt.setString(1, status);
                        pstmt.setObject(2, LocalDateTime.now());
                        pstmt.setString(3, obj_id);
                        pstmt.executeUpdate();
                    }

                    s3Client.getObject(GetObjectRequest.builder()
                                            .bucket(bucketName)
                                            .key(key)
                                            .build(),
                                    AsyncResponseTransformer.toBytes())
                            .thenAccept(response -> {
                                String fileContent = new String(response.asByteArray(), StandardCharsets.UTF_8);
                                long rowCount = new BufferedReader(new StringReader(fileContent)).lines().skip(1)
                                        .count();
                                numOfRows.set(rowCount);
                                try (Reader reader = new StringReader(fileContent)) {
                                    uploadFile(reader, table, userName, obj_id);
                                } catch (Exception e) {
                                    throw new RuntimeException("Error processing file content: " + e.getMessage(), e);
                                }
                            })
                            .exceptionally(ex -> {
                                System.out.println("Error during file processing: " + ex.getMessage());
                                throw new RuntimeException("Error during file processing: " + ex.getMessage(), ex);
                            })
                            .join();
                }
            }
        } catch (Exception e) {
            listOfErrors.add("Failure During Vaildation");
            System.out.println("Error in insertAndValidatedBasedOnTableName: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    public void uploadGroupPotentialSeatsFile(Reader file, String tableName, String userName, String obj_id, AtomicLong numOfRows) throws Exception {
        String uuid = obj_id;
        List<Map<String, String>> recordsToInsert = new ArrayList<>();
        List<String> currentErrors = new ArrayList<>();

        final Set<String> VALID_MONTHS = new HashSet<>(Arrays.asList(
                "January", "February", "March", "April", "May", "June",
                "July", "August", "September", "October", "November", "December"));

        try {
            insertAuditStatement(tableName, getCurrentDateTime(), userName, uuid);
            insertInputStatusTable(tableName);

            List<List<String>> csvData = getDataFromCSVFile(file);

            if (csvData.isEmpty() || csvData.size() < 2) {
                currentErrors.add("Group Potential Seats CSV file is empty or lacks data rows.");
            } else {
                List<String> headerRow = csvData.get(0).stream()
                        .map(h -> h.replace("\uFEFF", "").trim().toLowerCase())
                        .collect(Collectors.toList());

                if (headerRow.size() != 3 ||
                        !CSV_HEADER_SECTOR.equalsIgnoreCase(headerRow.get(0)) ||
                        !CSV_HEADER_MONTH.equalsIgnoreCase(headerRow.get(1)) ||
                        !CSV_HEADER_VALUE.equalsIgnoreCase(headerRow.get(2))) {
                    currentErrors.add("Invalid headers for Group Potential Seats. Expected 'sector', 'month', 'value'. Found: " + csvData.get(0));
                }

                if (currentErrors.isEmpty()) {
                    for (int i = 1; i < csvData.size(); i++) {
                        List<String> row = csvData.get(i);
                        int currentLineNum = i + 1;
                        String linePrefix = "Line " + currentLineNum + ": ";

                        if (row.size() < 3) {
                            currentErrors.add(linePrefix + "Insufficient columns. Expected 3, got " + row.size() + ".");
                            continue;
                        }

                        String sector = row.get(0).trim();
                        String month = row.get(1).trim();
                        String value = row.get(2).trim();

                        boolean rowIsValid = true;

                        if (sector.length() != 6 || !sector.chars().allMatch(Character::isLetter)) {
                            currentErrors.add(linePrefix + "Sector '" + sector + "' must be 6 letters only.");
                            rowIsValid = false;
                        }

                        if (!VALID_MONTHS.contains(month)) {
                            currentErrors.add(linePrefix + "Month '" + month + "' is not a valid full month name (e.g., 'January').");
                            rowIsValid = false;
                        }

                        if (!isNumeric(value)) {
                            currentErrors.add(linePrefix + "Value '" + value + "' is not a valid number.");
                            rowIsValid = false;
                        }

                        if (rowIsValid) {
                            Map<String, String> record = new HashMap<>();
                            record.put(SECTOR_COLUMN_NAME, sector.toUpperCase());
                            record.put(MONTH_COLUMN_NAME, month);
                            record.put(VALUE_COLUMN_NAME, value);
                            recordsToInsert.add(record);
                        }
                    }
                }
            }

            if (!currentErrors.isEmpty()) {
                error_occurred = 1;
                listOfErrors.addAll(currentErrors);
                addErrorsToTable(listOfErrors, obj_id);
                throw new DataMismatchedException(listOfErrors);
            }

            deleteValues(Collections.singleton(ZERO), tableName, ZERO);

            if (!recordsToInsert.isEmpty()) {
                List<String> dbHeaders = Arrays.asList(SECTOR_COLUMN_NAME, MONTH_COLUMN_NAME, VALUE_COLUMN_NAME);
                insertStatement(recordsToInsert, dbHeaders, tableName);
                numOfRows.set(recordsToInsert.size());
            } else {
                numOfRows.set(0);
            }

            s3FileStorageDao.storeFile(createCSV(tableName), tableName, uuid);
            updateAuditStatement(tableName, getCurrentDateTime(), uuid);
            System.out.println("Successfully processed " + tableName + " file.");

        } catch (Exception e) {
            error_occurred = 1;
            System.err.println("Critical error during Group Potential Seats processing for " + tableName + ": " + e.getMessage());
            if (listOfErrors.isEmpty()) {
                if (!currentErrors.isEmpty()) listOfErrors.addAll(currentErrors);
                else listOfErrors.add("Failed to process Group Potential Seats file due to an unexpected error");
            }
            addErrorsToTable(listOfErrors, obj_id);
            throw e;
        } finally {
            updateInputStatus(tableName);
        }
    }

    public void uploadSalesInvRbdDistributionFile(Reader file, String tableName, String userName, String obj_id, AtomicLong numOfRows) throws Exception {
        String uuid = obj_id;
        List<Map<String, String>> recordsToInsert = new ArrayList<>();
        List<String> currentErrors = new ArrayList<>();
        Set<String> uniqueSeatTypeCombinationsInCsv = new HashSet<>();
        Set<String> processedPrimaryKeys = new HashSet<>();

        try {
            insertAuditStatement(tableName, getCurrentDateTime(), userName, uuid);
            insertInputStatusTable(tableName);

            List<List<String>> csvData = getDataFromCSVFile(file);

            if (csvData.isEmpty() || csvData.size() < 2) {
                currentErrors.add("Sales RBD Distribution CSV file is empty or lacks data rows.");
            } else {
                List<String> headerRow = csvData.get(0);
                if (headerRow.size() < 2 ||
                        !CSV_HEADER_SI_RBD_SEATS.equalsIgnoreCase(headerRow.get(0).replace("\uFEFF", "").trim()) ||
                        !CSV_HEADER_SI_RBD_TYPE.equalsIgnoreCase(headerRow.get(1).replace("\uFEFF", "").trim())) {
                    currentErrors.add(MISSING_SI_RBD_HEADERS_ERROR + " Found: " + headerRow);
                } else if (headerRow.size() < 3) {
                    currentErrors.add(NO_RBD_COLUMNS_FOUND_ERROR + " Found: " + headerRow);
                }

                if (currentErrors.isEmpty()) {
                    List<String> rbdColumnHeaders = new ArrayList<>();
                    for (int hIdx = 2; hIdx < headerRow.size(); hIdx++) {
                        rbdColumnHeaders.add(headerRow.get(hIdx).replace("\uFEFF", "").trim());
                    }

                    for (int i = 1; i < csvData.size(); i++) {
                        List<String> row = csvData.get(i);
                        int currentLineNum = i + 1;
                        String linePrefix = "Line " + currentLineNum + ": ";

                        if (row.size() < headerRow.size()) {
                            currentErrors.add(linePrefix + INSUFFICIENT_COLUMNS_ERROR_MESSAGE + " Expected " + headerRow.size() + ", got " + row.size() + ".");
                            continue;
                        }

                        String seatsStr = row.get(0).trim();
                        String typeStr = row.get(1).trim();
                        boolean currentRowCoreValid = true;

                        if (!isNonNegativeInteger(seatsStr)) {
                            currentErrors.add(linePrefix + INVALID_SI_RBD_SEATS_ERROR + " (value: '" + seatsStr + "')");
                            currentRowCoreValid = false;
                        }

                        if (typeStr.isEmpty()) {
                            currentErrors.add(linePrefix + EMPTY_SI_RBD_TYPE_ERROR);
                            currentRowCoreValid = false;
                        } else if (!ALLOWED_SI_RBD_TYPES.contains(typeStr.toLowerCase())) {
                            currentErrors.add(linePrefix + INVALID_SI_RBD_TYPE_VALUE_ERROR_PREFIX + "'" + typeStr + "'. Allowed: " + ALLOWED_SI_RBD_TYPES);
                            currentRowCoreValid = false;
                        }

                        if (!currentRowCoreValid) {
                            continue;
                        }

                        uniqueSeatTypeCombinationsInCsv.add(seatsStr + "|" + typeStr.toLowerCase());

                        for (int rbdCsvColIdx = 2; rbdCsvColIdx < row.size(); rbdCsvColIdx++) {
                            String rbdHeaderName = rbdColumnHeaders.get(rbdCsvColIdx - 2);
                            String rbdValueStr = row.get(rbdCsvColIdx).trim();

                            if (!isNonNegativeInteger(rbdValueStr)) {
                                currentErrors.add(linePrefix + "RBD column '" + rbdHeaderName + "' " + INVALID_SI_RBD_RBD_VALUE_ERROR + " (value: '" + rbdValueStr + "')");
                                continue;
                            }

                            String primaryKey = seatsStr + "|" + typeStr.toLowerCase() + "|" + rbdHeaderName;
                            if (!processedPrimaryKeys.add(primaryKey)) {
                                currentErrors.add(linePrefix + DUPLICATE_PK_SI_RBD_ERROR_PREFIX + primaryKey + " (for RBD '" + rbdHeaderName + "').");
                                continue;
                            }

                            Map<String, String> record = new HashMap<>();
                            record.put(SI_RBD_SEATS_COLUMN_NAME, seatsStr);
                            record.put(SI_RBD_TYPE_COLUMN_NAME, typeStr.toLowerCase());
                            record.put(SI_RBD_RBD_COLUMN_NAME, rbdHeaderName);
                            record.put(SI_RBD_VALUE_COLUMN_NAME, rbdValueStr);
                            recordsToInsert.add(record);
                        }
                    }
                }
            }

            if (!currentErrors.isEmpty()) {
                error_occurred = 1;
                listOfErrors.addAll(currentErrors);
                addErrorsToTable(listOfErrors, obj_id);
                updateInputStatus(tableName);
                System.err.println(FILE_UPLOAD_ERROR_MESSAGE + " for " + tableName + " due to validation errors.");
                throw new DataMismatchedException(listOfErrors);
            }

            if (!recordsToInsert.isEmpty()) {
                deleteSalesInvRbdDistributionBySeatType(uniqueSeatTypeCombinationsInCsv, tableName);

                List<String> dbHeaders = List.of(
                        SI_RBD_SEATS_COLUMN_NAME,
                        SI_RBD_TYPE_COLUMN_NAME,
                        SI_RBD_RBD_COLUMN_NAME,
                        SI_RBD_VALUE_COLUMN_NAME
                );
                System.out.println("Inserting " + recordsToInsert.size() + " records into " + tableName);
                insertStatement(recordsToInsert, dbHeaders, tableName);
                numOfRows.set(recordsToInsert.size());
            } else {
                if(!uniqueSeatTypeCombinationsInCsv.isEmpty()){
                    deleteSalesInvRbdDistributionBySeatType(uniqueSeatTypeCombinationsInCsv, tableName);
                    System.out.println("Cleared data for (seats,type) combinations from " + tableName + " as per CSV, with no new records to insert.");
                } else {
                    System.out.println("No valid records to insert into " + tableName + " and no (seats,type) combinations to clear.");
                }
                numOfRows.set(0);
            }

            s3FileStorageDao.storeFile(createCSV(tableName), tableName, uuid);
            updateAuditStatement(tableName, getCurrentDateTime(), uuid);
            System.out.println("Successfully processed " + tableName + " file.");

        } catch (DataMismatchedException dme) {
            System.err.println("Data mismatched, processing halted for " + tableName + ": " + dme.getMessage());
            throw dme;
        } catch (Exception e) {
            error_occurred = 1;
            System.err.println("Critical error during Sales RBD Distribution processing for " + tableName + ": " + e.getMessage());
            e.printStackTrace();
            if (listOfErrors.isEmpty()) {
                if(!currentErrors.isEmpty()) listOfErrors.addAll(currentErrors);
                else listOfErrors.add("Failed to process Sales RBD Distribution file due to an unexpected error");
            } else if (!currentErrors.isEmpty() && !listOfErrors.containsAll(currentErrors)){
                listOfErrors.addAll(currentErrors);
            }
            addErrorsToTable(listOfErrors, obj_id);
            throw e;
        } finally {
            updateInputStatus(tableName);
            System.out.println("Finished processing attempt for " + tableName + " with obj_id " + obj_id + ". Status: " + (error_occurred == 1 ? "FAILED" : "DONE"));
        }
    }

    private void deleteSalesInvRbdDistributionBySeatType(Set<String> seatTypeCombinations, String tableName) {
        if (seatTypeCombinations == null || seatTypeCombinations.isEmpty()) {
            return;
        }
        String deleteQuery = "DELETE FROM " + tableName + " WHERE " + SI_RBD_SEATS_COLUMN_NAME + " = ? AND " + SI_RBD_TYPE_COLUMN_NAME + " = ?";

        System.out.println("Preparing to delete from " + tableName + " for " + seatTypeCombinations.size() + " (seats, type) combinations.");
        int totalDeletedRows = 0;

        try (PreparedStatement pstmt = connection.prepareStatement(deleteQuery)) {
            connection.setAutoCommit(false);
            for (String combo : seatTypeCombinations) {
                String[] parts = combo.split("\\|");
                if (parts.length == 2) {
                    try {
                        int seats = Integer.parseInt(parts[0]);
                        String type = parts[1];

                        pstmt.setInt(1, seats);
                        pstmt.setString(2, type);
                        pstmt.addBatch();
                    } catch (NumberFormatException e) {
                        listOfErrors.add("Skipping deletion for invalid seat format in combo: " + combo);
                        System.err.println("Skipping deletion for invalid seat format in combo: " + combo);
                    }
                } else {
                    System.err.println("Skipping deletion for malformed (seats,type) combo: " + combo);
                }
            }
            int[] batchResult = pstmt.executeBatch();
            connection.commit();

            for(int count : batchResult) {
                if (count > 0) totalDeletedRows += count;
            }
            System.out.println("Total deleted approximately " + totalDeletedRows + " records from " + tableName + " based on CSV (seats,type) groups.");

        } catch (SQLException e) {
            try { connection.rollback(); } catch (SQLException ex) { System.err.println("Rollback failed: " + ex.getMessage());}
            error_occurred = 1;
            String errorMsg = "Failed to delete existing Sales RBD Distribution data";
            System.err.println(errorMsg + ":"+ e.getMessage());
            listOfErrors.add(errorMsg);
        } finally {
            try { connection.setAutoCommit(true); } catch (SQLException ex) { System.err.println("Failed to reset auto-commit: " + ex.getMessage());}
        }
    }

    private boolean isNonNegativeInteger(String s) {
        if (s == null || s.trim().isEmpty()) return false;
        try {
            int val = Integer.parseInt(s.trim());
            return val >= 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public void uploadMarketListFile(Reader file, String tableName, String userName, Object role, String route,
                                     String obj_id, AtomicLong numOfRows) {
        List<MarketListEntity> validMarketListEntityList = new ArrayList<>();
        try {
            List<String> permissions = getRoleAccess(role);
            if (tableName.startsWith(MARKET_LIST_ADHOC_TABLE_NAME)) {
                createTable(tableName, MARKET_LIST_TABLE_NAME);
            }

            String uuid = obj_id;

            insertAuditStatement(tableName, getCurrentDateTime(), userName, uuid);
            insertInputStatusTable(tableName);

            List<Object> listOfCurves = searchDistinctResults(CURVES_COLUMN_NAME, CURVES_TABLE_NAME);
            List<Object> listOfStrategies = searchDistinctResults(STRATEGY_COLUMN_NAME, STRATEGY_TABLE_NAME);
            List<Object> listOfB2BStrategies = searchDistinctResults(AUTO_B2B_NAME_COLUMN_NAME, AUTO_B2B_TABLE_NAME);
            List<Object> listOfCriteria = searchDistinctResults(CRITERIA_COLUMN_NAME, CRITERIA_TABLE);

            List<List<String>> csvData = getDataFromCSVFile(file);
            csvData = splitRowsByDateRange(csvData, listOfErrors);
            numOfRows.set(csvData.size() > 1 ? csvData.size() - 1 : 0);

            for (int i = 1; i < csvData.size(); i++) {
                String uuidForEntries = UUID.randomUUID().toString();
                int errorCountBefore = listOfErrors.size();
                String analystName = validationForMarketListFile(csvData.get(i), permissions, route, listOfErrors, i,
                        role, userName, listOfCurves, listOfStrategies, listOfB2BStrategies, listOfCriteria);
                if (listOfErrors.size() == errorCountBefore) {
                    createMarketListEntity(csvData.get(i), validMarketListEntityList, uuidForEntries, analystName);
                }
            }

            if (!listOfErrors.isEmpty()) {
                error_occurred = 1;
                addErrorsToTable(listOfErrors, obj_id);
            }

            if (!validMarketListEntityList.isEmpty()) {
                AuditHelper auditHelper = new AuditHelper(connection);
                LocalDateTime currentDateTime = ZonedDateTime.now(ZoneId.of(INDIAN_REGION)).toLocalDateTime();

                if (!tableName.startsWith(MARKET_LIST_ADHOC_TABLE_NAME)) {
                    try {
                        auditHelper.buildAuditLogs(validMarketListEntityList, userName, currentDateTime, tableName);
                    } catch (Exception e) {
                        System.err.println("Failed to build audit logs for table: " + tableName + ". Error: " + e.getMessage());
                    }
                }

                if (tableName.startsWith(MARKET_LIST_ADHOC_TABLE_NAME)) {
                    deleteValues(Collections.singleton(ZERO), tableName, ZERO);
                } else {
                    deleteRecords(validMarketListEntityList, tableName);
                }

                for (MarketListEntity marketEntity : validMarketListEntityList) {
                    insertIntoMarketList(marketEntity, tableName);
                }

                s3FileStorageDao.storeFile(createCSV(tableName), tableName, uuid);
                updateAuditStatement(tableName, getCurrentDateTime(), uuid);

                if (error_occurred == 1) {
                    partial_upload_occurred = 1;
                }
            } else {
                if (error_occurred == 1) {
                    System.out.println(FILE_UPLOAD_ERROR_MESSAGE + ": No valid records to upload.");
                }
            }
        } catch (Exception e) {
            error_occurred = 1;
            listOfErrors.add(ERROR_PROCESSING_FILE);
            addErrorsToTable(listOfErrors, obj_id);
            System.err.println("An exception occurred in uploadMarketListFile: " + e.getMessage());
            e.printStackTrace();
        } finally {
            updateInputStatus(tableName);
        }
    }


    public static List<List<String>> splitRowsByDateRange(List<List<String>> csvData, List<String> listOfErrors) {
        DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("MM/dd/yyyy");
        List<List<String>> intermediateResult = new ArrayList<>();
        List<List<String>> result = new ArrayList<>();
        Set<String> uniqueKeys = new HashSet<>();

        if (!csvData.isEmpty()) {
            intermediateResult.add(new ArrayList<>(csvData.get(0)));
        }

        for (int rowIndex = 1; rowIndex < csvData.size(); rowIndex++) {
            List<String> row = csvData.get(rowIndex);
            LocalDate startDate = LocalDate.parse(row.get(3), DATE_FORMATTER);
            LocalDate endDate = LocalDate.parse(row.get(4), DATE_FORMATTER);
            String dowPattern = row.get(5);

            if (startDate.equals(endDate)) {
                int dayOfWeek = startDate.getDayOfWeek().getValue() % 7;
                if (dowPattern.charAt(dayOfWeek) == '1') {
                    addRowWithDuplicateCheck(intermediateResult, uniqueKeys, row, listOfErrors, rowIndex);
                }
            } else {
                long daysBetween = ChronoUnit.DAYS.between(startDate, endDate);

                for (int i = 0; i <= daysBetween; i++) {
                    LocalDate currentDate = startDate.plusDays(i);
                    int dayOfWeek = currentDate.getDayOfWeek().getValue() % 7;

                    if (dowPattern.charAt(dayOfWeek) == '1') {
                        List<String> newRow = new ArrayList<>(row);
                        newRow.set(3, currentDate.format(DATE_FORMATTER));
                        newRow.set(4, currentDate.format(DATE_FORMATTER));
                        addRowWithDuplicateCheck(intermediateResult, uniqueKeys, newRow, listOfErrors, rowIndex);
                    }
                }
            }
        }
        result.add(intermediateResult.get(0));
        for (int i = 1; i < intermediateResult.size(); i++) {
            List<String> row = intermediateResult.get(i);
            LocalDate date = LocalDate.parse(row.get(3), DATE_FORMATTER);
            String adjustedDowPattern = adjustDowPattern(date, row.get(5));
            row.set(5, adjustedDowPattern);
            result.add(row);
        }
        return result;
    }

    private static void addRowWithDuplicateCheck(List<List<String>> result, Set<String> uniqueKeys, List<String> row,
                                                 List<String> listOfErrors, int rowIndex) {
        String key = row.get(0) + "|" + row.get(1) + "|" + row.get(2) + "|" + row.get(3);
        if (!uniqueKeys.add(key)) {
            listOfErrors.add("Duplicate record found at Line" + (rowIndex + 1) + ": " + key);
        } else {
            result.add(row);
        }
    }

    private static String adjustDowPattern(LocalDate date, String dowPattern) {
        StringBuilder adjustedPattern = new StringBuilder("9999999");
        int dayOfWeek = date.getDayOfWeek().getValue() % 7;
        adjustedPattern.setCharAt(dayOfWeek, '1');
        return adjustedPattern.toString();
    }

    public void addErrorsToTable(List<String> errorsList, String obj_id) {
        if (errorsList == null || errorsList.isEmpty()) {
            return;
        }
        String errorsString = String.join(", ", errorsList);
        try (PreparedStatement pstmt = connection.prepareStatement(INPUT_FILE_UPLOAD_ERROR_OCCURRED())) {
            pstmt.setObject(1, errorsString);
            pstmt.setString(2, obj_id);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println(DATABASE_ACCESS_ERROR_MESSAGE);
            e.printStackTrace();
        }

    }

    public void uploadStrategyGridFile(Reader file, String userName, String obj_id) throws Exception {
        String uuid = obj_id;
        try {
            insertAuditStatement(STRATEGY_TABLE_NAME, getCurrentDateTime(), userName, uuid);
            insertInputStatusTable(STRATEGY_TABLE_NAME);
            List<List<String>> csvData = getDataFromCSVFile(file);
            Map<String, Map<String, String>> strategyMap = new HashMap<>();
            try {
                strategyMap = getStrategyGrid(csvData, STRATEGY_HEADER_VALUES, listOfErrors);
            } catch (Exception e) {
                addErrorsToTable(listOfErrors, obj_id);
                throw e;
            }
            Set<String> strategy = getUniqueStrategies(strategyMap);
            deleteValuesStrategyTable(strategy, STRATEGY_TABLE_NAME, STRATEGY_COLUMN_NAME, CHANNEL_COLUMN_NAME);
            List<String> headers = Arrays.asList(getColumnHeaders(STRATEGY_INPUT_NAME).split(COMMA));
            insertStatement(strategyMap.values().stream().toList(), headers, STRATEGY_TABLE_NAME);
            s3FileStorageDao.storeFile(createCSV(STRATEGY_TABLE_NAME), STRATEGY_TABLE_NAME, uuid);
            updateAuditStatement(STRATEGY_TABLE_NAME, getCurrentDateTime(), uuid);
        } catch (Exception e) {
            throw e;
        } finally {
            updateInputStatus(STRATEGY_TABLE_NAME);
        }
    }

    public void uploadAutoB2BGridFile(Reader file, String userName, String obj_id) throws Exception {
        String uuid = obj_id;
        try {
            insertAuditStatement(AUTO_B2B_TABLE_NAME, getCurrentDateTime(), userName, uuid);
            insertInputStatusTable(AUTO_B2B_TABLE_NAME);
            List<List<String>> csvData = getDataFromCSVFile(file);
            Map<String, Map<String, String>> autoB2BMap = new HashMap<>();
            try {
                autoB2BMap = getAutoB2BGrid(csvData, STRATEGY_HEADER_VALUES, listOfErrors);
            } catch (Exception e) {
                addErrorsToTable(listOfErrors, obj_id);
                throw e;
            }
            Set<String> names = getUniqueNames(autoB2BMap);
            deleteValuesAutoB2BTable(names, AUTO_B2B_TABLE_NAME, AUTO_B2B_NAME_COLUMN_NAME);
            List<String> headers = Arrays.asList(getColumnHeaders(AUTO_B2B_INPUT_NAME).split(COMMA));
            insertStatement(autoB2BMap.values().stream().toList(), headers, AUTO_B2B_TABLE_NAME);
            s3FileStorageDao.storeFile(createCSV(AUTO_B2B_TABLE_NAME), AUTO_B2B_TABLE_NAME, uuid);
            updateAuditStatement(AUTO_B2B_TABLE_NAME, getCurrentDateTime(), uuid);
        } catch (Exception e) {
            throw e;
        } finally {
            updateInputStatus(AUTO_B2B_TABLE_NAME);
        }
    }

    public void uploadFile(Reader file, String tableName, String userName, String obj_id) throws Exception {
        String uuid = obj_id;

        try {
            String tempTableName = tableName + UNDERSCORE + TEMP;
            List<String> headers = Arrays.asList(getColumnHeaders(tableName).split(COMMA));
            insertAuditStatement(tableName, getCurrentDateTime(), userName, uuid);
            createTable(tempTableName, tableName);
            List<List<String>> csvData = getDataFromCSVFile(file);

            List<Map<String, String>> data = switch (tableName) {
                case HISTORIC_HOURLY_DROP_TABLE_NAME -> getHourlyDropData(csvData).values().stream().toList();
                case HISTORIC_GRID_DROP_TABLE_NAME -> getGridDropData(csvData).values().stream().toList();
                case DISTRESS_INVENTORY_STRATEGY_TABLE_NAME -> getDistressStrategy(csvData).values().stream().toList();
                case SERIES_BLOCKING_TABLE_NAME -> createListOfMapSeriesBlock(csvData);
                default -> createListOfMap(csvData, headers);
            };

            Validator validate = getValidator(tableName);
            String val = null;
            if (tableName.equalsIgnoreCase(QP_FARES_TABLE_NAME)) {
                val = searchResults(PARAMETER_RBD_VALUES);
            }
            if (validate != null && !validate.isValid(data, val, listOfErrors)) {
                updateInputStatus(tableName);
                error_occurred = 1;
                listOfErrors.add(INVALID_DATA_ERROR_MESSAGE);
                addErrorsToTable(listOfErrors, obj_id);
                throw new DataMismatchedException(listOfErrors);
            }
            try {
                insertStatement(data, headers, tempTableName);
            } catch (Exception e) {
                throw e;
            }
            insertInputStatusTable(tableName);

            String columnName;

            Set<String> uniqueValues = switch (tableName) {
                case CURVES_TABLE_NAME -> {
                    columnName = CURVES_COLUMN_NAME;
                    yield getUniqueValues(data, columnName);
                }
                case QP_FARES_TABLE_NAME -> {
                    columnName = SECTOR_COLUMN_NAME;
                    yield getUniqueFareSector(data, SECTOR_COLUMN_NAME, ROUTE_COLUMN_NAME);
                }
                case PROFILE_FARES_TABLE_NAME -> {
                    columnName = SECTOR_COLUMN_NAME.toLowerCase();
                    yield getUniqueValues(data, columnName);
                }
                default -> {
                    columnName = ZERO;
                    yield Collections.singleton(ZERO);
                }
            };

            if (tableName.equalsIgnoreCase(QP_FARES_TABLE_NAME)) {
                deleteValuesFare(uniqueValues, tableName);
            } else {
                deleteValues(uniqueValues, tableName, columnName);
            }
            copyFromTempToMainTable(tableName, tempTableName);
            s3FileStorageDao.storeFile(createCSV(tableName), tableName, uuid);
            updateAuditStatement(tableName, getCurrentDateTime(), uuid);
            deleteTable(tempTableName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            updateInputStatus(tableName);
        }
    }

    public void uploadSalesInventoryFile(Reader file, String tableName, String userName, String obj_id, AtomicLong numOfRows) throws Exception {
        String uuid = obj_id;
        List<Map<String, String>> recordsToProcess = new ArrayList<>();
        List<String> currentErrors = new ArrayList<>();

        Set<String> monthsToClearYYYYMM00 = new HashSet<>();
        LocalDate overallMinDateForDailyDeletion = null;
        LocalDate overallMaxDateForDailyDeletion = null;

        try {
            insertAuditStatement(tableName, getCurrentDateTime(), userName, uuid);
            insertInputStatusTable(tableName);

            List<List<String>> csvData = getDataFromCSVFile(file);

            if (csvData.isEmpty() || csvData.size() < 2) {
                currentErrors.add("Sales Inventory CSV file is empty or lacks data rows.");
                error_occurred = 1;
                listOfErrors.addAll(currentErrors);
                addErrorsToTable(listOfErrors, obj_id);
                updateInputStatus(tableName);
                throw new DataMismatchedException(listOfErrors);
            }

            List<String> headerRow = csvData.get(0);
            Map<String, Integer> headerMap = new HashMap<>();
            for (int hIdx = 0; hIdx < headerRow.size(); hIdx++) {
                headerMap.put(headerRow.get(hIdx).replace("\uFEFF", "").trim().toLowerCase(), hIdx);
            }

            boolean isMonthBasedFormat = headerMap.containsKey(HEADER_TEXT_SALES_MONTH.toLowerCase());
            boolean isDateRangeBasedFormat = headerMap.containsKey(HEADER_TEXT_SALES_STARTDATE.toLowerCase());

            Integer colIdxMonth = headerMap.get(HEADER_TEXT_SALES_MONTH.toLowerCase());
            Integer colIdxMinSeats = headerMap.get(HEADER_TEXT_SALES_MINSEATS.toLowerCase());
            Integer colIdxMaxSeats = headerMap.get(HEADER_TEXT_SALES_MAXSEATS.toLowerCase());
            Integer colIdxStartDate = headerMap.get(HEADER_TEXT_SALES_STARTDATE.toLowerCase());
            Integer colIdxEndDate = headerMap.get(HEADER_TEXT_SALES_ENDDATE.toLowerCase());
            Integer colIdxMinPax = headerMap.get(HEADER_TEXT_SALES_MINPAX.toLowerCase());
            Integer colIdxMaxPax = headerMap.get(HEADER_TEXT_SALES_MAXPAX.toLowerCase());
            Integer colIdxSeatsAlloc = headerMap.get(HEADER_TEXT_SALES_SEATSALLOC.toLowerCase());

            if (isMonthBasedFormat) {
                if (colIdxMonth == null || colIdxMinSeats == null || colIdxMaxSeats == null || colIdxSeatsAlloc == null) {
                    currentErrors.add(MISSING_SALES_HEADER_ERROR_PREFIX + "Expected for month format: " + HEADER_TEXT_SALES_MONTH + ", " + HEADER_TEXT_SALES_MINSEATS + ", " + HEADER_TEXT_SALES_MAXSEATS + ", " + HEADER_TEXT_SALES_SEATSALLOC + ". Found: " + headerRow);
                    isMonthBasedFormat = false;
                }
            } else if (isDateRangeBasedFormat) {
                if (colIdxStartDate == null || colIdxEndDate == null || colIdxMinPax == null || colIdxMaxPax == null || colIdxSeatsAlloc == null) {
                    currentErrors.add(MISSING_SALES_HEADER_ERROR_PREFIX + "Expected for date-range format: " + HEADER_TEXT_SALES_STARTDATE + ", " + HEADER_TEXT_SALES_ENDDATE + ", " + HEADER_TEXT_SALES_MINPAX + ", " + HEADER_TEXT_SALES_MAXPAX + ", " + HEADER_TEXT_SALES_SEATSALLOC + ". Found: " + headerRow);
                    isDateRangeBasedFormat = false;
                }
            } else {
                currentErrors.add(UNKNOWN_SALES_INVENTORY_CSV_FORMAT_ERROR + " Found headers: " + headerRow);
            }

            if (!currentErrors.isEmpty() || (!isMonthBasedFormat && !isDateRangeBasedFormat)) {
                error_occurred = 1;
                if (!currentErrors.isEmpty()) listOfErrors.addAll(currentErrors); else if (listOfErrors.isEmpty() && !headerRow.isEmpty()) {
                    listOfErrors.add(UNKNOWN_SALES_INVENTORY_CSV_FORMAT_ERROR + " Found headers: " + headerRow);
                } else if (listOfErrors.isEmpty() && headerRow.isEmpty()){
                    listOfErrors.add(UNKNOWN_SALES_INVENTORY_CSV_FORMAT_ERROR + " CSV header row is empty.");
                }
                addErrorsToTable(listOfErrors, obj_id);
                updateInputStatus(tableName);
                throw new DataMismatchedException(listOfErrors);
            }

            for (int i = 1; i < csvData.size(); i++) {
                List<String> row = csvData.get(i);
                int currentLineNum = i + 1;
                String linePrefix = "Line " + currentLineNum + ": ";
                boolean rowIsValid = true;

                String minValStr = null, maxValStr = null, seatsAllocValStr = null;
                String minHeaderName = "", maxHeaderName = "";
                String dbDateValue = null;
                LocalDate itemStartDateForDailyDeletionCalc = null;
                LocalDate itemEndDateForDailyDeletionCalc = null;

                if (isMonthBasedFormat) {
                    minHeaderName = HEADER_TEXT_SALES_MINSEATS;
                    maxHeaderName = HEADER_TEXT_SALES_MAXSEATS;
                    int maxIndexRequired = Collections.max(Arrays.asList(colIdxMonth, colIdxMinSeats, colIdxMaxSeats, colIdxSeatsAlloc));
                    if (row.size() <= maxIndexRequired) {
                        currentErrors.add(linePrefix + INSUFFICIENT_COLUMNS_ERROR_MESSAGE + " Expected at least " + (maxIndexRequired + 1) + " columns for month format.");
                        rowIsValid = false;
                    } else {
                        String monthYearStr = row.get(colIdxMonth);
                        minValStr = row.get(colIdxMinSeats);
                        maxValStr = row.get(colIdxMaxSeats);
                        seatsAllocValStr = row.get(colIdxSeatsAlloc);
                        try {
                            YearMonth ym = YearMonth.parse(monthYearStr, SALES_MONTH_YEAR_INPUT_FORMATTER);
                            dbDateValue = ym.format(DateTimeFormatter.ofPattern("yyyyMM")) + MONTH_DAY_PLACEHOLDER;
                            monthsToClearYYYYMM00.add(dbDateValue);
                            itemStartDateForDailyDeletionCalc = ym.atDay(1);
                            itemEndDateForDailyDeletionCalc = ym.atEndOfMonth();
                        } catch (DateTimeParseException e) {
                            currentErrors.add(linePrefix + INVALID_SALES_MONTH_YEAR_FORMAT_ERROR + " (value: " + monthYearStr + ")");
                            rowIsValid = false;
                        }
                    }
                } else {
                    minHeaderName = HEADER_TEXT_SALES_MINPAX;
                    maxHeaderName = HEADER_TEXT_SALES_MAXPAX;
                    int maxIndexRequired = Collections.max(Arrays.asList(colIdxStartDate, colIdxEndDate, colIdxMinPax, colIdxMaxPax, colIdxSeatsAlloc));
                    if (row.size() <= maxIndexRequired) {
                        currentErrors.add(linePrefix + INSUFFICIENT_COLUMNS_ERROR_MESSAGE + " Expected at least " + (maxIndexRequired + 1) + " columns for date-range format.");
                        rowIsValid = false;
                    } else {
                        String startDateStr = row.get(colIdxStartDate);
                        String endDateStr = row.get(colIdxEndDate);
                        minValStr = row.get(colIdxMinPax);
                        maxValStr = row.get(colIdxMaxPax);
                        seatsAllocValStr = row.get(colIdxSeatsAlloc);
                        try {
                            LocalDate parsedStartDate = LocalDate.parse(startDateStr, SALES_DATE_INPUT_FORMATTER);
                            LocalDate parsedEndDate = LocalDate.parse(endDateStr, SALES_DATE_INPUT_FORMATTER);
                            if (parsedEndDate.isBefore(parsedStartDate)) {
                                currentErrors.add(linePrefix + INVALID_SALES_DATE_RANGE_ERROR);
                                rowIsValid = false;
                            }
                            itemStartDateForDailyDeletionCalc = parsedStartDate;
                            itemEndDateForDailyDeletionCalc = parsedEndDate;
                        } catch (DateTimeParseException e) {
                            currentErrors.add(linePrefix + INVALID_SALES_DATE_FORMAT_ERROR + " (value: " + startDateStr + "/" + endDateStr + ")");
                            rowIsValid = false;
                        }
                    }
                }

                if (!rowIsValid) continue;

                if (!isNumeric(minValStr)) {
                    currentErrors.add(linePrefix + "'" + minHeaderName + "' " + INVALID_SALES_NUMBER_FORMAT_ERROR + " (value: " + minValStr + ")");
                    rowIsValid = false;
                }
                if (!isNumeric(maxValStr)) {
                    currentErrors.add(linePrefix + "'" + maxHeaderName + "' " + INVALID_SALES_NUMBER_FORMAT_ERROR + " (value: " + maxValStr + ")");
                    rowIsValid = false;
                }
                if (!isNumeric(seatsAllocValStr)) {
                    currentErrors.add(linePrefix + "'" + HEADER_TEXT_SALES_SEATSALLOC + "' " + INVALID_SALES_NUMBER_FORMAT_ERROR + " (value: " + seatsAllocValStr + ")");
                    rowIsValid = false;
                }
                if (rowIsValid && isNumeric(minValStr) && isNumeric(maxValStr)) {
                    if (Integer.parseInt(minValStr) > Integer.parseInt(maxValStr)) {
                        currentErrors.add(linePrefix + "'" + minHeaderName + "' (" + minValStr + ") cannot be greater than '" + maxHeaderName + "' (" + maxValStr + ").");
                        rowIsValid = false;
                    }
                }

                if (rowIsValid) {
                    if (overallMinDateForDailyDeletion == null || itemStartDateForDailyDeletionCalc.isBefore(overallMinDateForDailyDeletion)) {
                        overallMinDateForDailyDeletion = itemStartDateForDailyDeletionCalc;
                    }
                    if (overallMaxDateForDailyDeletion == null || itemEndDateForDailyDeletionCalc.isAfter(overallMaxDateForDailyDeletion)) {
                        overallMaxDateForDailyDeletion = itemEndDateForDailyDeletionCalc;
                    }

                    if (isMonthBasedFormat) {
                        Map<String, String> record = new HashMap<>();
                        record.put(SALES_DB_DATE, dbDateValue);
                        record.put(SALES_DB_MINVALUE, minValStr);
                        record.put(SALES_DB_MAXVALUE, maxValStr);
                        record.put(SALES_DB_SEATSALLOCATION, seatsAllocValStr);
                        recordsToProcess.add(record);
                    } else {
                        LocalDate currentDate = itemStartDateForDailyDeletionCalc;
                        while (!currentDate.isAfter(itemEndDateForDailyDeletionCalc)) {
                            Map<String, String> record = new HashMap<>();
                            record.put(SALES_DB_DATE, currentDate.format(SALES_DATE_DB_FORMATTER));
                            record.put(SALES_DB_MINVALUE, minValStr);
                            record.put(SALES_DB_MAXVALUE, maxValStr);
                            record.put(SALES_DB_SEATSALLOCATION, seatsAllocValStr);
                            recordsToProcess.add(record);
                            currentDate = currentDate.plusDays(1);
                        }
                    }
                }
            }

            List<Map<String, String>> recordsToInsert = new ArrayList<>();
            Set<String> uniqueDbDates = new HashSet<>();

            for (Map<String, String> record : recordsToProcess) {
                String dateKey = record.get(SALES_DB_DATE);
                if (!uniqueDbDates.add(dateKey)) {
                    String errorMsg = "Duplicate entry generated for date: " + dateKey +
                            ". This can occur if multiple CSV rows (after processing month/date ranges) resolve to the same date string.";
                    if (!currentErrors.contains(errorMsg)) {
                        currentErrors.add(errorMsg);
                    }
                } else {
                    recordsToInsert.add(record);
                }
            }

            if (!currentErrors.isEmpty()) {
                error_occurred = 1;
                listOfErrors.addAll(currentErrors);
                addErrorsToTable(listOfErrors, obj_id);
                updateInputStatus(tableName);
                System.err.println(FILE_UPLOAD_ERROR_MESSAGE + " for " + tableName + " due to validation errors (see details in error table).");
                throw new DataMismatchedException(listOfErrors);
            }

            if (!recordsToInsert.isEmpty()) {
                if (!monthsToClearYYYYMM00.isEmpty()) {
                    deleteSalesInventoryByExactDates(monthsToClearYYYYMM00, tableName);
                }

                if (overallMinDateForDailyDeletion != null && overallMaxDateForDailyDeletion != null) {
                    deleteSalesInventoryDailyByDateRange(
                            overallMinDateForDailyDeletion.format(SALES_DATE_DB_FORMATTER),
                            overallMaxDateForDailyDeletion.format(SALES_DATE_DB_FORMATTER),
                            tableName
                    );
                }

                List<String> dbHeaders = List.of(SALES_DB_DATE, SALES_DB_MINVALUE, SALES_DB_MAXVALUE, SALES_DB_SEATSALLOCATION);
                System.out.println("Inserting " + recordsToInsert.size() + " unique records into " + tableName);
                insertStatement(recordsToInsert, dbHeaders, tableName);
                numOfRows.set(recordsToInsert.size());

            } else {
                System.out.println("No valid, unique sales inventory data to insert for " + tableName + ".");
                numOfRows.set(0);
            }

            System.out.println("Creating CSV backup for " + tableName);
            s3FileStorageDao.storeFile(createCSV(tableName), tableName, uuid);
            updateAuditStatement(tableName, getCurrentDateTime(), uuid);
            System.out.println("Successfully processed " + tableName + " file.");

        } catch (DataMismatchedException dme) {
            System.err.println("Data mismatched, processing halted for " + tableName + ": " + dme.getMessage());
            throw dme;
        } catch (Exception e) {
            error_occurred = 1;
            System.err.println("Critical error during sales inventory processing for " + tableName + ": " + e.getMessage());
            e.printStackTrace();
            if (listOfErrors.isEmpty()) {
                if (!currentErrors.isEmpty()) listOfErrors.addAll(currentErrors);
                else listOfErrors.add("Failed to process sales inventory file due to an unexpected error");
            } else if (!currentErrors.isEmpty() && !listOfErrors.containsAll(currentErrors)){
                listOfErrors.addAll(currentErrors);
            }
            addErrorsToTable(listOfErrors, obj_id);
            throw e;
        } finally {
            updateInputStatus(tableName);
            System.out.println("Finished processing attempt for " + tableName + " with obj_id " + obj_id + ". Status: " + (error_occurred == 1 ? "FAILED" : "DONE"));
        }
    }

    private void deleteSalesInventoryByExactDates(Set<String> datesToDelete, String tableName) {
        if (datesToDelete == null || datesToDelete.isEmpty()) {
            return;
        }
        String placeholders = String.join(",", Collections.nCopies(datesToDelete.size(), "?"));
        String deleteQuery = "DELETE FROM " + tableName + " WHERE " + SALES_DB_DATE + " IN (" + placeholders + ")";
        System.out.println("Executing delete on " + tableName + " for exact dates: " + datesToDelete);
        try (PreparedStatement pstmt = connection.prepareStatement(deleteQuery)) {
            int i = 1;
            for (String dateValue : datesToDelete) {
                pstmt.setString(i++, dateValue);
            }
            int deletedRows = pstmt.executeUpdate();
            System.out.println("Deleted " + deletedRows + " exact date records from " + tableName + " matching: " + datesToDelete);
        } catch (SQLException e) {
            error_occurred = 1;
            String errorMsg = "Failed to delete exact sales inventory data for dates " + datesToDelete;
            System.err.println(errorMsg);
            listOfErrors.add(errorMsg);
            throw new RuntimeException(errorMsg, e);
        }
    }
    private void deleteSalesInventoryDailyByDateRange(String startDateYYYYMMDD, String endDateYYYYMMDD, String tableName) {
        String deleteQuery = "DELETE FROM " + tableName + " WHERE " +
                SALES_DB_DATE + " >= ? AND " +
                SALES_DB_DATE + " <= ? AND " +
                SALES_DB_DATE + " NOT LIKE '%00'";

        System.out.println("Executing delete for daily records (not ending in '00') on " + tableName + " from " + startDateYYYYMMDD + " to " + endDateYYYYMMDD);
        try (PreparedStatement pstmt = connection.prepareStatement(deleteQuery)) {
            pstmt.setString(1, startDateYYYYMMDD);
            pstmt.setString(2, endDateYYYYMMDD);
            int deletedRows = pstmt.executeUpdate();
            System.out.println("Deleted " + deletedRows + " existing daily records (not ending in '00') from " + tableName + " in the range.");
        } catch (SQLException e) {
            error_occurred = 1;
            String errorMsg = "Failed to delete existing daily sales inventory data for range " + startDateYYYYMMDD + "-" + endDateYYYYMMDD;
            System.err.println(errorMsg);
            listOfErrors.add(errorMsg);
            throw new RuntimeException(errorMsg, e);
        }
    }

    private boolean isNumeric(String str) {
        if (str == null || str.trim().isEmpty()) {
            return false;
        }
        try {
            Integer.parseInt(str.trim());
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
    public ByteArrayInputStream createCSV(String tableName) throws DataMismatchedException {

        if (findIfTableExists(tableName) == 0) {
            throw new DataMismatchedException(Collections.singletonList(INVALID_TABLE_NAME_ERROR_MESSAGE));
        }
        String tableNameForHeader = tableName;
        if (tableNameForHeader.startsWith(MARKET_LIST_SALESINV_TABLE_NAME))
            tableNameForHeader = MARKET_LIST_SALESINV_TABLE_NAME;
        if (tableNameForHeader.startsWith(MARKET_LIST_TABLE_NAME + UNDERSCORE + CONNECTIONS.toLowerCase()) || tableNameForHeader.startsWith(MARKET_LIST_ADHOC_CONNECTIONS_TABLE_NAME + UNDERSCORE + CONNECTIONS))
            tableNameForHeader = MARKET_LIST_CONNECTIONS_TABLE_NAME;
        else if (tableNameForHeader.startsWith(MARKET_LIST_TABLE_NAME) && !tableNameForHeader.startsWith(MARKET_LIST_SALESINV_TABLE_NAME))
            tableNameForHeader = MARKET_LIST_TABLE_NAME_S3;


        List<String> headers = Arrays.asList(getColumnHeaders(tableNameForHeader).split(COMMA));
        List<Map<String, Object>> values;
        if (tableNameForHeader.equals(STRATEGY_TABLE_NAME)) {
            values = retrieveStrategyGrid(null);
        } else if (tableNameForHeader.equals(AUTO_B2B_TABLE_NAME)) {
            values = retrieveAutoB2BGrid(null);
        } else {
            values = findAllRecordsFromTable(tableName, headers);
        }

        return convertToCSV(values, headers);
    }

    public List<Map<String, Object>> retrieveStrategyGrid(String value) {
        int limit = getCountOfEntries(DPLF_BANDS_TABLE);
        List<Map<String, Object>> listObj = new ArrayList<>();
        try (PreparedStatement pstmt = connection.prepareStatement(GET_STRATEGY_GRID(limit, value))) {
            try (ResultSet resultSet = pstmt.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> row = new HashMap<>();
                    row.put(STRATEGY_COLUMN_NAME, resultSet.getString(STRATEGY_COLUMN_NAME));
                    row.put(CHANNEL_COLUMN_NAME, resultSet.getString(CHANNEL_COLUMN_NAME));
                    row.put(DECISION_COLUMN_NAME, resultSet.getString(DECISION_COLUMN_NAME));
                    row.put(BAND_COLUMN_NAME, resultSet.getString(BAND_COLUMN_NAME));
                    for (int i = 0; i <= 6; i++) {
                        row.put(String.valueOf(i), resultSet.getString(String.valueOf(i)));
                    }

                    listObj.add(row);
                }
            }
        } catch (SQLException e) {
            System.out.println(DATABASE_ACCESS_ERROR_MESSAGE);
            e.printStackTrace();
        }
        return listObj;
    }

    public List<Map<String, Object>> retrieveAutoB2BGrid(String value) {
        int limit = getCountOfEntries(DPLF_BANDS_TABLE);
        List<Map<String, Object>> listObj = new ArrayList<>();
        try (PreparedStatement pstmt = connection.prepareStatement(GET_AUTO_B2B_FACTOR_GRID(limit))) {
            try (ResultSet resultSet = pstmt.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> row = new HashMap<>();
                    row.put(AUTO_B2B_NAME_COLUMN_NAME, resultSet.getString(AUTO_B2B_NAME_COLUMN_NAME));
                    row.put(NDO_BAND_COLUMN_NAME, resultSet.getString(NDO_BAND_COLUMN_NAME));
                    row.put(FORECAST_BAND_COLUMN_NAME, resultSet.getString(FORECAST_BAND_COLUMN_NAME));
                    for (int i = 0; i <= 6; i++) {
                        row.put(String.valueOf(i), resultSet.getString(String.valueOf(i)));
                    }

                    listObj.add(row);
                }
            }
        } catch (SQLException e) {
            System.out.println(DATABASE_ACCESS_ERROR_MESSAGE);
            e.printStackTrace();
        }
        return listObj;
    }

    public int findIfTableExists(String tableName) {
        List<Map<String, Object>> listObj = new ArrayList<>();
        if (tableName.toUpperCase().startsWith("QP_DW_PRIAUT.")) {
            tableName = tableName.substring("QP_DW_PRIAUT.".length());
        }

        String query = "SELECT table_name FROM information_schema. tables WHERE table_type = 'BASE TABLE' AND table_name = '"+ tableName+ "'";

        try (Statement stmt = connection.createStatement();
             ResultSet resultSet = stmt.executeQuery(query)) {
            while (resultSet.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("tableName", resultSet.getString(1));
                listObj.add(row);
            }
        } catch (SQLException e) {
            System.out.println(DATABASE_ACCESS_ERROR_MESSAGE);
            e.printStackTrace();
        }
        return listObj.size();
    }

    private Set<String> getUniqueValues(List<Map<String, String>> data, String columnName) {
        Set<String> uniqueValues = new HashSet<>();
        for (Map<String, String> singleData : data) {
            uniqueValues.add(singleData.get(columnName));
        }
        return uniqueValues;
    }

    private Set<String> getUniqueFareSector(List<Map<String, String>> data, String sector, String route) {
        Set<String> uniqueValues = new HashSet<>();
        for (Map<String, String> singleData : data) {
            uniqueValues.add(singleData.get(sector) + singleData.get(route));
        }
        return uniqueValues;
    }

    public String searchResults(String value) {
        String listObj = null;
        try (PreparedStatement pstmt = connection.prepareStatement(GET_PARAMETER_VALUES())) {
            pstmt.setString(1, value);
            try (ResultSet resultSet = pstmt.executeQuery()) {
                if (resultSet.next()) {
                    listObj = resultSet.getString(PARAMETER_VALUE_COLUMN_NAME);
                }
            }
        } catch (SQLException e) {
            System.out.println(DATABASE_ACCESS_ERROR_MESSAGE);
            e.printStackTrace();
        }
        return listObj;
    }

    private  Validator getValidator(String tableName) {
        Validator validator;
        switch (tableName) {
            case CURVES_TABLE_NAME -> validator = new CurvesValidator(searchResults(LF_VALUE_KEY));
            case QP_FARES_TABLE_NAME -> validator = new FaresValidator();
            case PROFILE_FARES_TABLE_NAME -> validator = new ProfileValidator();
            default -> validator = null;
        }
        return validator;
    }

    private List<Map<String, String>> createListOfMap(List<List<String>> csvData, List<String> headers) {
        csvData.remove(0);
        List<Map<String, String>> data = new ArrayList<>();
        for (List<String> singleCsvData : csvData) {
            Map<String, String> map = new HashMap<>();
            for (int i = 0; i < headers.size(); i++) {
                map.put(headers.get(i), singleCsvData.get(i));
            }
            data.add(map);
        }
        return data;
    }

    private List<Map<String, String>> createListOfMapSeriesBlock(List<List<String>> csvData) {
        List<String> headers = csvData.get(0);
        csvData.remove(0);
        List<Map<String, String>> data = new ArrayList<>();
        DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");
        DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        for (List<String> singleCsvData : csvData) {
            String inputDate = singleCsvData.get(0);

            LocalDate date = LocalDate.parse(inputDate, inputFormatter);
            String departureDate = date.format(outputFormatter);

            for (int i = 1; i < singleCsvData.size(); i++) {
                Map<String, String> map = new HashMap<>();
                map.put("DepartureDate", departureDate);
                map.put("OrgCode", headers.get(i));
                map.put("SeatsBlocked", singleCsvData.get(i));
                data.add(map);
            }
        }
        return data;
    }

    public Map<String, Map<String, String>> getGridDropData(List<List<String>> csvData) {
        int ndoBandHeader = 0;
        int channelHeader = 1;
        int startColumn = 2;
        String[] gridValueNames = csvData.get(0).subList(startColumn, csvData.get(0).size()).toArray(new String[0]);
        return createTransformDataSet(csvData, ndoBandHeader, channelHeader, startColumn, NDO_BANDS_COLUMN_NAME,
                CHANNEL_COLUMN_NAME, gridValueNames, DLF_BANDS_COLUMN_NAME);
    }

    public Map<String, Map<String, String>> getDistressStrategy(List<List<String>> csvData) {
        int ndoBandHeader = 0;
        int startColumn = 1;
        String[] gridValueNames = csvData.get(0).subList(startColumn, csvData.get(0).size()).toArray(new String[0]);
        return createTransformDataSetDistress(csvData, ndoBandHeader, startColumn, DISTRESS_NDO_BANDS_COLUMN_NAME,
                gridValueNames, DISTRESS_DLF_BANDS_COLUMN_NAME);
    }

    public Map<String, Map<String, String>> getHourlyDropData(List<List<String>> csvData) {
        int ndoStartHeader = 0;
        int ndoEndHeader = 1;
        int startColumn = 2;
        String[] hourlyValueNames = csvData.get(0).subList(startColumn, csvData.get(0).size()).toArray(new String[0]);
        return createTransformDataSet(csvData, ndoStartHeader, ndoEndHeader, startColumn, NDO_START_COLUMN_NAME,
                NDO_END_COLUMN_NAME, hourlyValueNames, HOUR_COLUMN_NAME);
    }

    public Map<String, Map<String, String>> createTransformDataSet(List<List<String>> csvData, int key1Header,
                                                                   int key2Header, int valueStartColumn, String key1Name, String key2Name, String[] valueNames,
                                                                   String headerKey) {
        Map<String, Map<String, String>> resultMap = new LinkedHashMap<>();
        for (int i = 1; i < csvData.size(); i++) {
            List<String> inputRow = csvData.get(i);
            String key1 = csvData.get(i).get(key1Header);
            String key2 = csvData.get(i).get(key2Header);
            for (int j = valueStartColumn; j < inputRow.size(); j++) {
                String value = csvData.get(i).get(j);
                String valueName = valueNames[j - valueStartColumn];
                String keyVal = key1 + key2 + valueName;
                if (!resultMap.containsKey(keyVal)) {
                    Map<String, String> map = new HashMap<>();
                    map.put(key1Name, key1);
                    map.put(key2Name, key2);
                    map.put(headerKey, valueName);
                    resultMap.put(keyVal, map);
                }
                Map<String, String> map = resultMap.get(keyVal);
                map.put(VALUE_COLUMN_NAME, value);
            }
        }
        return resultMap;
    }

    public Map<String, Map<String, String>> createTransformDataSetDistress(List<List<String>> csvData, int key1Header,
                                                                           int valueStartColumn, String key1Name, String[] valueNames, String headerKey) {
        Map<String, Map<String, String>> resultMap = new LinkedHashMap<>();
        for (int i = 1; i < csvData.size(); i++) {
            List<String> inputRow = csvData.get(i);
            String key1 = csvData.get(i).get(key1Header);
            for (int j = valueStartColumn; j < inputRow.size(); j++) {
                String value = csvData.get(i).get(j);
                String valueName = valueNames[j - valueStartColumn];
                String keyVal = key1 + valueName;
                if (!resultMap.containsKey(keyVal)) {
                    Map<String, String> map = new HashMap<>();
                    map.put(key1Name, key1);
                    map.put(headerKey, valueName);
                    resultMap.put(keyVal, map);
                }
                Map<String, String> map = resultMap.get(keyVal);
                map.put(AU_VALUE_COLUMN_NAME, value);
            }
        }
        return resultMap;
    }

    public void uploadMarketListConnectionsFile(Reader file, String tableName, String userName, Object role,
                                                String obj_id) {
        List<String> methodScopedErrors = new ArrayList<>();
        List<MarketListConnectionsEntity> validMarketListConnectionsEntityList = new ArrayList<>();
        AtomicLong numOfRowsProcessed = new AtomicLong(0);

        try {
            List<String> permissions = getRoleAccess(role);
            String auditUuid = obj_id;

            insertAuditStatement(tableName, getCurrentDateTime(), userName, auditUuid);
            insertInputStatusTable(tableName);

            if (tableName.startsWith(MARKET_LIST_ADHOC_TABLE_NAME)) {
                createTable(tableName, MARKET_LIST_CONNECTIONS_TABLE_NAME);
            }

            List<List<String>> rawCsvData = getDataFromCSVFile(file);

            if (rawCsvData.isEmpty() || rawCsvData.size() <= 1) {
                methodScopedErrors.add("CSV file is empty or contains only headers. No data to process for Market List Connections.");
                error_occurred = 1;
                this.listOfErrors.addAll(methodScopedErrors);
                return;
            }

            List<List<String>> processedCsvData = splitConnectionsRowsByDateRange(rawCsvData, methodScopedErrors);
            numOfRowsProcessed.set(processedCsvData.isEmpty() ? 0 : Math.max(0, processedCsvData.size() - 1));

            for (int i = 1; i < processedCsvData.size(); i++) {
                List<String> record = processedCsvData.get(i);
                String uuidForEntry = generateUUID();
                int errorCountBefore = methodScopedErrors.size();

                String analystName = validationForMarketListConnectionsFile(record, i, permissions, role, userName, methodScopedErrors);

                if (methodScopedErrors.size() == errorCountBefore) {
                    createMarketListConnectionsEntity(record, validMarketListConnectionsEntityList, uuidForEntry, analystName);
                }
            }

            if (!methodScopedErrors.isEmpty()) {
                error_occurred = 1;
                this.listOfErrors.addAll(methodScopedErrors);
            }

            if (!validMarketListConnectionsEntityList.isEmpty()) {
                AuditHelper auditHelper = new AuditHelper(connection);
                LocalDateTime currentDateTime = ZonedDateTime.now(ZoneId.of(INDIAN_REGION)).toLocalDateTime();

                if (!tableName.startsWith(MARKET_LIST_ADHOC_TABLE_NAME)) {
                    try {
                        auditHelper.buildConnAuditLogs(validMarketListConnectionsEntityList, userName, currentDateTime, tableName);
                    } catch (Exception e) {
                        System.err.println("Failed to build connection audit logs for table: " + tableName + ". Error: " + e.getMessage());
                    }
                }

                if (tableName.startsWith(MARKET_LIST_ADHOC_TABLE_NAME)) {
                    deleteValues(Collections.singleton(ZERO), tableName, ZERO);
                } else {
                    deleteSpecificConnectionRecords(validMarketListConnectionsEntityList, tableName);
                }

                for (MarketListConnectionsEntity marketEntity : validMarketListConnectionsEntityList) {
                    insertIntoMarketListConnections(marketEntity, tableName);
                }

                s3FileStorageDao.storeFile(createCSV(tableName), tableName, auditUuid);
                updateAuditStatement(tableName, getCurrentDateTime(), auditUuid);

                if(error_occurred == 1) {
                    partial_upload_occurred = 1;
                }
            } else {
                if (error_occurred == 1) {
                    System.out.println(FILE_UPLOAD_ERROR_MESSAGE + ": No valid connection records to upload.");
                }
            }
        } catch (SQLException e) {
            error_occurred = 1;
            String sqlErrorMsg = "Database operation failed during market list connections upload";
            if (this.listOfErrors.isEmpty() || !this.listOfErrors.get(this.listOfErrors.size()-1).contains(e.getMessage())) {
                this.listOfErrors.add(sqlErrorMsg);
            }
            System.out.println("SQL Exception in uploadMarketListConnectionsFile: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            error_occurred = 1;
            String unexpectedErrorMsg = "Unexpected error during market list connections upload";
            this.listOfErrors.add(unexpectedErrorMsg);
            System.out.println("Unexpected error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (!this.listOfErrors.isEmpty()) {
                addErrorsToTable(this.listOfErrors, obj_id);
            }
            updateInputStatus(tableName);
        }
    }


    private void deleteSpecificConnectionRecords(List<MarketListConnectionsEntity> marketListConnectionsEntityList, String tableName) throws SQLException {
        if (marketListConnectionsEntityList.isEmpty()) {
            return;
        }
        String deleteSQL = "DELETE FROM " + tableName + " WHERE sector1 = ? AND flight1 = ? AND sector2 = ? AND flight2 = ? AND PerStart = ?";

        boolean autoCommitOriginalStatus = connection.getAutoCommit();
        try {
            connection.setAutoCommit(false);

            try (PreparedStatement pstmt = connection.prepareStatement(deleteSQL)) {
                for (MarketListConnectionsEntity entity : marketListConnectionsEntityList) {
                    pstmt.setString(1, entity.getSector1());
                    pstmt.setString(2, entity.getFlight1());
                    pstmt.setString(3, entity.getSector2());
                    pstmt.setString(4, entity.getFlight2());
                    pstmt.setString(5, entity.getStartDate());
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            error_occurred = 1;
            String errorMsg = "Error deleting existing connection records for replacement";
            System.out.println(DATABASE_ACCESS_ERROR_MESSAGE + " " + errorMsg);
            e.printStackTrace();
            this.listOfErrors.add(errorMsg);
            throw e;
        } finally {
            connection.setAutoCommit(autoCommitOriginalStatus);
        }
    }

    public void insertIntoMarketListConnections(MarketListConnectionsEntity marketListEntity, String tableName) {
        try (PreparedStatement pstmt = connection.prepareStatement(INSERT_INTO_MARKET_LIST_CONNECTIONS(tableName))) {
            pstmt.setString(1, marketListEntity.getSector1());
            pstmt.setString(2, marketListEntity.getSector2());
            pstmt.setString(3, marketListEntity.getFlight1());
            pstmt.setString(4, marketListEntity.getFlight2());
            pstmt.setString(5, marketListEntity.getPerType());
            pstmt.setString(6, marketListEntity.getStartDate());
            pstmt.setString(7, marketListEntity.getEndDate());
            pstmt.setInt(8, new BigInteger(marketListEntity.getDayOfWeek()).intValue());
            pstmt.setString(9, marketListEntity.getPriceStrategy());
            pstmt.setString(10, marketListEntity.getDiscountValue());
            pstmt.setInt(11, new BigInteger(marketListEntity.getFirstAllocation()).intValue());
            pstmt.setInt(12, new BigInteger(marketListEntity.getOtherAllocation()).intValue());
            pstmt.setInt(13, new BigInteger(marketListEntity.getB2bBackstop()).intValue());
            pstmt.setInt(14, new BigInteger(marketListEntity.getB2cBackstop()).intValue());
            pstmt.setString(15, marketListEntity.getB2bFactor());
            pstmt.setString(16, marketListEntity.getSkippingFactor());
            pstmt.setString(17, marketListEntity.getAnalystName());
            pstmt.setString(18, marketListEntity.getUuid());
            pstmt.setString(19, marketListEntity.getOutboundStop());
            pstmt.setString(20, marketListEntity.getOutboundDuration());
            pstmt.setString(21, marketListEntity.getCarrierExclusionB2B());
            pstmt.setString(22, marketListEntity.getCarrierExclusionB2C());
            pstmt.setString(23, marketListEntity.getCurrency());
            pstmt.setString(24, marketListEntity.getFareAnchor());
            pstmt.setString(25, marketListEntity.getOffset());
            pstmt.setString(26, marketListEntity.getDiscountFlag());
            pstmt.setString(27, marketListEntity.getSosFlag());

            pstmt.executeUpdate();
        } catch (SQLException e) {
            error_occurred = 1;
            String errorMsg = "Failed to insert market list connection for sectors " + marketListEntity.getSector1() + "/" + marketListEntity.getSector2() +
                    ", flights " + marketListEntity.getFlight1() + "/" + marketListEntity.getFlight2() +
                    " on " + marketListEntity.getStartDate();
            System.out.println(DATABASE_ACCESS_ERROR_MESSAGE + ": " + errorMsg);
            e.printStackTrace();
            this.listOfErrors.add(errorMsg);
        }
    }

    private void deleteRecordsConnections(List<MarketListConnectionsEntity> marketListConnectionsEntityList,
                                          String tableName) {
        Set<String> uuid = new HashSet<>();
        for (MarketListConnectionsEntity marketEntity : marketListConnectionsEntityList) {
            List<Map<String, Object>> listOfResult = searchMarketListConnections(marketEntity, tableName);
            for (Map<String, Object> mapOfResult : listOfResult) {
                uuid.add(mapOfResult.get(UUID_COLUMN_NAME).toString());
            }
        }
        deleteValues(uuid, tableName, UUID_COLUMN_NAME);
    }

    public List<Map<String, Object>> searchMarketListConnections(MarketListConnectionsEntity marketListEntity,
                                                                 String tableName) {
        List<Map<String, Object>> listObj = new ArrayList<>();
        try (PreparedStatement pstmt = connection.prepareStatement(SELECT_MARKET_LIST_CONNECTIONS(tableName))) {
            pstmt.setString(1, marketListEntity.getSector1());
            pstmt.setString(2, marketListEntity.getSector2());
            pstmt.setString(3, marketListEntity.getAnalystName());
            try (ResultSet resultSet = pstmt.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> row = new HashMap<>();
                    row.put(UUID_COLUMN_NAME, resultSet.getObject(UUID_COLUMN_NAME));
                    listObj.add(row);
                }
            }
        } catch (SQLException e) {
            System.out.println(DATABASE_ACCESS_ERROR_MESSAGE);
            e.printStackTrace();
        }
        return listObj;
    }

    private static void createMarketListConnectionsEntity(List<String> record,
                                                          List<MarketListConnectionsEntity> MarketListConnectionsEntityList, String uuid, String analystName) {
        MarketListConnectionsEntityList.add(MarketListConnectionsEntity.builder()
                .uuid(uuid)
                .sector1(record.get(CSV_HEADER_CONNECTIONS_SECTOR1))
                .sector2(record.get(CSV_HEADER_CONNECTIONS_SECTOR2))
                .flight1(record.get(CSV_HEADER_CONNECTIONS_FLIGHT1))
                .flight2(record.get(CSV_HEADER_CONNECTIONS_FLIGHT2))
                .perType(PER_TYPE_VALUE)
                .startDate(record.get(CSV_HEADER_CONNECTIONS_PERSTART))
                .endDate(record.get(CSV_HEADER_CONNECTIONS_PEREND))
                .dayOfWeek(record.get(CSV_HEADER_CONNECTIONS_DOW))
                .priceStrategy(record.get(CSV_HEADER_CONNECTIONS_PRICESTRATEGY))
                .discountValue(record.get(CSV_HEADER_CONNECTIONS_DISCOUNTVALUE))
                .firstAllocation(record.get(CSV_HEADER_CONNECTIONS_FIRSTRBDALLOC))
                .otherAllocation(record.get(CSV_HEADER_CONNECTIONS_OTHERRBDALLOC))
                .b2bBackstop(record.get(CSV_HEADER_CONNECTIONS_B2BBACKSTOP))
                .b2cBackstop(record.get(CSV_HEADER_CONNECTIONS_B2CBACKSTOP))
                .b2bFactor(record.get(CSV_HEADER_CONNECTIONS_B2BFACTOR))
                .skippingFactor(record.get(CSV_HEADER_CONNECTIONS_SKIPPINGFACTOR))
                .outboundStop(record.get(CSV_HEADER_CONNECTIONS_OUTBOUNDSTOP))
                .outboundDuration(record.get(CSV_HEADER_CONNECTIONS_OUTBOUNDDURATION))
                .currency(record.get(CSV_HEADER_CONNECTIONS_CURRENCY))
                .fareAnchor(record.get(CSV_HEADER_CONNECTIONS_FAREANCHOR))
                .offset(record.get(CSV_HEADER_CONNECTIONS_OFFSET))
                .discountFlag(record.get(CSV_HEADER_CONNECTIONS_DISCOUNTFLAG))
                .analystName(analystName)
                .carrierExclusionB2B(record.get(CSV_HEADER_CONNECTIONS_CARRIEREXCLB2B))
                .carrierExclusionB2C(record.get(CSV_HEADER_CONNECTIONS_CARRIEREXCLB2C))
                .sosFlag(record.get(CSV_HEADER_CONNECTIONS_SOSFLAG))
                .build());
    }

    public void uploadPartialDateEvent(Reader file, String userName, String obj_id, List<String> listOfErrors) throws RuntimeException {
        String uuid = obj_id;
        try {
            String dateEventTempTableName = DATE_EVENT_TABLE_NAME + UNDERSCORE + TEMP;
            String ProfileFareStationUpsellTempTableName = CONFIG_PROFILE_FARE_STATION_UPSELL + UNDERSCORE + TEMP;

            insertAuditStatement(DATE_EVENT_TABLE_NAME, getCurrentDateTime(), userName, uuid);
            insertAuditStatement(CONFIG_PROFILE_FARE_STATION_UPSELL, getCurrentDateTime(), userName, uuid);

            createTable(dateEventTempTableName, DATE_EVENT_TABLE_NAME);
            createTable(ProfileFareStationUpsellTempTableName, CONFIG_PROFILE_FARE_STATION_UPSELL);

            insertInputStatusTable(DATE_EVENT_TABLE_NAME);
            insertInputStatusTable(CONFIG_PROFILE_FARE_STATION_UPSELL);

            List<List<String>> csvData = getDataFromCSVFile(file);

            Map<String, Map<String, String>> dateEventMap = getPartialDateEvent(csvData, listOfErrors);

            if (!listOfErrors.isEmpty()) {
                updateInputStatus(DATE_EVENT_TABLE_NAME);
                error_occurred = 1;
                addErrorsToTable(listOfErrors, obj_id);
                System.out.println(FILE_UPLOAD_ERROR_MESSAGE);
                throw new DataMismatchedException(listOfErrors);
            }

            List<String> dateEventHeaders = Arrays.asList(getColumnHeaders(DATE_EVENT_TABLE_NAME).split(COMMA));
            List<String> upsellHeaders = Arrays.asList(getColumnHeaders(CONFIG_PROFILE_FARE_STATION_UPSELL).split(COMMA));
            List<Map<String, String>> data = createListOfMapDateEvent(csvData, dateEventHeaders);

            insertStatement(new ArrayList<>(dateEventMap.values()), upsellHeaders,
                    ProfileFareStationUpsellTempTableName);
            insertStatement(data, dateEventHeaders, dateEventTempTableName);

            String columnName = SECTOR_COLUMN_NAME.toLowerCase();
            Set<String> uniqueValues = dateEventMap.values().stream()
                    .map(obj -> obj.get(columnName))
                    .collect(Collectors.toSet());

            Set<String> uniqueDates = data.stream()
                    .map(map -> map.get(DATE_COLUMN_NAME))
                    .collect(Collectors.toSet());

            deleteValues(uniqueDates, DATE_EVENT_TABLE_NAME, DATE_COLUMN_NAME);

            for (String sector : uniqueValues) {
                deletePartialSectorValues(uniqueDates, sector, CONFIG_PROFILE_FARE_STATION_UPSELL);
            }

            copyFromTempToMainTable(CONFIG_PROFILE_FARE_STATION_UPSELL, ProfileFareStationUpsellTempTableName);
            copyFromTempToMainTable(DATE_EVENT_TABLE_NAME, dateEventTempTableName);

            updateAuditStatement(CONFIG_PROFILE_FARE_STATION_UPSELL, getCurrentDateTime(), uuid);
            updateAuditStatement(DATE_EVENT_TABLE_NAME, getCurrentDateTime(), uuid);

            deleteTable(ProfileFareStationUpsellTempTableName);
            deleteTable(dateEventTempTableName);

            error_occurred = 0;
        } catch (Exception e) {
            error_occurred = 1;
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            updateInputStatus(CONFIG_PROFILE_FARE_STATION_UPSELL);
            updateInputStatus(DATE_EVENT_TABLE_NAME);
        }
    }

    public Map<String, Map<String, String>> getPartialDateEvent(List<List<String>> csvData, List<String> listOfErrors) {
        Map<String, Map<String, String>> strategyMap = new LinkedHashMap<>();
        int dateHeader = 0;
        int startColumn = 3;

        for (int i = 1; i < csvData.size(); i++) {
            List<String> inputRow = csvData.get(i);
            if (inputRow.size() <= dateHeader) continue;

            String dateKey = inputRow.get(dateHeader).trim();
            if (dateKey == null || dateKey.trim().isEmpty()) continue;
            if (!dateKey.matches("\\d{2}/\\d{2}/\\d{4}")) {
                listOfErrors.add(dateKey + ": Date should should have mm/dd/yyyy format");
            }

            for (int j = startColumn; j < inputRow.size(); j++) {
                if (j >= csvData.get(0).size()) continue;

                String stationKey = csvData.get(0).get(j).trim();
                if (stationKey == null || stationKey.trim().isEmpty()) continue;
                if (i == 1 && !(stationKey.matches("[A-Za-z]{6}"))) {
                    listOfErrors.add(stationKey + ": Sector value should be valid");
                }

                String value = (j < inputRow.size()) ? inputRow.get(j).trim() : "";
                String keyVal = dateKey + stationKey;

                if (!(stationKey.equals("Date") || stationKey.equals("Mapped Month") || stationKey.equals("Event"))) {
                    if (!((value == null) || (value.isEmpty()) || (value.matches("-?\\d+%")))) {
                        listOfErrors.add(dateKey + "_" + stationKey + ": Upsell value must be a valid Integer value followed by %");
                    }
                }

                Map<String, String> map = new HashMap<>();
                map.put(DATE_COLUMN_NAME, dateKey);
                map.put(SECTOR_COLUMN_NAME.toLowerCase(), stationKey);
                map.put(VALUE_COLUMN_NAME, value);
                strategyMap.put(keyVal, map);
            }
        }
        return strategyMap;
    }

    public void deletePartialSectorValues(Set<String> dates, String sector, String tableName) {
        if (dates.isEmpty()) return;

        String placeholders = dates.stream().map(v -> "?").collect(Collectors.joining(", "));

        String deleteQuery = "DELETE FROM " + tableName +
                " WHERE " + DATE_COLUMN_NAME + " IN (" + placeholders + ")" +
                " AND " + SECTOR_COLUMN_NAME.toLowerCase() + " = ?";

        try (PreparedStatement pstmt = connection.prepareStatement(deleteQuery)) {
            int index = 1;
            for (String date : dates) {
                pstmt.setString(index++, date);
            }

            pstmt.setString(index, sector);

            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println("Error deleting sector values: " + e.getMessage());
        }
    }

    public void uploadDateEventFile(Reader file, String userName, String obj_id,UploadFileType fileType) throws RuntimeException {
        String uuid = obj_id;
        String dateEventTempTableName = DATE_EVENT_TABLE_NAME + UNDERSCORE + TEMP;

        String secondaryTableName;
        String secondaryTempTableName;

        if (fileType == UploadFileType.DISTRESS) {
            secondaryTableName = DISTRESS_CLAENDAR;
        } else {
            secondaryTableName = CONFIG_PROFILE_FARE_STATION_UPSELL;
        }
        secondaryTempTableName = secondaryTableName + UNDERSCORE + TEMP;

        try {
            insertAuditStatement(DATE_EVENT_TABLE_NAME, getCurrentDateTime(), userName, uuid);
            insertAuditStatement(secondaryTableName, getCurrentDateTime(), userName, uuid);

            createTable(dateEventTempTableName, DATE_EVENT_TABLE_NAME);
            createTable(secondaryTempTableName, secondaryTableName);

            insertInputStatusTable(DATE_EVENT_TABLE_NAME);
            insertInputStatusTable(secondaryTableName);

            List<List<String>> csvData = getDataFromCSVFile(file);
            Map<String, Map<String, String>> dateEventMap = getDateEvent(csvData);

            List<String> dateEventHeaders = Arrays.asList(getColumnHeaders(DATE_EVENT_TABLE_NAME).split(COMMA));
            List<String> secondaryTableHeaders = Arrays
                    .asList(getColumnHeaders(secondaryTableName).split(COMMA));

            List<Map<String, String>> dataForDateEventTable = createListOfMapDateEvent(csvData, dateEventHeaders);

            List<Map<String, String>> dataForSecondaryTable = new ArrayList<>(dateEventMap.values());

            insertStatement(dataForSecondaryTable, secondaryTableHeaders,
                    secondaryTempTableName);
            insertStatement(dataForDateEventTable, dateEventHeaders, dateEventTempTableName);

            String columnNameForDeletion = SECTOR_COLUMN_NAME.toLowerCase();
            Set<String> uniqueValuesForSecondaryTableDeletion = dataForSecondaryTable.stream()
                    .map(obj -> obj.get(columnNameForDeletion))
                    .filter(java.util.Objects::nonNull)
                    .collect(Collectors.toSet());

            deleteValues(Collections.singleton(ZERO), DATE_EVENT_TABLE_NAME, ZERO);
            if (!uniqueValuesForSecondaryTableDeletion.isEmpty()) {
                deleteValues(uniqueValuesForSecondaryTableDeletion, secondaryTableName, columnNameForDeletion);
            }


            copyFromTempToMainTable(secondaryTableName, secondaryTempTableName);
            copyFromTempToMainTable(DATE_EVENT_TABLE_NAME, dateEventTempTableName);

            updateAuditStatement(secondaryTableName, getCurrentDateTime(), uuid);
            updateAuditStatement(DATE_EVENT_TABLE_NAME, getCurrentDateTime(), uuid);

            deleteTable(secondaryTempTableName);
            deleteTable(dateEventTempTableName);
            error_occurred = 0;
        } catch (Exception e) {
            throw new RuntimeException("Error during date event file upload: " + e.getMessage(), e);
        } finally {
            updateInputStatus(secondaryTableName);
            updateInputStatus(DATE_EVENT_TABLE_NAME);
        }
    }

    public void deleteTable(String tableName) {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(DELETE_TABLE(tableName));
        } catch (SQLException e) {
            System.out.println("Failed to delete table");
            e.printStackTrace();
        }
    }

    public void copyFromTempToMainTable(String tableName, String tempTableName) {
        String sql = COPY_DATA_INTO_MAIN_TABLE(tableName, tempTableName);
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            System.out.println("Failed to copy data from temp to main table");
            e.printStackTrace();
        }
    }

    public List<Map<String, String>> createListOfMapDateEvent(List<List<String>> csvData, List<String> headers) {
        csvData.remove(0);
        List<Map<String, String>> data = new ArrayList<>();
        for (List<String> singleCsvData : csvData) {
            String date = singleCsvData.get(0).trim();
            String dayOfWeek = getAbbreviatedDayOfWeek(parseDate(date));

            singleCsvData.add(1, dayOfWeek);

            Map<String, String> map = new HashMap<>();
            for (int i = 0; i < headers.size(); i++) {
                map.put(headers.get(i), singleCsvData.get(i).trim());
            }
            data.add(map);
        }
        return data;
    }

    public static Date parseDate(String dateString) {
        try {
            return SIMPLE_DATE_OUTPUT_FORMATTER_2.parse(dateString);
        } catch (ParseException e) {
            return null;
        }
    }

    public static String getAbbreviatedDayOfWeek(Date date) {
        return SIMPLE_DAY_FORMATTER.format(date);
    }

    public Map<String, Map<String, String>> getDateEvent(List<List<String>> csvData) {
        Map<String, Map<String, String>> strategyMap = new LinkedHashMap<>();
        int dateHeader = 0;
        int startColumn = 3;
        for (int i = 1; i < csvData.size(); i++) {
            List<String> inputRow = csvData.get(i);
            String dateKey = csvData.get(i).get(dateHeader);
            for (int j = startColumn; j < inputRow.size()-1; j++) {
                String value = csvData.get(i).get(j);
                String stationKey = csvData.get(0).get(j);
                String keyVal = dateKey + stationKey;
                if (!strategyMap.containsKey(keyVal) && !value.equalsIgnoreCase(EMPTY)) {
                    Map<String, String> map = new HashMap<>();
                    map.put(DATE_COLUMN_NAME, dateKey);
                    map.put(SECTOR_COLUMN_NAME.toLowerCase(), stationKey);
                    map.put(VALUE_COLUMN_NAME, value);
                    strategyMap.put(keyVal, map);
                }
            }
        }
        return strategyMap;
    }

    public void insertStatement(List<Map<String, String>> data, List<String> columnHeaders, String tableName)
            throws Exception {
        StringBuilder columnNames = new StringBuilder();
        StringBuilder placeholders = new StringBuilder();
        for (String header : columnHeaders) {
            columnNames.append("`"+header+"`").append(",");
            placeholders.append("?,");
        }

        String insertSql = "INSERT INTO " + tableName + " (" + columnNames.substring(0, columnNames.length() - 1)
                + ") VALUES (" + placeholders.substring(0, placeholders.length() - 1) + ")";

        try {
            connection2.setAutoCommit(false);
            try (PreparedStatement pstmt = connection2.prepareStatement(insertSql)) {
                int count = 0;
                for (Map<String, String> dataMap : data) {
                    for (int i = 0; i < columnHeaders.size(); i++) {
                        pstmt.setString(i + 1, dataMap.get(columnHeaders.get(i)));
                    }
                    pstmt.addBatch();
                    if (++count % 10000 == 0) {
                        pstmt.executeBatch();
                        connection2.commit();
                        System.out.println("10000 records inserted successfully.");
                    }
                }
                if (count % 10000 != 0) {
                    pstmt.executeBatch();
                    connection2.commit();
                    System.out.println(count % 10000 + " records inserted successfully.");
                }
            } catch (SQLException e) {
                connection2.rollback();
                System.out.println("Failed to execute batch insert, transaction rolled back.");
                e.printStackTrace();
                throw e;
            }
        } catch (SQLException e) {
            System.out.println("Database connection or transaction error:");
            e.printStackTrace();
            throw e;
        }
    }

    public String getColumnHeaders(String value) {
        String listObj = null;
        if (SALESINV_RBD_DISTRIBUTION_TABLE_NAME.equalsIgnoreCase(value)) {
            return String.join(",", SI_RBD_SEATS_COLUMN_NAME, SI_RBD_TYPE_COLUMN_NAME, SI_RBD_RBD_COLUMN_NAME, SI_RBD_VALUE_COLUMN_NAME);
        }
        if (SALES_INVENTORY_TABLE_NAME.equalsIgnoreCase(value)) {
            return String.join(",", SALES_DB_DATE, SALES_DB_MINVALUE, SALES_DB_MAXVALUE, SALES_DB_SEATSALLOCATION);
        }
        if (value.startsWith(MARKET_LIST_SALESINV_TABLE_NAME)) {
            return "Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,CurveID,saleflag";
        }
        try (PreparedStatement pstmt = connection.prepareStatement(GET_COLUMN_HEADERS())) {
            pstmt.setString(1, value);
            try (ResultSet resultSet = pstmt.executeQuery()) {
                if (resultSet.next()) {
                    listObj = resultSet.getString(COLUMNS_COLUMN_NAME);
                }
            }
        } catch (SQLException e) {
            System.out.println(DATABASE_ACCESS_ERROR_MESSAGE);
            e.printStackTrace();
        }
        if (listObj == null) {
            System.err.println("Warning: Column headers not found for table identifier: " + value + ". Returning empty string.");
            return "";
        }
        return listObj;
    }

    public void deleteValuesStrategyTable(Set<String> value, String tableName, String strategyColumnName,
                                          String channelColumnName) {
        if (!value.isEmpty()) {
            String placeholders = value.stream().map(v -> "?").collect(Collectors.joining(", "));
            String deleteQuery = "DELETE FROM " + tableName + " WHERE concat(" + strategyColumnName + ","
                    + channelColumnName + ") IN (" + placeholders + ")";
            try (PreparedStatement pstmt = connection.prepareStatement(deleteQuery)) {
                int index = 1;
                for (String val : value) {
                    pstmt.setString(index++, val);
                }
                pstmt.executeUpdate();
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    public void deleteValuesAutoB2BTable(Set<String> value, String tableName, String columnName) {
        if (!value.isEmpty()) {
            String placeholders = value.stream().map(v -> "?").collect(Collectors.joining(", "));
            String deleteQuery = "DELETE FROM " + tableName + " WHERE " + columnName + " IN (" + placeholders + ")";
            try (PreparedStatement pstmt = connection.prepareStatement(deleteQuery)) {
                int index = 1;
                for (String val : value) {
                    pstmt.setString(index++, val);
                }
                pstmt.executeUpdate();
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    private Set<String> getUniqueStrategies(Map<String, Map<String, String>> strategyMap) {
        Set<String> strategy = new HashSet<>();
        for (Map<String, String> strategies : strategyMap.values()) {
            strategy.add(strategies.get(STRATEGY_COLUMN_NAME) + strategies.get(CHANNEL_COLUMN_NAME));
        }
        return strategy;
    }
    private Set<String> getUniqueNames(Map<String, Map<String, String>> strategyMap) {
        Set<String> strategy = new HashSet<>();
        for (Map<String, String> strategies : strategyMap.values()) {
            strategy.add(strategies.get(AUTO_B2B_NAME_COLUMN_NAME));
        }
        return strategy;
    }

    public Map<String, Map<String, String>> getStrategyGrid(List<List<String>> csvData,
                                                            Map<String, String> headerColumnMap, List<String> listOfErrors2) throws DataMismatchedException {
        Map<String, Map<String, String>> strategyMap = new LinkedHashMap<>();
        int strategyGridHeader = 0;
        int channelHeader = 1;
        int decisionGridHeader = 2;
        int ndoBandHeader = 3;
        int startColumn = 4;
        Set<String> strategyCounter = new HashSet<>();
        int countTotalValues = 0;
        for (int i = 1; i < csvData.size(); i++) {
            List<String> inputRow = csvData.get(i);
            String strategyKey = csvData.get(i).get(strategyGridHeader);
            strategyCounter.add(strategyKey);

            String decision = csvData.get(i).get(decisionGridHeader);
            String ndoBandKey = csvData.get(i).get(ndoBandHeader);
            String channelKey = csvData.get(i).get(channelHeader);
            for (int j = startColumn; j < inputRow.size(); j++) {
                String value = csvData.get(i).get(j);
                String dlfBand = csvData.get(0).get(j);
                String keyVal = strategyKey + ndoBandKey + dlfBand + channelKey;
                if (!strategyMap.containsKey(keyVal)) {
                    Map<String, String> map = new HashMap<>();
                    map.put(STRATEGY_COLUMN_NAME, strategyKey);
                    map.put(NDO_BAND_COLUMN_NAME, ndoBandKey);
                    map.put(DPLF_BAND_COLUMN_NAME, dlfBand);
                    map.put(CHANNEL_COLUMN_NAME, channelKey);
                    strategyMap.put(keyVal, map);
                }
                Map<String, String> map = strategyMap.get(keyVal);

                if (!validationForStrategyGrid(decision, value)) {
                    error_occurred = 1;
                    updateInputStatus(STRATEGY_TABLE_NAME);
                    String errorMsg = "Line " + (i+1) + ": " + INVALID_DATA_ERROR_MESSAGE
                            + " for " + strategyKey + " with decision :" + decision + " having value as " + value;
                    listOfErrors2.add(errorMsg);
                }
                map.put(headerColumnMap.get(decision), value);
                countTotalValues++;
            }
        }
        findActualCountBetweenRowsAndColumns(strategyCounter, countTotalValues, listOfErrors2);

        if (!listOfErrors2.isEmpty()) {
            throw new DataMismatchedException(listOfErrors2);
        }

        return strategyMap;
    }
    public Map<String, Map<String, String>> getAutoB2BGrid(List<List<String>> csvData,
                                                           Map<String, String> headerColumnMap, List<String> listOfErrors2) throws DataMismatchedException {
        Map<String, Map<String, String>> autob2bMap = new LinkedHashMap<>();
        int nameHeader = 0;
        int forecastBandHeader = 2;
        int ndoBandHeader = 1;
        int startColumn = 3;
        Set<String> counter = new HashSet<>();
        int countTotalValues = 0;
        for (int i = 1; i < csvData.size(); i++) {
            List<String> inputRow = csvData.get(i);
            String nameKey = csvData.get(i).get(nameHeader);
            counter.add(nameKey);

            String ndoBandKey = csvData.get(i).get(ndoBandHeader);
            String forecastBandKey = csvData.get(i).get(forecastBandHeader);
            for (int j = startColumn; j < inputRow.size(); j++) {
                String value = csvData.get(i).get(j);
                String dlfBand = csvData.get(0).get(j);
                String keyVal = nameKey + ndoBandKey + forecastBandKey + dlfBand;
                if (!autob2bMap.containsKey(keyVal)) {
                    Map<String, String> map = new HashMap<>();
                    map.put(AUTO_B2B_NAME_COLUMN_NAME, nameKey);
                    map.put(NDO_BAND_COLUMN_NAME, ndoBandKey);
                    map.put(FORECAST_BAND_COLUMN_NAME, forecastBandKey);
                    map.put(DPLF_BAND_COLUMN_NAME, dlfBand);
                    autob2bMap.put(keyVal, map);
                }
                Map<String, String> map = autob2bMap.get(keyVal);

                if (!value.matches("^(-1(\\.0+)?|0(\\.\\d+)?|1(\\.0+)?)$")) {
                    listOfErrors2.add(nameKey + ": auto_b2b_factor should either be -1 or witin the range of 0 to 1");
                    continue;
                }
                map.put(AUTO_B2B_FACTOR_COLUMN_NAME, value);
                countTotalValues++;
            }
        }
        findActualCountForAutoB2BFactor(counter, countTotalValues, listOfErrors2);

        if (!listOfErrors2.isEmpty()) {
            throw new DataMismatchedException(listOfErrors2);
        }

        return autob2bMap;
    }

    private void findActualCountBetweenRowsAndColumns(Set<String> strategyCounter, int countTotalValues,
                                                      List<String> errorList) throws DataMismatchedException {
        int dplfBand = getCountOfEntries(DPLF_BANDS_TABLE);
        int ndoBand = getCountOfEntries(NDO_BANDS_TABLE);
        int expectedValueCount = strategyCounter.size() * dplfBand * ndoBand * NUMBER_OF_DECISIONS * 2;
        if (expectedValueCount != countTotalValues) {
            updateInputStatus(STRATEGY_TABLE_NAME);
            errorList.add(INVALID_NUMBER_OF_ROW_COLUMN_ERROR_MESSAGE);
            throw new DataMismatchedException(Collections.singletonList(INVALID_NUMBER_OF_ROW_COLUMN_ERROR_MESSAGE));
        }
    }

    private void findActualCountForAutoB2BFactor(Set<String> strategyCounter, int countTotalValues,
                                                 List<String> errorList) throws DataMismatchedException {
        int dplfBand = getCountOfEntries(DPLF_BANDS_TABLE);
        int ndoBand = getCountOfEntries(NDO_BANDS_TABLE);
        int forecastBand = getCountOfEntries(FORECAST_BANDS_TABLE_NAME);
        int expectedValueCount = strategyCounter.size() * dplfBand * ndoBand * forecastBand;
        if (expectedValueCount != countTotalValues) {
            updateInputStatus(STRATEGY_TABLE_NAME);
            errorList.add(INVALID_NUMBER_OF_ROW_COLUMN_ERROR_MESSAGE);
            throw new DataMismatchedException(Collections.singletonList(INVALID_NUMBER_OF_ROW_COLUMN_ERROR_MESSAGE));
        }
    }

    public int getCountOfEntries(String tableName) {
        int count = 0;
        try (Statement stmt = connection.createStatement();
             ResultSet resultSet = stmt.executeQuery(COUNT_OF_RECORDS(tableName))) {
            if (resultSet.next()) {
                count = resultSet.getInt(COUNT_COLUMN_NAME);
            }
        } catch (SQLException e) {
            System.out.println(DATABASE_ACCESS_ERROR_MESSAGE);
            e.printStackTrace();
        }

        return count;
    }

    public boolean validationForStrategyGrid(String decision, String value) {
        if (decision.equalsIgnoreCase(CSV_HEADER_TIME)) {
            return getListOfTimeRange().contains(value);
        }
        if (decision.equalsIgnoreCase(CSV_HEADER_CRITERIA)) {
            return getListOfCriteria().contains(value);
        }
        if (decision.equalsIgnoreCase(CSV_HEADER_OFFSET)) {
            return getListOfOffset().contains(Integer.parseInt(value));
        }
        return false;
    }

    public List<String> getListOfTimeRange() {
        List<Map<String, Object>> listOfTimeRange = findAllRecordsFromTable(TIME_RANGE_TABLE,
                Collections.singletonList(TIME_RANGE_COLUMN_NAME));
        List<String> timeRangeValues = new ArrayList<>();
        for (Map<String, Object> timeRange : listOfTimeRange) {
            timeRangeValues.add(timeRange.get(TIME_RANGE_COLUMN_NAME).toString());
        }
        return timeRangeValues;
    }

    public List<String> getListOfCriteria() {
        List<Map<String, Object>> listOfCriteria = findAllRecordsFromTable(CRITERIA_TABLE,
                Collections.singletonList(CRITERIA_COLUMN_NAME));
        List<String> criteriaValues = new ArrayList<>();
        for (Map<String, Object> criteria : listOfCriteria) {
            criteriaValues.add(criteria.get(CRITERIA_COLUMN_NAME).toString());
        }
        return criteriaValues;
    }

    public List<Integer> getListOfOffset() {
        return IntStream
                .rangeClosed(OFFSET_START_VALUE, OFFSET_END_VALUE)
                .boxed()
                .sorted()
                .collect(Collectors.toList());
    }

    public List<Map<String, Object>> findAllRecordsFromTable(String tableName, List<String> headers, int limit) {
        List<Map<String, Object>> listObj = new ArrayList<>();

        List<String> booleanHeaders = Arrays.asList(
                "OwnFareFlag", "AutoTimeRangeFlag", "openingFares", "OverBooking",
                "profileFares", "rbdPushFlag", "distressInventoryFlag", "seriesBlock",
                "autoGroup", "autoBackstopFlag");

        String headerValues = "`" + String.join("`,`", headers) + "`";
        String limitCondition = (limit > 0) ? " LIMIT " + limit : "";
        String query = "SELECT " + headerValues + " FROM " + tableName + limitCondition;

        try (Statement stmt = connection.createStatement();
             ResultSet resultSet = stmt.executeQuery(query)) {
            while (resultSet.next()) {
                Map<String, Object> row = new HashMap<>();
                for (String header : headers) {
                    Object value = resultSet.getObject(header);

                    if (booleanHeaders.contains(header) && value != null) {
                        String stringValue = value.toString();
                        if (stringValue.equals("1")) {
                            value = "Y";
                        } else if (stringValue.equals("0")) {
                            value = "N";
                        }
                    }

                    row.put(header, value);
                }
                listObj.add(row);
            }
        } catch (SQLException e) {
            System.out.println(DATABASE_ACCESS_ERROR_MESSAGE);
            e.printStackTrace();
        }

        return listObj;
    }

    public List<Map<String, Object>> findAllRecordsFromTable(String tableName, List<String> headers) {
        return findAllRecordsFromTable(tableName, headers, 0);
    }

    public int getCurrentInputStatus(String name) {
        List<Map<String, Object>> listObj = new ArrayList<>();
        try (PreparedStatement preparedStatement = connection.prepareStatement(GET_CURRENT_INPUT_STATUS())) {
            preparedStatement.setString(1, name);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> row = new HashMap<>();
                    int columnCount = resultSet.getMetaData().getColumnCount();
                    for (int i = 1; i <= columnCount; i++) {
                        String column = resultSet.getMetaData().getColumnName(i);
                        Object value = resultSet.getObject(i);
                        row.put(column, value);
                    }
                    listObj.add(row);
                }
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return listObj.size();
    }

    public void updateInputStatus(String name) {
        int value = 0;
        try (PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_INPUT_STATUS(value, name))) {
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void updateAuditStatement(String tableName, String endTime, String uuid) {
        try {
            try (PreparedStatement pstmt = connection.prepareStatement(UPDATE_FILE_UPLOAD())) {
                pstmt.setString(1, endTime);
                pstmt.setString(2, uuid);
                pstmt.setString(3, tableName);
                pstmt.executeUpdate();
            }
            int count = 0;
            try (PreparedStatement pstmt = connection.prepareStatement(GET_CURRENT_VERSION_COUNT())) {
                pstmt.setString(1, tableName);
                ResultSet rs = pstmt.executeQuery();
                if (rs.next()) {
                    count = rs.getInt(COUNT_COLUMN_NAME);
                }
            }
            if (count == 1) {
                try (PreparedStatement pstmt = connection.prepareStatement(UPDATE_CURRENT_VERSION())) {
                    pstmt.setString(1, uuid);
                    pstmt.setString(2, tableName);
                    pstmt.executeUpdate();
                }
            } else {
                try (PreparedStatement pstmt = connection.prepareStatement(INSERT_CURRENT_VERSION())) {
                    pstmt.setString(1, uuid);
                    pstmt.setString(2, tableName);
                    pstmt.executeUpdate();
                }
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void insertIntoMarketList(MarketListEntity marketListEntity, String tableName) {
        try (PreparedStatement pstmt = connection.prepareStatement(INSERT_INTO_MARKET_LIST(tableName))) {
            pstmt.setString(1, marketListEntity.getOrigin());
            pstmt.setString(2, marketListEntity.getDestination());
            pstmt.setString(3, marketListEntity.getFlightNo());
            pstmt.setString(4, marketListEntity.getPerType());
            pstmt.setString(5, marketListEntity.getStartDate());
            pstmt.setString(6, marketListEntity.getEndDate());
            pstmt.setInt(7, new BigInteger(marketListEntity.getDayOfWeek()).intValue());
            pstmt.setString(8, marketListEntity.getHardAnchor());
            pstmt.setString(9, marketListEntity.getPlfThreshold());
            pstmt.setString(10, marketListEntity.getStartTime());
            pstmt.setString(11, marketListEntity.getEndTime());
            pstmt.setString(12, marketListEntity.getCurveId());
            pstmt.setString(13, marketListEntity.getCarrExclusionB2B());
            pstmt.setString(14, marketListEntity.getCarrExclusionB2C());
            pstmt.setString(15, marketListEntity.getFlightExclusionB2B());
            pstmt.setString(16, marketListEntity.getFlightExclusionB2C());
            pstmt.setString(17, marketListEntity.getFareAnchor());
            pstmt.setString(18, marketListEntity.getFareOffset());
            pstmt.setInt(19, new BigInteger(marketListEntity.getFirstAllocation()).intValue());
            pstmt.setInt(20, new BigInteger(marketListEntity.getOtherAllocation()).intValue());
            pstmt.setInt(21, new BigInteger(marketListEntity.getB2bBackstop()).intValue());
            pstmt.setInt(22, new BigInteger(marketListEntity.getB2cBackstop()).intValue());
            pstmt.setString(23, marketListEntity.getB2bFactor());
            pstmt.setString(24, marketListEntity.getOBSeats());
            pstmt.setString(25, marketListEntity.getOBFare());
            pstmt.setString(26, marketListEntity.getSkippingFactor());
            pstmt.setString(27, marketListEntity.getDaySpan());
            pstmt.setString(28, marketListEntity.getAutoTimeRangeFlag());
            pstmt.setString(29, marketListEntity.getAnalystName());
            pstmt.setString(30, marketListEntity.getOpeningFares());
            pstmt.setString(31, marketListEntity.getOverBooking());
            pstmt.setString(32, marketListEntity.getRbdPushFlag());
            pstmt.setString(33, marketListEntity.getProfileFares());
            pstmt.setString(34, marketListEntity.getB2cTolerance());
            pstmt.setString(35, marketListEntity.getB2bTolerance());
            pstmt.setString(36, marketListEntity.getDistressInventoryFlag());
            pstmt.setString(37, marketListEntity.getSeriesBlock());
            pstmt.setString(38, marketListEntity.getAutoGroup());
            pstmt.setString(39, marketListEntity.getAutoBackstopFlag());
            pstmt.setString(40, marketListEntity.getTbfFlag());
            pstmt.setString(41, marketListEntity.getCugFlag());
            pstmt.setString(42, marketListEntity.getMktFareFlag());
            pstmt.setString(43, marketListEntity.getUuid());
            pstmt.setString(44, marketListEntity.getDirectFlag());
            pstmt.setString(45, marketListEntity.getTbfEnhFlag());
            pstmt.setString(46, marketListEntity.getDefaultMlFlag());
            pstmt.setString(47, marketListEntity.getB2bStrategy());
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void deleteValues(Set<String> value, String tableName, String columnName) {
        if (!value.isEmpty()) {
            String placeholders = value.stream().map(v -> "?").collect(Collectors.joining(", "));
            String deleteQuery = "DELETE FROM " + tableName + " WHERE " + columnName + " IN (" + placeholders + ")";
            try (PreparedStatement pstmt = connection.prepareStatement(deleteQuery)) {
                int index = 1;
                for (String val : value) {
                    pstmt.setString(index++, val);
                }
                pstmt.executeUpdate();
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    public void deleteValuesFare(Set<String> value, String tableName) {
        if (!value.isEmpty()) {
            String placeholders = value.stream().map(v -> "?").collect(Collectors.joining(", "));
            String deleteQuery = "DELETE FROM " + tableName + " WHERE CONCAT(" + SECTOR_COLUMN_NAME + ","
                    + ROUTE_COLUMN_NAME + ") IN ("
                    + placeholders + ")";
            try (PreparedStatement pstmt = connection.prepareStatement(deleteQuery)) {
                int index = 1;
                for (String val : value) {
                    pstmt.setString(index++, val);
                }
                pstmt.executeUpdate();
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    private void deleteRecords(List<MarketListEntity> marketListEntityList, String tableName) {
        Set<String> uuid = new HashSet<>();
        for (MarketListEntity marketEntity : marketListEntityList) {
            List<Map<String, Object>> listOfResult = searchMarketList(marketEntity, tableName);
            for (Map<String, Object> mapOfResult : listOfResult) {
                uuid.add(mapOfResult.get(UUID_COLUMN_NAME).toString());
            }
        }
        deleteValues(uuid, tableName, UUID_COLUMN_NAME);
    }

    public List<Map<String, Object>> searchMarketList(MarketListEntity marketListEntity, String tableName) {
        List<Map<String, Object>> listObj = new ArrayList<>();
        String query = SELECT_MARKET_LIST(tableName);
        try (PreparedStatement pstmt = connection.prepareStatement(query)) {
            pstmt.setString(1, marketListEntity.getOrigin());
            pstmt.setString(2, marketListEntity.getDestination());
            pstmt.setString(3, marketListEntity.getAnalystName());
            pstmt.setString(4, marketListEntity.getStartDate());
            pstmt.setString(5, marketListEntity.getFlightNo());

            try (ResultSet resultSet = pstmt.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> row = new HashMap<>();
                    int columnCount = resultSet.getMetaData().getColumnCount();
                    for (int i = 1; i <= columnCount; i++) {
                        String column = resultSet.getMetaData().getColumnName(i);
                        Object value = resultSet.getObject(i);
                        row.put(column, value);
                    }
                    listObj.add(row);
                }
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return listObj;
    }

    public static String getCurrentDateTime() {
        return LocalDateTime.now().format(DATE_TIME_OUTPUT_FORMATTER);
    }

    public List<String> getRoleAccess(Object role) {
        List<String> permissions = new ArrayList<>();
        role = role.toString().replaceAll("\\[", "")
                .replaceAll("]", "");
        try (PreparedStatement pstmt = connection.prepareStatement(GET_ROLE_PERMISSION_LIST(role))) {
            try (ResultSet resultSet = pstmt.executeQuery()) {
                while (resultSet.next()) {
                    permissions.add(resultSet.getString(PERMISSION_COLUMN_NAME));
                }
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return permissions;
    }

    public void createTable(String tableName, String newTableName) {
        String createTableSQL = "CREATE TABLE IF NOT EXISTS " + tableName + " LIKE " + newTableName;
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(createTableSQL);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void insertAuditStatement(String tableName, String startTime, String userName, String uuid) {
        try (PreparedStatement pstmt = connection.prepareStatement(INSERT_FILE_UPLOAD())) {
            pstmt.setString(1, tableName);
            pstmt.setString(2, uuid);
            pstmt.setString(3, startTime);
            pstmt.setString(4, userName);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void insertInputStatusTable(String tableName) {
        List<Map<String, Object>> listObj = new ArrayList<>();
        int value = 1;
        if(tableName.contains("QP_DW_PRIAUT.")){
            value = 0;
        }
        try (PreparedStatement countStmt = connection.prepareStatement(GET_INPUT_STATUS_COUNT())) {
            countStmt.setString(1, tableName);
            try (ResultSet resultSet = countStmt.executeQuery()) {
                if (resultSet.next()) {
                    Map<String, Object> row = new HashMap<>();
                    row.put(COUNT_COLUMN_NAME, resultSet.getInt(COUNT_COLUMN_NAME));
                    listObj.add(row);
                }
            }
            if (!listObj.isEmpty() && Integer.parseInt(listObj.get(0).get(COUNT_COLUMN_NAME).toString()) == 1) {
                try (PreparedStatement updateStmt = connection.prepareStatement(UPDATE_INPUT_STATUS(value,tableName))) {
                    updateStmt.executeUpdate();
                }
            } else {
                try (PreparedStatement insertStmt = connection.prepareStatement(INSERT_INPUT_STATUS())) {
                    insertStmt.setString(1,tableName);
                    insertStmt.setInt(2,value);
                    insertStmt.executeUpdate();
                }
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void createMarketListEntity(List<String> record, List<MarketListEntity> marketListEntityList,
                                               String uuid, String analystName) {
        marketListEntityList.add(MarketListEntity.builder()
                .uuid(uuid)
                .origin(record.get(CSV_HEADER_ORIGIN))
                .destination(record.get(CSV_HEADER_DESTINATION))
                .flightNo(record.get(CSV_HEADER_FLIGHT_NO))
                .perType(PER_TYPE_VALUE)
                .startDate(record.get(CSV_HEADER_START_DATE))
                .endDate(record.get(CSV_HEADER_END_DATE))
                .dayOfWeek(record.get(CSV_HEADER_DAY_OF_WEEK))
                .hardAnchor(record.get(CSV_HEADER_HARD_ANCHOR))
                .plfThreshold(record.get(CSV_HEADER_PLF_THRESHOLD))
                .startTime(record.get(CSV_HEADER_START_TIME))
                .endTime(record.get(CSV_HEADER_END_TIME))
                .curveId(record.get(CSV_HEADER_CURVE_ID))
                .carrExclusionB2B(record.get(CSV_HEADER_CARR_EXCLUSION_B2B))
                .carrExclusionB2C(record.get(CSV_HEADER_CARR_EXCLUSION_B2C))
                .flightExclusionB2B(record.get(CSV_HEADER_FLIGHT_EXCLUSION_B2B))
                .flightExclusionB2C(record.get(CSV_HEADER_FLIGHT_EXCLUSION_B2C))
                .fareAnchor(record.get(CSV_HEADER_FARE_ANCHOR))
                .fareOffset(record.get(CSV_HEADER_FARE_OFFSET))
                .firstAllocation(record.get(CSV_HEADER_FIRST_ALLOCATION))
                .otherAllocation(record.get(CSV_HEADER_OTHER_ALLOCATION))
                .b2bBackstop(record.get(CSV_HEADER_B2B_BACKSTOP))
                .b2cBackstop(record.get(CSV_HEADER_B2C_BACKSTOP))
                .b2bFactor(record.get(CSV_HEADER_B2B_FACTOR))
                .skippingFactor(record.get(CSV_HEADER_SKIPPING_FACTOR))
                .daySpan(record.get(CSV_HEADER_DAY_SPAN))
                .openingFares(convertYNToBoolean(record.get(CSV_HEADER_OPENING_FARES)))
                .autoTimeRangeFlag(convertYNToBoolean(record.get(CSV_HEADER_AUTO_TIME_RANGE_FLAG)))
                .overBooking(convertYNToBoolean(record.get(CSV_HEADER_OVER_BOOKING)))
                .OBSeats(record.get(CSV_HEADER_OB_SEATS))
                .OBFare(record.get(CSV_HEADER_OB_FARE))
                .rbdPushFlag(convertYNToBoolean(record.get(CSV_HEADER_RBD_PUSH_FLAG)))
                .profileFares(convertYNToBoolean(record.get(CSV_HEADER_PROFILE_FARES)))
                .b2bTolerance(record.get(CSV_HEADER_B2B_TOLERANCE))
                .b2cTolerance(record.get(CSV_HEADER_B2C_TOLERANCE))
                .distressInventoryFlag(convertYNToBoolean(record.get(CSV_HEADER_DISTRESS_INVENTORY_FLAG)))
                .seriesBlock(convertYNToBoolean(record.get(CSV_HEADER_SERIES_BLOCK_FLAG)))
                .autoGroup(convertYNToBoolean(record.get(CSV_HEADER_AUTO_GROUP)))
                .autoBackstopFlag(convertYNToBoolean(record.get(CSV_HEADER_AUTO_BACKSTOP_FLAG)))
                .tbfFlag(convertYNToBoolean(record.get(CSV_HEADER_TBF_FLAG)))
                .cugFlag(convertYNToBoolean(record.get(CSV_HEADER_CUG_FLAG)))
                .mktFareFlag(convertYNToBoolean(record.get(CSV_HEADER_MKT_FARE_FLAG)))
                .directFlag(convertYNToBoolean(record.get(CSV_HEADER_DIRECT_FLAG)))
                .tbfEnhFlag(convertYNToBoolean(record.get(CSV_HEADER_TBF_ENH_FLAG)))
                .defaultMlFlag(convertYNToBoolean(record.get(CSV_HEADER_DEFAULT_ML_FLAG)))
                .b2bStrategy(record.get(CSV_HEADER_B2B_STRATEGY))
                .analystName(analystName)
                .build());
    }

    private static String convertYNToBoolean(String value) {
        return "Y".equalsIgnoreCase(value) ? "1" : "0";
    }

    public List<Object> searchDistinctResults(String columnName, String tableName) {
        List<Object> listObj = new ArrayList<>();
        String query = "SELECT DISTINCT " + columnName + " FROM " + tableName;

        try (PreparedStatement pstmt = connection.prepareStatement(query)) {
            try (ResultSet resultSet = pstmt.executeQuery()) {
                while (resultSet.next()) {
                    listObj.add(resultSet.getObject(columnName));
                }
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return listObj;
    }

    public List<Map<String, Object>> getJourneyDestination(String origin, String destination, String flag) {
        List<Map<String, Object>> listObj = new ArrayList<>();
        try (PreparedStatement pstmt = connection.prepareStatement(GET_STATION_MASTER_LIST())) {
            pstmt.setString(1, origin);
            pstmt.setString(2, destination);
            pstmt.setString(3, flag);
            try (ResultSet resultSet = pstmt.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> row = new HashMap<>();
                    row.put(STATION_COLUMN_NAME, resultSet.getObject(STATION_COLUMN_NAME));
                    listObj.add(row);
                }
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return listObj;
    }

    public String getSectors(String sector, String users) {
        String listObj = null;
        try (PreparedStatement pstmt = connection.prepareStatement(GET_USER_SECTOR_MAP(users))) {
            pstmt.setString(1, sector);
            try (ResultSet resultSet = pstmt.executeQuery()) {
                if (resultSet.next()) {
                    listObj = resultSet.getString(USER_COLUMN_NAME);
                }
            }
        } catch (SQLException e) {
            System.out.println(DATABASE_ACCESS_ERROR_MESSAGE);
            e.printStackTrace();
            return null;
        }
        return listObj;
    }

    private String validationForMarketListFile(List<String> record, List<String> permissions, String route,
                                               List<String> listOfErrors, int rowCount, Object role, String userName, List<Object> listOfCurves,
                                               List<Object> listOfStrategies, List<Object> listOfB2BStrategies, List<Object> listOfCriteria) throws Exception {
        try {
            JSONArray roles = new JSONArray();
            if (role instanceof String roleStr) {
                roleStr = roleStr.substring(1, roleStr.length() - 1);
                roleStr = roleStr.substring(1, roleStr.length() - 1);
                roles.add(roleStr);
            } else {
                System.err.println("The 'role' object is not a string.");
            }

            String routeSymbol = "";
            String user = "";
            String sector = record.get(CSV_HEADER_ORIGIN) + record.get(CSV_HEADER_DESTINATION);
            String sector_flight_dept = sector + "_" + record.get(CSV_HEADER_FLIGHT_NO) + "_" +record.get(CSV_HEADER_START_DATE);
            if (route.equalsIgnoreCase(INTERNATIONAL)) {
                routeSymbol = INTERNATIONAL_SYMBOL;
            } else if (route.equalsIgnoreCase(DOMESTIC)) {
                routeSymbol = DOMESTIC_SYMBOL;
            }

            List<Map<String, Object>> routeJourney = getJourneyDestination(record.get(CSV_HEADER_ORIGIN),
                    record.get(CSV_HEADER_DESTINATION), routeSymbol);
            String lineNum = "Line " + (rowCount + 1) + ": " + sector_flight_dept + ":";
            if (!isLengthEqualTo(record.get(CSV_HEADER_ORIGIN), 3)) {
                listOfErrors.add(lineNum + "Origin Station is not equal to 3 characters");
            }
            if (!isLengthEqualTo(record.get(CSV_HEADER_DESTINATION), 3)) {
                listOfErrors.add(lineNum + "Destination Station is not equal to 3 characters");
            }
            if (routeJourney.isEmpty()) {
                listOfErrors.add(lineNum + "Origin Destination is not " + route + " Route");
            }
            if (!isNumeric(record.get(CSV_HEADER_FLIGHT_NO))) {
                listOfErrors.add(lineNum + "Flight Number is not a Number");
            }
            try {
                String startDate = record.get(CSV_HEADER_START_DATE);
                String endDate = record.get(CSV_HEADER_END_DATE);

                LocalDate daysFromToday = LocalDate.now().plusDays(DAYS_TO_ENTER_INTO_MARKET_LIST);

                LocalDate formattedStartDate = LocalDate.parse(startDate, DATE_OUTPUT_FORMATTER);
                LocalDate formattedEndDate = LocalDate.parse(endDate, DATE_OUTPUT_FORMATTER);
                if (permissions.contains(PERMISSION_UPDATE_MARKET_LIST_TILL_30_DAYS)) {
                    user = getSectors(sector, SECTOR_MAP_USER1_COLUMN_NAME);
                } else if (permissions.contains(PERMISSION_UPDATE_MARKET_LIST_AFTER_30_DAYS)) {
                    user = getSectors(sector, SECTOR_MAP_USER2_COLUMN_NAME);

                } else if (permissions.contains(PERMISSION_UPDATE_MARKET_LIST_ALL)) {
                    if (!daysFromToday.isBefore(formattedEndDate) && !formattedStartDate.isAfter(daysFromToday)) {
                        user = getSectors(sector, SECTOR_MAP_USER1_COLUMN_NAME);
                    } else {
                        user = getSectors(sector, SECTOR_MAP_USER2_COLUMN_NAME);
                    }

                }
                if (!roles.contains(MANAGER) && !roles.contains(ADMIN)) {
                    if (!userName.split(SPACE)[0].toUpperCase().equals(user)) {
                        listOfErrors.add(lineNum + "you don't have access for sector " + sector);
                    }
                } else {
                    if (!daysFromToday.isBefore(formattedEndDate) && !formattedStartDate.isAfter(daysFromToday)) {
                        user = getSectors(sector, SECTOR_MAP_USER1_COLUMN_NAME);
                    } else {
                        user = getSectors(sector, SECTOR_MAP_USER2_COLUMN_NAME);
                    }
                }
                if (formattedEndDate.isBefore(formattedStartDate)) {
                    listOfErrors.add(lineNum + "End date must be greater than or equal to start date");
                }
                if (permissions.contains(PERMISSION_UPDATE_MARKET_LIST_TILL_30_DAYS)) {
                    if (daysFromToday.isBefore(formattedEndDate)) {
                        listOfErrors.add(lineNum + "End date is above 30 days");
                    }
                    if (formattedStartDate.isAfter(daysFromToday)) {
                        listOfErrors.add(lineNum + "End date is above 30 days");
                    }
                }
                if (permissions.contains(PERMISSION_UPDATE_MARKET_LIST_AFTER_30_DAYS)) {
                    if (formattedStartDate.isBefore(daysFromToday)) {
                        listOfErrors.add(lineNum + "Start date is below 30 days");
                    }
                    if (daysFromToday.isAfter(formattedEndDate)) {
                        listOfErrors.add(lineNum + "End date is below 30 days");
                    }
                }
            } catch (Exception e) {
                listOfErrors.add(lineNum + "Date is an invalid format");
            }
            if (!(isValidDate(record.get(CSV_HEADER_END_DATE)))) {
                listOfErrors.add(lineNum + "End date is not a valid date");
            }
            if (!(isLengthEqualTo(record.get(CSV_HEADER_DAY_OF_WEEK), 7)
                    && isNumeric(record.get(CSV_HEADER_DAY_OF_WEEK))
                    && checkDOWDigits(record.get(CSV_HEADER_DAY_OF_WEEK)))) {
                listOfErrors.add(lineNum + "DOW is not 7 digits or has value other than '1' or '9'");
            }
            if (!(isValidTime(record.get(CSV_HEADER_START_TIME)))) {
                listOfErrors.add(lineNum + "Start Time is not invalid time format");
            }
            if (!(isValidTime(record.get(CSV_HEADER_END_TIME)))) {
                listOfErrors.add(lineNum + "End Time is not 4 digits or has an invalid input");
            }
            if (!listOfCurves.contains(record.get(CSV_HEADER_CURVE_ID))) {
                listOfErrors.add(lineNum + "Curve Id is not valid");
            }
            if (!isValidCarrExcl(record.get(CSV_HEADER_CARR_EXCLUSION_B2C))) {
                listOfErrors.add(lineNum + "One of the Carrier Exclusion B2C is not 2 digits");
            }
            if (!isValidCarrExcl(record.get(CSV_HEADER_CARR_EXCLUSION_B2B))) {
                listOfErrors.add(lineNum + "One of the Carrier Exclusion B2B is not 2 digits");
            }
            if (!isValidFlightNumber(record.get(CSV_HEADER_FLIGHT_EXCLUSION_B2C))) {
                listOfErrors.add(lineNum + "Flight exclusion B2C is not in correct format");
            }
            if (!isValidFlightNumber(record.get(CSV_HEADER_FLIGHT_EXCLUSION_B2B))) {
                listOfErrors.add(lineNum + "Flight exclusion B2B is not in correct format");
            }
            if (!checkDaySpan(record.get(CSV_HEADER_DAY_SPAN))) {
                listOfErrors.add(lineNum + "DaySpan has value other than '1','-1','0'");
            }
            if (!checkAutoTimeFlag(record.get(CSV_HEADER_AUTO_TIME_RANGE_FLAG))) {
                listOfErrors.add(lineNum + "AutoTimeRangeFlag has value other than 'Y' or 'N'");
            }
            if (!checkAutoTimeFlag(record.get(CSV_HEADER_DIRECT_FLAG))) {
                listOfErrors.add(lineNum + "directFlag has value other than 'Y' or 'N'");
            }
            if (!checkAutoTimeFlag(record.get(CSV_HEADER_TBF_ENH_FLAG))) {
                listOfErrors.add(lineNum + "TBFEnhFlag has value other than 'Y' or 'N'");
            }
            if (!checkAutoTimeFlag(record.get(CSV_HEADER_DEFAULT_ML_FLAG))) {
                listOfErrors.add(lineNum + "defaultMlFlag has value other than 'Y' or 'N'");
            }
            if (!listOfStrategies.contains(record.get(CSV_HEADER_FARE_ANCHOR))) {
                listOfErrors.add(lineNum + "Fare Anchor must be one of the valid strategies");
            }
            if (!(listOfB2BStrategies.contains(record.get(CSV_HEADER_B2B_STRATEGY)) || record.get(CSV_HEADER_B2B_STRATEGY).equals(""))) {
                listOfErrors.add(lineNum + "B2B Strategy must be one of the valid strategies");
            }
            String hardAnchor = record.get(CSV_HEADER_HARD_ANCHOR);
            String fareOffset = record.get(CSV_HEADER_FARE_OFFSET);
            String plfThreshold = record.get(CSV_HEADER_PLF_THRESHOLD);

            if (listOfCriteria.contains(hardAnchor)) {
                if (fareOffset == null || fareOffset.trim().isEmpty()) {
                    listOfErrors.add(lineNum + "Fare Offset is required when Hard Anchor is applied");
                }
                if (plfThreshold != null && !plfThreshold.trim().isEmpty()) {
                    String trimmedValue = plfThreshold.trim();
                    if (!trimmedValue.endsWith("%")) {
                        listOfErrors.add(lineNum + "PLF Threshold must include a percentage symbol (%)");
                    } else {
                        String numericValue = trimmedValue.substring(0, trimmedValue.length() - 1);
                        if (!isNumeric(numericValue)) {
                            listOfErrors.add(lineNum + "PLF Threshold must be a valid number");
                        } else {
                            double value = Double.parseDouble(numericValue);
                            if (value > PLFTHRESHOLD_VALUE) {
                                listOfErrors.add(lineNum + "PLF Threshold must be lesser than 103 percent");
                            }
                        }
                    }
                }
                if (plfThreshold == null || plfThreshold.trim().isEmpty()) {
                    listOfErrors.add(lineNum + "plfThreshold is required when Hard Anchor is applied");
                }
            }
            else if (hardAnchor != null && !hardAnchor.trim().isEmpty()) {
                if (!isNumeric(hardAnchor)) {
                    StringBuilder validOptions = new StringBuilder();
                    for (int i = 0; i < listOfCriteria.size(); i++) {
                        if (i > 0) {
                            validOptions.append("/ ");
                        }
                        validOptions.append(listOfCriteria.get(i));
                    }
                    listOfErrors.add(lineNum + "Hard Anchor must be either a number or one of: " + validOptions);
                } else {
                    if (!isNumeric(fareOffset) && !fareOffset.isEmpty()) {
                        listOfErrors.add(lineNum + "Fare Offset must be a valid number");
                    }
                }
                if (plfThreshold != null && !plfThreshold.trim().isEmpty()) {
                    String trimmedValue = plfThreshold.trim();
                    if (!trimmedValue.endsWith("%")) {
                        listOfErrors.add(lineNum + "PLF Threshold must include a percentage symbol (%)");
                    } else {
                        String numericValue = trimmedValue.substring(0, trimmedValue.length() - 1);
                        if (!isNumeric(numericValue)) {
                            listOfErrors.add(lineNum + "PLF Threshold must be a valid number");
                        } else {
                            double value = Double.parseDouble(numericValue);
                            if (value > PLFTHRESHOLD_VALUE) {
                                listOfErrors.add(lineNum + "PLF Threshold must be lesser than 103 percent");
                            }
                        }
                    }
                }
                if (fareOffset != null && !fareOffset.trim().isEmpty() && !isNumeric(fareOffset)) {
                    listOfErrors.add(lineNum + "If present, Fare Offset must be a valid number");
                }
                if (plfThreshold == null || plfThreshold.trim().isEmpty()) {
                    listOfErrors.add(lineNum + "plfThreshold is required when Hard Anchor is applied");
                }
            }
            else {
                if (fareOffset != null && !fareOffset.trim().isEmpty()) {
                    listOfErrors.add(lineNum + "Fare Offset must be empty");
                }
                if (plfThreshold != null && !plfThreshold.trim().isEmpty()) {
                    listOfErrors.add(lineNum + "PLF Threshold must be empty");
                }
            }

            if (!isNumeric(record.get(CSV_HEADER_FIRST_ALLOCATION))) {
                listOfErrors.add(lineNum + "First Allocation is not a number");
            }
            if (!(isNumeric(record.get(CSV_HEADER_OTHER_ALLOCATION))
                    && isPositive(record.get(CSV_HEADER_OTHER_ALLOCATION)))) {
                listOfErrors.add(lineNum + "Other Allocation is not a positive number");
            }
            if (!(isNumeric(record.get(CSV_HEADER_B2B_BACKSTOP))
                    && isPositive(record.get(CSV_HEADER_B2B_BACKSTOP)))) {
                listOfErrors.add(lineNum + "B2B Backstop is not a positive number");
            }
            if (!(isNumeric(record.get(CSV_HEADER_B2C_BACKSTOP))
                    && isPositive(record.get(CSV_HEADER_B2C_BACKSTOP)))) {
                listOfErrors.add(lineNum + "B2C Backstop is not a positive number");
            }
            if (!(isDouble(record.get(CSV_HEADER_B2B_FACTOR)))) {
                listOfErrors.add(lineNum + "B2B Factor is not a valid float");
            }
            if (!(isNumeric(record.get(CSV_HEADER_SKIPPING_FACTOR)))) {
                listOfErrors.add(lineNum + "Skipping Factor is not a number");
            }
            if (!checkAutoTimeFlag(record.get(CSV_HEADER_OPENING_FARES))) {
                listOfErrors.add(lineNum + "Opening Fares has value other than 'Y' or 'N'");
            }
            if (!(isNumeric(record.get(CSV_HEADER_B2B_TOLERANCE)))
                    && !record.get(CSV_HEADER_B2B_TOLERANCE).trim().isEmpty()) {
                listOfErrors.add(lineNum + "B2B Tolerance is not a number or empty");
            }
            if (!(isNumeric(record.get(CSV_HEADER_B2C_TOLERANCE)))
                    && !record.get(CSV_HEADER_B2C_TOLERANCE).trim().isEmpty()) {
                listOfErrors.add(lineNum + "B2C Tolerance is not a number or empty");
            }

            boolean hasSeats = !record.get(CSV_HEADER_OB_SEATS).trim().isEmpty();
            boolean hasFare = !record.get(CSV_HEADER_OB_FARE).trim().isEmpty();

            if (hasSeats != hasFare) {
                listOfErrors.add(lineNum + "Overbooking seats and fare must be provided together");
            }else{
                if (!(isFloat(record.get(CSV_HEADER_OB_SEATS)))
                        && !record.get(CSV_HEADER_OB_SEATS).trim().isEmpty()) {
                    listOfErrors.add(lineNum + "Overbooking seats is not a number or empty");
                } else if (!record.get(CSV_HEADER_OB_SEATS).trim().isEmpty()) {
                    float seats = Float.parseFloat(record.get(CSV_HEADER_OB_SEATS));
                    if (seats > 10 || seats < 0) {
                        listOfErrors.add(lineNum + "Overbooking seats cannot exceed 10 and should be greater than 0");
                    }
                }

                if (!(isFloat(record.get(CSV_HEADER_OB_FARE)))
                        && !record.get(CSV_HEADER_OB_FARE).trim().isEmpty()) {
                    listOfErrors.add(lineNum + "Overbooking fare is not a number or empty");
                } else if (!record.get(CSV_HEADER_OB_FARE).trim().isEmpty()) {
                    float fare = Float.parseFloat(record.get(CSV_HEADER_OB_FARE));
                    if (fare <= 1000) {
                        listOfErrors.add(lineNum + "Overbooking fare cannot be below 1000");
                    }
                }
            }
            if (!checkOwnFareFlag(record.get(CSV_HEADER_OVER_BOOKING))) {
                listOfErrors.add(lineNum + "Opening Fares has value other than 'Y' or 'N'");
            }
            if (!checkOwnFareFlag(record.get(CSV_HEADER_SERIES_BLOCK_FLAG))) {
                listOfErrors.add(lineNum + "Series Block has value other than 'Y' or 'N'");
            }
            if (!checkOwnFareFlag(record.get(CSV_HEADER_AUTO_GROUP))) {
                listOfErrors.add(lineNum + "auto group has value other than 'Y' or 'N'");
            }
            if (!checkOwnFareFlag(record.get(CSV_HEADER_AUTO_BACKSTOP_FLAG))) {
                listOfErrors.add(lineNum + "auto backstop flag has value other than 'Y' or 'N'");
            }
            if (!checkAutoTimeFlag(record.get(CSV_HEADER_RBD_PUSH_FLAG))) {
                listOfErrors.add(lineNum + "RBD Push has value other than 'Y' or 'N'");
            }
            if (!checkAutoTimeFlag(record.get(CSV_HEADER_DISTRESS_INVENTORY_FLAG))) {
                listOfErrors.add(lineNum + "DistressInventoryFlag has value other than 'Y' or 'N'");
            }
            if (!checkOwnFareFlag(record.get(CSV_HEADER_PROFILE_FARES))) {
                listOfErrors.add(lineNum + "Profile Fares has value other than 'Y' or 'N'");
            }
            if (!checkOwnFareFlag(record.get(CSV_HEADER_TBF_FLAG))) {
                listOfErrors.add(lineNum + "TBF Flag has value other than 'Y' or 'N'");
            }
            if (!checkOwnFareFlag(record.get(CSV_HEADER_CUG_FLAG))) {
                listOfErrors.add(lineNum + "CUG Flag has value other than 'Y' or 'N'");
            }
            if (!checkOwnFareFlag(record.get(CSV_HEADER_MKT_FARE_FLAG))) {
                listOfErrors.add(lineNum + "Mktfare Flag has value other than 'Y' or 'N'");
            }

            return user;
        } catch (Exception e) {
            listOfErrors.add("Unknown Error Encountered in Validation");
            throw new Exception(e);
        }
    }

    private String validationForMarketListConnectionsFile(List<String> record, int rowCountInProcessedCsv, List<String> permissions,
                                                          Object role, String userName, List<String> errorsFound) {
        JSONArray roles = new JSONArray();
        if (role instanceof String roleStr) {
            roleStr = roleStr.substring(1, roleStr.length() - 1);
            roleStr = roleStr.substring(1, roleStr.length() - 1);
            roles.add(roleStr);
        } else if (role instanceof ArrayList) {
            ((ArrayList<?>) role).forEach(r -> roles.add(r.toString()));
        }


        String user1 = null;
        String user2 = null;
        String analystNameToStore = "UNKNOWN_ANALYST";

        String sector1 = record.get(CSV_HEADER_CONNECTIONS_SECTOR1);
        String sector2 = record.get(CSV_HEADER_CONNECTIONS_SECTOR2);
        String recordIdentifier = "Processed Record " + rowCountInProcessedCsv + " (Origin Sector1: " + sector1 +
                ", Flight1: " + record.get(CSV_HEADER_CONNECTIONS_FLIGHT1) +
                ", Sector2: " + sector2 +
                ", Flight2: " + record.get(CSV_HEADER_CONNECTIONS_FLIGHT2) +
                ", Date: " + record.get(CSV_HEADER_CONNECTIONS_PERSTART) + ")";
        String lineNum = recordIdentifier + ": ";

        String carrierExclB2B = record.get(CSV_HEADER_CONNECTIONS_CARRIEREXCLB2B);
        String carrierExclB2C = record.get(CSV_HEADER_CONNECTIONS_CARRIEREXCLB2C);

        if (!MarketListValidator.isValidCarrExcl(carrierExclB2B)) {
            errorsFound.add(lineNum + "CarrierExclusionB2B (" + carrierExclB2B + ") contains invalid carrier codes. Each should be 2 characters, comma-separated if multiple.");
        }
        if (!MarketListValidator.isValidCarrExcl(carrierExclB2C)) {
            errorsFound.add(lineNum + "CarrierExclusionB2C (" + carrierExclB2C + ") contains invalid carrier codes. Each should be 2 characters, comma-separated if multiple.");
        }
        if (!isLengthEqualTo(sector1, 6)) {
            errorsFound.add(lineNum + "Sector1 (" + sector1 + ") is not equal to 6 characters.");
        }
        if (!isLengthEqualTo(sector2, 6)) {
            errorsFound.add(lineNum + "Sector2 (" + sector2 + ") is not equal to 6 characters.");
        }
        if (!isNumeric(record.get(CSV_HEADER_CONNECTIONS_FLIGHT1))) {
            errorsFound.add(lineNum + "Flight Number1 (" + record.get(CSV_HEADER_CONNECTIONS_FLIGHT1) + ") is not a Number.");
        }
        if (!isNumeric(record.get(CSV_HEADER_CONNECTIONS_FLIGHT2))) {
            errorsFound.add(lineNum + "Flight Number2 (" + record.get(CSV_HEADER_CONNECTIONS_FLIGHT2) + ") is not a Number.");
        }
        if (!isValidDate(record.get(CSV_HEADER_CONNECTIONS_PEREND))) {
            errorsFound.add(lineNum + "PerEnd date ("+ record.get(CSV_HEADER_CONNECTIONS_PEREND) +") is not a valid date. Should be MM/dd/yyyy.");
        }
        if (!(isLengthEqualTo(record.get(CSV_HEADER_CONNECTIONS_DOW), 7)
                && isNumeric(record.get(CSV_HEADER_CONNECTIONS_DOW))
                && checkDOWDigits(record.get(CSV_HEADER_CONNECTIONS_DOW)))) {
            errorsFound.add(lineNum + "DOW ("+ record.get(CSV_HEADER_CONNECTIONS_DOW) +") is not 7 digits or has value other than '1' or '9'.");
        }
        if (!checkPriceStrategy(record.get(CSV_HEADER_CONNECTIONS_PRICESTRATEGY))) {
            errorsFound.add(lineNum + "Price Strategy ("+ record.get(CSV_HEADER_CONNECTIONS_PRICESTRATEGY) +") has value other than '1' or '2'.");
        }

        if (!checkStrategyFlag(record.get(CSV_HEADER_CONNECTIONS_SOSFLAG))) {
            errorsFound.add(lineNum + "SOS Flag ("+ record.get(CSV_HEADER_CONNECTIONS_SOSFLAG) +") has value other than '0' or '1'.");
        }
        if (!isPositive(record.get(CSV_HEADER_CONNECTIONS_FIRSTRBDALLOC))) {
            errorsFound.add(lineNum + "First Allocation ("+ record.get(CSV_HEADER_CONNECTIONS_FIRSTRBDALLOC) +") is not a positive number.");
        }

        String perStartDateStr = record.get(CSV_HEADER_CONNECTIONS_PERSTART);
        LocalDate perStartDate = null;
        try {
            perStartDate = LocalDate.parse(perStartDateStr, DateTimeFormatter.ofPattern("MM/dd/yyyy"));
        } catch (Exception e) {
            errorsFound.add(lineNum + "Cannot parse PerStart date ("+ perStartDateStr +") for analyst mapping logic.");
            return analystNameToStore;
        }

        LocalDate daysFromToday = LocalDate.now().plusDays(DAYS_TO_ENTER_INTO_MARKET_LIST);

        String userMapColumnSector1 = SECTOR_MAP_USER3_COLUMN_NAME;
        String userMapColumnSector2 = SECTOR_MAP_USER3_COLUMN_NAME;

        if (permissions.contains(PERMISSION_UPDATE_MARKET_LIST_AFTER_30_DAYS)) {
            userMapColumnSector1 = SECTOR_MAP_USER4_COLUMN_NAME;
            userMapColumnSector2 = SECTOR_MAP_USER4_COLUMN_NAME;
        } else if (permissions.contains(PERMISSION_UPDATE_MARKET_LIST_ALL)) {
            if (perStartDate.isAfter(daysFromToday.minusDays(1))) {
                userMapColumnSector1 = SECTOR_MAP_USER4_COLUMN_NAME;
                userMapColumnSector2 = SECTOR_MAP_USER4_COLUMN_NAME;
            }
        }

        user1 = getSectors(sector1, userMapColumnSector1);
        user2 = getSectors(sector2, userMapColumnSector2);

        if (user1 == null) {
            errorsFound.add(lineNum + "Analyst mapping not found for Sector1 (" + sector1 + ") using map column " + userMapColumnSector1 + ".");
        }
        if (user2 == null) {
            errorsFound.add(lineNum + "Analyst mapping not found for Sector2 (" + sector2 + ") using map column " + userMapColumnSector2 + ".");
        }

        if (user1 != null && user2 != null) {
            analystNameToStore = user1.equalsIgnoreCase(user2) ? user1 : user1 + SLASH + user2;
        } else if (user1 != null) {
            analystNameToStore = user1;
        } else if (user2 != null) {
            analystNameToStore = user2;
        } else {
            analystNameToStore = "NO_MAPPING_FOUND";
        }

        if (permissions.contains(PERMISSION_UPDATE_MARKET_LIST_TILL_30_DAYS)) {
            if (perStartDate.isAfter(daysFromToday)) {
                errorsFound.add(lineNum + "User with 'TILL_30_DAYS' permission cannot update record for date " + perStartDateStr + " (which is >30 days away).");
            }
        }
        if (permissions.contains(PERMISSION_UPDATE_MARKET_LIST_AFTER_30_DAYS)) {
            if (perStartDate.isBefore(daysFromToday.plusDays(1))) {
                errorsFound.add(lineNum + "User with 'AFTER_30_DAYS' permission cannot update record for date " + perStartDateStr + " (which is <=30 days away).");
            }
        }

        boolean isAdminOrManager = roles.contains(ADMIN) || roles.contains(MANAGER);
        if (!isAdminOrManager) {
            String currentAnalystInitial = userName.split(SPACE)[0].toUpperCase();
            boolean hasAccessToMappedAnalyst1 = (user1 != null && currentAnalystInitial.equalsIgnoreCase(user1));
            boolean hasAccessToMappedAnalyst2 = (user2 != null && currentAnalystInitial.equalsIgnoreCase(user2));

            if (!hasAccessToMappedAnalyst1 && !hasAccessToMappedAnalyst2) {
                if (user1 != null || user2 != null) {
                    errorsFound.add(lineNum + "User " + currentAnalystInitial + " does not have permission to manage records for sector(s) mapped to analyst(s): " + analystNameToStore + ".");
                } else {
                    errorsFound.add(lineNum + "User " + currentAnalystInitial + " cannot manage records as no analyst mapping found for sectors.");
                }
            }
        }

        return analystNameToStore;
    }
    public static List<List<String>> splitConnectionsRowsByDateRange(List<List<String>> csvData, List<String> errorsFound) {
        DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("MM/dd/yyyy");
        List<List<String>> intermediateResult = new ArrayList<>();
        List<List<String>> result = new ArrayList<>();
        Set<String> uniqueKeysInCsv = new HashSet<>();

        if (csvData.isEmpty()) {
            return result;
        }

        intermediateResult.add(new ArrayList<>(csvData.get(0)));

        for (int rowIndex = 1; rowIndex < csvData.size(); rowIndex++) {
            List<String> row = csvData.get(rowIndex);
            String originalRowNumStr = "Original CSV Row " + (rowIndex + 1);

            if (row.size() <= Math.max(Math.max(CSV_HEADER_CONNECTIONS_PEREND, CSV_HEADER_CONNECTIONS_DOW),
                    Math.max(Math.max(CSV_HEADER_CONNECTIONS_SECTOR1, CSV_HEADER_CONNECTIONS_FLIGHT1),
                            Math.max(CSV_HEADER_CONNECTIONS_SECTOR2, CSV_HEADER_CONNECTIONS_FLIGHT2)))) {
                errorsFound.add(originalRowNumStr + ": Insufficient columns for connection data.");
                continue;
            }

            String sector1 = row.get(CSV_HEADER_CONNECTIONS_SECTOR1);
            String flight1 = row.get(CSV_HEADER_CONNECTIONS_FLIGHT1);
            String sector2 = row.get(CSV_HEADER_CONNECTIONS_SECTOR2);
            String flight2 = row.get(CSV_HEADER_CONNECTIONS_FLIGHT2);
            String startDateStr = row.get(CSV_HEADER_CONNECTIONS_PERSTART);
            String endDateStr = row.get(CSV_HEADER_CONNECTIONS_PEREND);
            String dowPattern = row.get(CSV_HEADER_CONNECTIONS_DOW);

            LocalDate startDate;
            LocalDate endDate;

            try {
                startDate = LocalDate.parse(startDateStr, DATE_FORMATTER);
                endDate = LocalDate.parse(endDateStr, DATE_FORMATTER);
            } catch (Exception e) {
                errorsFound.add(originalRowNumStr + ": Invalid date format for PerStart/PerEnd ('" + startDateStr + "', '" + endDateStr + "'). Expected MM/dd/yyyy.");
                continue;
            }

            if (startDate.isAfter(endDate)) {
                errorsFound.add(originalRowNumStr + ": PerStart date (" + startDateStr + ") cannot be after PerEnd date (" + endDateStr + ").");
                continue;
            }

            if (dowPattern == null || !dowPattern.matches("[19]{7}")) {
                errorsFound.add(originalRowNumStr + ": Invalid DOW pattern: '" + dowPattern + "'. Must be 7 digits, consisting of '1' or '9'.");
                continue;
            }

            if (startDate.equals(endDate)) {
                int dayOfWeek = startDate.getDayOfWeek().getValue() % 7;
                if (dowPattern.charAt(dayOfWeek) == '1') {
                    String key = sector1 + "|" + flight1 + "|" + sector2 + "|" + flight2 + "|" + startDateStr;
                    if (!uniqueKeysInCsv.add(key)) {
                        errorsFound.add(originalRowNumStr + " (maps to date " + startDateStr + "): Duplicate entry found in CSV file for key: " + key);
                    } else {
                        intermediateResult.add(new ArrayList<>(row));
                    }
                }
            } else {
                long daysBetween = ChronoUnit.DAYS.between(startDate, endDate);
                for (long i = 0; i <= daysBetween; i++) {
                    LocalDate currentDate = startDate.plusDays(i);
                    int dayOfWeek = currentDate.getDayOfWeek().getValue() % 7;

                    if (dowPattern.charAt(dayOfWeek) == '1') {
                        List<String> newRow = new ArrayList<>(row);
                        String currentDateStr = currentDate.format(DATE_FORMATTER);
                        newRow.set(CSV_HEADER_CONNECTIONS_PERSTART, currentDateStr);
                        newRow.set(CSV_HEADER_CONNECTIONS_PEREND, currentDateStr);

                        String key = sector1 + "|" + flight1 + "|" + sector2 + "|" + flight2 + "|" + currentDateStr;
                        if (!uniqueKeysInCsv.add(key)) {
                            errorsFound.add(originalRowNumStr + " (maps to date " + currentDateStr + "): Duplicate entry found in CSV file for key: " + key);
                        } else {
                            intermediateResult.add(newRow);
                        }
                    }
                }
            }
        }

        if(!intermediateResult.isEmpty()){
            result.add(new ArrayList<>(intermediateResult.get(0)));
        }

        for (int i = 1; i < intermediateResult.size(); i++) {
            List<String> row = intermediateResult.get(i);
            try {
                LocalDate date = LocalDate.parse(row.get(CSV_HEADER_CONNECTIONS_PERSTART), DATE_FORMATTER);
                String adjustedDowPattern = adjustDowPattern(date, "9999999");
                row.set(CSV_HEADER_CONNECTIONS_DOW, adjustedDowPattern);
                result.add(row);
            } catch (Exception e) {
                errorsFound.add("Error adjusting DOW for processed row (derived from " + row.get(CSV_HEADER_CONNECTIONS_SECTOR1)+"-"+row.get(CSV_HEADER_CONNECTIONS_FLIGHT1) + " for date " + row.get(CSV_HEADER_CONNECTIONS_PERSTART) + ")");
            }
        }
        return result;
    }

    public void uploadMarketListSalesInvFile(Reader file, String tableName, String userName, Object role,
                                             String obj_id, AtomicLong numOfRows) throws DataMismatchedException {
        List<MarketListSalesInvEntity> entityList = new ArrayList<>();
        try {
            createTable(tableName, MARKET_LIST_SALESINV_TABLE_NAME);

            String uuid = obj_id;
            insertAuditStatement(tableName, getCurrentDateTime(), userName, uuid);
            insertInputStatusTable(tableName);

            List<Object> listOfCurves = searchDistinctResults(CURVES_COLUMN_NAME, CURVES_TABLE_NAME);

            List<List<String>> csvData = getDataFromCSVFile(file);
            csvData = splitRowsByDateRange(csvData, listOfErrors);
            numOfRows.set(csvData.size() > 1 ? csvData.size() - 1 : 0);

            for (int i = 1; i < csvData.size(); i++) {
                validationForMarketListSalesInvFile(csvData.get(i), listOfErrors, i + 1, listOfCurves);
                createMarketListSalesInvEntity(csvData.get(i), entityList);
            }

            if (!listOfErrors.isEmpty()) {
                updateInputStatus(tableName);
                error_occurred = 1;
                addErrorsToTable(listOfErrors, obj_id);
                throw new DataMismatchedException(listOfErrors);
            }

            deleteValues(Collections.singleton(ZERO), tableName, ZERO);

            for (MarketListSalesInvEntity entity : entityList) {
                insertIntoMarketListSalesInv(entity, tableName);
            }

            s3FileStorageDao.storeFile(createCSV(tableName), tableName, uuid);
            updateAuditStatement(tableName, getCurrentDateTime(), uuid);
        } catch (Exception e) {
            error_occurred = 1;
            if (listOfErrors.isEmpty()) {
                listOfErrors.add("An unexpected error occurred");
            }
            addErrorsToTable(listOfErrors, obj_id);
            throw new RuntimeException(e);
        } finally {
            updateInputStatus(tableName);
        }
    }

    private void validationForMarketListSalesInvFile(List<String> record, List<String> listOfErrors, int rowCount, List<Object> listOfCurves) {
        String origin = record.get(CSV_HEADER_SALESINV_ORIGIN);
        String destin = record.get(CSV_HEADER_SALESINV_DESTIN);
        String flightNo = record.get(CSV_HEADER_SALESINV_FLIGHT_NO);
        String startDate = record.get(CSV_HEADER_SALESINV_START_DATE);
        String endDate = record.get(CSV_HEADER_SALESINV_END_DATE);
        String dow = record.get(CSV_HEADER_SALESINV_DOW);
        String curveId = record.get(CSV_HEADER_SALESINV_CURVE_ID);
        String saleFlag = record.get(CSV_HEADER_SALESINV_SALE_FLAG);

        String sector = origin + destin;
        String lineNum = "Line " + rowCount + ": " + sector + "_" + flightNo + "_" + startDate + ": ";

        if (!isLengthEqualTo(origin, 3)) listOfErrors.add(lineNum + "Origin must be 3 characters.");
        if (!isLengthEqualTo(destin, 3)) listOfErrors.add(lineNum + "Destin must be 3 characters.");
        if (!isNumeric(flightNo)) listOfErrors.add(lineNum + "FlightNumber must be numeric.");

        if (!"1".equals(saleFlag) && !"0".equals(saleFlag)) {
            listOfErrors.add(lineNum + "saleflag must be '1' or '0'.");
        }

        if (!(isLengthEqualTo(dow, 7) && isNumeric(dow) && checkDOWDigits(dow))) {
            listOfErrors.add(lineNum + "DOW is not 7 digits or has a value other than '1' or '9'.");
        }

        if (!listOfCurves.contains(curveId)) {
            listOfErrors.add(lineNum + "CurveID is not valid.");
        }

        try {
            LocalDate formattedStartDate = LocalDate.parse(startDate, DATE_OUTPUT_FORMATTER);
            LocalDate formattedEndDate = LocalDate.parse(endDate, DATE_OUTPUT_FORMATTER);
            if (formattedEndDate.isBefore(formattedStartDate)) {
                listOfErrors.add(lineNum + "EndDate must be on or after StartDate.");
            }
        } catch (Exception e) {
            listOfErrors.add(lineNum + "Date is in an invalid format.");
        }
    }

    private static void createMarketListSalesInvEntity(List<String> record, List<MarketListSalesInvEntity> entityList) {
        entityList.add(MarketListSalesInvEntity.builder()
                .origin(record.get(CSV_HEADER_SALESINV_ORIGIN))
                .destin(record.get(CSV_HEADER_SALESINV_DESTIN))
                .flightNumber(record.get(CSV_HEADER_SALESINV_FLIGHT_NO))
                .perStart(record.get(CSV_HEADER_SALESINV_START_DATE))
                .perEnd(record.get(CSV_HEADER_SALESINV_END_DATE))
                .dow(record.get(CSV_HEADER_SALESINV_DOW))
                .curveId(record.get(CSV_HEADER_SALESINV_CURVE_ID))
                .saleFlag(record.get(CSV_HEADER_SALESINV_SALE_FLAG))
                .build());
    }

    public void insertIntoMarketListSalesInv(MarketListSalesInvEntity entity, String tableName) {
        try (PreparedStatement pstmt = connection.prepareStatement(INSERT_INTO_MARKET_LIST_SALESINV(tableName))) {
            pstmt.setString(1, entity.getOrigin());
            pstmt.setString(2, entity.getDestin());
            pstmt.setLong(3, Long.parseLong(entity.getFlightNumber()));
            pstmt.setString(4, entity.getPerStart());
            pstmt.setString(5, entity.getPerEnd());
            pstmt.setLong(6, Long.parseLong(entity.getDow()));
            pstmt.setString(7, entity.getCurveId());
            pstmt.setString(8, entity.getSaleFlag());
            pstmt.executeUpdate();
        } catch (SQLException e) {
            error_occurred = 1;
            String errorMsg = "Failed to insert salesinv record for sector " + entity.getOrigin() + entity.getDestin();
            listOfErrors.add(errorMsg);
            System.out.println(DATABASE_ACCESS_ERROR_MESSAGE + ": " + e.getMessage());
            e.printStackTrace();
        }
    }
}