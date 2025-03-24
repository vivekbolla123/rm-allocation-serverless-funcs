package com.akasaair.queues;

import com.akasaair.queues.common.Validations.CurvesValidator;
import com.akasaair.queues.common.Validations.FaresValidator;
import com.akasaair.queues.common.Validations.ProfileValidator;
import com.akasaair.queues.common.Validations.Validator;
import com.akasaair.queues.common.aws.DependencyFactory;
import com.akasaair.queues.common.aws.S3FileStorageDao;
import com.akasaair.queues.common.entity.MarketListConnectionsEntity;
import com.akasaair.queues.common.entity.MarketListEntity;
import com.akasaair.queues.common.helpers.JdbcConnection;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.nimbusds.jose.shaded.json.JSONArray;
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
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.akasaair.queues.common.Validations.MarketListValidator.*;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.AU_VALUE_COLUMN_NAME;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.BAND_COLUMN_NAME;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.CHANNEL_COLUMN_NAME;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.COLUMNS_COLUMN_NAME;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.COUNT_COLUMN_NAME;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.CRITERIA_COLUMN_NAME;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.CURVES_COLUMN_NAME;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.DATE_COLUMN_NAME;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.DECISION_COLUMN_NAME;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.DISTRESS_DLF_BANDS_COLUMN_NAME;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.DISTRESS_NDO_BANDS_COLUMN_NAME;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.DLF_BANDS_COLUMN_NAME;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.DPLF_BAND_COLUMN_NAME;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.HOUR_COLUMN_NAME;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.NDO_BANDS_COLUMN_NAME;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.NDO_BAND_COLUMN_NAME;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.NDO_END_COLUMN_NAME;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.NDO_START_COLUMN_NAME;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.PARAMETER_VALUE_COLUMN_NAME;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.PERMISSION_COLUMN_NAME;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.ROUTE_COLUMN_NAME;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.SECTOR_COLUMN_NAME;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.SECTOR_MAP_USER1_COLUMN_NAME;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.SECTOR_MAP_USER2_COLUMN_NAME;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.SECTOR_MAP_USER3_COLUMN_NAME;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.SECTOR_MAP_USER4_COLUMN_NAME;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.STATION_COLUMN_NAME;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.STRATEGY_COLUMN_NAME;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.TIME_RANGE_COLUMN_NAME;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.USER_COLUMN_NAME;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.UUID_COLUMN_NAME;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.VALUE_COLUMN_NAME;
import static com.akasaair.queues.common.constants.Constants.*;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_AUTO_BACKSTOP_FLAG;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_AUTO_GROUP;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_AUTO_TIME_RANGE_FLAG;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_B2B_BACKSTOP;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_B2B_FACTOR;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_B2B_TOLERANCE;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_B2C_BACKSTOP;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_B2C_TOLERANCE;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_CARR_EXCLUSION_B2B;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_CARR_EXCLUSION_B2C;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_FLIGHT_EXCLUSION_B2B;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_FLIGHT_EXCLUSION_B2C;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_CONNECTIONS_B2BBACKSTOP;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_CONNECTIONS_B2BFACTOR;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_CONNECTIONS_B2CBACKSTOP;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_CONNECTIONS_CURRENCY;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_CONNECTIONS_DISCOUNTFLAG;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_CONNECTIONS_DISCOUNTVALUE;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_CONNECTIONS_DOW;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_CONNECTIONS_FAREANCHOR;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_CONNECTIONS_FIRSTRBDALLOC;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_CONNECTIONS_FLIGHT1;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_CONNECTIONS_FLIGHT2;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_CONNECTIONS_OFFSET;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_CONNECTIONS_OTHERRBDALLOC;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_CONNECTIONS_OUTBOUNDDURATION;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_CONNECTIONS_OUTBOUNDSTOP;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_CONNECTIONS_PEREND;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_CONNECTIONS_PERSTART;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_CONNECTIONS_PRICESTRATEGY;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_CONNECTIONS_SECTOR1;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_CONNECTIONS_SECTOR2;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_CONNECTIONS_SKIPPINGFACTOR;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_CRITERIA;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_CURVE_ID;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_DAY_OF_WEEK;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_DAY_SPAN;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_DESTINATION;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_DISTRESS_INVENTORY_FLAG;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_END_DATE;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_END_TIME;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_FARE_ANCHOR;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_FARE_OFFSET;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_FIRST_ALLOCATION;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_FLIGHT_NO;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_HARD_ANCHOR;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_OFFSET;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_OPENING_FARES;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_ORIGIN;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_OTHER_ALLOCATION;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_OVER_BOOKING;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_PLF_THRESHOLD;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_PROFILE_FARES;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_RBD_PUSH_FLAG;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_SERIES_BLOCK_FLAG;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_SKIPPING_FACTOR;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_START_DATE;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_START_TIME;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_TBF_FLAG;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.CSV_HEADER_TIME;
import static com.akasaair.queues.common.constants.CsvHeaderConstants.ROW_START_COUNT;
import static com.akasaair.queues.common.constants.DateTimeConstants.DATE_OUTPUT_FORMATTER;
import static com.akasaair.queues.common.constants.DateTimeConstants.DATE_TIME_OUTPUT_FORMATTER;
import static com.akasaair.queues.common.constants.DateTimeConstants.SIMPLE_DATE_OUTPUT_FORMATTER_2;
import static com.akasaair.queues.common.constants.DateTimeConstants.SIMPLE_DAY_FORMATTER;
import static com.akasaair.queues.common.constants.ErrorMessagesConstants.DATABASE_ACCESS_ERROR_MESSAGE;
import static com.akasaair.queues.common.constants.ErrorMessagesConstants.FILE_UPLOAD_ERROR_MESSAGE;
import static com.akasaair.queues.common.constants.ErrorMessagesConstants.INVALID_DATA_ERROR_MESSAGE;
import static com.akasaair.queues.common.constants.ErrorMessagesConstants.INVALID_NUMBER_OF_ROW_COLUMN_ERROR_MESSAGE;
import static com.akasaair.queues.common.constants.ErrorMessagesConstants.INVALID_TABLE_NAME_ERROR_MESSAGE;
import static com.akasaair.queues.common.constants.PermissionsConstants.ADMIN;
import static com.akasaair.queues.common.constants.PermissionsConstants.MANAGER;
import static com.akasaair.queues.common.constants.PermissionsConstants.PERMISSION_UPDATE_MARKET_LIST_AFTER_30_DAYS;
import static com.akasaair.queues.common.constants.PermissionsConstants.PERMISSION_UPDATE_MARKET_LIST_ALL;
import static com.akasaair.queues.common.constants.PermissionsConstants.PERMISSION_UPDATE_MARKET_LIST_TILL_30_DAYS;
import static com.akasaair.queues.common.constants.QueryConstants.*;
import static com.akasaair.queues.common.constants.TableNamesConstants.CONFIG_PROFILE_FARE_STATION_UPSELL;
import static com.akasaair.queues.common.constants.TableNamesConstants.CRITERIA_TABLE;
import static com.akasaair.queues.common.constants.TableNamesConstants.CURVES_TABLE_NAME;
import static com.akasaair.queues.common.constants.TableNamesConstants.DATE_EVENT_TABLE_NAME;
import static com.akasaair.queues.common.constants.TableNamesConstants.DISTRESS_INVENTORY_STRATEGY_TABLE_NAME;
import static com.akasaair.queues.common.constants.TableNamesConstants.DPLF_BANDS_TABLE;
import static com.akasaair.queues.common.constants.TableNamesConstants.HISTORIC_GRID_DROP_TABLE_NAME;
import static com.akasaair.queues.common.constants.TableNamesConstants.HISTORIC_HOURLY_DROP_TABLE_NAME;
import static com.akasaair.queues.common.constants.TableNamesConstants.MARKET_LIST_ADHOC_CONNECTIONS_TABLE_NAME;
import static com.akasaair.queues.common.constants.TableNamesConstants.MARKET_LIST_ADHOC_TABLE_NAME;
import static com.akasaair.queues.common.constants.TableNamesConstants.MARKET_LIST_CONNECTIONS_TABLE_NAME;
import static com.akasaair.queues.common.constants.TableNamesConstants.MARKET_LIST_INTERNATIONAL_TABLE_NAME;
import static com.akasaair.queues.common.constants.TableNamesConstants.MARKET_LIST_TABLE_NAME;
import static com.akasaair.queues.common.constants.TableNamesConstants.MARKET_LIST_TABLE_NAME_S3;
import static com.akasaair.queues.common.constants.TableNamesConstants.NDO_BANDS_TABLE;
import static com.akasaair.queues.common.constants.TableNamesConstants.PROFILE_FARES_TABLE_NAME;
import static com.akasaair.queues.common.constants.TableNamesConstants.QP_FARES_TABLE_NAME;
import static com.akasaair.queues.common.constants.TableNamesConstants.SERIES_BLOCKING_TABLE_NAME;
import static com.akasaair.queues.common.constants.TableNamesConstants.STRATEGY_INPUT_NAME;
import static com.akasaair.queues.common.constants.TableNamesConstants.STRATEGY_TABLE_NAME;
import static com.akasaair.queues.common.constants.TableNamesConstants.TIME_RANGE_TABLE;
import static com.akasaair.queues.common.helpers.CsvHelper.convertToCSV;
import static com.akasaair.queues.common.helpers.CsvHelper.getDataFromCSVFile;

public class App implements RequestHandler<S3Event, Object> {
    final Connection connection = new JdbcConnection().connection();
    final Connection connection2 = new JdbcConnection().connection();
    List<String> listOfErrors = new ArrayList<>();
    private final S3AsyncClient s3Client;
    final S3FileStorageDao s3FileStorageDao = new S3FileStorageDao();
    public static int error_occurred = 0;

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
        return null;
    }

    private void uploadFile(String key, String bucketName) {
        final String finalKey = key;
        int lengthOfKey = finalKey.length();
        String obj_id = finalKey.substring(S3_FILE_PATH_NAME.length() + 7, lengthOfKey - 4);

        // Initialize the tableName variable
        String tableName = null;
        Object role = null;
        String usern = null;
        String routeStr = null;
        String nameForadhoc = null;
        LocalDateTime startTime = null;
        AtomicLong numOfRows = new AtomicLong();

        // Database query to get the tableName
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
            listOfErrors.add(e.getMessage());
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

        // Now you can use tableName in your subsequent logic
        System.out.println("The table name is -> " + table);
        try {
            insertAndValidatedBasedOnTableName(bucketName, key, obj_id, tableName, numOfRows, table, role_obj, userName,
                    adhocUsername, routeString, status);
        } catch (Exception e) {
            error_occurred = 1;
            if (listOfErrors.isEmpty()) {
                listOfErrors.add(e.getMessage());
                addErrorsToTable(listOfErrors, obj_id);
            }
            System.out.println(e);
            e.printStackTrace();
        }
        listOfErrors.clear();
        if (error_occurred == 0) {
            status = UPLOAD_QUEUE_STATUS_DONE;
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

        // Formatting the string as hh:mm:ss
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
            System.out.println(DATABASE_ACCESS_ERROR_MESSAGE);
            e.printStackTrace();
        }

        // Deleting the object from S3
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
                                    throw new RuntimeException("Error processing file content: " + e.getMessage(), e);
                                }
                            })
                            .exceptionally(ex -> {
                                System.out.println("Error during file processing: " + ex.getMessage());
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
                                    throw new RuntimeException(
                                            "Error processing strategy file content: " + e.getMessage(), e);
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
                                    uploadDateEventFile(reader, userName, obj_id);
                                } catch (Exception e) {
                                    throw new RuntimeException(
                                            "Error processing date event file content: " + e.getMessage(), e);
                                }
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
            System.out.println("Error in insertAndValidatedBasedOnTableName: " + e.getMessage());
            e.printStackTrace();
            throw e; // Rethrow to propagate up the call stack
        }
    }

    public void uploadMarketListFile(Reader file, String tableName, String userName, Object role, String route,
            String obj_id, AtomicLong numOfRows) throws DataMismatchedException {
        List<MarketListEntity> marketListEntityList = new ArrayList<>();
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
            List<Object> listOfCriteria = searchDistinctResults(CRITERIA_COLUMN_NAME, CRITERIA_TABLE);

            List<List<String>> csvData = getDataFromCSVFile(file);
            csvData = splitRowsByDateRange(csvData, listOfErrors);
            numOfRows.set(csvData.size() - 1);
            for (int i = 0; i < csvData.size(); i++) {
                String uuidForEntries = UUID.randomUUID().toString();
                String analystName = validationForMarketListFile(csvData.get(i), permissions, route, listOfErrors, i,
                        role, userName, listOfCurves, listOfStrategies, listOfCriteria);
                createMarketListEntity(csvData.get(i), marketListEntityList, uuidForEntries, analystName);
            }

            if (!listOfErrors.isEmpty()) {
                updateInputStatus(tableName);
                error_occurred = 1;
                addErrorsToTable(listOfErrors, obj_id);
                System.out.println(FILE_UPLOAD_ERROR_MESSAGE);
                throw new DataMismatchedException(listOfErrors);
            }

            if (tableName.startsWith(MARKET_LIST_ADHOC_TABLE_NAME)) {
                deleteValues(Collections.singleton(ZERO), tableName, ZERO);
            } else {
                deleteRecords(marketListEntityList, tableName);
            }
            for (MarketListEntity marketEntity : marketListEntityList) {
                insertIntoMarketList(marketEntity, tableName);
            }
            s3FileStorageDao.storeFile(createCSV(tableName), tableName, uuid);
            updateAuditStatement(tableName, getCurrentDateTime(), uuid);
        } catch (IOException | ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            updateInputStatus(tableName);
        }
    }

    public static List<List<String>> splitRowsByDateRange(List<List<String>> csvData, List<String> listOfErrors) {
        DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("MM/dd/yyyy");
        List<List<String>> intermediateResult = new ArrayList<>();
        List<List<String>> result = new ArrayList<>();
        Set<String> uniqueKeys = new HashSet<>();

        // Add the header row to the result without processing
        if (!csvData.isEmpty()) {
            intermediateResult.add(new ArrayList<>(csvData.get(0)));
        }

        // Process the rest of the rows, starting from index 1
        for (int rowIndex = 1; rowIndex < csvData.size(); rowIndex++) {
            List<String> row = csvData.get(rowIndex);
            LocalDate startDate = LocalDate.parse(row.get(3), DATE_FORMATTER);
            LocalDate endDate = LocalDate.parse(row.get(4), DATE_FORMATTER);
            String dowPattern = row.get(5);

            if (startDate.equals(endDate)) {
                int dayOfWeek = startDate.getDayOfWeek().getValue() % 7; // 0 for Sunday, 1 for Monday, etc.
                if (dowPattern.charAt(dayOfWeek) == '1') {
                    addRowWithDuplicateCheck(intermediateResult, uniqueKeys, row, listOfErrors, rowIndex);
                }
                // If DOW doesn't match, the record is implicitly removed by not adding it to
                // the result
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
            listOfErrors.add("Duplicate record found at Line" + rowIndex + ": " + key);
        }
        result.add(row);
    }

    private static String adjustDowPattern(LocalDate date, String dowPattern) {
        // Create a new DOW pattern with only the relevant day set to '1'
        StringBuilder adjustedPattern = new StringBuilder("9999999");
        int dayOfWeek = date.getDayOfWeek().getValue() % 7; // 0 for Sunday, 1 for Monday, etc.
        adjustedPattern.setCharAt(dayOfWeek, '1');
        return adjustedPattern.toString();
    }

    public void addErrorsToTable(List<String> errorsList, String obj_id) {
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

    public ByteArrayInputStream createCSV(String tableName) throws DataMismatchedException {

        if (findIfTableExists(tableName) == 0) {
            throw new DataMismatchedException(Collections.singletonList(INVALID_TABLE_NAME_ERROR_MESSAGE));
        }
        String tableNameForHeader = tableName;
        if (tableNameForHeader.startsWith(MARKET_LIST_ADHOC_TABLE_NAME + UNDERSCORE + CONNECTIONS))
            tableNameForHeader = MARKET_LIST_CONNECTIONS_TABLE_NAME;
        if (tableNameForHeader.startsWith(MARKET_LIST_TABLE_NAME))
            tableNameForHeader = MARKET_LIST_TABLE_NAME_S3;

        List<String> headers = Arrays.asList(getColumnHeaders(tableNameForHeader).split(COMMA));
        List<Map<String, Object>> values;
        if (tableNameForHeader.equals(STRATEGY_TABLE_NAME)) {
            values = retrieveStrategyGrid(null);
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

    public int findIfTableExists(String tableName) {
        List<Map<String, Object>> listObj = new ArrayList<>();
        String query = "SHOW TABLES LIKE '" + tableName + "'";

        try (Statement stmt = connection.createStatement();
                ResultSet resultSet = stmt.executeQuery(query)) {
            while (resultSet.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("tableName", resultSet.getString(1)); // Usually the table name is in the first column
                listObj.add(row);
            }
        } catch (SQLException e) {
            System.out.println(DATABASE_ACCESS_ERROR_MESSAGE);
            e.printStackTrace();
        }
        return listObj.size(); // Will be >0 if table exists
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

    private static Validator getValidator(String tableName) {
        Validator validator;
        switch (tableName) {
            case CURVES_TABLE_NAME -> validator = new CurvesValidator();
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
            String obj_id) throws DataMismatchedException {
        try {
            List<String> permissions = getRoleAccess(role);
            String uuid = obj_id;
            insertAuditStatement(tableName, getCurrentDateTime(), userName, uuid);
            insertInputStatusTable(tableName);
            List<String> errorsFound = new ArrayList<>();
            List<MarketListConnectionsEntity> MarketListConnectionsEntityList = new ArrayList<>();
            int rowCount = ROW_START_COUNT;
            if (tableName.startsWith(MARKET_LIST_ADHOC_TABLE_NAME)) {
                createTable(tableName, MARKET_LIST_CONNECTIONS_TABLE_NAME);
            }
            List<List<String>> csvData = getDataFromCSVFile(file);

            for (int i = 1; i < csvData.size(); i++) {
                String uuidForEntries = generateUUID();
                String analystName = validationForMarketListConnectionsFile(csvData.get(i), rowCount, permissions, role,
                        userName, errorsFound);
                createMarketListConnectionsEntity(csvData.get(i), MarketListConnectionsEntityList, uuidForEntries,
                        analystName);
                rowCount++;
            }

            if (!errorsFound.isEmpty()) {
                error_occurred = 1;
                addErrorsToTable(errorsFound, obj_id);
                updateInputStatus(tableName);
                throw new DataMismatchedException(errorsFound);
            }
            if (tableName.startsWith(MARKET_LIST_ADHOC_TABLE_NAME)) {
                deleteValues(Collections.singleton(ZERO), tableName, ZERO);
            } else {
                deleteRecordsConnections(MarketListConnectionsEntityList, tableName);
            }

            for (MarketListConnectionsEntity marketEntity : MarketListConnectionsEntityList) {
                insertIntoMarketListConnections(marketEntity, tableName);
            }
            s3FileStorageDao.storeFile(createCSV(tableName), tableName, uuid);
            updateAuditStatement(tableName, getCurrentDateTime(), uuid);
        } catch (IOException | ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            updateInputStatus(tableName);
        }
    }

    public void insertIntoMarketListConnections(MarketListConnectionsEntity marketListEntity, String tableName) {
        try (PreparedStatement pstmt = connection.prepareStatement(INSERT_INTO_MARKET_LIST_CONNECTIONS(tableName))) {
            // Setting the PreparedStatement parameters
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
            pstmt.setString(21, marketListEntity.getCurrency());
            pstmt.setString(22, marketListEntity.getFareAnchor());
            pstmt.setString(23, marketListEntity.getOffset());
            pstmt.setString(24, marketListEntity.getDiscountFlag());
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println(DATABASE_ACCESS_ERROR_MESSAGE);
            e.printStackTrace();
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
                .build());
    }

    public void uploadDateEventFile(Reader file, String userName, String obj_id) throws RuntimeException {
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
            Map<String, Map<String, String>> dateEventMap = getDateEvent(csvData);
            List<String> dateEventHeaders = Arrays.asList(getColumnHeaders(DATE_EVENT_TABLE_NAME).split(COMMA));
            List<String> upsellHeaders = Arrays
                    .asList(getColumnHeaders(CONFIG_PROFILE_FARE_STATION_UPSELL).split(COMMA));
            List<Map<String, String>> data = createListOfMapDateEvent(csvData, dateEventHeaders);

            insertStatement(new ArrayList<>(dateEventMap.values()), upsellHeaders,
                    ProfileFareStationUpsellTempTableName);
            insertStatement(data, dateEventHeaders, dateEventTempTableName);
            String columnName = SECTOR_COLUMN_NAME.toLowerCase();
            Set<String> uniqueValues = dateEventMap.values().stream()
                    .map(obj -> obj.get(columnName))
                    .collect(Collectors.toSet());

            deleteValues(Collections.singleton(ZERO), DATE_EVENT_TABLE_NAME, ZERO);
            deleteValues(uniqueValues, CONFIG_PROFILE_FARE_STATION_UPSELL, columnName);

            copyFromTempToMainTable(CONFIG_PROFILE_FARE_STATION_UPSELL, ProfileFareStationUpsellTempTableName);
            copyFromTempToMainTable(DATE_EVENT_TABLE_NAME, dateEventTempTableName);

            updateAuditStatement(CONFIG_PROFILE_FARE_STATION_UPSELL, getCurrentDateTime(), uuid);
            updateAuditStatement(DATE_EVENT_TABLE_NAME, getCurrentDateTime(), uuid);

            deleteTable(ProfileFareStationUpsellTempTableName);
            deleteTable(dateEventTempTableName);
            error_occurred = 0;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            updateInputStatus(CONFIG_PROFILE_FARE_STATION_UPSELL);
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
            String date = singleCsvData.get(0);
            String dayOfWeek = getAbbreviatedDayOfWeek(parseDate(date));

            // Add day of the week to the second position
            singleCsvData.add(1, dayOfWeek);

            Map<String, String> map = new HashMap<>();
            for (int i = 0; i < headers.size(); i++) {
                map.put(headers.get(i), singleCsvData.get(i));
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
            for (int j = startColumn; j < inputRow.size(); j++) {
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
            columnNames.append(header).append(",");
            placeholders.append("?,");
        }

        // Finalize the SQL statement
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
                    // Every 5000 records, execute batch and reset count
                    if (++count % 10000 == 0) {
                        pstmt.executeBatch();
                        connection2.commit();
                        System.out.println("10000 records inserted successfully.");
                    }
                }
                // Execute any remaining records if they don't make up a full batch
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
        return listObj;
    }

    public void deleteValuesStrategyTable(Set<String> value, String tableName, String strategyColumnName,
            String channelColumnName) {
        if (!value.isEmpty()) {
            // Creating the placeholder string for the SQL IN clause
            String placeholders = value.stream().map(v -> "?").collect(Collectors.joining(", "));
            // Constructing to delete SQL query
            // Note: Direct table and column name insertion into the query can lead to SQL
            // injection if not handled properly.
            String deleteQuery = "DELETE FROM " + tableName + " WHERE concat(" + strategyColumnName + ","
                    + channelColumnName + ") IN (" + placeholders + ")";
            try (PreparedStatement pstmt = connection.prepareStatement(deleteQuery)) {
                // Execute the update
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
                    String errorMsg = "Line " + i + ": " + INVALID_DATA_ERROR_MESSAGE
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

        // List of headers that need Y/N conversion
        List<String> booleanHeaders = Arrays.asList(
                "OwnFareFlag", "AutoTimeRangeFlag", "openingFares", "OverBooking",
                "profileFares", "rbdPushFlag", "distressInventoryFlag", "seriesBlock",
                "autoGroup", "autoBackstopFlag");

        String headerValues = String.join(",", headers);
        String limitCondition = (limit > 0) ? " LIMIT " + limit : "";
        String query = "SELECT " + headerValues + " FROM " + tableName + limitCondition;

        try (Statement stmt = connection.createStatement();
                ResultSet resultSet = stmt.executeQuery(query)) {
            while (resultSet.next()) {
                Map<String, Object> row = new HashMap<>();
                for (String header : headers) {
                    Object value = resultSet.getObject(header);

                    // Convert 1/0 to Y/N for specified headers
                    if (booleanHeaders.contains(header) && value != null) {
                        // Handle both Integer and String cases
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
        try (PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_INPUT_STATUS(0, name))) {
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void updateAuditStatement(String tableName, String endTime, String uuid) {
        try {
            // Update file_upload_audit table
            try (PreparedStatement pstmt = connection.prepareStatement(UPDATE_FILE_UPLOAD())) {
                pstmt.setString(1, endTime);
                pstmt.setString(2, uuid);
                pstmt.setString(3, tableName);
                pstmt.executeUpdate();
            }
            // Retrieve the current version count
            int count = 0;
            try (PreparedStatement pstmt = connection.prepareStatement(GET_CURRENT_VERSION_COUNT())) {
                pstmt.setString(1, tableName);
                ResultSet rs = pstmt.executeQuery();
                if (rs.next()) {
                    count = rs.getInt(COUNT_COLUMN_NAME);
                }
            }
            // Update or insert into currentVersion table based on count
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
            pstmt.setString(41, marketListEntity.getUuid());
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void deleteValues(Set<String> value, String tableName, String columnName) {
        if (!value.isEmpty()) {
            // Build the placeholder string (?, ?, ?, ...)
            String placeholders = value.stream().map(v -> "?").collect(Collectors.joining(", "));
            // Build to delete SQL query using the placeholders
            String deleteQuery = "DELETE FROM " + tableName + " WHERE " + columnName + " IN (" + placeholders + ")";
            try (PreparedStatement pstmt = connection.prepareStatement(deleteQuery)) {
                // Set each value in the PreparedStatement
                int index = 1;
                for (String val : value) {
                    pstmt.setString(index++, val);
                }
                // Execute the update
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
                // Set each value in the PreparedStatement
                int index = 1;
                for (String val : value) {
                    pstmt.setString(index++, val);
                }
                // Execute the update
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
                try (PreparedStatement updateStmt = connection.prepareStatement(UPDATE_INPUT_STATUS(1, tableName))) {
                    updateStmt.executeUpdate();
                }
            } else {
                try (PreparedStatement insertStmt = connection.prepareStatement(INSERT_INPUT_STATUS())) {
                    insertStmt.setString(1, tableName);
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
                .analystName(analystName)
                .build());
    }

    private static String convertYNToBoolean(String value) {
        return "Y".equalsIgnoreCase(value) ? "1" : "0";
    }

    public List<Object> searchDistinctResults(String columnName, String tableName) {
        List<Object> listObj = new ArrayList<>();
        // Constructing SQL query with direct concatenation;
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
            List<Object> listOfStrategies, List<Object> listOfCriteria) throws Exception {
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
            String lineNum = "Line " + rowCount + ": " + sector_flight_dept + ":";
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
            if (!listOfStrategies.contains(record.get(CSV_HEADER_FARE_ANCHOR))) {
                listOfErrors.add(lineNum + "Fare Anchor must be one of the valid strategies");
            }
            String hardAnchor = record.get(CSV_HEADER_HARD_ANCHOR);
            String fareOffset = record.get(CSV_HEADER_FARE_OFFSET);
            String plfThreshold = record.get(CSV_HEADER_PLF_THRESHOLD);

            if (listOfCriteria.contains(hardAnchor)) {
                // When hardAnchor is from listOfCriteria, fareOffset is mandatory and must be
                // numeric
                if (fareOffset == null || fareOffset.trim().isEmpty()) {
                    listOfErrors.add(lineNum + "Fare Offset is required when Hard Anchor is applied");
                }
                if (plfThreshold != null && !plfThreshold.trim().isEmpty()) {
                    String trimmedValue = plfThreshold.trim();
                    if (!trimmedValue.endsWith("%")) {
                        listOfErrors.add(lineNum + "PLF Threshold must include a percentage symbol (%)");
                    } else {
                        // Remove the % symbol for numeric validation
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
            // If hardAnchor is numeric
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
                        // Remove the % symbol for numeric validation
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
                // When hardAnchor is numeric, fareOffset is optional but must be numeric if
                // present
                if (fareOffset != null && !fareOffset.trim().isEmpty() && !isNumeric(fareOffset)) {
                    listOfErrors.add(lineNum + "If present, Fare Offset must be a valid number");
                }
                if (plfThreshold == null || plfThreshold.trim().isEmpty()) {
                    listOfErrors.add(lineNum + "plfThreshold is required when Hard Anchor is applied");
                }
            }
            // If hardAnchor is empty - all three fields can be empty
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

            return user;
        } catch (Exception e) {
            throw new Exception(e);
        }
    }

    private String validationForMarketListConnectionsFile(List<String> record, int rowCount, List<String> permissions,
            Object role, String userName, List<String> errorsFound) {
        JSONArray roles = new JSONArray();
        if (role instanceof String roleStr) {
            roleStr = roleStr.substring(1, roleStr.length() - 1);
            roleStr = roleStr.substring(1, roleStr.length() - 1);
            roles.add(roleStr);
        }

        String user1 = EMPTY;
        String user2 = EMPTY;
        String user = EMPTY;

        String sector1 = record.get(CSV_HEADER_CONNECTIONS_SECTOR1);
        String sector2 = record.get(CSV_HEADER_CONNECTIONS_SECTOR2);
        String flight_line = sector1 + "_" + record.get(CSV_HEADER_CONNECTIONS_FLIGHT1) + "_" + sector2  + "_" + record.get(CSV_HEADER_CONNECTIONS_FLIGHT2)  + "_" + record.get(CSV_HEADER_CONNECTIONS_PERSTART);
        String lineNum = "Line " + rowCount + ": " + flight_line + ": ";
        if (!isLengthEqualTo(record.get(CSV_HEADER_CONNECTIONS_SECTOR1), 6)) {
            errorsFound.add(lineNum + "Sector1  is not equal to 6 characters");
        }
        if (!isLengthEqualTo(record.get(CSV_HEADER_CONNECTIONS_SECTOR2), 6)) {
            errorsFound.add(lineNum + "Sector2  is not equal to 6 characters");
        }
        if (!isNumeric(record.get(CSV_HEADER_CONNECTIONS_FLIGHT2))) {
            errorsFound.add(lineNum + "Flight Number2 is not a Number");
        }
        if (!isNumeric(record.get(CSV_HEADER_CONNECTIONS_FLIGHT1))) {
            errorsFound.add(lineNum + "Flight Number1 is not a Number");
        }
        try {
            String startDate = record.get(CSV_HEADER_CONNECTIONS_PERSTART);
            String endDate = record.get(CSV_HEADER_CONNECTIONS_PEREND);
            LocalDate daysFromToday = LocalDate.now().plusDays(DAYS_TO_ENTER_INTO_MARKET_LIST);
            LocalDate formattedStartDate = LocalDate.parse(startDate, DATE_OUTPUT_FORMATTER);
            LocalDate formattedEndDate = LocalDate.parse(endDate, DATE_OUTPUT_FORMATTER);
            if (permissions.contains(PERMISSION_UPDATE_MARKET_LIST_TILL_30_DAYS)) {
                user1 = getSectors(sector1, SECTOR_MAP_USER3_COLUMN_NAME);
                user2 = getSectors(sector2, SECTOR_MAP_USER3_COLUMN_NAME);
            } else if (permissions.contains(PERMISSION_UPDATE_MARKET_LIST_AFTER_30_DAYS)) {
                user1 = getSectors(sector1, SECTOR_MAP_USER4_COLUMN_NAME);
                user2 = getSectors(sector2, SECTOR_MAP_USER4_COLUMN_NAME);

            } else if (permissions.contains(PERMISSION_UPDATE_MARKET_LIST_ALL)) {
                if (!daysFromToday.isBefore(formattedEndDate) && !formattedStartDate.isAfter(daysFromToday)) {
                    user1 = getSectors(sector1, SECTOR_MAP_USER3_COLUMN_NAME);
                    user2 = getSectors(sector2, SECTOR_MAP_USER3_COLUMN_NAME);
                } else {
                    user1 = getSectors(sector1, SECTOR_MAP_USER4_COLUMN_NAME);
                    user2 = getSectors(sector2, SECTOR_MAP_USER4_COLUMN_NAME);
                }
            }
            if (!roles.contains(MANAGER) && !roles.contains(ADMIN)) {
                user = userName.split(SPACE)[0].toUpperCase();
                if (!userName.split(SPACE)[0].equalsIgnoreCase(user1)
                        && !userName.split(SPACE)[0].equalsIgnoreCase(user2)) {
                    errorsFound.add(lineNum + "You don't have access for this sector " + sector1 + SLASH + sector2);
                }
            } else {
                if (!daysFromToday.isBefore(formattedEndDate) && !formattedStartDate.isAfter(daysFromToday)) {
                    user1 = getSectors(sector1, SECTOR_MAP_USER3_COLUMN_NAME);
                    user2 = getSectors(sector2, SECTOR_MAP_USER3_COLUMN_NAME);
                } else {
                    user1 = getSectors(sector1, SECTOR_MAP_USER4_COLUMN_NAME);
                    user2 = getSectors(sector2, SECTOR_MAP_USER4_COLUMN_NAME);
                }
                if (user1 == null || user2 == null) {
                    errorsFound.add(lineNum + "You don't have access to any of these sector");
                } else {
                    if (user1.equalsIgnoreCase(user2)) {
                        user = user1;
                    } else {
                        user = user1 + SLASH + user2;
                    }
                }
            }
            if (formattedEndDate.isBefore(formattedStartDate)) {
                errorsFound.add(lineNum + "End date must be greater than or equal to start date");
            }
            if (permissions.contains(PERMISSION_UPDATE_MARKET_LIST_TILL_30_DAYS)) {
                if (daysFromToday.isBefore(formattedEndDate)) {
                    errorsFound.add(lineNum + "End date is above 30 days");
                }
                if (formattedStartDate.isAfter(daysFromToday)) {
                    errorsFound.add(lineNum + "Start date is above 30 days");
                }
            }
            if (permissions.contains(PERMISSION_UPDATE_MARKET_LIST_AFTER_30_DAYS)) {
                if (formattedStartDate.isBefore(daysFromToday)) {
                    errorsFound.add(lineNum + "Start date is below 30 days");
                }
                if (daysFromToday.isAfter(formattedEndDate)) {
                    errorsFound.add(lineNum + "End date is below 30 days");
                }
            }
        } catch (Exception e) {
            errorsFound.add(lineNum + "Date is an invalid format" + e);
        }
        if (!isValidDate(record.get(CSV_HEADER_CONNECTIONS_PEREND))) {
            errorsFound.add(lineNum + "End date is not a valid date");
        }
        if (!(isLengthEqualTo(record.get(CSV_HEADER_CONNECTIONS_DOW), 7)
                && isNumeric(record.get(CSV_HEADER_CONNECTIONS_DOW))
                && checkDOWDigits(record.get(CSV_HEADER_CONNECTIONS_DOW)))) {
            errorsFound.add(lineNum + "DOW is not 7 digits or has value other than '1' or '9'");
        }
        if (!checkPriceStrategy(record.get(CSV_HEADER_CONNECTIONS_PRICESTRATEGY))) {
            errorsFound.add(lineNum + "Strategy Flag has value other than '1' or '2'");
        }
        if (!isPositive(record.get(CSV_HEADER_CONNECTIONS_FIRSTRBDALLOC))) {
            errorsFound.add(lineNum + "First Allocation is not a number");
        }
        if (!(isPositive(record.get(CSV_HEADER_CONNECTIONS_OTHERRBDALLOC))
                && isPositive(record.get(CSV_HEADER_CONNECTIONS_OTHERRBDALLOC)))) {
            errorsFound.add(lineNum + "Other Allocation is not a positive number");
        }
        if (!(isNumeric(record.get(CSV_HEADER_CONNECTIONS_B2BBACKSTOP))
                && isPositive(record.get(CSV_HEADER_CONNECTIONS_B2BBACKSTOP)))) {
            errorsFound.add(lineNum + "B2B Backstop is not a positive number");
        }
        if (!(isNumeric(record.get(CSV_HEADER_CONNECTIONS_B2CBACKSTOP))
                && isPositive(record.get(CSV_HEADER_CONNECTIONS_B2CBACKSTOP)))) {
            errorsFound.add(lineNum + "B2C Backstop is not a positive number");
        }
        if (!(isDouble(record.get(CSV_HEADER_CONNECTIONS_B2BFACTOR)))) {
            errorsFound.add(lineNum + "B2B Factor is not a valid float");
        }
        if (!(isNumeric(record.get(CSV_HEADER_CONNECTIONS_SKIPPINGFACTOR)))) {
            errorsFound.add(lineNum + "Skipping Factor is not a number");
        }
        if (!(isDouble(record.get(CSV_HEADER_CONNECTIONS_OUTBOUNDDURATION)))) {
            errorsFound.add(lineNum + "Outbound Duration is not a number");
        }
        if (!(isLengthEqualTo(record.get(CSV_HEADER_CONNECTIONS_CURRENCY), 3))) {
            errorsFound.add(lineNum + "Currency is not equal to 3 characters");
        }
        if (!(checkFareAnchor(record.get(CSV_HEADER_CONNECTIONS_FAREANCHOR)))) {
            errorsFound.add(lineNum + "Fare Anchor is not a min,max,maxo,mino");
        }
        if (!(isNumeric(record.get(CSV_HEADER_CONNECTIONS_OFFSET)))) {
            errorsFound.add(lineNum + "offset is not a number");
        }
        if (!(checkStrategyFlag(record.get(CSV_HEADER_CONNECTIONS_DISCOUNTFLAG)))) {
            errorsFound.add(lineNum + "Discount Flag is not a 1 or 0");
        }
        return user;
    }

}