package com.akasaair.queues;

import com.akasaair.queues.common.Validations.*;
import com.akasaair.queues.common.aws.DependencyFactory;
import com.akasaair.queues.common.aws.S3FileStorageDao;
import com.akasaair.queues.common.entity.MarketListAuditEntity;
import com.akasaair.queues.common.entity.MarketListConnectionsEntity;
import com.akasaair.queues.common.entity.MarketListEntity;
import com.akasaair.queues.common.helpers.AuditHelper;
import com.akasaair.queues.common.helpers.MongoConnection;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import net.minidev.json.JSONArray;
import com.akasaair.queues.common.entity.MarketListSalesInvEntity;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.*;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.time.format.DateTimeFormatter;

import org.bson.Document;
import org.bson.conversions.Bson;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
//import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.*;
import java.math.BigInteger;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.text.SimpleDateFormat;
import static com.akasaair.queues.common.Validations.MarketListValidator.*;
import static com.akasaair.queues.common.Validations.CommonValidator.*;
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
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class, MongoAutoConfiguration.class, MongoDataAutoConfiguration.class})
@EnableScheduling
@EnableAsync
@Slf4j
public class App implements CommandLineRunner {
    @Value("${rm.file-path}")
    private String S3_FILE_PATH_NAME;

    @Value("${rm.s3-bucket}")
    private String S3_BUCKET_NAME;

    @Value("${rm.upload-path}")
    private String S3_UPLOAD_PATH_NAME;

    private final MongoDatabase database;
    private final MongoDatabase database2;
    private final MongoDatabase masterDatabase;

    private final S3FileStorageDao s3FileStorageDao;
    private final AuditHelper auditHelper;

    private final S3Client s3Client;
    private static final ThreadLocal<List<String>> listOfErrors = ThreadLocal.withInitial(ArrayList::new);
    private static final ThreadLocal<Integer> error_occurred = ThreadLocal.withInitial(() -> 0);
    private static final ThreadLocal<Integer> partial_upload_occurred = ThreadLocal.withInitial(() -> 0);
    private static final ThreadLocal<Integer> numOfRows = ThreadLocal.withInitial(() -> 0);
    //    final S3FileStorageDao s3FileStorageDao = new S3FileStorageDao();
    public static final String UPLOAD_QUEUE_STATUS_PARTIALLY_UPLOADED = "PARTIALLY UPLOADED";

    private static final Set<String> ALLOWED_SI_RBD_TYPES = Set.of("closer", "farther");
    public enum UploadFileType {
        UPSELL,
        DISTRESS
    }
    public App(S3FileStorageDao s3FileStorageDao, DependencyFactory dependencyFactory, MongoConnection mongoConnection, AuditHelper auditHelper) {
        this.s3FileStorageDao = s3FileStorageDao;
        this.s3Client = dependencyFactory.s3Client();
        this.database = mongoConnection.getDatabase();
        this.database2 = mongoConnection.getDatabase();
        this.masterDatabase = mongoConnection.getMasterDatabase();
        this.masterCollections = loadMasterCollections();
        this.auditHelper = auditHelper;
    }

    // Collections that reside in the master database (qp_rm_master).
    // Loaded from master_collections_config collection at startup; falls back to hardcoded defaults.
    private static final Set<String> MASTER_DB_COLLECTIONS;

    static {
        MASTER_DB_COLLECTIONS = Set.of(
                "active_sectors", "config_station_master",
                "market_fares_ag", "market_fares_rg", "tax_mast_dom", "tax_mast_intl",
                "active_flight_connections", "active_flight_legs", "cleanup_config"
        );
    }

    /**
     * Load master collection names by listing collections that exist in qp_rm_master.
     * Called after masterDatabase is initialized.
     */
    private Set<String> loadMasterCollections() {
        try {
            Set<String> collections = new HashSet<>();
            for (String name : masterDatabase.listCollectionNames()) {
                collections.add(name);
            }
            if (!collections.isEmpty()) {
                return collections;
            }
        } catch (Exception e) {
            log.warn("Failed to list collections from master db, using static fallback: " + e.getMessage());
        }
        return MASTER_DB_COLLECTIONS;
    }

    private final Set<String> masterCollections;

    /**
     * Returns the appropriate database for a given collection name.
     * Common/shared collections go to masterDatabase, others to the default database.
     */
    private MongoDatabase getDbForCollection(String collectionName) {
        if (masterCollections.contains(collectionName.toLowerCase())) {
            return masterDatabase;
        }
        return database;
    }

    public static void main(String[] args) {
        SpringApplication.run(App.class, args);

        // Local testing:
        // app.uploadFile(FILE_NAME, S3_BUCKET_NAME);
    }

    @Override
    public void run(String... args) throws Exception {
        // Comment after testing
//        uploadFile(FILE_NAME);
//        System.exit(0);
    }

    private static final int MAX_CONCURRENT_UPLOADS = 4;
    private static final int QUEUE_CAPACITY = 1000;
    private ThreadPoolExecutor uploadExecutor;
    private Thread scannerThread;
    private volatile boolean isRunning = true;
    private final BlockingQueue<S3Object> fileQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);

    // Deduplication set (keeps only unique keys)
    private final Set<String> processingSet = ConcurrentHashMap.newKeySet();

    @PostConstruct
    public void init() {
        initExecutor();
        startScannerThread();
        startWorkerThreads();
    }

    private void initExecutor() {
        uploadExecutor = new ThreadPoolExecutor(
                MAX_CONCURRENT_UPLOADS, MAX_CONCURRENT_UPLOADS, 20L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                r -> {
                    Thread t = new Thread(r);
//                    t.setName("Worker-" + t.getId());
                    t.setDaemon(true);
                    return t;
                });
        uploadExecutor.allowCoreThreadTimeOut(true);
    }

    /**
     * Dedicated S3 scanning thread.
     */
    private void startScannerThread() {
        scannerThread = new Thread(() -> {
            while (isRunning) {
                try {
                    scanAndEnqueueFiles();
                    Thread.sleep(5000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("Error scanning S3: {}", e.getMessage(), e);
                }
            }
        }, "S3-Scanner");
        scannerThread.setDaemon(true);
        scannerThread.start();
    }

    /**
     * Start worker pool threads that poll from fileQueue.
     */
    private void startWorkerThreads() {
        for (int i = 0; i < 4; i++) {
            uploadExecutor.submit(() -> {
                while (isRunning) {
                    try {
                        S3Object file = fileQueue.take();
                        String key = file.key();
                        if (processingSet.add(key)) {
                        try {
                            processFile(file);
                        } finally {
                                processingSet.remove(key);
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception ex) {
                        log.error("Worker error: {}", ex.getMessage(), ex);
                    }
                }
            });
        }
    }

    /**
     * Scans S3 and pushes unique files to queue.
     */
    private void scanAndEnqueueFiles() {
        ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(S3_BUCKET_NAME)
                .prefix(S3_UPLOAD_PATH_NAME)
                .build();

        ListObjectsV2Response response = s3Client.listObjectsV2(listRequest);

        List<S3Object> sortedFiles = response.contents().stream()
                .filter(obj -> !obj.key().endsWith("/"))
                .sorted(Comparator.comparing(S3Object::lastModified))
                .collect(Collectors.toList());

        int added = 0;
        for (S3Object file : sortedFiles) {
            String key = file.key();
            if (!processingSet.contains(key) && !fileQueue.contains(file)) {
                if (fileQueue.offer(file)) {
                    added++;
                } else {
                    log.warn("Queue full — skipping {}", key);
                    break;
                }
            }
        }

        if (added > 0) {
            log.info("Scanner found {} files, queued {} new, queue size = {}",
                    sortedFiles.size(), added, fileQueue.size());
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down RM File Handler...");

        // Interrupt the scanner thread (if sleeping)
        if (scannerThread != null && scannerThread.isAlive()) {
            scannerThread.interrupt();
        }

        // Stop accepting new tasks and let running ones finish
        uploadExecutor.shutdown();

        try {
            if (!uploadExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("Forcing worker shutdown...");
                uploadExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.warn("Interrupted during shutdown; forcing close.");
            uploadExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        log.info("Shutdown complete: all workers and scanner stopped.");
    }


    private void processFile(S3Object file) {
        String[] parts = file.key().split("/");
        String fileName = parts[parts.length - 1];
        try {
            log.info("Processing file: {}", fileName);
            uploadFile(file.key());
            log.info("Completed file: {}", fileName);
        } catch (Exception e) {
            log.error("Error processing {}: {}", fileName, e.getMessage(), e);
        }
    }

    private void uploadFile(String key) {
        listOfErrors.get().clear();
        error_occurred.set(0);
        partial_upload_occurred.set(0);
        numOfRows.set(0);

        final String finalKey = key;
        int lengthOfKey = finalKey.length();
        String obj_id = finalKey.substring(S3_UPLOAD_PATH_NAME.length(), lengthOfKey - 4);

        String tableName = null;
        Object role = null;
        String usern = null;
        String routeStr = null;
        String nameForadhoc = null;
        LocalDateTime startTime = null;

        try {
            MongoCollection<Document> collection = database.getCollection("inputs_file_upload");
            Document result = collection.find(eq("obj_id", obj_id)).first();

            if (result != null) {
                tableName = result.getString("tableName");
                role = result.get("role");
                usern = result.getString("username");
                nameForadhoc = result.getString("name");

                Object timestampObj = result.get("init_timestamp");
                if (timestampObj instanceof Date) {
                    startTime = ((Date) timestampObj).toInstant()
                            .atZone(ZoneId.systemDefault()).toLocalDateTime();
                }else if (timestampObj instanceof String) {
                    // Parse ISO date string
                    startTime = LocalDateTime.parse((String) timestampObj);
                }
                routeStr = result.getString("route_for_adhoc");
            }
        } catch (Exception e) {
            error_occurred.set(1);
            listOfErrors.get().add(DATABASE_ACCESS_ERROR_MESSAGE);
            addErrorsToTable(listOfErrors.get(), obj_id);
            System.out.println(DATABASE_ACCESS_ERROR_MESSAGE);
            e.printStackTrace();
        }
        final String table = tableName;
        final Object role_obj = role;
        final String userName = usern;
        final String adhocUsername = nameForadhoc;
        final String routeString = routeStr;
        String status = UPLOAD_QUEUE_STATUS_IN_PROGRESS;

        log.info("The table name is -> " + table);
        try {
            insertAndValidatedBasedOnTableName(S3_BUCKET_NAME, key, obj_id, tableName, table, role_obj, userName,
                    adhocUsername, routeString, status);
        } catch (Exception e) {
            error_occurred.set(1);
            if (listOfErrors.get().isEmpty()) {
                listOfErrors.get().add("Validation has failed while inserting data in table");
                addErrorsToTable(listOfErrors.get(), obj_id);
            }
            log.info(e.toString());
            e.printStackTrace();
        }
        listOfErrors.get().clear();
        if (error_occurred.get() == 0) {
            status = UPLOAD_QUEUE_STATUS_DONE;
        } else if (partial_upload_occurred.get() == 1) {
            status = UPLOAD_QUEUE_STATUS_PARTIALLY_UPLOADED;
        } else {
            status = UPLOAD_QUEUE_STATUS_FAILED;
        }

        int rNum = numOfRows.get();
        String formattedDuration = "00:00:00";
        LocalDateTime endTime = LocalDateTime.now();
        if (startTime != null) {
            try {
                Duration duration = Duration.between(startTime, endTime);
                long seconds = duration.getSeconds();
                long hours = seconds / 3600;
                long minutes = (seconds % 3600) / 60;
                long secs = seconds % 60;
                formattedDuration = String.format("%02d:%02d:%02d", hours, minutes, secs);
            } catch (Exception e) {
                log.info("Error calculating duration: " + e.getMessage());
                e.printStackTrace();
            }
        }
        log.info("Status--->" + status);

        try {
            MongoCollection<Document> collection = database.getCollection("inputs_file_upload");
            collection.updateOne(
                    eq("obj_id", obj_id),
                    combine(
                            set("status", status),
                            set("done_timestamp", Date.from(endTime.atZone(ZoneId.systemDefault()).toInstant())),
                            set("total_rows", rNum),
                            set("total_time_taken", formattedDuration)
                    )
            );
            System.out.println("Status Updated");
        } catch (Exception e) {
            listOfErrors.get().add(DATABASE_ACCESS_ERROR_MESSAGE);
            System.out.println(DATABASE_ACCESS_ERROR_MESSAGE);
            e.printStackTrace();
        }

        deleteS3Object(finalKey);
    }

    private void deleteS3Object(String finalKey) {
        if (finalKey == null || finalKey.length() < 20 || S3_FILE_PATH_NAME == null || S3_FILE_PATH_NAME.length() > finalKey.length()) {
            log.warn("Invalid key or path for S3 deletion: {}", finalKey);
            return;
        }
        // CWE-476/CWE-125: Ensure substring end index does not exceed key length
        int substringEnd = Math.min(S3_FILE_PATH_NAME.length() + 6, finalKey.length());
        if (!finalKey.substring(S3_FILE_PATH_NAME.length(), substringEnd).equalsIgnoreCase("locals")) {
            s3Client.deleteObject(DeleteObjectRequest.builder()
                    .bucket(S3_BUCKET_NAME)
                    .key(finalKey)
                    .build());
        }
    }

    private void insertAndValidatedBasedOnTableName(String bucketName, String key, String obj_id, String tableName,
                                                     String table, Object role_obj, String userName, String adhocUsername,
                                                    String routeString, String status) throws Exception {
        if (tableName == null) {
            error_occurred.set(1);
            listOfErrors.get().add("Table name is null. Cannot process file.");
            addErrorsToTable(listOfErrors.get(), obj_id);
            return;
        }
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
                    log.info("Route is ->" + route);
                    try {
                        MongoCollection<Document> collection = database.getCollection("inputs_file_upload");
                        collection.updateOne(
                                eq("obj_id", obj_id),
                                combine(
                                        set("status", status),
                                        set("inprogress_timestamp", Date.from(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant()))
                                )
                        );
                    } catch (Exception e) {
                        listOfErrors.get().add(DATABASE_ACCESS_ERROR_MESSAGE);
                        log.info(DATABASE_ACCESS_ERROR_MESSAGE);
                        e.printStackTrace();
                    }

                    String finalRoute = route;
                    String finalTableName = tableName;
                    try {
                        ResponseBytes<GetObjectResponse> response = s3Client.getObject(
                                GetObjectRequest.builder()
                                        .bucket(bucketName)
                                        .key(key)
                                        .build(),
                                ResponseTransformer.toBytes()
                        );

                        String fileContent = new String(response.asByteArray(), StandardCharsets.UTF_8);

                        try (Reader reader = new StringReader(fileContent)) {
                            if (finalTableName.equalsIgnoreCase(MARKET_LIST_CONNECTIONS_TABLE_NAME)) {
                                uploadMarketListConnectionsFile(reader, table, userName, role_obj, obj_id);
                            } else if (finalTableName.toLowerCase()
                                    .startsWith(MARKET_LIST_ADHOC_CONNECTIONS_TABLE_NAME)) {
                                uploadMarketListConnectionsFile(reader, finalTableName, userName, role_obj, obj_id);
                            } else {
                                uploadMarketListFile(reader, finalTableName, userName, role_obj, finalRoute,
                                        obj_id);
                            }
                        } catch (Exception e) {
                            listOfErrors.get().add("Error Uploading MarketList to DB. Please check File content.");
                            throw new RuntimeException("Error processing file content: " + e.getMessage(), e);
                        }
                    } catch (Exception ex) {
                        listOfErrors.get().add("Error during file processing for S3");
                        throw new RuntimeException("Error during file processing: " + ex.getMessage(), ex);
                    }
                }
                case STRATEGY_TABLE_NAME -> {
                    try {
                        MongoCollection<Document> collection = database.getCollection("inputs_file_upload");
                        collection.updateOne(
                                eq("obj_id", obj_id),
                                combine(
                                        set("status", status),
                                        set("inprogress_timestamp", Date.from(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant()))
                                )
                        );
                    } catch (Exception e) {
                        listOfErrors.get().add(DATABASE_ACCESS_ERROR_MESSAGE);
                        log.info(DATABASE_ACCESS_ERROR_MESSAGE);
                        e.printStackTrace();
                    }


                    try {
                        ResponseBytes<GetObjectResponse> response = s3Client.getObject(
                                GetObjectRequest.builder()
                                        .bucket(bucketName)
                                        .key(key)
                                        .build(),
                                ResponseTransformer.toBytes()
                        );

                        String fileContent = new String(response.asByteArray(), StandardCharsets.UTF_8);
                        long rowCount = new BufferedReader(new StringReader(fileContent)).lines().skip(1).count();
                        numOfRows.set((int) Math.min(rowCount, Integer.MAX_VALUE));

                        try (Reader reader = new StringReader(fileContent)) {
                            uploadStrategyTypeGridFile(reader, userName, obj_id, STRATEGY_TABLE_NAME, STRATEGY_HEADER_VALUES);
                        } catch (Exception e) {
                            listOfErrors.get().add("Error Uploading Strategy File to DB. Please check file content");
                            throw new RuntimeException("Error processing strategy file content: " + e.getMessage(), e);
                        }
                    } catch (Exception ex) {
                        throw new RuntimeException("Error during file processing: " + ex.getMessage(), ex);
                    }
                }
                case FORECAST_STRATEGY_TABLE_NAME -> {
                    try {
                        MongoCollection<Document> collection = database.getCollection("inputs_file_upload");
                        collection.updateOne(
                                eq("obj_id", obj_id),
                                combine(
                                        set("status", status),
                                        set("inprogress_timestamp", Date.from(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant()))
                                )
                        );
                    } catch (Exception e) {
                        listOfErrors.get().add(DATABASE_ACCESS_ERROR_MESSAGE);
                        log.info(DATABASE_ACCESS_ERROR_MESSAGE);
                        e.printStackTrace();
                    }

                    try {
                        ResponseBytes<GetObjectResponse> response = s3Client.getObject(
                                GetObjectRequest.builder()
                                        .bucket(bucketName)
                                        .key(key)
                                        .build(),
                                ResponseTransformer.toBytes()
                        );

                        String fileContent = new String(response.asByteArray(), StandardCharsets.UTF_8);
                        long rowCount = new BufferedReader(new StringReader(fileContent)).lines().skip(1).count();
                        numOfRows.set((int) Math.min(rowCount, Integer.MAX_VALUE));

                        try (Reader reader = new StringReader(fileContent)) {
                            uploadStrategyTypeGridFile(reader, userName, obj_id, FORECAST_STRATEGY_TABLE_NAME, FORECAST_STRATEGY_HEADER_VALUES);
                        } catch (Exception e) {
                            listOfErrors.get().add("Error Uploading Forecast Strategy File to DB. Please check file content");
                            throw new RuntimeException("Error processing forecast strategy file content: " + e.getMessage(), e);
                        }
                    } catch (Exception ex) {
                        throw new RuntimeException("Error during file processing: " + ex.getMessage(), ex);
                    }
                }
                case PROFILE_FARES_TABLE_NAME, GROUP_INCREMENTED_PROFILE_FARES_TABLE_NAME -> {
                    try {
                        MongoCollection<Document> collection = database.getCollection("inputs_file_upload");
                        collection.updateOne(
                                eq("obj_id", obj_id),
                                combine(
                                        set("status", status),
                                        set("inprogress_timestamp", Date.from(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant()))
                                )
                        );
                    } catch (Exception e) {
                        listOfErrors.get().add(DATABASE_ACCESS_ERROR_MESSAGE);
                        log.info(DATABASE_ACCESS_ERROR_MESSAGE);
                        e.printStackTrace();
                    }


                    try {
                        ResponseBytes<GetObjectResponse> response = s3Client.getObject(
                                GetObjectRequest.builder()
                                        .bucket(bucketName)
                                        .key(key)
                                        .build(),
                                ResponseTransformer.toBytes()
                        );

                        String fileContent = new String(response.asByteArray(), StandardCharsets.UTF_8);
                        long rowCount = new BufferedReader(new StringReader(fileContent)).lines().skip(1).count();
                        numOfRows.set((int) Math.min(rowCount, Integer.MAX_VALUE));

                        try (Reader reader = new StringReader(fileContent)) {
                            uploadProfileFaresFile(reader, userName, obj_id, tableName);
                        } catch (Exception e) {
                            listOfErrors.get().add("Error Uploading Profile Fares File to DB. Please check file content");
                            throw new RuntimeException("Error processing profile fares file content: " + e.getMessage(), e);
                        }
                    } catch (Exception ex) {
                        throw new RuntimeException("Error during file processing: " + ex.getMessage(), ex);
                    }
                }
                case AUTO_B2B_TABLE_NAME -> {
                    try {
                        MongoCollection<Document> collection = database.getCollection("inputs_file_upload");
                        collection.updateOne(
                                eq("obj_id", obj_id),
                                combine(
                                        set("status", status),
                                        set("inprogress_timestamp", Date.from(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant()))
                                )
                        );
                    } catch (Exception e) {
                        listOfErrors.get().add(DATABASE_ACCESS_ERROR_MESSAGE);
                        log.info(DATABASE_ACCESS_ERROR_MESSAGE);
                        e.printStackTrace();
                    }


                    try {
                        ResponseBytes<GetObjectResponse> response = s3Client.getObject(
                                GetObjectRequest.builder()
                                        .bucket(bucketName)
                                        .key(key)
                                        .build(),
                                ResponseTransformer.toBytes()
                        );

                        String fileContent = new String(response.asByteArray(), StandardCharsets.UTF_8);
                        long rowCount = new BufferedReader(new StringReader(fileContent)).lines().skip(1).count();
                        numOfRows.set((int) Math.min(rowCount, Integer.MAX_VALUE));

                        try (Reader reader = new StringReader(fileContent)) {
                            uploadAutoB2BGridFile(reader, userName, obj_id);
                        } catch (Exception e) {
                            throw new RuntimeException("Error processing autoB2BFactor file content: " + e.getMessage(), e);
                        }
                    } catch (Exception ex) {
                        throw new RuntimeException("Error during file processing: " + ex.getMessage(), ex);
                    }
                }
                case DATE_EVENT_TABLE_NAME -> {
                    try {
                        MongoCollection<Document> collection = database.getCollection("inputs_file_upload");
                        collection.updateOne(
                                eq("obj_id", obj_id),
                                combine(
                                        set("status", status),
                                        set("inprogress_timestamp", Date.from(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant()))
                                )
                        );
                    } catch (Exception e) {
                        listOfErrors.get().add(DATABASE_ACCESS_ERROR_MESSAGE);
                        log.info(DATABASE_ACCESS_ERROR_MESSAGE);
                        e.printStackTrace();
                    }


                    try {
                        ResponseBytes<GetObjectResponse> response = s3Client.getObject(
                                GetObjectRequest.builder()
                                        .bucket(bucketName)
                                        .key(key)
                                        .build(),
                                ResponseTransformer.toBytes()
                        );

                        String fileContent = new String(response.asByteArray(), StandardCharsets.UTF_8);
                        long rowCount = new BufferedReader(new StringReader(fileContent)).lines().skip(1).count();
                        numOfRows.set((int) Math.min(rowCount, Integer.MAX_VALUE));

                        try (Reader reader = new StringReader(fileContent)) {
                            uploadPartialDateEvent(reader, userName, obj_id);
                        } catch (Exception e) {
                            listOfErrors.get().add("Error Uploading Calendar File to DB. Please check file content");
                            throw new RuntimeException("Error processing date event file content: " + e.getMessage(), e);
                        }
                    } catch (Exception ex) {
                        throw new RuntimeException("Error during file processing: " + ex.getMessage(), ex);
                    }
                }
                case DISTRESS_CLAENDAR -> {
                    try {
                        MongoCollection<Document> collection = database.getCollection("inputs_file_upload");
                        collection.updateOne(
                                eq("obj_id", obj_id),
                                combine(
                                        set("status", status),
                                        set("inprogress_timestamp", Date.from(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant()))
                                )
                        );
                    } catch (Exception e) {
                        listOfErrors.get().add(DATABASE_ACCESS_ERROR_MESSAGE);
                        log.info(DATABASE_ACCESS_ERROR_MESSAGE);
                        e.printStackTrace();
                    }


                    try {
                        ResponseBytes<GetObjectResponse> response = s3Client.getObject(
                                GetObjectRequest.builder()
                                        .bucket(bucketName)
                                        .key(key)
                                        .build(),
                                ResponseTransformer.toBytes()
                        );

                        String fileContent = new String(response.asByteArray(), StandardCharsets.UTF_8);
                        long rowCount = new BufferedReader(new StringReader(fileContent)).lines().skip(1).count();
                        numOfRows.set((int) Math.min(rowCount, Integer.MAX_VALUE));

                        try (Reader reader = new StringReader(fileContent)) {
                            uploadDateEventFile(reader, userName, obj_id, UploadFileType.DISTRESS);
                        } catch (Exception e) {
                            listOfErrors.get().add("Error Distress Calendar File to DB. Please check file content");
                            throw new RuntimeException("Error processing date event file content: " + e.getMessage(), e);
                        }
                    } catch (Exception ex) {
                        throw new RuntimeException("Error during file processing: " + ex.getMessage(), ex);
                    }
                }
                case SALES_INVENTORY_TABLE_NAME-> {
                    try {
                        MongoCollection<Document> collection = database.getCollection("inputs_file_upload");
                        collection.updateOne(
                                eq("obj_id", obj_id),
                                combine(
                                        set("status", status),
                                        set("inprogress_timestamp", Date.from(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant()))
                                )
                        );
                    } catch (Exception e) {
                        listOfErrors.get().add(DATABASE_ACCESS_ERROR_MESSAGE);
                        log.info(DATABASE_ACCESS_ERROR_MESSAGE);
                        e.printStackTrace();
                    }


                    String finalTableName1 = tableName;
                    try {
                        ResponseBytes<GetObjectResponse> response = s3Client.getObject(
                                GetObjectRequest.builder()
                                        .bucket(bucketName)
                                        .key(key)
                                        .build(),
                                ResponseTransformer.toBytes()
                        );

                        String fileContent = new String(response.asByteArray(), StandardCharsets.UTF_8);

                        try (Reader reader = new StringReader(fileContent)) {
                            uploadSalesInventoryFile(reader, finalTableName1, userName, obj_id);
                        } catch (Exception e) {
                            error_occurred.set(1);
                            if (listOfErrors.get().isEmpty()) {
                                listOfErrors.get().add("Error processing sales inventory file: ");
                                addErrorsToTable(listOfErrors.get(), obj_id);
                            }
                            throw new RuntimeException("Error processing sales inventory file content: " + e.getMessage(), e);
                        }
                    } catch (Exception ex) {
                        error_occurred.set(1);
                        log.error("Error during S3 getObject or processing: " + ex.getMessage());
                        if (listOfErrors.get().isEmpty()) {
                            listOfErrors.get().add("Failed to retrieve or process file from S3: " + ex.getMessage());
                            addErrorsToTable(listOfErrors.get(), obj_id);
                        }
                        throw new RuntimeException(ex);
                    }
                    break;
                }
                case SALESINV_RBD_DISTRIBUTION_TABLE_NAME -> {
                    try {
                        MongoCollection<Document> collection = database.getCollection("inputs_file_upload");
                        collection.updateOne(
                                eq("obj_id", obj_id),
                                combine(
                                        set("status", status),
                                        set("inprogress_timestamp", Date.from(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant()))
                                )
                        );
                    } catch (Exception e) {
                        listOfErrors.get().add(DATABASE_ACCESS_ERROR_MESSAGE);
                        log.info(DATABASE_ACCESS_ERROR_MESSAGE);
                        e.printStackTrace();
                    }

                    String finalTableNameRbd = tableName;
                    try {
                        ResponseBytes<GetObjectResponse> response = s3Client.getObject(
                                GetObjectRequest.builder()
                                        .bucket(bucketName)
                                        .key(key)
                                        .build(),
                                ResponseTransformer.toBytes()
                        );

                        String fileContent = new String(response.asByteArray(), StandardCharsets.UTF_8);

                        try (Reader reader = new StringReader(fileContent)) {
                            uploadSalesInvRbdDistributionFile(reader, finalTableNameRbd, userName, obj_id);
                        } catch (Exception e) {
                            error_occurred.set(1);
                            if (listOfErrors.get().isEmpty()) {
                                listOfErrors.get().add("Error processing Sales RBD Distribution file");
                                addErrorsToTable(listOfErrors.get(), obj_id);
                            }
                            throw new RuntimeException("Error processing Sales RBD Distribution file content: " + e.getMessage(), e);
                        }
                    } catch (Exception ex) {
                        error_occurred.set(1);
                        log.error("Error during S3 getObject or processing for Sales RBD Distribution: " + ex.getMessage());
                        if (listOfErrors.get().isEmpty()) {
                            listOfErrors.get().add("Failed to retrieve or process Sales RBD Distribution file from S3");
                            addErrorsToTable(listOfErrors.get(), obj_id);
                        }
                        throw new RuntimeException(ex);
                    }
                    break;
                }
                case GROUP_POTENTIAL_SEATS_TABLE_NAME -> {
                    try {
                        MongoCollection<Document> collection = database.getCollection("inputs_file_upload");
                        collection.updateOne(
                                eq("obj_id", obj_id),
                                combine(
                                        set("status", status),
                                        set("inprogress_timestamp", Date.from(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant()))
                                )
                        );
                    } catch (Exception e) {
                        listOfErrors.get().add(DATABASE_ACCESS_ERROR_MESSAGE);
                        log.info(DATABASE_ACCESS_ERROR_MESSAGE);
                        e.printStackTrace();
                    }


                    String finalTableName2 = tableName;
                    try {
                        ResponseBytes<GetObjectResponse> response = s3Client.getObject(
                                GetObjectRequest.builder()
                                        .bucket(bucketName)
                                        .key(key)
                                        .build(),
                                ResponseTransformer.toBytes()
                        );

                        String fileContent = new String(response.asByteArray(), StandardCharsets.UTF_8);

                        try (Reader reader = new StringReader(fileContent)) {
                            uploadGroupPotentialSeatsFile(reader, finalTableName2, userName, obj_id);
                        } catch (Exception e) {
                            error_occurred.set(1);
                            if (listOfErrors.get().isEmpty()) {
                                listOfErrors.get().add("Error processing Group Potential Seats file");
                                addErrorsToTable(listOfErrors.get(), obj_id);
                            }
                            throw new RuntimeException("Error processing Group Potential Seats file content: " + e.getMessage(), e);
                        }
                    } catch (Exception ex) {
                        error_occurred.set(1);
                        log.error("Error during S3 getObject or processing for Group Potential Seats: " + ex.getMessage());
                        if (listOfErrors.get().isEmpty()) {
                            listOfErrors.get().add("Failed to retrieve or process Group Potential Seats file from S3");
                            addErrorsToTable(listOfErrors.get(), obj_id);
                        }
                        throw new RuntimeException(ex);
                    }
                    break;
                }
                case MARKET_LIST_SALESINV_TABLE_NAME,MARKET_LIST_SALESINV_ADHOC_TABLE_NAME -> {
                    try {
                        MongoCollection<Document> collection = database.getCollection("inputs_file_upload");
                        collection.updateOne(
                                eq("obj_id", obj_id),
                                combine(
                                        set("status", status),
                                        set("inprogress_timestamp", Date.from(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant()))
                                )
                        );
                    } catch (Exception e) {
                        listOfErrors.get().add(DATABASE_ACCESS_ERROR_MESSAGE);
                        log.info(DATABASE_ACCESS_ERROR_MESSAGE);
                        e.printStackTrace();
                    }


                    if (tableName.startsWith(MARKET_LIST_SALESINV_ADHOC_TABLE_NAME)) {
                        tableName = MARKET_LIST_SALESINV_ADHOC_TABLE_NAME + UNDERSCORE + adhocUsername;
                    }
                    String finalTableName = tableName;
                    try {
                        ResponseBytes<GetObjectResponse> response = s3Client.getObject(
                                GetObjectRequest.builder()
                                        .bucket(bucketName)
                                        .key(key)
                                        .build(),
                                ResponseTransformer.toBytes()
                        );

                        String fileContent = new String(response.asByteArray(), StandardCharsets.UTF_8);

                        try (Reader reader = new StringReader(fileContent)) {
                            uploadMarketListSalesInvFile(reader, finalTableName, userName, role_obj, obj_id);
                        } catch (Exception e) {
                            listOfErrors.get().add("Error processing market_list_salesinv file content");
                            throw new RuntimeException("Error processing market_list_salesinv file content: " + e.getMessage(), e);
                        }
                    } catch (Exception ex) {
                        listOfErrors.get().add("Error processing market_list_salesinv file content");
                        log.info("Error during file processing: " + ex.getMessage());
                        throw new RuntimeException("Error during file processing: " + ex.getMessage(), ex);
                    }
                }
                case MARKET_FARE_ADJUSTMENTS_TABLE_NAME -> {
                    try {
                        MongoCollection<Document> collection = database.getCollection("inputs_file_upload");
                        collection.updateOne(
                                eq("obj_id", obj_id),
                                combine(
                                        set("status", status),
                                        set("inprogress_timestamp", Date.from(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant()))
                                )
                        );
                    } catch (Exception e) {
                        listOfErrors.get().add(DATABASE_ACCESS_ERROR_MESSAGE);
                        log.info(DATABASE_ACCESS_ERROR_MESSAGE);
                        e.printStackTrace();
                    }

                    String finalTableName = tableName;
                    try {
                        ResponseBytes<GetObjectResponse> response = s3Client.getObject(
                                GetObjectRequest.builder()
                                        .bucket(bucketName)
                                        .key(key)
                                        .build(),
                                ResponseTransformer.toBytes()
                        );

                        String fileContent = new String(response.asByteArray(), StandardCharsets.UTF_8);

                        try (Reader reader = new StringReader(fileContent)) {
                            uploadMarketFareAdjustments(reader, finalTableName, userName, role_obj, obj_id);
                        } catch (Exception e) {
                            listOfErrors.get().add("Error processing market_fare_adjustments file content");
                            throw new RuntimeException("Error processing market_fare_adjustments file content: " + e.getMessage(), e);
                        }
                    } catch (Exception ex) {
                        listOfErrors.get().add("Error processing market_fare_adjustments file content");
                        log.info("Error during file processing: " + ex.getMessage());
                        throw new RuntimeException("Error during file processing: " + ex.getMessage(), ex);
                    }
                }
                case FORECAST_DATE_CONFIG_TABLE_NAME -> {
                    try {
                        MongoCollection<Document> collection = database.getCollection("inputs_file_upload");
                        collection.updateOne(
                                eq("obj_id", obj_id),
                                combine(
                                        set("status", status),
                                        set("inprogress_timestamp", Date.from(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant()))
                                )
                        );
                    } catch (Exception e) {
                        listOfErrors.get().add(DATABASE_ACCESS_ERROR_MESSAGE);
                        log.info(DATABASE_ACCESS_ERROR_MESSAGE);
                        e.printStackTrace();
                    }

                    String finalTableName = tableName;
                    try {
                        ResponseBytes<GetObjectResponse> response = s3Client.getObject(
                                GetObjectRequest.builder()
                                        .bucket(bucketName)
                                        .key(key)
                                        .build(),
                                ResponseTransformer.toBytes()
                        );

                        String fileContent = new String(response.asByteArray(), StandardCharsets.UTF_8);

                        try (Reader reader = new StringReader(fileContent)) {
                            uploadForecastDateConfig(reader, finalTableName, userName, role_obj, obj_id);
                        } catch (Exception e) {
                            listOfErrors.get().add("Error processing forecast_date_config file content");
                            throw new RuntimeException("Error processing forecast_date_config file content: " + e.getMessage(), e);
                        }
                    } catch (Exception ex) {
                        listOfErrors.get().add("Error processing forecast_date_config file content");
                        log.info("Error during file processing: " + ex.getMessage());
                        throw new RuntimeException("Error during file processing: " + ex.getMessage(), ex);
                    }
                }
                case ACTIVE_SECTORS_TABLE_NAME -> {
                    try {
                        MongoCollection<Document> collection = database.getCollection("inputs_file_upload");
                        collection.updateOne(
                                eq("obj_id", obj_id),
                                combine(
                                        set("status", status),
                                        set("inprogress_timestamp", Date.from(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant()))
                                )
                        );
                    } catch (Exception e) {
                        listOfErrors.get().add(DATABASE_ACCESS_ERROR_MESSAGE);
                        log.info(DATABASE_ACCESS_ERROR_MESSAGE);
                        e.printStackTrace();
                    }

                    String finalTableName = tableName;
                    try {
                        ResponseBytes<GetObjectResponse> response = s3Client.getObject(
                                GetObjectRequest.builder()
                                        .bucket(bucketName)
                                        .key(key)
                                        .build(),
                                ResponseTransformer.toBytes()
                        );

                        String fileContent = new String(response.asByteArray(), StandardCharsets.UTF_8);

                        try (Reader reader = new StringReader(fileContent)) {
                            uploadActiveSectors(reader, finalTableName, userName, role_obj, obj_id);
                        } catch (Exception e) {
                            listOfErrors.get().add("Error processing active_sectors file content");
                            throw new RuntimeException("Error processing active_sectors file content: " + e.getMessage(), e);
                        }
                    } catch (Exception ex) {
                        listOfErrors.get().add("Error processing active_sectors file content");
                        log.info("Error during file processing: " + ex.getMessage());
                        throw new RuntimeException("Error during file processing: " + ex.getMessage(), ex);
                    }
                }
                case QP_FARES_TABLE_NAME -> {
                    try {
                        MongoCollection<Document> collection = database.getCollection("inputs_file_upload");
                        collection.updateOne(
                                eq("obj_id", obj_id),
                                combine(
                                        set("status", status),
                                        set("inprogress_timestamp", Date.from(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant()))
                                )
                        );
                    } catch (Exception e) {
                        listOfErrors.get().add(DATABASE_ACCESS_ERROR_MESSAGE);
                        log.info(DATABASE_ACCESS_ERROR_MESSAGE);
                        e.printStackTrace();
                    }

                    String finalTableName = tableName;
                    try {
                        ResponseBytes<GetObjectResponse> response = s3Client.getObject(
                                GetObjectRequest.builder()
                                        .bucket(bucketName)
                                        .key(key)
                                        .build(),
                                ResponseTransformer.toBytes()
                        );

                        String fileContent = new String(response.asByteArray(), StandardCharsets.UTF_8);

                        try (Reader reader = new StringReader(fileContent)) {
                            uploadFares(reader, finalTableName, userName, role_obj, obj_id);
                        } catch (Exception e) {
                            listOfErrors.get().add("Error processing Fares file content");
                            throw new RuntimeException("Error processing Fares file content: " + e.getMessage(), e);
                        }
                    } catch (Exception ex) {
                        listOfErrors.get().add("Error processing Fares file content");
                        log.info("Error during file processing: " + ex.getMessage());
                        throw new RuntimeException("Error during file processing: " + ex.getMessage(), ex);
                    }
                }
                case SALEINV_RBL_TABLE_NAME -> {
                    try {
                        MongoCollection<Document> collection = database.getCollection("inputs_file_upload");
                        collection.updateOne(
                                eq("obj_id", obj_id),
                                combine(
                                        set("status", status),
                                        set("inprogress_timestamp", Date.from(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant()))
                                )
                        );
                    } catch (Exception e) {
                        listOfErrors.get().add(DATABASE_ACCESS_ERROR_MESSAGE);
                        log.info(DATABASE_ACCESS_ERROR_MESSAGE);
                        e.printStackTrace();
                    }

                    try {
                        ResponseBytes<GetObjectResponse> response = s3Client.getObject(
                                GetObjectRequest.builder()
                                        .bucket(bucketName)
                                        .key(key)
                                        .build(),
                                ResponseTransformer.toBytes()
                        );

                        String fileContent = new String(response.asByteArray(), StandardCharsets.UTF_8);

                        try (Reader reader = new StringReader(fileContent)) {
                            uploadSaleInvRblFile(reader, tableName, userName, obj_id);
                        } catch (Exception e) {
                            error_occurred.set(1);
                            if (listOfErrors.get().isEmpty()) {
                                listOfErrors.get().add("Error processing SaleInv RBL file");
                                addErrorsToTable(listOfErrors.get(), obj_id);
                            }
                            throw new RuntimeException("Error processing SaleInv RBL file content: " + e.getMessage(), e);
                        }
                    } catch (Exception ex) {
                        error_occurred.set(1);
                        log.error("Error during S3 getObject or processing for SaleInv RBL: " + ex.getMessage());
                        if (listOfErrors.get().isEmpty()) {
                            listOfErrors.get().add("Failed to retrieve or process SaleInv RBL file from S3");
                            addErrorsToTable(listOfErrors.get(), obj_id);
                        }
                        throw new RuntimeException(ex);
                    }
                    break;
                }
                default -> {
                    try {
                        MongoCollection<Document> collection = database.getCollection("inputs_file_upload");
                        collection.updateOne(
                                eq("obj_id", obj_id),
                                combine(
                                        set("status", status),
                                        set("inprogress_timestamp", Date.from(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant()))
                                )
                        );
                    } catch (Exception e) {
                        listOfErrors.get().add(DATABASE_ACCESS_ERROR_MESSAGE);
                        log.info(DATABASE_ACCESS_ERROR_MESSAGE);
                        e.printStackTrace();
                    }


                    try {
                        ResponseBytes<GetObjectResponse> response = s3Client.getObject(
                                GetObjectRequest.builder()
                                        .bucket(bucketName)
                                        .key(key)
                                        .build(),
                                ResponseTransformer.toBytes()
                        );

                        String fileContent = new String(response.asByteArray(), StandardCharsets.UTF_8);
                        long rowCount = new BufferedReader(new StringReader(fileContent)).lines().skip(1).count();
                        numOfRows.set((int) Math.min(rowCount, Integer.MAX_VALUE));

                        try (Reader reader = new StringReader(fileContent)) {
                            uploadFile(reader, table, userName, obj_id);
                        } catch (Exception e) {
                            throw new RuntimeException("Error processing file content: " + e.getMessage(), e);
                        }
                    } catch (Exception ex) {
                        log.info("Error during file processing: " + ex.getMessage());
                        throw new RuntimeException("Error during file processing: " + ex.getMessage(), ex);
                    }
                }
            }
        } catch (Exception e) {
            listOfErrors.get().add("Failure During Vaildation");
            log.info("Error in insertAndValidatedBasedOnTableName: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    public void uploadGroupPotentialSeatsFile(Reader file, String tableName, String userName, String obj_id) throws Exception {
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
                error_occurred.set(1);
                listOfErrors.get().addAll(currentErrors);
                addErrorsToTable(listOfErrors.get(), obj_id);
                throw new DataMismatchedException(listOfErrors.get());
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
            log.info("Successfully processed " + tableName + " file.");

        } catch (Exception e) {
            error_occurred.set(1);
            log.error("Critical error during Group Potential Seats processing for " + tableName + ": " + e.getMessage());
            if (listOfErrors.get().isEmpty()) {
                if (!currentErrors.isEmpty()) listOfErrors.get().addAll(currentErrors);
                else listOfErrors.get().add("Failed to process Group Potential Seats file due to an unexpected error");
            }
            addErrorsToTable(listOfErrors.get(), obj_id);
            throw e;
        } finally {
            updateInputStatus(tableName);
        }
    }

    public void uploadSalesInvRbdDistributionFile(Reader file, String tableName, String userName, String obj_id) throws Exception {
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
                error_occurred.set(1);
                listOfErrors.get().addAll(currentErrors);
                addErrorsToTable(listOfErrors.get(), obj_id);
                updateInputStatus(tableName);
                log.error(FILE_UPLOAD_ERROR_MESSAGE + " for " + tableName + " due to validation errors.");
                throw new DataMismatchedException(listOfErrors.get());
            }

            if (!recordsToInsert.isEmpty()) {
                deleteSalesInvRbdDistributionBySeatType(uniqueSeatTypeCombinationsInCsv, tableName);

                List<String> dbHeaders = List.of(
                        SI_RBD_SEATS_COLUMN_NAME,
                        SI_RBD_TYPE_COLUMN_NAME,
                        SI_RBD_RBD_COLUMN_NAME,
                        SI_RBD_VALUE_COLUMN_NAME
                );
                log.info("Inserting " + recordsToInsert.size() + " records into " + tableName);
                insertStatement(recordsToInsert, dbHeaders, tableName);
                numOfRows.set(recordsToInsert.size());
            } else {
                if(!uniqueSeatTypeCombinationsInCsv.isEmpty()){
                    deleteSalesInvRbdDistributionBySeatType(uniqueSeatTypeCombinationsInCsv, tableName);
                    log.info("Cleared data for (seats,type) combinations from " + tableName + " as per CSV, with no new records to insert.");
                } else {
                    log.info("No valid records to insert into " + tableName + " and no (seats,type) combinations to clear.");
                }
                numOfRows.set(0);
            }

            s3FileStorageDao.storeFile(createCSV(tableName), tableName, uuid);
            updateAuditStatement(tableName, getCurrentDateTime(), uuid);
            log.info("Successfully processed " + tableName + " file.");

        } catch (DataMismatchedException dme) {
            log.error("Data mismatched, processing halted for " + tableName + ": " + dme.getMessage());
            throw dme;
        } catch (Exception e) {
            error_occurred.set(1);
            log.error("Critical error during Sales RBD Distribution processing for " + tableName + ": " + e.getMessage());
            e.printStackTrace();
            if (listOfErrors.get().isEmpty()) {
                if(!currentErrors.isEmpty()) listOfErrors.get().addAll(currentErrors);
                else listOfErrors.get().add("Failed to process Sales RBD Distribution file due to an unexpected error");
            } else if (!currentErrors.isEmpty() && !listOfErrors.get().containsAll(currentErrors)){
                listOfErrors.get().addAll(currentErrors);
            }
            addErrorsToTable(listOfErrors.get(), obj_id);
            throw e;
        } finally {
            updateInputStatus(tableName);
            log.info("Finished processing attempt for " + tableName + " with obj_id " + obj_id + ". Status: " + (error_occurred.get() == 1 ? "FAILED" : "DONE"));
        }
    }

    private void deleteSalesInvRbdDistributionBySeatType(Set<String> seatTypeCombinations, String collectionName) {
        if (seatTypeCombinations == null || seatTypeCombinations.isEmpty()) {
            return;
        }

        log.info("Preparing to delete from " + collectionName +
                " for " + seatTypeCombinations.size() + " seats, type combinations.");

        int totalDeletedRows = 0;

        try {
            MongoCollection<Document> collection = database.getCollection(collectionName.toLowerCase());

            for (String combo : seatTypeCombinations) {
                String[] parts = combo.split(",");
                if (parts.length == 2) {
                    try {
                        int seats = Integer.parseInt(parts[0]);
                        String type = parts[1];

                        Bson filter = and(
                                eq(SI_RBD_SEATS_COLUMN_NAME, seats),
                                eq(SI_RBD_TYPE_COLUMN_NAME, type)
                        );

                        DeleteResult result = collection.deleteMany(filter);
                        totalDeletedRows += result.getDeletedCount();

                    } catch (NumberFormatException e) {
                        listOfErrors.get().add("Skipping deletion for invalid seat format in combo: " + combo);
                        log.error("Skipping deletion for invalid seat format in combo: " + combo);
                    }
                } else {
                    log.error("Skipping deletion for malformed (seats,type) combo: " + combo);
                }
            }


        } catch (Exception e) {
            error_occurred.set(1);
            String errorMsg = "Failed to delete existing Sales RBD Distribution data: " + e.getMessage();
            log.error(errorMsg);
            listOfErrors.get().add(errorMsg);
            throw new RuntimeException(errorMsg, e);
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
                                     String obj_id) {
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
            List<Object> forecastStrategies = searchDistinctResults(STRATEGY_COLUMN_NAME, FORECAST_STRATEGY_TABLE_NAME);
            Set<Object> uniqueStrategies = new HashSet<>(listOfStrategies);
            uniqueStrategies.addAll(forecastStrategies);
            listOfStrategies = new ArrayList<>(uniqueStrategies);
            List<Object> listOfB2BStrategies = searchDistinctResults(AUTO_B2B_NAME_COLUMN_NAME, AUTO_B2B_TABLE_NAME);
            List<Object> listOfCriteria = searchDistinctResults(CRITERIA_COLUMN_NAME, CRITERIA_TABLE);

            List<List<String>> csvData = getDataFromCSVFile(file);
            csvData = splitRowsByDateRange(csvData);
            numOfRows.set(csvData.size() > 1 ? csvData.size() - 1 : 0);

            for (int i = 1; i < csvData.size(); i++) {
                List<String> record = csvData.get(i);
                // Skip records with defaultMlFlag turned on
                if (record.size() > CSV_HEADER_DEFAULT_ML_FLAG && "Y".equalsIgnoreCase(record.get(CSV_HEADER_DEFAULT_ML_FLAG))) {
                    continue;
                }
                String uuidForEntries = UUID.randomUUID().toString();
                int errorCountBefore = listOfErrors.get().size();
                String analystName = validationForMarketListFile(record, permissions, route, i,
                        role, userName, listOfCurves, listOfStrategies, listOfB2BStrategies, listOfCriteria);
                if (listOfErrors.get().size() == errorCountBefore) {
                    createMarketListEntity(record, validMarketListEntityList, uuidForEntries, analystName);
                }
            }

            if (!listOfErrors.get().isEmpty()) {
                error_occurred.set(1);
                addErrorsToTable(listOfErrors.get(), obj_id);
            }

            if (!validMarketListEntityList.isEmpty()) {
                LocalDateTime currentDateTime = ZonedDateTime.now(ZoneId.of(INDIAN_REGION)).toLocalDateTime();

                if (!tableName.startsWith(MARKET_LIST_ADHOC_TABLE_NAME)) {
                    try {
                        auditHelper.buildAuditLogs(validMarketListEntityList, userName, currentDateTime, tableName);
                    } catch (Exception e) {
                        log.error("Failed to build audit logs for table: {}. Error: {}", tableName, e.getMessage());
                    }
                }

                if (tableName.startsWith(MARKET_LIST_ADHOC_TABLE_NAME)) {
                    deleteValues(Collections.singleton(ZERO), tableName, ZERO);
                } else {
                    deleteRecords(validMarketListEntityList, tableName);
                }

                bulkInsertIntoMarketList(validMarketListEntityList, tableName);

                s3FileStorageDao.storeFile(createCSV(tableName), tableName, uuid);
                updateAuditStatement(tableName, getCurrentDateTime(), uuid);

                if (error_occurred.get() == 1) {
                    partial_upload_occurred.set(1);
                }
            } else {
                if (error_occurred.get() == 1) {
                    log.info(FILE_UPLOAD_ERROR_MESSAGE + ": No valid records to upload.");
                }
            }
        } catch (Exception e) {
            error_occurred.set(1);
            listOfErrors.get().add(ERROR_PROCESSING_FILE);
            addErrorsToTable(listOfErrors.get(), obj_id);
            log.error("An exception occurred in uploadMarketListFile: " + e.getMessage());
            e.printStackTrace();
        } finally {
            updateInputStatus(tableName);
        }
    }


    public static List<List<String>> splitRowsByDateRange(List<List<String>> csvData) {
        DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("MM/dd/yyyy");
        List<List<String>> intermediateResult = new ArrayList<>();
        List<List<String>> result = new ArrayList<>();
        Set<String> uniqueKeys = new HashSet<>();

        if (!csvData.isEmpty()) {
            intermediateResult.add(new ArrayList<>(csvData.get(0)));
        }

        for (int rowIndex = 1; rowIndex < csvData.size(); rowIndex++) {
            List<String> row = csvData.get(rowIndex);
            if (row.size() < 6) {
                listOfErrors.get().add("Line " + (rowIndex + 1) + ": Insufficient columns (expected at least 6, found " + row.size() + ")");
                continue;
            }
            LocalDate startDate = LocalDate.parse(row.get(3), DATE_FORMATTER);
            LocalDate endDate = LocalDate.parse(row.get(4), DATE_FORMATTER);
            String dowPattern = row.get(5);

            if (startDate.equals(endDate)) {
                int dayOfWeek = startDate.getDayOfWeek().getValue() % 7;
                if (dowPattern.charAt(dayOfWeek) == '1') {
                    addRowWithDuplicateCheck(intermediateResult, uniqueKeys, row, rowIndex);
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
                        addRowWithDuplicateCheck(intermediateResult, uniqueKeys, newRow, rowIndex);
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
                                                 int rowIndex) {
        if (row.size() < 4) {
            listOfErrors.get().add("Line " + (rowIndex + 1) + ": Insufficient columns for duplicate check (expected at least 4, found " + row.size() + ")");
            return;
        }
        String key = row.get(0) + "|" + row.get(1) + "|" + row.get(2) + "|" + row.get(3);
        if (!uniqueKeys.add(key)) {
            listOfErrors.get().add("Duplicate record found at Line" + (rowIndex + 1) + ": " + key);
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

        try {
            MongoCollection<Document> collection = database.getCollection("inputs_file_upload");
            collection.updateOne(
                    eq("obj_id", obj_id),
                    set("errors", errorsString)
            );
        } catch (Exception e) {
            log.info(DATABASE_ACCESS_ERROR_MESSAGE);
            e.printStackTrace();
        }
    }


    public void uploadStrategyTypeGridFile(Reader file, String userName, String obj_id, String tableName, Map<String, String> headerValues) throws Exception {
        String uuid = obj_id;
        try {
            insertAuditStatement(tableName, getCurrentDateTime(), userName, uuid);
            insertInputStatusTable(tableName);
            List<List<String>> csvData = getDataFromCSVFile(file);
            Map<String, Map<String, String>> strategyMap = new HashMap<>();
            try {
                strategyMap = getStrategyGrid(csvData, headerValues, tableName);
            } catch (Exception e) {
                addErrorsToTable(listOfErrors.get(), obj_id);
                throw e;
            }
            Set<String> strategy = getUniqueStrategies(strategyMap);

            Set<String> uniqueStrategyNames = new HashSet<>();
            for (Map<String, String> strategies : strategyMap.values()) {
                uniqueStrategyNames.add(strategies.get(STRATEGY_COLUMN_NAME));
            }

            String otherTableName = FORECAST_STRATEGY_TABLE_NAME.equals(tableName) ? STRATEGY_TABLE_NAME : FORECAST_STRATEGY_TABLE_NAME;
            for (String stratName : uniqueStrategyNames) {
                if (stratName == null || stratName.trim().isEmpty()) continue;
                try {
                    MongoCollection<Document> collection = database.getCollection(otherTableName.toLowerCase());
                    Document doc = collection.find(eq(STRATEGY_COLUMN_NAME, stratName)).first();
                    if (doc != null) {
                        listOfErrors.get().add("Validation Error: Strategy '" + stratName + "' already exists in " + otherTableName + ". Strategy names must be unique across both tables.");
                        addErrorsToTable(listOfErrors.get(), obj_id);
                        throw new RuntimeException("Strategy validation failed: " + stratName + " already exists in " + otherTableName);
                    }
                } catch (Exception e) {
                    if (e instanceof RuntimeException && e.getMessage().contains("Strategy validation failed")) {
                        throw e;
                    }
                    log.error("Error checking strategy exclusively", e);
                }
            }
            deleteValuesStrategyTable(strategy, tableName, STRATEGY_COLUMN_NAME, CHANNEL_COLUMN_NAME);
            List<String> headers = new ArrayList<>(Arrays.asList(getColumnHeaders(STRATEGY_INPUT_NAME).split(COMMA)));
            if (FORECAST_STRATEGY_TABLE_NAME.equals(tableName)) {
                int dplfIndex = headers.indexOf(DPLF_BAND_COLUMN_NAME);
                if (dplfIndex != -1) {
                    headers.set(dplfIndex, FORECAST_BAND_COLUMN_NAME);
                }
            }
            insertStatement(strategyMap.values().stream().toList(), headers, tableName);
            s3FileStorageDao.storeFile(createCSV(tableName), tableName, uuid);
            updateAuditStatement(tableName, getCurrentDateTime(), uuid);
        } catch (Exception e) {
            throw e;
        } finally {
            updateInputStatus(tableName);
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
                autoB2BMap = getAutoB2BGrid(csvData, STRATEGY_HEADER_VALUES, AUTO_B2B_TABLE_NAME);
            } catch (Exception e) {
                addErrorsToTable(listOfErrors.get(), obj_id);
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

    public void uploadProfileFaresFile(Reader file, String userName, String obj_id, String tableName) throws Exception {
        String uuid = obj_id;
        try {
            insertAuditStatement(tableName, getCurrentDateTime(), userName, uuid);
            insertInputStatusTable(tableName);
            
            List<List<String>> csvData = getDataFromCSVFile(file);
            
            if (csvData.isEmpty() || csvData.size() < 2) {
                listOfErrors.get().add("Profile Fares CSV file is empty or lacks data rows.");
                error_occurred.set(1);
                addErrorsToTable(listOfErrors.get(), obj_id);
                throw new DataMismatchedException(listOfErrors.get());
            }
            
            // Parse CSV into list of maps
            List<String> headers = csvData.get(0);
            
            // Remove BOM from first header if present
            if (!headers.isEmpty() && headers.get(0) != null) {
                headers.set(0, headers.get(0).replace("\uFEFF", "").trim());
            }
            
            log.info("Profile Fares CSV headers: {}", headers);
            log.info("Profile Fares total rows (excluding header): {}", csvData.size() - 1);
            
            List<Map<String, String>> data = new ArrayList<>();
            
            for (int i = 1; i < csvData.size(); i++) {
                List<String> row = csvData.get(i);
                Map<String, String> rowMap = new HashMap<>();
                for (int j = 0; j < headers.size() && j < row.size(); j++) {
                    String headerKey = headers.get(j).replace("\uFEFF", "").trim().toLowerCase();
                    String value = (row.get(j) != null) ? row.get(j).trim() : "";
                    rowMap.put(headerKey, value);
                }
                data.add(rowMap);
                
                // Log first row for debugging
                if (i == 1) {
                    log.info("First row data - sector: '{}', month: '{}', dow: '{}', cluster: '{}'", 
                        rowMap.get("sector"), rowMap.get("month"), rowMap.get("dow"), rowMap.get("cluster"));
                    log.info("First row all keys: {}", rowMap.keySet());
                }
            }
            
            // Validate using ProfileValidator
            Validator validator = new ProfileValidator();
            if (!validator.isValid(data, null, listOfErrors.get())) {
                error_occurred.set(1);
                listOfErrors.get().add("Profile Fares validation failed");
                addErrorsToTable(listOfErrors.get(), obj_id);
                throw new DataMismatchedException(listOfErrors.get());
            }
            
            // Upsert to MongoDB
            bulkUpsertProfileFares(data, tableName);
            
            s3FileStorageDao.storeFile(createCSV(tableName), tableName, uuid);
            updateAuditStatement(tableName, getCurrentDateTime(), uuid);
        } catch (Exception e) {
            throw e;
        } finally {
            updateInputStatus(tableName);
        }
    }

    private void bulkUpsertProfileFares(List<Map<String, String>> data, String tableName) {
        try {
            MongoCollection<Document> collection = database.getCollection(tableName);
            
            // Create unique compound index on the key fields if it doesn't exist
            try {
                collection.createIndex(
                    Indexes.compoundIndex(
                        Indexes.ascending("sector"),
                        Indexes.ascending("month"),
                        Indexes.ascending("dow"),
                        Indexes.ascending("cluster")
                    ),
                    new IndexOptions().unique(true).name("profile_fares_unique_key")
                );
                log.info("Ensured unique index exists on profile_fares composite key");
            } catch (Exception e) {
                // Index might already exist, that's fine
                log.debug("Index creation skipped (may already exist): {}", e.getMessage());
            }
            
            List<WriteModel<Document>> bulkOps = new ArrayList<>();
            
            int skippedRows = 0;
            int processedRows = 0;
            
            for (Map<String, String> rowData : data) {
                String sector = rowData.get("sector");
                String month = rowData.get("month");
                String dow = rowData.get("dow");
                String clusterStr = rowData.get("cluster");
                
                // Skip if any key field is missing
                if (sector == null || sector.trim().isEmpty() || 
                    month == null || month.trim().isEmpty() || 
                    dow == null || dow.trim().isEmpty() || 
                    clusterStr == null || clusterStr.trim().isEmpty()) {
                    skippedRows++;
                    log.warn("Skipping row due to missing key fields - sector: {}, month: {}, dow: {}, cluster: {}", 
                        sector, month, dow, clusterStr);
                    continue;
                }
                
                // Convert cluster to the appropriate type (try integer first, fallback to string)
                Object clusterValue;
                try {
                    clusterValue = Integer.parseInt(clusterStr.trim());
                } catch (NumberFormatException e) {
                    clusterValue = clusterStr.trim();
                }
                
                // Create composite key filter - try to match both string and integer cluster
                Bson filter = Filters.and(
                        Filters.eq("sector", sector.trim()),
                        Filters.eq("month", month.trim()),
                        Filters.eq("dow", dow.trim()),
                        Filters.eq("cluster", clusterValue)
                );
                
                // Handle both "starttime" and "start time" variations
                String startTime = rowData.get("starttime");
                if (startTime == null) {
                    startTime = rowData.get("start time");
                }
                
                String endTime = rowData.get("endtime");
                if (endTime == null) {
                    endTime = rowData.get("end time");
                }
                
                // Create document with all columns
                Document doc = new Document()
                        .append("sector", sector.trim())
                        .append("currency", rowData.get("currency"))
                        .append("month", month.trim())
                        .append("dow", dow.trim())
                        .append("cluster", clusterValue)
                        .append("Q0", autoConvert(rowData.get("q0")))
                        .append("Q1", autoConvert(rowData.get("q1")))
                        .append("Q2", autoConvert(rowData.get("q2")))
                        .append("Q3", autoConvert(rowData.get("q3")))
                        .append("Q4", autoConvert(rowData.get("q4")))
                        .append("Q5", autoConvert(rowData.get("q5")))
                        .append("Q6", autoConvert(rowData.get("q6")))
                        .append("Q7", autoConvert(rowData.get("q7")))
                        .append("Q8", autoConvert(rowData.get("q8")))
                        .append("Q9", autoConvert(rowData.get("q9")))
                        .append("Q10", autoConvert(rowData.get("q10")))
                        .append("Q11", autoConvert(rowData.get("q11")))
                        .append("Q12", autoConvert(rowData.get("q12")))
                        .append("Q13", autoConvert(rowData.get("q13")))
                        .append("Q14", autoConvert(rowData.get("q14")))
                        .append("Q15", autoConvert(rowData.get("q15")))
                        .append("Q16", autoConvert(rowData.get("q16")))
                        .append("Q17", autoConvert(rowData.get("q17")))
                        .append("Q18", autoConvert(rowData.get("q18")))
                        .append("Q19", autoConvert(rowData.get("q19")))
                        .append("Q20", autoConvert(rowData.get("q20")))
                        .append("startTime", startTime)
                        .append("endTime", endTime);
                
                if (!GROUP_INCREMENTED_PROFILE_FARES_TABLE_NAME.equals(tableName) && rowData.get("year") != null && !rowData.get("year").trim().isEmpty()) {
                    doc.append("year", autoConvert(rowData.get("year").trim()));
                }
                
                if (GROUP_INCREMENTED_PROFILE_FARES_TABLE_NAME.equals(tableName)) {
                    doc.append("created_at", new Date());
                }
                
                // Upsert operation
                ReplaceOptions options = new ReplaceOptions().upsert(true);
                bulkOps.add(new ReplaceOneModel<>(filter, doc, options));
                processedRows++;
            }
            
            log.info("Profile Fares processing summary - Total rows: {}, Processed: {}, Skipped: {}", 
                data.size(), processedRows, skippedRows);
            
            if (!bulkOps.isEmpty()) {
                BulkWriteOptions bulkOptions = new BulkWriteOptions().ordered(false);
                BulkWriteResult result = collection.bulkWrite(bulkOps, bulkOptions);
                log.info("Profile Fares bulk upsert completed: {} inserted, {} modified, {} upserted",
                        result.getInsertedCount(),
                        result.getModifiedCount(),
                        result.getUpserts().size());
            } else {
                log.warn("No bulk operations to perform - all rows were skipped or data was empty");
            }
        } catch (Exception e) {
            log.error("Error in bulkUpsertProfileFares: " + e.getMessage(), e);
            throw new RuntimeException("Failed to upsert profile fares: " + e.getMessage(), e);
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
                case AUTO_TOLERANCE_GRID_VALUES_TABLE_NAME -> getAutoToleranceGridData(csvData).values().stream().toList();
                default -> createListOfMap(csvData, headers);
            };

            Validator validate = getValidator(tableName);
            String val = null;
            if (tableName.equalsIgnoreCase(QP_FARES_TABLE_NAME)) {
                val = getAllRBDs();
            }
            if (validate != null && !validate.isValid(data, val, listOfErrors.get())) {
                updateInputStatus(tableName);
                error_occurred.set(1);
                listOfErrors.get().add(INVALID_DATA_ERROR_MESSAGE);
                addErrorsToTable(listOfErrors.get(), obj_id);
                throw new DataMismatchedException(listOfErrors.get());
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
                case PROFILE_FARES_TABLE_NAME, GROUP_INCREMENTED_PROFILE_FARES_TABLE_NAME -> {
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

    public void uploadSalesInventoryFile(Reader file, String tableName, String userName, String obj_id) throws Exception {
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
                error_occurred.set(1);
                listOfErrors.get().addAll(currentErrors);
                addErrorsToTable(listOfErrors.get(), obj_id);
                updateInputStatus(tableName);
                throw new DataMismatchedException(listOfErrors.get());
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
                error_occurred.set(1);
                if (!currentErrors.isEmpty()) listOfErrors.get().addAll(currentErrors); else if (listOfErrors.get().isEmpty() && !headerRow.isEmpty()) {
                    listOfErrors.get().add(UNKNOWN_SALES_INVENTORY_CSV_FORMAT_ERROR + " Found headers: " + headerRow);
                } else if (listOfErrors.get().isEmpty() && headerRow.isEmpty()){
                    listOfErrors.get().add(UNKNOWN_SALES_INVENTORY_CSV_FORMAT_ERROR + " CSV header row is empty.");
                }
                addErrorsToTable(listOfErrors.get(), obj_id);
                updateInputStatus(tableName);
                throw new DataMismatchedException(listOfErrors.get());
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
                error_occurred.set(1);
                listOfErrors.get().addAll(currentErrors);
                addErrorsToTable(listOfErrors.get(), obj_id);
                updateInputStatus(tableName);
                log.error(FILE_UPLOAD_ERROR_MESSAGE + " for " + tableName + " due to validation errors (see details in error table).");
                throw new DataMismatchedException(listOfErrors.get());
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
                log.info("Inserting " + recordsToInsert.size() + " unique records into " + tableName);
                insertStatement(recordsToInsert, dbHeaders, tableName);
                numOfRows.set(recordsToInsert.size());

            } else {
                log.info("No valid, unique sales inventory data to insert for " + tableName + ".");
                numOfRows.set(0);
            }

            log.info("Creating CSV backup for " + tableName);
            s3FileStorageDao.storeFile(createCSV(tableName), tableName, uuid);
            updateAuditStatement(tableName, getCurrentDateTime(), uuid);
            log.info("Successfully processed " + tableName + " file.");

        } catch (DataMismatchedException dme) {
            log.error("Data mismatched, processing halted for " + tableName + ": " + dme.getMessage());
            throw dme;
        } catch (Exception e) {
            error_occurred.set(1);
            log.error("Critical error during sales inventory processing for " + tableName + ": " + e.getMessage());
            e.printStackTrace();
            if (listOfErrors.get().isEmpty()) {
                if (!currentErrors.isEmpty()) listOfErrors.get().addAll(currentErrors);
                else listOfErrors.get().add("Failed to process sales inventory file due to an unexpected error");
            } else if (!currentErrors.isEmpty() && !listOfErrors.get().containsAll(currentErrors)){
                listOfErrors.get().addAll(currentErrors);
            }
            addErrorsToTable(listOfErrors.get(), obj_id);
            throw e;
        } finally {
            updateInputStatus(tableName);
            log.info("Finished processing attempt for " + tableName + " with obj_id " + obj_id + ". Status: " + (error_occurred.get() == 1 ? "FAILED" : "DONE"));
        }
    }

    private void deleteSalesInventoryByExactDates(Set<String> datesToDelete, String collectionName) {
        if (datesToDelete == null || datesToDelete.isEmpty()) {
            return;
        }

        log.info("Executing delete on " + collectionName + " for exact dates: " + datesToDelete);

        try {
            MongoCollection<Document> collection = database.getCollection(collectionName.toLowerCase());

            // WHERE date IN (date1, date2, date3, ...)
            Bson filter = in(SALES_DB_DATE, datesToDelete);

            DeleteResult result = collection.deleteMany(filter);

            log.info("Deleted " + result.getDeletedCount() + " exact date records from " +
                    collectionName + " matching: " + datesToDelete);

        } catch (Exception e) {
            error_occurred.set(1);
            String errorMsg = "Failed to delete exact sales inventory data for dates " + datesToDelete +
                    ". Error: " + e.getMessage();
            log.error(errorMsg);
            listOfErrors.get().add(errorMsg);
            throw new RuntimeException(errorMsg, e);
        }
    }

    private void deleteSalesInventoryDailyByDateRange(String startDateYYYYMMDD, String endDateYYYYMMDD,
                                                      String collectionName) {
        log.info("Executing delete for daily records (not ending in '00') on " + collectionName +
                " from " + startDateYYYYMMDD + " to " + endDateYYYYMMDD);

        try {
            MongoCollection<Document> collection = database.getCollection(collectionName.toLowerCase());

            // Build filter: date >= start AND date <= end AND date NOT LIKE '%00'
            Bson filter = and(
                    gte(SALES_DB_DATE, startDateYYYYMMDD),
                    lte(SALES_DB_DATE, endDateYYYYMMDD),
                    not(regex(SALES_DB_DATE, "00$"))  // NOT LIKE '%00' = does NOT end with '00'
            );

            DeleteResult result = collection.deleteMany(filter);

            log.info("Deleted " + result.getDeletedCount() +
                    " existing daily records (not ending in '00') from " +
                    collectionName + " in the range.");

        } catch (Exception e) {
            error_occurred.set(1);
            String errorMsg = "Failed to delete existing daily sales inventory data for range " +
                    startDateYYYYMMDD + "-" + endDateYYYYMMDD + ". Error: " + e.getMessage();
            log.error(errorMsg);
            listOfErrors.get().add(errorMsg);
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

        try {
            MongoCollection<Document> collection = database.getCollection("strategy_grid");
            Bson filter = value != null ? eq("strategy", value) : new Document();

            try (MongoCursor<Document> cursor = collection.find(filter).limit(limit).iterator()) {
                while (cursor.hasNext()) {
                    Document doc = cursor.next();
                    Map<String, Object> row = new HashMap<>();
                    row.put(STRATEGY_COLUMN_NAME, doc.getString(STRATEGY_COLUMN_NAME));
                    row.put(CHANNEL_COLUMN_NAME, doc.getString(CHANNEL_COLUMN_NAME));
                    row.put(DECISION_COLUMN_NAME, doc.getString(DECISION_COLUMN_NAME));
                    row.put(BAND_COLUMN_NAME, doc.getString(BAND_COLUMN_NAME));
                    for (int i = 0; i <= 6; i++) {
                        row.put(String.valueOf(i), doc.getString(String.valueOf(i)));
                    }
                    listObj.add(row);
                }
            }
        } catch (Exception e) {
            log.error(DATABASE_ACCESS_ERROR_MESSAGE);
            e.printStackTrace();
        }
        return listObj;
    }

    public List<Map<String, Object>> retrieveAutoB2BGrid(String value) {
        int limit = getCountOfEntries(DPLF_BANDS_TABLE);
        List<Map<String, Object>> listObj = new ArrayList<>();

        try {
            MongoCollection<Document> collection = database.getCollection("auto_b2b_grid");

            try (MongoCursor<Document> cursor = collection.find().limit(limit).iterator()) {
                while (cursor.hasNext()) {
                    Document doc = cursor.next();
                    Map<String, Object> row = new HashMap<>();
                    row.put(AUTO_B2B_NAME_COLUMN_NAME, doc.getString(AUTO_B2B_NAME_COLUMN_NAME));
                    row.put(NDO_BAND_COLUMN_NAME, doc.getString(NDO_BAND_COLUMN_NAME));
                    for (int i = 0; i <= 6; i++) {
                        row.put(String.valueOf(i), doc.getString(String.valueOf(i)));
                    }
                    listObj.add(row);
                }
            }
        } catch (Exception e) {
            System.out.println(DATABASE_ACCESS_ERROR_MESSAGE);
            e.printStackTrace();
        }
        return listObj;
    }

    public int findIfTableExists(String collectionName) {
        if (collectionName.toUpperCase().startsWith("QPDWPRIAUT.")) {
            collectionName = collectionName.substring("QPDWPRIAUT.".length());
        }

        try {
            // Check in the appropriate database (master or default)
            MongoDatabase targetDb = getDbForCollection(collectionName);
            for (String name : targetDb.listCollectionNames()) {
                if (name.equals(collectionName.toLowerCase())) {
                    return 1;
                }
            }
            return 0;
        } catch (Exception e) {
            log.error(DATABASE_ACCESS_ERROR_MESSAGE);
            e.printStackTrace();
            return 0;
        }
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
        try {
            MongoCollection<Document> collection = getDbForCollection(RM_PARAMS_TABLE_NAME).getCollection(RM_PARAMS_TABLE_NAME);
            Document doc = collection.find(eq("parameterKey", value)).first();

            if (doc != null) {
                listObj = doc.get(PARAMETER_VALUE_COLUMN_NAME).toString();
            }
        } catch (Exception e) {
            log.error(DATABASE_ACCESS_ERROR_MESSAGE);
            e.printStackTrace();
        }
        return listObj;
    }

    public String getAllRBDs() {
        String rbdList = null;
        try {
            MongoCollection<Document> collection = getDbForCollection(RM_PARAMS_TABLE_NAME).getCollection(RM_PARAMS_TABLE_NAME);
            FindIterable<Document> docs = collection.find(regex("parameterKey", "^QP_.*"));

            for (Document doc : docs) {
                String key = doc.getString("parameterKey");
                Object val = doc.get(PARAMETER_VALUE_COLUMN_NAME);

                if (key != null && val != null) {
                    String value = val.toString();
                    rbdList = (rbdList == null) ? value : rbdList + "," + value;
                }
            }
        } catch (Exception e) {
            log.error(DATABASE_ACCESS_ERROR_MESSAGE, e);
        }
        return rbdList;
    }

    public Map<String, Set<String>> getRBDGroups() {
        Map<String, Set<String>> rbdGroups = new HashMap<>();
        try {
            MongoCollection<Document> collection = getDbForCollection(RM_PARAMS_TABLE_NAME).getCollection(RM_PARAMS_TABLE_NAME);
            FindIterable<Document> docs = collection.find(regex("parameterKey", "^QP_.*"));

            for (Document doc : docs) {
                String key = doc.getString("parameterKey");
                Object val = doc.get(PARAMETER_VALUE_COLUMN_NAME);

                if (key != null && val != null) {
                    String value = val.toString();
                    Set<String> rbdSet = new HashSet<>(Arrays.asList(value.split(",")));
                    rbdGroups.put(key, rbdSet);
                }
            }
        } catch (Exception e) {
            log.error(DATABASE_ACCESS_ERROR_MESSAGE, e);
        }
        return rbdGroups;
    }


    private  Validator getValidator(String tableName) {
        Validator validator;
        switch (tableName) {
            case CURVES_TABLE_NAME -> validator = new CurvesValidator(searchResults(LF_VALUE_KEY));
            case QP_FARES_TABLE_NAME -> validator = new FaresValidator();
            case PROFILE_FARES_TABLE_NAME, GROUP_INCREMENTED_PROFILE_FARES_TABLE_NAME -> validator = new ProfileValidator();
            default -> validator = null;
        }
        return validator;
    }

    private List<Map<String, String>> createListOfMap(List<List<String>> csvData, List<String> headers) {
        csvData.remove(0);
        List<Map<String, String>> data = new ArrayList<>();
        for (List<String> singleCsvData : csvData) {
            Map<String, String> map = new HashMap<>();
            for (int i = 0; i < headers.size() && i < singleCsvData.size(); i++) {
                map.put(headers.get(i), singleCsvData.get(i));
            }
            data.add(map);
        }
        return data;
    }

    private List<Map<String, String>> createListOfMapSeriesBlock(List<List<String>> csvData) {
        if (csvData == null || csvData.isEmpty()) {
            return new ArrayList<>();
        }
        List<String> headers = csvData.get(0);
        csvData.remove(0);
        List<Map<String, String>> data = new ArrayList<>();
        DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");
        DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        for (List<String> singleCsvData : csvData) {
            // CWE-125: Bounds check before accessing index 0
            if (singleCsvData.isEmpty()) {
                continue;
            }
            String inputDate = singleCsvData.get(0);

            LocalDate date = LocalDate.parse(inputDate, inputFormatter);
            String departureDate = date.format(outputFormatter);

            for (int i = 1; i < singleCsvData.size() && i < headers.size(); i++) {
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

    public Map<String, Map<String, String>> getAutoToleranceGridData(List<List<String>> csvData) {
        Map<String, Map<String, String>> resultMap = new LinkedHashMap<>();
        int dplfIdHeader = 0;
        int dplfNameHeader = 1;
        int startColumn = 2;
        String[] valueNames = csvData.get(0).subList(startColumn, csvData.get(0).size()).toArray(new String[0]);
        
        for (int i = 1; i < csvData.size(); i++) {
            List<String> inputRow = csvData.get(i);
            String dplfId = csvData.get(i).get(dplfIdHeader);
            String dplfName = csvData.get(i).get(dplfNameHeader);
            for (int j = startColumn; j < inputRow.size(); j++) {
                String value = csvData.get(i).get(j);
                String deltaHeader = valueNames[j - startColumn];
                String deltaId = deltaHeader.replace("delta_", "");
                
                String keyVal = dplfId + "_" + deltaId;
                if (!resultMap.containsKey(keyVal)) {
                    Map<String, String> map = new HashMap<>();
                    map.put("dplf_band_id", dplfId);
                    map.put("dplf_band_name", dplfName);
                    map.put("delta_band_id", deltaId);
                    resultMap.put(keyVal, map);
                }
                Map<String, String> map = resultMap.get(keyVal);
                map.put("g_value", value);
            }
        }
        return resultMap;
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
                error_occurred.set(1);
                listOfErrors.get().addAll(methodScopedErrors);
                return;
            }

            List<List<String>> processedCsvData = splitConnectionsRowsByDateRange(rawCsvData, methodScopedErrors);
            numOfRows.set(processedCsvData.isEmpty() ? 0 : Math.max(0, processedCsvData.size() - 1));

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
                error_occurred.set(1);
                listOfErrors.get().addAll(methodScopedErrors);
            }

            if (!validMarketListConnectionsEntityList.isEmpty()) {
                LocalDateTime currentDateTime = ZonedDateTime.now(ZoneId.of(INDIAN_REGION)).toLocalDateTime();

                if (!tableName.startsWith(MARKET_LIST_ADHOC_TABLE_NAME)) {
                    try {
                        auditHelper.buildConnAuditLogs(validMarketListConnectionsEntityList, userName, currentDateTime, tableName);
                    } catch (Exception e) {
                        log.error("Failed to build connection audit logs for table: {}. Error: {}", tableName, e.getMessage());
                    }
                }

                if (tableName.startsWith(MARKET_LIST_ADHOC_TABLE_NAME)) {
                    deleteValues(Collections.singleton(ZERO), tableName, ZERO);
                } else {
                    deleteSpecificConnectionRecords(validMarketListConnectionsEntityList, tableName);
                }

                bulkInsertIntoMarketListConnections(validMarketListConnectionsEntityList, tableName);

                s3FileStorageDao.storeFile(createCSV(tableName), tableName, auditUuid);
                updateAuditStatement(tableName, getCurrentDateTime(), auditUuid);

                if(error_occurred.get() == 1) {
                    partial_upload_occurred.set(1);
                }
            } else {
                if (error_occurred.get() == 1) {
                    log.info(FILE_UPLOAD_ERROR_MESSAGE + ": No valid connection records to upload.");
                }
            }
        } catch (Exception e) {
            error_occurred.set(1);
            String sqlErrorMsg = "Database operation failed during market list connections upload";
            if (listOfErrors.get().isEmpty() || !listOfErrors.get().get(listOfErrors.get().size()-1).contains(e.getMessage())) {
                listOfErrors.get().add(sqlErrorMsg);
            }
            log.info("SQL Exception in uploadMarketListConnectionsFile: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (!listOfErrors.get().isEmpty()) {
                addErrorsToTable(listOfErrors.get(), obj_id);
            }
            updateInputStatus(tableName);
        }
    }


    private void deleteSpecificConnectionRecords(List<MarketListConnectionsEntity> marketListConnectionsEntityList,
                                                 String collectionName) {
        batchedDelete(marketListConnectionsEntityList, collectionName, entity -> and(
                eq("Sector1", entity.getSector1()),
                eq("Flight1", Integer.parseInt(entity.getFlight1())),
                eq("Sector2", entity.getSector2()),
                eq("Flight2", Integer.parseInt(entity.getFlight2())),
                eq("PerStart", entity.getStartDate())
        ), "MarketListConnections");
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

    public void uploadPartialDateEvent(Reader file, String userName, String obj_id) throws RuntimeException {
        String uuid = obj_id;

        try {
            // 1. Audit logging
            insertAuditStatement(DATE_EVENT_TABLE_NAME, getCurrentDateTime(), userName, uuid);
            insertAuditStatement(CONFIG_PROFILE_FARE_STATION_UPSELL, getCurrentDateTime(), userName, uuid);

            // 2. Set input status
            insertInputStatusTable(DATE_EVENT_TABLE_NAME);
            insertInputStatusTable(CONFIG_PROFILE_FARE_STATION_UPSELL);

            // 3. Parse and validate CSV
            List<List<String>> csvData = getDataFromCSVFile(file);
            Map<String, Map<String, String>> dateEventMap = getPartialDateEvent(csvData);

            if (!listOfErrors.get().isEmpty()) {
                updateInputStatus(DATE_EVENT_TABLE_NAME);
                error_occurred.set(1);
                addErrorsToTable(listOfErrors.get(), obj_id);
                log.info(FILE_UPLOAD_ERROR_MESSAGE);
                throw new DataMismatchedException(listOfErrors.get());
            }

            // 4. Prepare data
            List<String> dateEventHeaders = Arrays.asList(getColumnHeaders(DATE_EVENT_TABLE_NAME).split(COMMA));
            List<Map<String, String>> dateEventData = createListOfMapDateEvent(csvData, dateEventHeaders);

            Set<String> uniqueDates = dateEventData.stream()
                    .map(map -> map.get(DATE_COLUMN_NAME))
                    .collect(Collectors.toSet());

            // 5. Fetch existing upsell records to determine mode
            Map<String, Document> existingUpsells = fetchExistingUpsells(uniqueDates);

            // 6. Update tables using bulk upsert
            bulkUpsertDateEvents(dateEventData, dateEventHeaders);
            bulkUpsertUpsells(dateEventMap, existingUpsells);

            // 7. Update audit
            updateAuditStatement(CONFIG_PROFILE_FARE_STATION_UPSELL, getCurrentDateTime(), uuid);
            updateAuditStatement(DATE_EVENT_TABLE_NAME, getCurrentDateTime(), uuid);

            error_occurred.set(0);

        } catch (Exception e) {
            error_occurred.set(1);
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            updateInputStatus(CONFIG_PROFILE_FARE_STATION_UPSELL);
            updateInputStatus(DATE_EVENT_TABLE_NAME);
        }
    }

    public Map<String, Map<String, String>> getPartialDateEvent(List<List<String>> csvData) {
        Map<String, Map<String, String>> strategyMap = new LinkedHashMap<>();

        if (csvData == null || csvData.isEmpty()) {
            return strategyMap;
        }

        // Get header row
        List<String> headerRow = csvData.get(0);

        // Detect which columns are present and their positions
        int dateColumnIndex = findColumnIndex(headerRow, "Date");
        int dowColumnIndex = findColumnIndex(headerRow, "DOW");
        int mappedMonthColumnIndex = findColumnIndex(headerRow, "Mapped Month", "mappedMonth");
        int eventColumnIndex = findColumnIndex(headerRow, "Event");

        if (dateColumnIndex == -1) {
            listOfErrors.get().add("Date column is required");
            return strategyMap;
        }

        // Determine start column for sectors (after special columns)
        int startColumn = Math.max(
                Math.max(dateColumnIndex, dowColumnIndex),
                Math.max(mappedMonthColumnIndex, eventColumnIndex)
        ) + 1;

        // If DOW doesn't exist in header, start from after Date column
        if (dowColumnIndex == -1) {
            startColumn = dateColumnIndex + 1;
            // Skip Mapped Month and Event if they exist
            if (mappedMonthColumnIndex > dateColumnIndex) {
                startColumn = Math.max(startColumn, mappedMonthColumnIndex + 1);
            }
            if (eventColumnIndex > dateColumnIndex) {
                startColumn = Math.max(startColumn, eventColumnIndex + 1);
            }
        }

        // Process data rows
        for (int i = 1; i < csvData.size(); i++) {
            List<String> inputRow = csvData.get(i);
            if (inputRow.size() <= dateColumnIndex) continue;

            String dateKey = inputRow.get(dateColumnIndex).trim();
            if (dateKey == null || dateKey.trim().isEmpty()) continue;
            if (!dateKey.matches("\\d{2}/\\d{2}/\\d{4}")) {
                listOfErrors.get().add(dateKey + ": Date should have mm/dd/yyyy format");
            }

            // Process sector columns
            for (int j = startColumn; j < headerRow.size(); j++) {
                if (j >= headerRow.size()) continue;

                String sectorKey = headerRow.get(j).trim();
                if (sectorKey == null || sectorKey.trim().isEmpty()) continue;

                // Skip special columns if they appear later in the row
                if (isSpecialColumn(sectorKey)) {
                    continue;
                }

                // Validate sector format ONLY for actual sector columns
                if (i == 1 && !(sectorKey.matches("[A-Za-z]{6}"))) {
                    listOfErrors.get().add(sectorKey + ": Sector value should be valid");
                }

                String value = (j < inputRow.size()) ? inputRow.get(j).trim() : "";
                String keyVal = dateKey + sectorKey;

                // Validate upsell value format - % symbol is not allowed
                if (!((value == null) || (value.isEmpty()) || (value.matches("-?\\d+")))) {
                    listOfErrors.get().add(dateKey + "_" + sectorKey + ": Upsell value must be a valid Integer value without % symbol");
                }
                
                // Additional validation to explicitly reject % symbol
                if (value != null && !value.isEmpty() && value.contains("%")) {
                    listOfErrors.get().add(dateKey + "_" + sectorKey + ": Upsell value cannot contain % symbol. Please provide numeric values only");
                }

                Map<String, String> map = new HashMap<>();
                map.put(DATE_COLUMN_NAME, dateKey);
                map.put(SECTOR_COLUMN_NAME.toLowerCase(), sectorKey);
                map.put(VALUE_COLUMN_NAME, value);
                strategyMap.put(keyVal, map);
            }
        }
        return strategyMap;
    }

    // Helper method to find column index (case-insensitive, handles multiple variations)
    private int findColumnIndex(List<String> headerRow, String... possibleNames) {
        for (int i = 0; i < headerRow.size(); i++) {
            String header = headerRow.get(i).trim();
            for (String name : possibleNames) {
                if (header.equalsIgnoreCase(name)) {
                    return i;
                }
            }
        }
        return -1;
    }

    // Helper method to check if column is a special column
    private boolean isSpecialColumn(String columnName) {
        if (columnName == null) return false;
        String normalized = columnName.trim().toLowerCase();
        return normalized.equals("date") ||
                normalized.equals("dow") ||
                normalized.equals("mapped month") ||
                normalized.equals("mappedmonth") ||
                normalized.equals("event");
    }

    private Map<String, Document> fetchExistingUpsells(Set<String> dates) {
        Map<String, Document> existingMap = new HashMap<>();

        if (dates.isEmpty()) {
            return existingMap;
        }

        try {
            MongoCollection<Document> collection = database.getCollection(
                    CONFIG_PROFILE_FARE_STATION_UPSELL.toLowerCase()
            );

            Bson filter = Filters.in(DATE_COLUMN_NAME, dates);

            for (Document doc : collection.find(filter)) {
                String date = doc.getString(DATE_COLUMN_NAME);
                String sector = doc.getString(SECTOR_COLUMN_NAME.toLowerCase());
                String key = date + sector;
                existingMap.put(key, doc);
            }

            log.info("Fetched " + existingMap.size() + " existing upsell records");
        } catch (Exception e) {
            log.error("Error fetching existing upsells: " + e.getMessage());
            e.printStackTrace();
        }

        return existingMap;
    }

    private void bulkUpsertDateEvents(List<Map<String, String>> data, List<String> headers) {
        MongoCollection<Document> collection = database.getCollection(
                DATE_EVENT_TABLE_NAME.toLowerCase()
        );

        List<WriteModel<Document>> bulkOps = new ArrayList<>();

        for (Map<String, String> dataMap : data) {
            String date = dataMap.get(DATE_COLUMN_NAME);

            // Create filter (unique key = date)
            Bson filter = Filters.eq(DATE_COLUMN_NAME, date);

            // Create document
            Document doc = new Document();
            for (String header : headers) {
                doc.append(header, autoConvert(dataMap.get(header)));
            }

            // Upsert operation (replace entire document)
            ReplaceOptions options = new ReplaceOptions().upsert(true);
            bulkOps.add(new ReplaceOneModel<>(filter, doc, options));
        }

        if (!bulkOps.isEmpty()) {
            BulkWriteOptions bulkOptions = new BulkWriteOptions().ordered(false);
            BulkWriteResult result = collection.bulkWrite(bulkOps, bulkOptions);
            log.info("Bulk upserted " + result.getInsertedCount() + " inserted, " +
                    result.getModifiedCount() + " modified date events");
        }
    }

    private void bulkUpsertUpsells(Map<String, Map<String, String>> dateEventMap, Map<String, Document> existingUpsells) {
        try {
            MongoCollection<Document> collection = database.getCollection(CONFIG_PROFILE_FARE_STATION_UPSELL);

            List<WriteModel<Document>> bulkOps = new ArrayList<>();

            for (Map.Entry<String, Map<String, String>> entry : dateEventMap.entrySet()) {
                Map<String, String> rowData = entry.getValue();

                String date = rowData.get(DATE_COLUMN_NAME);
                String sector = rowData.get(SECTOR_COLUMN_NAME.toLowerCase());
                String newValue = rowData.get(VALUE_COLUMN_NAME);

                // Skip if both date and sector are missing
                if (date == null || date.trim().isEmpty() || sector == null || sector.trim().isEmpty()) {
                    continue;
                }

                // Use the value as-is since % symbol is not allowed
                String cleanValue = (newValue != null && !newValue.trim().isEmpty())
                        ? newValue.trim()
                        : "";  // Empty string if null or empty

                // Create unique key for lookup
                String lookupKey = date + sector;
                Document existingDoc = existingUpsells.get(lookupKey);

                // REMOVED: No more deletion logic
                // Empty value now just sets value to "" with mode "M"

                // Determine mode
                String mode = "M"; // Default for analyst upload
                if (existingDoc != null && !cleanValue.isEmpty()) {
                    // Only preserve mode if value is NOT empty AND value unchanged
                    String existingValue = existingDoc.getString(VALUE_COLUMN_NAME);
                    String existingMode = existingDoc.getString("mode");

                    // If value UNCHANGED, preserve existing mode
                    if (cleanValue.equals(existingValue)) {
                        mode = (existingMode != null) ? existingMode : "S";
                    }
                    // If value CHANGED, mark as Manual (mode = "M")
                }
                // If cleanValue is empty, always use mode = "M"

                // Create filter that matches BOTH date AND sector
                Bson filter = Filters.and(
                        Filters.eq(DATE_COLUMN_NAME, date),
                        Filters.eq(SECTOR_COLUMN_NAME.toLowerCase(), sector)
                );

                // Create update document
                Document updateDoc = new Document()
                        .append(DATE_COLUMN_NAME, date)
                        .append(SECTOR_COLUMN_NAME.toLowerCase(), sector)
                        .append(VALUE_COLUMN_NAME, cleanValue)  // Can be empty string
                        .append("mode", mode)
                        .append("updatedAt", new Date())
                        .append("updatedBy", "ANALYST");

                // Use ReplaceOneModel with upsert=true
                bulkOps.add(new ReplaceOneModel<>(
                        filter,
                        updateDoc,
                        new ReplaceOptions().upsert(true)
                ));
            }

            if (!bulkOps.isEmpty()) {
                BulkWriteResult result = collection.bulkWrite(bulkOps);
                log.info("Upsell bulk write completed: {} inserted, {} modified",
                        result.getInsertedCount(),
                        result.getModifiedCount());
            }

        } catch (Exception e) {
            log.error("Error in bulkUpsertUpsells: " + e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public void deletePartialSectorValues(Set<String> dates, String sector, String collectionName) {
        if (dates.isEmpty()) {
            return;
        }

        try {
            MongoCollection<Document> collection = database.getCollection(collectionName.toLowerCase());
            Bson filter = and(
                    in(DATE_COLUMN_NAME, dates),
                    eq(SECTOR_COLUMN_NAME.toLowerCase(), sector)
            );
            DeleteResult result = collection.deleteMany(filter);
            log.info("Deleted " + result.getDeletedCount() + " records for sector " + sector);
        } catch (Exception e) {
            log.error("Error deleting sector values: " + e.getMessage());
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
            error_occurred.set(0);
        } catch (Exception e) {
            throw new RuntimeException("Error during date event file upload: " + e.getMessage(), e);
        } finally {
            updateInputStatus(secondaryTableName);
            updateInputStatus(DATE_EVENT_TABLE_NAME);
        }
    }

    public void deleteTable(String collectionName) {
        try {
            database.getCollection(collectionName.toLowerCase()).drop();
            log.info("Dropped collection: " + collectionName);
        } catch (Exception e) {
            log.error("Failed to delete collection");
            e.printStackTrace();
        }
    }


    public void copyFromTempToMainTable(String collectionName, String tempCollectionName) {
        try {
            MongoCollection<Document> tempColl = database.getCollection(tempCollectionName.toLowerCase());
            MongoCollection<Document> mainColl = getDbForCollection(collectionName).getCollection(collectionName.toLowerCase());

            List<Document> docs = tempColl.find().into(new ArrayList<>());
            if (!docs.isEmpty()) {
                mainColl.insertMany(docs);
                log.info("Copied " + docs.size() + " documents from " +
                        tempCollectionName + " to " + collectionName);
            }
        } catch (Exception e) {
            log.error("Failed to copy data from temp to main collection");
            e.printStackTrace();
        }
    }

    public List<Map<String, String>> createListOfMapDateEvent(List<List<String>> csvData, List<String> headers) {
        csvData.remove(0);
        List<Map<String, String>> data = new ArrayList<>();
        for (List<String> singleCsvData : csvData) {
            if (singleCsvData.isEmpty()) {
                continue;
            }
            String date = singleCsvData.get(0).trim();
            Date parsedDate = parseDate(date);
            // CWE-476: parseDate can return null, already handled by ternary
            String dayOfWeek = (parsedDate != null) ? getAbbreviatedDayOfWeek(parsedDate) : "";

            singleCsvData.add(1, dayOfWeek);

            Map<String, String> map = new HashMap<>();
            for (int i = 0; i < headers.size() && i < singleCsvData.size(); i++) {
                // CWE-125: bounds already guarded by loop condition
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
        if (date == null) {
            return "";
        }
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

    public void insertStatement(List<Map<String, String>> data, List<String> columnHeaders, String collectionName) throws Exception {
        try {
            MongoCollection<Document> collection = getDbForCollection(collectionName).getCollection(collectionName.toLowerCase());
            List<Document> documents = new ArrayList<>();

            for (Map<String, String> dataMap : data) {
                Document doc = new Document();
                for (String header : columnHeaders) {
                    String value = dataMap.get(header);

                    // Auto-convert to appropriate type
                    Object convertedValue = autoConvert(value);
                    doc.append(header, convertedValue);
                }
                documents.add(doc);
            }

            if (!documents.isEmpty()) {
                collection.insertMany(documents);
                log.info("Inserted " + documents.size() + " documents into " + collectionName);
            }
        } catch (Exception e) {
            error_occurred.set(1);
            System.err.println("Insert failed into " + collectionName + ": " + e.getMessage());
            throw e;
        }
    }

    // Simple auto-conversion method
    private Object autoConvert(String value) {
        if (value == null || value.trim().isEmpty()) {
            return value;
        }

        value = value.trim();

        // Try integer
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            // Not an integer, continue
        }

        // Try double
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            // Not a double, continue
        }

        // Try boolean
        if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
            return Boolean.parseBoolean(value);
        }

        // Return as string
        return value;
    }

    public String getColumnHeaders(String value) {
        String listObj = null;

        if (SALESINV_RBD_DISTRIBUTION_TABLE_NAME.equalsIgnoreCase(value)) {
            return String.join(",", SI_RBD_SEATS_COLUMN_NAME, SI_RBD_TYPE_COLUMN_NAME,
                    SI_RBD_RBD_COLUMN_NAME, SI_RBD_VALUE_COLUMN_NAME);
        }

        if (SALES_INVENTORY_TABLE_NAME.equalsIgnoreCase(value)) {
            return String.join(",", SALES_DB_DATE, SALES_DB_MINVALUE,
                    SALES_DB_MAXVALUE, SALES_DB_SEATSALLOCATION);
        }

        if (value.startsWith(MARKET_LIST_SALESINV_TABLE_NAME)) {
            return "Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,CurveID,saleflag";
        }

        if (AUTO_TOLERANCE_NDO_CONFIG_TABLE_NAME.equalsIgnoreCase(value)) {
            return "ndo_min,ndo_max,n_value";
        }

        if (AUTO_TOLERANCE_DELTA_BANDS_TABLE_NAME.equalsIgnoreCase(value)) {
            return "id,band_name,min_value,max_value";
        }

        if (AUTO_TOLERANCE_GRID_VALUES_TABLE_NAME.equalsIgnoreCase(value)) {
            return "dplf_band_id,dplf_band_name,delta_band_id,g_value";
        }

        try {
            MongoCollection<Document> collection = database.getCollection("config_column_names");
            Document doc = collection.find(eq("tableName", value)).first();

            if (doc != null) {
                listObj = doc.getString(COLUMNS_COLUMN_NAME);
            }
        } catch (Exception e) {
            log.error(DATABASE_ACCESS_ERROR_MESSAGE);
            e.printStackTrace();
        }

        if (listObj == null) {
            log.error("Warning: Column headers not found for table identifier: " + value + ". Returning empty string.");
            return "";
        }
        return listObj;
    }

    public void deleteValuesStrategyTable(Set<String> value, String collectionName,
                                          String strategyColumnName, String channelColumnName) {
        if (!value.isEmpty()) {
            try {
                MongoCollection<Document> collection = database.getCollection(collectionName.toLowerCase());

                // For each concatenated value, split and delete
                List<Bson> filters = new ArrayList<>();
                for (String concatenatedValue : value) {
                    filters.add(
                            Filters.expr(
                                    new Document("$eq", Arrays.asList(
                                            new Document("$concat", Arrays.asList(
                                                    "$" + strategyColumnName,
                                                    "$" + channelColumnName
                                            )),
                                            concatenatedValue
                                    ))
                            )
                    );
                }

                Bson finalFilter = or(filters);
                DeleteResult result = collection.deleteMany(finalFilter);
                log.info("Deleted " + result.getDeletedCount() + " documents");
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }
    }

    public void deleteValuesAutoB2BTable(Set<String> value, String tableName, String columnName) {
        if (value == null || value.isEmpty()) {
            return;
        }

        try {
            MongoCollection<Document> collection = database.getCollection(tableName.toLowerCase());

            // One-line filter - MongoDB handles Set directly
            Bson filter = in(columnName, value);

            DeleteResult result = collection.deleteMany(filter);

            log.info("Deleted " + result.getDeletedCount() + " records from " + tableName);

        } catch (Exception e) {
            log.error(e.getMessage());
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
                                                            Map<String, String> headerColumnMap, String tableName) throws DataMismatchedException {
        Map<String, Map<String, String>> strategyMap = new LinkedHashMap<>();
        int strategyGridHeader = 0;
        int channelHeader = 1;
        int decisionGridHeader = 2;
        int ndoBandHeader = 3;
        int startColumn = 4;
        Set<String> strategyCounter = new HashSet<>();
        Set<String> ndoBandCounter = new HashSet<>();
        Set<String> channelCounter = new HashSet<>();
        Set<String> decisionCounter = new HashSet<>();
        Set<String> bandCounter = new HashSet<>();
        int countTotalValues = 0;
        for (int i = 1; i < csvData.size(); i++) {
            List<String> inputRow = csvData.get(i);
            String strategyKey = csvData.get(i).get(strategyGridHeader);
            strategyCounter.add(strategyKey);

            String decision = csvData.get(i).get(decisionGridHeader);
            decisionCounter.add(decision);
            String ndoBandKey = csvData.get(i).get(ndoBandHeader);
            ndoBandCounter.add(ndoBandKey);
            String channelKey = csvData.get(i).get(channelHeader);
            channelCounter.add(channelKey);
            for (int j = startColumn; j < inputRow.size(); j++) {
                String value = csvData.get(i).get(j);
                String dlfBand = csvData.get(0).get(j);
                bandCounter.add(dlfBand);
                String keyVal = strategyKey + ndoBandKey + dlfBand + channelKey;
                if (!strategyMap.containsKey(keyVal)) {
                    Map<String, String> map = new HashMap<>();
                    map.put(STRATEGY_COLUMN_NAME, strategyKey);
                    map.put(NDO_BAND_COLUMN_NAME, ndoBandKey);
                    if (FORECAST_STRATEGY_TABLE_NAME.equals(tableName)) {
                        map.put(FORECAST_BAND_COLUMN_NAME, dlfBand);
                    } else {
                        map.put(DPLF_BAND_COLUMN_NAME, dlfBand);
                    }
                    map.put(CHANNEL_COLUMN_NAME, channelKey);
                    strategyMap.put(keyVal, map);
                }
                Map<String, String> map = strategyMap.get(keyVal);

                if (!validationForStrategyGrid(decision, value)) {
                    error_occurred.set(1);
                    updateInputStatus(tableName);
                    String errorMsg = "Line " + (i+1) + ": " + INVALID_DATA_ERROR_MESSAGE
                            + " for " + strategyKey + " with decision :" + decision + " having value as " + value;
                    listOfErrors.get().add(errorMsg);
                }
                map.put(headerColumnMap.get(decision), value);
                countTotalValues++;
            }
        }
        int expectedValueCount = strategyCounter.size() * ndoBandCounter.size() * channelCounter.size() * decisionCounter.size() * bandCounter.size();
        findActualCountBetweenRowsAndColumns(expectedValueCount, countTotalValues, listOfErrors.get(), tableName);

        if (!listOfErrors.get().isEmpty()) {
            throw new DataMismatchedException(listOfErrors.get());
        }

        return strategyMap;
    }

    public static String extractNumber(String input) {
        return input.replaceAll("\\s*\\(.*\\)", "").trim();
    }

    public Map<String, Map<String, String>> getAutoB2BGrid(List<List<String>> csvData,
                                                           Map<String, String> headerColumnMap, String tableName) throws DataMismatchedException {
        Map<String, Map<String, String>> autob2bMap = new LinkedHashMap<>();
        int nameHeader = 0;
        int ndoBandHeader = 1;
        int startColumn = 2;
        Set<String> counter = new HashSet<>();
        Set<String> ndoBandCounter = new HashSet<>();
        Set<String> forecastBandCounter = new HashSet<>();
        int countTotalValues = 0;
        for (int i = 1; i < csvData.size(); i++) {
            List<String> inputRow = csvData.get(i);
            String nameKey = csvData.get(i).get(nameHeader);
            counter.add(nameKey);

            String ndoBandKey = extractNumber(csvData.get(i).get(ndoBandHeader));
            ndoBandCounter.add(ndoBandKey);
            for (int j = startColumn; j < inputRow.size(); j++) {
                String value = csvData.get(i).get(j);
                String forecastBand = extractNumber(csvData.get(0).get(j));
                forecastBandCounter.add(forecastBand);
                String keyVal = nameKey + ndoBandKey + forecastBand;
                if (!autob2bMap.containsKey(keyVal)) {
                    Map<String, String> map = new HashMap<>();
                    map.put(AUTO_B2B_NAME_COLUMN_NAME, nameKey);
                    map.put(NDO_BAND_COLUMN_NAME, ndoBandKey);
                    map.put(FORECAST_BAND_COLUMN_NAME, forecastBand);
                    autob2bMap.put(keyVal, map);
                }
                Map<String, String> map = autob2bMap.get(keyVal);

                if (!value.matches("^(-1(\\.0+)?|0(\\.\\d+)?|1(\\.0+)?)$")) {
                    listOfErrors.get().add(nameKey + ": auto_b2b_factor should either be -1 or witin the range of 0 to 1");
                    continue;
                }
                map.put(AUTO_B2B_FACTOR_COLUMN_NAME, value);
                countTotalValues++;
            }
        }
        int expectedValueCount = counter.size() * ndoBandCounter.size() * forecastBandCounter.size();
        findActualCountForAutoB2BFactor(expectedValueCount, countTotalValues, listOfErrors.get(), tableName);

        if (!listOfErrors.get().isEmpty()) {
            throw new DataMismatchedException(listOfErrors.get());
        }

        return autob2bMap;
    }

    private void findActualCountBetweenRowsAndColumns(int expectedValueCount, int countTotalValues,
                                                      List<String> errorList, String tableName) throws DataMismatchedException {
        if (expectedValueCount != countTotalValues) {
            updateInputStatus(tableName);
            errorList.add(INVALID_NUMBER_OF_ROW_COLUMN_ERROR_MESSAGE);
            throw new DataMismatchedException(Collections.singletonList(INVALID_NUMBER_OF_ROW_COLUMN_ERROR_MESSAGE));
        }
    }

    private void findActualCountForAutoB2BFactor(int expectedValueCount, int countTotalValues,
                                                 List<String> errorList, String tableName) throws DataMismatchedException {
        if (expectedValueCount != countTotalValues) {
            updateInputStatus(tableName);
            errorList.add(INVALID_NUMBER_OF_ROW_COLUMN_ERROR_MESSAGE);
            throw new DataMismatchedException(Collections.singletonList(INVALID_NUMBER_OF_ROW_COLUMN_ERROR_MESSAGE));
        }
    }

    public int getCountOfEntries(String collectionName) {
        int count = 0;
        try {
            MongoCollection<Document> collection = database.getCollection(collectionName.toLowerCase());
            count = (int) collection.countDocuments();
        } catch (Exception e) {
            System.out.println(DATABASE_ACCESS_ERROR_MESSAGE);
            e.printStackTrace();
        }
        return count;
    }

    public boolean validationForStrategyGrid(String decision, String value) {
        if (decision == null || value == null) {
            return false;
        }
        if (decision.equalsIgnoreCase(CSV_HEADER_TIME)) {
            return getListOfTimeRange().contains(value);
        }
        if (decision.equalsIgnoreCase(CSV_HEADER_CRITERIA)) {
            return getListOfCriteria().contains(value);
        }
        if (decision.equalsIgnoreCase(CSV_HEADER_OFFSET)) {
            // CWE-190: Safe parseInt with NumberFormatException handling
            try {
                return getListOfOffset().contains(Integer.parseInt(value));
            } catch (NumberFormatException e) {
                return false;
            }
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

    public List<Map<String, Object>> findAllRecordsFromTable(String collectionName, List<String> headers, int limit) {
        List<Map<String, Object>> listObj = new ArrayList<>();
        List<String> booleanHeaders = Arrays.asList(
                "OwnFareFlag", "AutoTimeRangeFlag", "openingFares", "OverBooking",
                "profileFares", "rbdPushFlag", "distressInventoryFlag", "seriesBlock",
                "autoGroup", "autoBackstopFlag"
        );

        try {
            MongoCollection<Document> collection = database.getCollection(collectionName.toLowerCase());

            MongoCursor<Document> cursor;
            if (limit > 0) {
                cursor = collection.find().limit(limit).iterator();
            } else {
                cursor = collection.find().iterator();
            }

            while (cursor.hasNext()) {
                Document doc = cursor.next();
                Map<String, Object> row = new HashMap<>();

                for (String header : headers) {
                    Object value = doc.get(header);

                    // Convert boolean headers from 1/0 to Y/N
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
            cursor.close();
        } catch (Exception e) {
            log.error(DATABASE_ACCESS_ERROR_MESSAGE);
            e.printStackTrace();
        }
        return listObj;
    }

    public List<Map<String, Object>> findAllRecordsFromTable(String tableName, List<String> headers) {
        return findAllRecordsFromTable(tableName, headers, 0);
    }

    public int getCurrentInputStatus(String name) {
        List<Map<String, Object>> listObj = new ArrayList<>();
        try {
            MongoCollection<Document> collection = database.getCollection("inputs_status");
            Document doc = collection.find(eq("name", name)).first();

            if (doc != null) {
                Map<String, Object> row = new HashMap<>();
                for (String key : doc.keySet()) {
                    row.put(key, doc.get(key));
                }
                listObj.add(row);
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return listObj.size();
    }

    public void updateInputStatus(String name) {
        int value = 0;
        try {
            MongoCollection<Document> collection = database.getCollection("inputs_status");
            Bson filter = eq("name", name);
            Bson update = set("is_running", value);
            collection.updateOne(filter, update);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    public void updateAuditStatement(String collectionName, String endTime, String uuid) {
        try {
            // Update file_upload_audit
            MongoCollection<Document> auditCollection = database.getCollection("file_upload_audit");
            Bson auditFilter = and(
                    eq("uuid", uuid),
                    eq("tableName", collectionName.toLowerCase())
            );
            Bson auditUpdate = set("end_time", endTime);
            auditCollection.updateOne(auditFilter, auditUpdate);

            // Check if version exists
            MongoCollection<Document> versionCollection = database.getCollection("currentversion");
            long count = versionCollection.countDocuments(eq("tableName", collectionName));

            if (count >= 1) {
                // Update existing version
                Bson versionFilter = eq("tableName", collectionName);
                Bson versionUpdate = set("curr_version", uuid);
                versionCollection.updateOne(versionFilter, versionUpdate);
            } else {
                // Insert new version
                Document versionDoc = new Document()
                        .append("curr_version", uuid)
                        .append("tableName", collectionName)
                        .append("createdAt", new Date());
                versionCollection.insertOne(versionDoc);
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    public void insertIntoMarketList(MarketListEntity marketListEntity, String collectionName) {
        try {
            MongoCollection<Document> collection = database.getCollection(collectionName.toLowerCase());

            Document doc = new Document()
                    .append("Origin", toIntOrKeepSame(marketListEntity.getOrigin()))
                    .append("Destin", toIntOrKeepSame(marketListEntity.getDestination()))
                    .append("FlightNumber", toIntOrKeepSame(marketListEntity.getFlightNo()))
                    .append("PerType", toIntOrKeepSame(marketListEntity.getPerType()))
                    .append("PerStart", toIntOrKeepSame(marketListEntity.getStartDate()))
                    .append("PerEnd", toIntOrKeepSame(marketListEntity.getEndDate()))
                    .append("DOW", toIntOrKeepSame(marketListEntity.getDayOfWeek()))
                    .append("hardAnchor", toIntOrKeepSame(marketListEntity.getHardAnchor()))
                    .append("plfThreshold", toIntOrKeepSame(marketListEntity.getPlfThreshold()))
                    .append("TimeWindowStart", toIntOrKeepSame(marketListEntity.getStartTime()))
                    .append("TimeWindowEnd", toIntOrKeepSame(marketListEntity.getEndTime()))
                    .append("CurveID", toIntOrKeepSame(marketListEntity.getCurveId()))
                    .append("CarrExlusionB2B", toIntOrKeepSame(marketListEntity.getCarrExclusionB2B()))
                    .append("CarrExlusionB2C", toIntOrKeepSame(marketListEntity.getCarrExclusionB2C()))
                    .append("flightExclusionB2B", toIntOrKeepSame(marketListEntity.getFlightExclusionB2B()))
                    .append("flightExclusionB2C", toIntOrKeepSame(marketListEntity.getFlightExclusionB2C()))
                    .append("fareAnchor", toIntOrKeepSame(marketListEntity.getFareAnchor()))
                    .append("fareOffset", toIntOrKeepSame(marketListEntity.getFareOffset()))
                    .append("FirstRBDAlloc", toIntOrKeepSame(marketListEntity.getFirstAllocation()))
                    .append("OtherRBDAlloc", toIntOrKeepSame(marketListEntity.getOtherAllocation()))
                    .append("B2BBackstop", toIntOrKeepSame(marketListEntity.getB2bBackstop()))
                    .append("B2CBackstop", toIntOrKeepSame(marketListEntity.getB2cBackstop()))
                    .append("B2BFactor", toIntOrKeepSame(marketListEntity.getB2bFactor()))
                    .append("obSeats", toIntOrKeepSame(marketListEntity.getOBSeats()))
                    .append("obFare", toIntOrKeepSame(marketListEntity.getOBFare()))
                    .append("SkippingFactor", toIntOrKeepSame(marketListEntity.getSkippingFactor()))
                    .append("DaySpan", toIntOrKeepSame(marketListEntity.getDaySpan()))
                    .append("AutoTimeRangeFlag", toIntOrKeepSame(marketListEntity.getAutoTimeRangeFlag()))
                    .append("analystName", toIntOrKeepSame(marketListEntity.getAnalystName()))
                    .append("openingFares", toIntOrKeepSame(marketListEntity.getOpeningFares()))
                    .append("OverBooking", toIntOrKeepSame(marketListEntity.getOverBooking()))
                    .append("rbdPushFlag", toIntOrKeepSame(marketListEntity.getRbdPushFlag()))
                    .append("profileFares", toIntOrKeepSame(marketListEntity.getProfileFares()))
                    .append("B2CTolerance", toIntOrKeepSame(marketListEntity.getB2cTolerance()))
                    .append("B2BTolerance", toIntOrKeepSame(marketListEntity.getB2bTolerance()))
                    .append("distressInventoryFlag", toIntOrKeepSame(marketListEntity.getDistressInventoryFlag()))
                    .append("seriesBlock", toIntOrKeepSame(marketListEntity.getSeriesBlock()))
                    .append("autoGroup", toIntOrKeepSame(marketListEntity.getAutoGroup()))
                    .append("autoBackstopFlag", toIntOrKeepSame(marketListEntity.getAutoBackstopFlag()))
                    .append("tbfFlag", toIntOrKeepSame(marketListEntity.getTbfFlag()))
                    .append("CugFlag", toIntOrKeepSame(marketListEntity.getCugFlag()))
                    .append("MktFareFlag", toIntOrKeepSame(marketListEntity.getMktFareFlag()))
                    .append("UUID", toIntOrKeepSame(marketListEntity.getUuid()))
                    .append("directFlag", toIntOrKeepSame(marketListEntity.getDirectFlag()))
                    .append("TBFEnhFlag", toIntOrKeepSame(marketListEntity.getTbfEnhFlag()))
                    .append("defaultMlFlag", toIntOrKeepSame(marketListEntity.getDefaultMlFlag()))
                    .append("b2bStrategy", toIntOrKeepSame(marketListEntity.getB2bStrategy()))
                    .append("Outbound_Stop", toIntOrKeepSame(marketListEntity.getB2bOutbound_Stop()))
                    .append("Marketfare_Upsell", toIntOrKeepSame(marketListEntity.getMarketfare_Upsell()))
                    .append("forecastAllocFlag", toIntOrKeepSame(marketListEntity.getForecastAllocFlag()))
                    .append("afForecastAllocFlag", toIntOrKeepSame(marketListEntity.getForecastAllocFlag()))
                    .append("incrementedProfileFareFlag", toIntOrKeepSame(marketListEntity.getIncrementedProfileFareFlag()))
                    .append("ProfileFareUpsell", toIntOrKeepSame(marketListEntity.getProfileFareUpsell()))
                    .append("Spoilage_Flag", toIntOrKeepSame(marketListEntity.getSpoilage_Flag()))
                    .append("CanAdjustedForecast", toIntOrKeepSame(marketListEntity.getCanAdjustedForecast()))
                    .append("market_station_mapping", toIntOrKeepSame(marketListEntity.getMarket_station_mapping()))
                    .append("createdAt", new Date());

            collection.insertOne(doc);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }
    public Object toIntOrKeepSame(String value) {
        if (value == null || value.isEmpty()) {
            return value;
        }

        if (value.toLowerCase().contains("e")) {
            return value;
        }

        try {
            double d = Double.parseDouble(value);
            // If it's a whole number, return as int
            if (d == Math.floor(d) && !Double.isInfinite(d)) {
                return (int) d;
            }
            return d;
        } catch (NumberFormatException e) {
            return value;
        }
    }

    /**
     * Bulk insert market list entities using insertMany in batches.
     * MongoDB's insertMany has a 16 MB BSON limit per batch, so we chunk at 5000 docs
     * (~50 fields × ~40 bytes × 5000 ≈ 10 MB — safe margin).
     */
    private static final int BULK_INSERT_BATCH_SIZE = 5000;
    private static final int DELETE_BATCH_SIZE = 5000;
    private static final int LOG_EVERY_N_RECORDS = 50000;

    public void bulkInsertIntoMarketList(List<MarketListEntity> entities, String collectionName) {
        Date now = new Date();
        batchedInsert(entities, collectionName, e -> marketListEntityToDocument(e, now), "MarketList");
    }

    private Document marketListEntityToDocument(MarketListEntity e, Date createdAt) {
        return new Document()
                .append("Origin", toIntOrKeepSame(e.getOrigin()))
                .append("Destin", toIntOrKeepSame(e.getDestination()))
                .append("FlightNumber", toIntOrKeepSame(e.getFlightNo()))
                .append("PerType", toIntOrKeepSame(e.getPerType()))
                .append("PerStart", toIntOrKeepSame(e.getStartDate()))
                .append("PerEnd", toIntOrKeepSame(e.getEndDate()))
                .append("DOW", toIntOrKeepSame(e.getDayOfWeek()))
                .append("hardAnchor", toIntOrKeepSame(e.getHardAnchor()))
                .append("plfThreshold", toIntOrKeepSame(e.getPlfThreshold()))
                .append("TimeWindowStart", toIntOrKeepSame(e.getStartTime()))
                .append("TimeWindowEnd", toIntOrKeepSame(e.getEndTime()))
                .append("CurveID", toIntOrKeepSame(e.getCurveId()))
                .append("CarrExlusionB2B", toIntOrKeepSame(e.getCarrExclusionB2B()))
                .append("CarrExlusionB2C", toIntOrKeepSame(e.getCarrExclusionB2C()))
                .append("flightExclusionB2B", toIntOrKeepSame(e.getFlightExclusionB2B()))
                .append("flightExclusionB2C", toIntOrKeepSame(e.getFlightExclusionB2C()))
                .append("fareAnchor", toIntOrKeepSame(e.getFareAnchor()))
                .append("fareOffset", toIntOrKeepSame(e.getFareOffset()))
                .append("FirstRBDAlloc", toIntOrKeepSame(e.getFirstAllocation()))
                .append("OtherRBDAlloc", toIntOrKeepSame(e.getOtherAllocation()))
                .append("B2BBackstop", toIntOrKeepSame(e.getB2bBackstop()))
                .append("B2CBackstop", toIntOrKeepSame(e.getB2cBackstop()))
                .append("B2BFactor", toIntOrKeepSame(e.getB2bFactor()))
                .append("obSeats", toIntOrKeepSame(e.getOBSeats()))
                .append("obFare", toIntOrKeepSame(e.getOBFare()))
                .append("SkippingFactor", toIntOrKeepSame(e.getSkippingFactor()))
                .append("DaySpan", toIntOrKeepSame(e.getDaySpan()))
                .append("AutoTimeRangeFlag", toIntOrKeepSame(e.getAutoTimeRangeFlag()))
                .append("analystName", toIntOrKeepSame(e.getAnalystName()))
                .append("openingFares", toIntOrKeepSame(e.getOpeningFares()))
                .append("OverBooking", toIntOrKeepSame(e.getOverBooking()))
                .append("rbdPushFlag", toIntOrKeepSame(e.getRbdPushFlag()))
                .append("profileFares", toIntOrKeepSame(e.getProfileFares()))
                .append("B2CTolerance", toIntOrKeepSame(e.getB2cTolerance()))
                .append("B2BTolerance", toIntOrKeepSame(e.getB2bTolerance()))
                .append("distressInventoryFlag", toIntOrKeepSame(e.getDistressInventoryFlag()))
                .append("seriesBlock", toIntOrKeepSame(e.getSeriesBlock()))
                .append("autoGroup", toIntOrKeepSame(e.getAutoGroup()))
                .append("autoBackstopFlag", toIntOrKeepSame(e.getAutoBackstopFlag()))
                .append("tbfFlag", toIntOrKeepSame(e.getTbfFlag()))
                .append("CugFlag", toIntOrKeepSame(e.getCugFlag()))
                .append("MktFareFlag", toIntOrKeepSame(e.getMktFareFlag()))
                .append("UUID", toIntOrKeepSame(e.getUuid()))
                .append("directFlag", toIntOrKeepSame(e.getDirectFlag()))
                .append("TBFEnhFlag", toIntOrKeepSame(e.getTbfEnhFlag()))
                .append("defaultMlFlag", toIntOrKeepSame(e.getDefaultMlFlag()))
                .append("b2bStrategy", toIntOrKeepSame(e.getB2bStrategy()))
                .append("Outbound_Stop", toIntOrKeepSame(e.getB2bOutbound_Stop()))
                .append("Marketfare_Upsell", toIntOrKeepSame(e.getMarketfare_Upsell()))
                .append("forecastAllocFlag", toIntOrKeepSame(e.getForecastAllocFlag()))
                .append("afForecastAllocFlag", toIntOrKeepSame(e.getAfForecastAllocFlag()))
                .append("incrementedProfileFareFlag", toIntOrKeepSame(e.getIncrementedProfileFareFlag()))
                .append("ProfileFareUpsell", toIntOrKeepSame(e.getProfileFareUpsell()))
                .append("Spoilage_Flag", toIntOrKeepSame(e.getSpoilage_Flag()))
                .append("CanAdjustedForecast", toIntOrKeepSame(e.getCanAdjustedForecast()))
                .append("market_station_mapping", toIntOrKeepSame(e.getMarket_station_mapping()))
                .append("createdAt", createdAt);
    }

    private Document connectionEntityToDocument(MarketListConnectionsEntity e, Date createdAt) {
        return new Document()
                .append("Sector1", toIntOrKeepSame(e.getSector1()))
                .append("Sector2", toIntOrKeepSame(e.getSector2()))
                .append("Flight1", toIntOrKeepSame(e.getFlight1()))
                .append("Flight2", toIntOrKeepSame(e.getFlight2()))
                .append("Pertype", toIntOrKeepSame(e.getPerType()))
                .append("PerStart", toIntOrKeepSame(e.getStartDate()))
                .append("PerEnd", toIntOrKeepSame(e.getEndDate()))
                .append("DOW", toIntOrKeepSame(e.getDayOfWeek()))
                .append("Price_Strategy", toIntOrKeepSame(e.getPriceStrategy()))
                .append("Discount_Value", toIntOrKeepSame(e.getDiscountValue()))
                .append("FirstRBDAlloc", toIntOrKeepSame(e.getFirstAllocation()))
                .append("OtherRBDAlloc", toIntOrKeepSame(e.getOtherAllocation()))
                .append("B2BBackstop", toIntOrKeepSame(e.getB2bBackstop()))
                .append("B2CBackstop", toIntOrKeepSame(e.getB2cBackstop()))
                .append("B2BFactor", toIntOrKeepSame(e.getB2bFactor()))
                .append("SkippingFactor", toIntOrKeepSame(e.getSkippingFactor()))
                .append("analystName", toIntOrKeepSame(e.getAnalystName()))
                .append("UUID", toIntOrKeepSame(e.getUuid()))
                .append("Outbound_stop", toIntOrKeepSame(e.getOutboundStop()))
                .append("Outbound_duration", toIntOrKeepSame(e.getOutboundDuration()))
                .append("carrierExclusionB2C", toIntOrKeepSame(e.getCarrierExclusionB2B()))
                .append("carrierExclusionB2B", toIntOrKeepSame(e.getCarrierExclusionB2C()))
                .append("Currency", toIntOrKeepSame(e.getCurrency()))
                .append("fareAnchor", toIntOrKeepSame(e.getFareAnchor()))
                .append("DiscountFlag", toIntOrKeepSame(e.getDiscountFlag()))
                .append("Offset", toIntOrKeepSame(e.getOffset()))
                .append("sosFlag", toIntOrKeepSame(e.getSosFlag()))
                .append("createdAt", createdAt);
    }

    public void bulkInsertIntoMarketListConnections(List<MarketListConnectionsEntity> entities, String collectionName) {
        Date now = new Date();
        batchedInsert(entities, collectionName, e -> connectionEntityToDocument(e, now), "MarketListConnections");
    }

    public void deleteValues(Set<String> value, String collectionName, String fieldName) {
        if (!value.isEmpty()) {
            MongoCollection<Document> collection = getDbForCollection(collectionName).getCollection(collectionName.toLowerCase());

            Bson filter;

            // Check if fieldName is a numeric literal (like "0", "1", etc.)
            // This mimics SQL's WHERE 0 IN ('0') behavior
            if (fieldName.matches("^\\d+$")) {
                // Numeric fieldName: check if the literal number is in the value set
                // If true, delete all documents (like SQL WHERE 0 IN ('0'))
                if (value.contains(fieldName)) {
                    filter = new Document(); // Empty filter = match all documents
                } else {
                    return; // No match, delete nothing
                }
            } else {
                // Normal case: fieldName is an actual field
                filter = Filters.in(fieldName, value);
            }

            try {
                DeleteResult result = collection.deleteMany(filter);
                log.info("Deleted " + result.getDeletedCount() + " document(s)");
            } catch (MongoException e) {
                log.error(e.getMessage());
            }
        }
    }

    public void deleteValuesFare(Set<String> value, String collectionName) {
        if (!value.isEmpty()) {
            try {
                MongoCollection<Document> collection = database.getCollection(collectionName.toLowerCase());

                // For concat(sector, route) matching, we need to query and filter
                List<Bson> filters = new ArrayList<>();
                for (String concatValue : value) {
                    // MongoDB doesn't have direct CONCAT in filter, use $expr
                    filters.add(
                            Filters.expr(
                                    new Document("$eq", Arrays.asList(
                                            new Document("$concat", Arrays.asList("$" + SECTOR_COLUMN_NAME, "$" + ROUTE_COLUMN_NAME)),
                                            concatValue
                                    ))
                            )
                    );
                }

                Bson finalFilter = or(filters);
                DeleteResult result = collection.deleteMany(finalFilter);
                log.info("Deleted " + result.getDeletedCount() + " fare records");
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }
    }

    /**
     * Generic batched delete: builds $or filters in batches using the provided
     * filterBuilder function, then issues one deleteMany per batch.
     */
    private <T> void batchedDelete(List<T> entities, String collectionName,
                                   Function<T, Bson> filterBuilder, String label) {
        if (entities == null || entities.isEmpty()) {
            return;
        }

        try {
            MongoCollection<Document> collection = database.getCollection(collectionName.toLowerCase());

            int totalRecords = entities.size();
            int totalBatches = (int) Math.ceil((double) totalRecords / DELETE_BATCH_SIZE);
            int logEveryNBatches = Math.max(1, LOG_EVERY_N_RECORDS / DELETE_BATCH_SIZE);
            long totalDeleted = 0;

            for (int batchIdx = 0; batchIdx < totalBatches; batchIdx++) {
                int fromIndex = batchIdx * DELETE_BATCH_SIZE;
                int toIndex = Math.min(fromIndex + DELETE_BATCH_SIZE, totalRecords);
                List<T> batch = entities.subList(fromIndex, toIndex);

                List<Bson> orConditions = new ArrayList<>(batch.size());
                for (T entity : batch) {
                    orConditions.add(filterBuilder.apply(entity));
                }

                Bson filter = or(orConditions);
                DeleteResult result = collection.deleteMany(filter);
                totalDeleted += result.getDeletedCount();

                if ((batchIdx + 1) % logEveryNBatches == 0 || batchIdx == totalBatches - 1) {
                    log.info("{} delete progress {}/{} batches, {} deleted so far", label, batchIdx + 1, totalBatches, totalDeleted);
                }
            }

            log.info("Total deleted {} {} records from {}", totalDeleted, label, collectionName);

        } catch (Exception e) {
            error_occurred.set(1);
            String errorMsg = "Failed to batch-delete " + label + " records: " + e.getMessage();
            log.error(errorMsg);
            listOfErrors.get().add(errorMsg);
        }
    }

    /**
     * Generic batched insert: converts entities to Documents in batches using the
     * provided documentBuilder function, then issues one insertMany per batch.
     */
    private <T> void batchedInsert(List<T> entities, String collectionName,
                                   Function<T, Document> documentBuilder, String label) {
        if (entities == null || entities.isEmpty()) {
            return;
        }

        try {
            MongoCollection<Document> collection = database.getCollection(collectionName.toLowerCase());

            int totalRecords = entities.size();
            int totalBatches = (int) Math.ceil((double) totalRecords / BULK_INSERT_BATCH_SIZE);
            int logEveryNBatches = Math.max(1, LOG_EVERY_N_RECORDS / BULK_INSERT_BATCH_SIZE);
            long totalInserted = 0;

            for (int batchIdx = 0; batchIdx < totalBatches; batchIdx++) {
                int fromIndex = batchIdx * BULK_INSERT_BATCH_SIZE;
                int toIndex = Math.min(fromIndex + BULK_INSERT_BATCH_SIZE, totalRecords);
                List<T> batch = entities.subList(fromIndex, toIndex);

                List<Document> docs = new ArrayList<>(batch.size());
                for (T entity : batch) {
                    docs.add(documentBuilder.apply(entity));
                }

                collection.insertMany(docs);
                totalInserted += docs.size();

                if ((batchIdx + 1) % logEveryNBatches == 0 || batchIdx == totalBatches - 1) {
                    log.info("{} insert progress {}/{} batches, {} inserted so far", label, batchIdx + 1, totalBatches, totalInserted);
                }
            }

            log.info("Total inserted {} {} records into {}", totalInserted, label, collectionName);
        } catch (Exception e) {
            error_occurred.set(1);
            log.error("Bulk insert of {} into {} failed: {}", label, collectionName, e.getMessage());
            listOfErrors.get().add("Failed to bulk insert " + label + " records: " + e.getMessage());
        }
    }

    private void deleteRecords(List<MarketListEntity> marketListEntityList, String tableName) {
        batchedDelete(marketListEntityList, tableName, entity -> {
            Object flightNumber;
            try {
                flightNumber = Integer.parseInt(entity.getFlightNo());
            } catch (NumberFormatException e) {
                flightNumber = entity.getFlightNo();
            }
            return and(
                    eq("Origin", entity.getOrigin()),
                    eq("Destin", entity.getDestination()),
                    eq("PerStart", entity.getStartDate()),
                    eq("FlightNumber", flightNumber)
            );
        }, "MarketList");
    }

    public List<Map<String, Object>> searchMarketList(MarketListEntity marketListEntity, String collectionName) {
        List<Map<String, Object>> listObj = new ArrayList<>();

        try {
            MongoCollection<Document> collection = database.getCollection(collectionName.toLowerCase());
            // CWE-190: Safe parse with fallback to string for FlightNumber
            Object flightNumber;
            try {
                flightNumber = Integer.parseInt(marketListEntity.getFlightNo());
            } catch (NumberFormatException e) {
                flightNumber = marketListEntity.getFlightNo();
            }
            Bson filter = and(
                    eq("Origin", marketListEntity.getOrigin()),
                    eq("Destin", marketListEntity.getDestination()),
                    eq("PerStart", marketListEntity.getStartDate()),
                    eq("FlightNumber", flightNumber)
            );

            try (MongoCursor<Document> cursor = collection.find(filter).iterator()) {
                while (cursor.hasNext()) {
                    Document doc = cursor.next();
                    Map<String, Object> row = new HashMap<>();

                    // Put all fields from document into map
                    for (String key : doc.keySet()) {
                        row.put(key, doc.get(key));
                    }
                    listObj.add(row);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return listObj;
    }

    public static String getCurrentDateTime() {
        return LocalDateTime.now().format(DATE_TIME_OUTPUT_FORMATTER);
    }

    public List<String> getRoleAccess(Object role) {
        List<String> permissions = new ArrayList<>();
        if (role == null) {
            log.error("Role is null, returning empty permissions");
            return permissions;
        }
        // CWE-476: Safe toString with null check already done above
        String roleStr = role.toString().replaceAll("\\[", "").replaceAll("\\]", "");

        try {
            MongoCollection<Document> collection = database.getCollection("role_permissions");
            Bson filter = eq("role", roleStr);

            try (MongoCursor<Document> cursor = collection.find(filter).iterator()) {
                while (cursor.hasNext()) {
                    Document doc = cursor.next();
                    permissions.add(doc.getString(PERMISSION_COLUMN_NAME));
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return permissions;
    }

    public void createTable(String collectionName, String templateCollectionName) {
        try {
            // Drop existing collection if it exists
            database.getCollection(collectionName.toLowerCase()).drop();

            // MongoDB creates collections automatically on first insert
            // Optional: Create with schema validation like template
            if (collectionExists(templateCollectionName.toLowerCase())) {
                database.createCollection(collectionName.toLowerCase());
            }
        } catch (Exception e) {
            log.error("Failed to create collection");
            e.printStackTrace();
        }
    }

    private boolean collectionExists(String collectionName) {
        for (String name : database.listCollectionNames()) {
            if (name.equals(collectionName)) {
                return true;
            }
        }
        return false;
    }

    public void insertAuditStatement(String collectionName, String startTime, String userName, String uuid) {
        try {
            MongoCollection<Document> collection = database.getCollection("file_upload_audit");

            Document doc = new Document()
                    .append("tableName", collectionName)
                    .append("uuid", uuid)
                    .append("start_time", startTime)
                    .append("userName", userName)
                    .append("createdAt", new Date());

            collection.insertOne(doc);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    public void insertInputStatusTable(String collectionName) {
        int value = 1;
        if (collectionName.contains("QPDWPRIAUT.")) {
            value = 0;
        }

        try {
            MongoCollection<Document> collection = database.getCollection("inputs_status");

            // Check if record exists
            long count = collection.countDocuments(eq("name", collectionName));

            if (count >= 1) {
                // Update existing
                Bson filter = eq("name", collectionName);
                Bson update = set("is_running", value);
                collection.updateOne(filter, update);
            } else {
                // Insert new
                Document doc = new Document()
                        .append("name", collectionName)
                        .append("is_running", value)
                        .append("createdAt", new Date());
                collection.insertOne(doc);
            }
        } catch (Exception e) {
            log.error(e.getMessage());
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
                .b2bOutbound_Stop(record.get(CSV_HEADER_OUTBOUNDSTOP))
                .Marketfare_Upsell(record.get(CSV_HEADER_MKTFARE_UPSELL))
                .forecastAllocFlag(convertYNToBoolean(record.get(CSV_HEADER_FORECAST_ALLOC_FLAG)))
                .afForecastAllocFlag(convertYNToBoolean(record.get(CSV_HEADER_AF_FORECAST_ALLOC_FLAG)))
                .incrementedProfileFareFlag(convertYNToBoolean(record.get(CSV_HEADER_INCREMENTED_PROFILE_FARE_FLAG)))
                .ProfileFareUpsell(record.get(CSV_HEADER_PROFILE_FARE_UPSELL))
                .Spoilage_Flag(convertYNToBoolean(record.get(CSV_HEADER_SPOILAGE_FLAG)))
                .canAdjustedForecast(convertYNToBoolean(record.get(CSV_HEADER_CAN_ADJUSTED_FORECAST)))
                .market_station_mapping(convertYNToBoolean(record.get(CSV_HEADER_MKT_ORIGIN_MAPPING)))
                .analystName(analystName)
                .build());
    }

    private static String convertYNToBoolean(String value) {
        return "Y".equalsIgnoreCase(value) ? "1" : "0";
    }

    public List<Object> searchDistinctResults(String fieldName, String collectionName) {
        List<Object> listObj = new ArrayList<>();

        try {
            MongoCollection<Document> collection = database.getCollection(collectionName.toLowerCase());
            listObj = collection.distinct(fieldName, String.class).into(new ArrayList<>());
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return listObj;
    }

    public List<Map<String, Object>> getJourneyDestination(String origin, String destination, String flag) {
        List<Map<String, Object>> listObj = new ArrayList<>();

        try {
            MongoCollection<Document> collection = getDbForCollection(STATION_MASTER_TABLE_NAME).getCollection(STATION_MASTER_TABLE_NAME);

            // Build filter: station IN (origin, destination) AND route = flag
            Bson filter = and(
                    in("station", Arrays.asList(origin, destination)),
                    eq("route", flag)
            );

            // Execute query
            try (MongoCursor<Document> cursor = collection.find(filter).iterator()) {
                while (cursor.hasNext()) {
                    Document doc = cursor.next();
                    Map<String, Object> row = new HashMap<>();
                    row.put(STATION_COLUMN_NAME, doc.get("station"));
                    listObj.add(row);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }

        return listObj;
    }

    public String getSectors(String sector, String users) {
        String listObj = null;
        try {
            MongoCollection<Document> collection = database.getCollection("config_user_sector_map");
            Document projection = new Document(users, 1).append("_id", 0);
            Document doc = collection.find(eq("sector", sector)).projection(projection).first();

            if (doc != null) {
                listObj = doc.getString(users);
            }
        } catch (Exception e) {
            log.error(DATABASE_ACCESS_ERROR_MESSAGE);
            e.printStackTrace();
            return null;
        }
        return listObj;
    }

    private String validationForMarketListFile(List<String> record, List<String> permissions, String route,
                                               int rowCount, Object role, String userName, List<Object> listOfCurves,
                                               List<Object> listOfStrategies, List<Object> listOfB2BStrategies, List<Object> listOfCriteria) throws Exception {
        try {
            JSONArray roles = new JSONArray();
            // CWE-843: Safe type handling for role object
            if (role instanceof String roleStr) {
                if (roleStr.length() >= 2) {
                    roleStr = roleStr.substring(1, roleStr.length() - 1);
                }
                if (roleStr.length() >= 2) {
                    roleStr = roleStr.substring(1, roleStr.length() - 1);
                }
                roles.add(roleStr);
            } else if (role instanceof ArrayList) {
                ((ArrayList<?>) role).forEach(r -> roles.add(r.toString()));
            } else if (role != null) {
                log.error("The 'role' object is of unexpected type: {}", role.getClass().getName());
            } else {
                log.error("The 'role' object is null.");
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
                listOfErrors.get().add(lineNum + "Origin Station is not equal to 3 characters");
            }
            if (!isLengthEqualTo(record.get(CSV_HEADER_DESTINATION), 3)) {
                listOfErrors.get().add(lineNum + "Destination Station is not equal to 3 characters");
            }
            if (routeJourney.isEmpty()) {
                listOfErrors.get().add(lineNum + "Origin Destination is not " + route + " Route");
            }
            if (!isNumeric(record.get(CSV_HEADER_FLIGHT_NO))) {
                listOfErrors.get().add(lineNum + "Flight Number is not a Number");
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
                    if (user == null || user.trim().isEmpty()) {
                        listOfErrors.get().add(lineNum + "Sector isn't assigned to any analyst.");
                        return "";
                    }
                    if (!userName.split(SPACE)[0].toUpperCase().equals(user)) {
                        listOfErrors.get().add(lineNum + "you don't have access for sector " + sector);
                    }
                } else {
                    if (!daysFromToday.isBefore(formattedEndDate) && !formattedStartDate.isAfter(daysFromToday)) {
                        user = getSectors(sector, SECTOR_MAP_USER1_COLUMN_NAME);
                    } else {
                        user = getSectors(sector, SECTOR_MAP_USER2_COLUMN_NAME);
                    }
                    if (user == null || user.trim().isEmpty()) {
                        listOfErrors.get().add(lineNum + "Sector isn't assigned to any analyst.");
                        return "";
                    }
                }
                if (formattedEndDate.isBefore(formattedStartDate)) {
                    listOfErrors.get().add(lineNum + "End date must be greater than or equal to start date");
                }
                if (permissions.contains(PERMISSION_UPDATE_MARKET_LIST_TILL_30_DAYS)) {
                    if (daysFromToday.isBefore(formattedEndDate)) {
                        listOfErrors.get().add(lineNum + "End date is above 30 days");
                    }
                    if (formattedStartDate.isAfter(daysFromToday)) {
                        listOfErrors.get().add(lineNum + "End date is above 30 days");
                    }
                }
                if (permissions.contains(PERMISSION_UPDATE_MARKET_LIST_AFTER_30_DAYS)) {
                    if (formattedStartDate.isBefore(daysFromToday)) {
                        listOfErrors.get().add(lineNum + "Start date is below 30 days");
                    }
                    if (daysFromToday.isAfter(formattedEndDate)) {
                        listOfErrors.get().add(lineNum + "End date is below 30 days");
                    }
                }
            } catch (Exception e) {
                listOfErrors.get().add(lineNum + "Date is an invalid format");
            }
            if (!(isValidDate(record.get(CSV_HEADER_END_DATE)))) {
                listOfErrors.get().add(lineNum + "End date is not a valid date");
            }
            if (!(isLengthEqualTo(record.get(CSV_HEADER_DAY_OF_WEEK), 7)
                    && isNumeric(record.get(CSV_HEADER_DAY_OF_WEEK))
                    && checkDOWDigits(record.get(CSV_HEADER_DAY_OF_WEEK)))) {
                listOfErrors.get().add(lineNum + "DOW is not 7 digits or has value other than '1' or '9'");
            }
            if (!(isValidTime(record.get(CSV_HEADER_START_TIME)))) {
                listOfErrors.get().add(lineNum + "Start Time is not invalid time format");
            }
            if (!(isValidTime(record.get(CSV_HEADER_END_TIME)))) {
                listOfErrors.get().add(lineNum + "End Time is not 4 digits or has an invalid input");
            }
            if (!listOfCurves.contains(record.get(CSV_HEADER_CURVE_ID))) {
                listOfErrors.get().add(lineNum + "Curve Id is not valid");
            }
            if (!isValidCarrExcl(record.get(CSV_HEADER_CARR_EXCLUSION_B2C))) {
                listOfErrors.get().add(lineNum + "One of the Carrier Exclusion B2C is not 2 digits");
            }
            if (!isValidCarrExcl(record.get(CSV_HEADER_CARR_EXCLUSION_B2B))) {
                listOfErrors.get().add(lineNum + "One of the Carrier Exclusion B2B is not 2 digits");
            }
            if (!isValidFlightNumber(record.get(CSV_HEADER_FLIGHT_EXCLUSION_B2C))) {
                listOfErrors.get().add(lineNum + "Flight exclusion B2C is not in correct format");
            }
            if (!isValidFlightNumber(record.get(CSV_HEADER_FLIGHT_EXCLUSION_B2B))) {
                listOfErrors.get().add(lineNum + "Flight exclusion B2B is not in correct format");
            }
            if (!checkDaySpan(record.get(CSV_HEADER_DAY_SPAN))) {
                listOfErrors.get().add(lineNum + "DaySpan has value other than '1','-1','0'");
            }
            if (!checkAutoTimeFlag(record.get(CSV_HEADER_AUTO_TIME_RANGE_FLAG))) {
                listOfErrors.get().add(lineNum + "AutoTimeRangeFlag has value other than 'Y' or 'N'");
            }
            if (!checkAutoTimeFlag(record.get(CSV_HEADER_DIRECT_FLAG))) {
                listOfErrors.get().add(lineNum + "directFlag has value other than 'Y' or 'N'");
            }
            if (!checkAutoTimeFlag(record.get(CSV_HEADER_TBF_ENH_FLAG))) {
                listOfErrors.get().add(lineNum + "TBFEnhFlag has value other than 'Y' or 'N'");
            }
            if (!checkAutoTimeFlag(record.get(CSV_HEADER_DEFAULT_ML_FLAG))) {
                listOfErrors.get().add(lineNum + "defaultMlFlag has value other than 'Y' or 'N'");
            }
            if (!checkAutoTimeFlag(record.get(CSV_HEADER_FORECAST_ALLOC_FLAG))) {
                listOfErrors.get().add(lineNum + "forecastAllocFlag has value other than 'Y' or 'N'");
            }
            if (!checkAutoTimeFlag(record.get(CSV_HEADER_AF_FORECAST_ALLOC_FLAG))) {
                listOfErrors.get().add(lineNum + "afForecastAllocFlag has value other than 'Y' or 'N'");
            }
            if (!checkAutoTimeFlag(record.get(CSV_HEADER_INCREMENTED_PROFILE_FARE_FLAG))) {
                listOfErrors.get().add(lineNum + "incrementedProfileFareFlag has value other than 'Y' or 'N'");
            }
            String profileFareUpsellStr = record.get(CSV_HEADER_PROFILE_FARE_UPSELL);
            if (profileFareUpsellStr != null && !profileFareUpsellStr.trim().isEmpty()) {
                try {
                    Double.parseDouble(profileFareUpsellStr);
                } catch (NumberFormatException e) {
                    listOfErrors.get().add(lineNum + "ProfileFareUpsell is not a valid number");
                }
            }
            if (!checkAutoTimeFlag(record.get(CSV_HEADER_SPOILAGE_FLAG))) {
                listOfErrors.get().add(lineNum + "SpoilageFlag has value other than 'Y' or 'N'");
            }
            if (!checkAutoTimeFlag(record.get(CSV_HEADER_CAN_ADJUSTED_FORECAST))) {
                listOfErrors.get().add(lineNum + "CanAdjustedForecast has value other than 'Y' or 'N'");
            }
            if (!listOfStrategies.contains(record.get(CSV_HEADER_FARE_ANCHOR))) {
                listOfErrors.get().add(lineNum + "Fare Anchor must be one of the valid strategies");
            }
            if (!(listOfB2BStrategies.contains(record.get(CSV_HEADER_B2B_STRATEGY)) || record.get(CSV_HEADER_B2B_STRATEGY).equals(""))) {
                listOfErrors.get().add(lineNum + "B2B Strategy must be one of the valid strategies");
            }
            String hardAnchor = record.get(CSV_HEADER_HARD_ANCHOR);
            String fareOffset = record.get(CSV_HEADER_FARE_OFFSET);
            String plfThreshold = record.get(CSV_HEADER_PLF_THRESHOLD);

            if (listOfCriteria.contains(hardAnchor)) {
                if (fareOffset == null || fareOffset.trim().isEmpty()) {
                    listOfErrors.get().add(lineNum + "Fare Offset is required when Hard Anchor is applied");
                }
                if (plfThreshold != null && !plfThreshold.trim().isEmpty()) {
                    String trimmedValue = plfThreshold.trim();
                    if (!trimmedValue.endsWith("%")) {
                        listOfErrors.get().add(lineNum + "PLF Threshold must include a percentage symbol (%)");
                    } else {
                        String numericValue = trimmedValue.substring(0, trimmedValue.length() - 1);
                        if (!isNumeric(numericValue)) {
                            listOfErrors.get().add(lineNum + "PLF Threshold must be a valid number");
                        } else {
                            Double plflimit = PLFTHRESHOLD_VALUE;
                            String plf_value = searchResults(PLFTHRESHOLD_VALUE_KEY);
                            if(!plf_value.isEmpty()){
                                plflimit = Double.parseDouble(plf_value);
                            }
                            double value = Double.parseDouble(numericValue);
                            if (value > plflimit) {
                                listOfErrors.get().add(lineNum + "PLF Threshold must be lesser than " + plflimit + " percent");
                            }
                        }
                    }
                }
                if (plfThreshold == null || plfThreshold.trim().isEmpty()) {
                    listOfErrors.get().add(lineNum + "plfThreshold is required when Hard Anchor is applied");
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
                    listOfErrors.get().add(lineNum + "Hard Anchor must be either a number or one of: " + validOptions);
                } else {
                    if (!isNumeric(fareOffset) && !fareOffset.isEmpty()) {
                        listOfErrors.get().add(lineNum + "Fare Offset must be a valid number");
                    }
                }
                if (plfThreshold != null && !plfThreshold.trim().isEmpty()) {
                    String trimmedValue = plfThreshold.trim();
                    if (!trimmedValue.endsWith("%")) {
                        listOfErrors.get().add(lineNum + "PLF Threshold must include a percentage symbol (%)");
                    } else {
                        String numericValue = trimmedValue.substring(0, trimmedValue.length() - 1);
                        if (!isNumeric(numericValue)) {
                            listOfErrors.get().add(lineNum + "PLF Threshold must be a valid number");
                        } else {
                            Double plflimit = PLFTHRESHOLD_VALUE;
                            String plf_value = searchResults(PLFTHRESHOLD_VALUE_KEY);
                            if(!plf_value.isEmpty()){
                                plflimit = Double.parseDouble(plf_value);
                            }
                            double value = Double.parseDouble(numericValue);
                            if (value > plflimit) {
                                listOfErrors.get().add(lineNum + "PLF Threshold must be lesser than " + plflimit + " percent");
                            }
                        }
                    }
                }
                if (fareOffset != null && !fareOffset.trim().isEmpty() && !isNumeric(fareOffset)) {
                    listOfErrors.get().add(lineNum + "If present, Fare Offset must be a valid number");
                }
                if (plfThreshold == null || plfThreshold.trim().isEmpty()) {
                    listOfErrors.get().add(lineNum + "plfThreshold is required when Hard Anchor is applied");
                }
            }
            else {
                if (fareOffset != null && !fareOffset.trim().isEmpty()) {
                    listOfErrors.get().add(lineNum + "Fare Offset must be empty");
                }
                if (plfThreshold != null && !plfThreshold.trim().isEmpty()) {
                    listOfErrors.get().add(lineNum + "PLF Threshold must be empty");
                }
            }

            if (!isNumeric(record.get(CSV_HEADER_FIRST_ALLOCATION))) {
                listOfErrors.get().add(lineNum + "First Allocation is not a number");
            }
            if (!(isNumeric(record.get(CSV_HEADER_OTHER_ALLOCATION))
                    && isPositive(record.get(CSV_HEADER_OTHER_ALLOCATION)))) {
                listOfErrors.get().add(lineNum + "Other Allocation is not a positive number");
            }
            if (!(isNumeric(record.get(CSV_HEADER_B2B_BACKSTOP))
                    && isPositive(record.get(CSV_HEADER_B2B_BACKSTOP)))) {
                listOfErrors.get().add(lineNum + "B2B Backstop is not a positive number");
            }
            if (!(isNumeric(record.get(CSV_HEADER_B2C_BACKSTOP))
                    && isPositive(record.get(CSV_HEADER_B2C_BACKSTOP)))) {
                listOfErrors.get().add(lineNum + "B2C Backstop is not a positive number");
            }
            if (!(isNumeric(record.get(CSV_HEADER_OUTBOUNDSTOP))
                    && isPositive(record.get(CSV_HEADER_OUTBOUNDSTOP)))) {
                listOfErrors.get().add(lineNum + "outboundstop is not a positive number");
            }
            if (!(isNumeric(record.get(CSV_HEADER_MKTFARE_UPSELL))))
            {
                listOfErrors.get().add(lineNum + "mktuosell is not a  number");
            }

            if (!(isDouble(record.get(CSV_HEADER_B2B_FACTOR)))) {
                listOfErrors.get().add(lineNum + "B2B Factor is not a valid float");
            }
            if (!(isNumeric(record.get(CSV_HEADER_SKIPPING_FACTOR)))) {
                listOfErrors.get().add(lineNum + "Skipping Factor is not a number");
            }
            if (!checkAutoTimeFlag(record.get(CSV_HEADER_OPENING_FARES))) {
                listOfErrors.get().add(lineNum + "Opening Fares has value other than 'Y' or 'N'");
            }
            if (!(isNumeric(record.get(CSV_HEADER_B2B_TOLERANCE)))
                    && !record.get(CSV_HEADER_B2B_TOLERANCE).trim().isEmpty()) {
                listOfErrors.get().add(lineNum + "B2B Tolerance is not a number or empty");
            }
            if (!(isNumeric(record.get(CSV_HEADER_B2C_TOLERANCE)))
                    && !record.get(CSV_HEADER_B2C_TOLERANCE).trim().isEmpty()) {
                listOfErrors.get().add(lineNum + "B2C Tolerance is not a number or empty");
            }

            boolean hasSeats = !record.get(CSV_HEADER_OB_SEATS).trim().isEmpty();
            boolean hasFare = !record.get(CSV_HEADER_OB_FARE).trim().isEmpty();

            if (hasSeats != hasFare) {
                listOfErrors.get().add(lineNum + "Overbooking seats and fare must be provided together");
            }else{
                if (!(isFloat(record.get(CSV_HEADER_OB_SEATS)))
                        && !record.get(CSV_HEADER_OB_SEATS).trim().isEmpty()) {
                    listOfErrors.get().add(lineNum + "Overbooking seats is not a number or empty");
                } else if (!record.get(CSV_HEADER_OB_SEATS).trim().isEmpty()) {
                    float seats = Float.parseFloat(record.get(CSV_HEADER_OB_SEATS));
                    if (seats > 10 || seats < 0) {
                        listOfErrors.get().add(lineNum + "Overbooking seats cannot exceed 10 and should be greater than 0");
                    }
                }

                if (!(isFloat(record.get(CSV_HEADER_OB_FARE)))
                        && !record.get(CSV_HEADER_OB_FARE).trim().isEmpty()) {
                    listOfErrors.get().add(lineNum + "Overbooking fare is not a number or empty");
                } else if (!record.get(CSV_HEADER_OB_FARE).trim().isEmpty()) {
                    float fare = Float.parseFloat(record.get(CSV_HEADER_OB_FARE));
                    if (fare <= 1000) {
                        listOfErrors.get().add(lineNum + "Overbooking fare cannot be below 1000");
                    }
                }
            }
            if (!checkOwnFareFlag(record.get(CSV_HEADER_OVER_BOOKING))) {
                listOfErrors.get().add(lineNum + "Opening Fares has value other than 'Y' or 'N'");
            }
            if (!checkOwnFareFlag(record.get(CSV_HEADER_SERIES_BLOCK_FLAG))) {
                listOfErrors.get().add(lineNum + "Series Block has value other than 'Y' or 'N'");
            }
            if (!checkOwnFareFlag(record.get(CSV_HEADER_AUTO_GROUP))) {
                listOfErrors.get().add(lineNum + "auto group has value other than 'Y' or 'N'");
            }
            if (!checkOwnFareFlag(record.get(CSV_HEADER_AUTO_BACKSTOP_FLAG))) {
                listOfErrors.get().add(lineNum + "auto backstop flag has value other than 'Y' or 'N'");
            }
            if (!checkAutoTimeFlag(record.get(CSV_HEADER_RBD_PUSH_FLAG))) {
                listOfErrors.get().add(lineNum + "RBD Push has value other than 'Y' or 'N'");
            }
            if (!checkAutoTimeFlag(record.get(CSV_HEADER_DISTRESS_INVENTORY_FLAG))) {
                listOfErrors.get().add(lineNum + "DistressInventoryFlag has value other than 'Y' or 'N'");
            }
            if (!checkOwnFareFlag(record.get(CSV_HEADER_PROFILE_FARES))) {
                listOfErrors.get().add(lineNum + "Profile Fares has value other than 'Y' or 'N'");
            }
            if (!checkOwnFareFlag(record.get(CSV_HEADER_TBF_FLAG))) {
                listOfErrors.get().add(lineNum + "TBF Flag has value other than 'Y' or 'N'");
            }
            if (!checkOwnFareFlag(record.get(CSV_HEADER_CUG_FLAG))) {
                listOfErrors.get().add(lineNum + "CUG Flag has value other than 'Y' or 'N'");
            }
            if (!checkOwnFareFlag(record.get(CSV_HEADER_MKT_FARE_FLAG))) {
                listOfErrors.get().add(lineNum + "Mktfare Flag has value other than 'Y' or 'N'");
            }
            if (!checkOwnFareFlag(record.get(CSV_HEADER_MKT_ORIGIN_MAPPING))) {
                listOfErrors.get().add(lineNum + "Market Origin Mapping Flag has value other than 'Y' or 'N'");
            }

            return user;
        } catch (Exception e) {
            listOfErrors.get().add("Unknown Error Encountered in Validation");
            throw new Exception(e);
        }
    }

    private String validationForMarketListConnectionsFile(List<String> record, int rowCountInProcessedCsv, List<String> permissions,
                                                          Object role, String userName, List<String> errorsFound) {
        JSONArray roles = new JSONArray();
        // CWE-843: Safe type handling for role object
        if (role instanceof String roleStr) {
            if (roleStr.length() >= 2) {
                roleStr = roleStr.substring(1, roleStr.length() - 1);
            }
            if (roleStr.length() >= 2) {
                roleStr = roleStr.substring(1, roleStr.length() - 1);
            }
            roles.add(roleStr);
        } else if (role instanceof ArrayList) {
            ((ArrayList<?>) role).forEach(r -> roles.add(r.toString()));
        } else if (role != null) {
            log.error("The 'role' object is of unexpected type: {}", role.getClass().getName());
        } else {
            log.error("The 'role' object is null.");
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
            errorsFound.add(lineNum + "Sector isn't assigned to any analyst.");
            return "";
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
                                             String obj_id) throws DataMismatchedException {
        List<MarketListSalesInvEntity> entityList = new ArrayList<>();
        try {
            createTable(tableName, MARKET_LIST_SALESINV_TABLE_NAME);

            String uuid = obj_id;
            insertAuditStatement(tableName, getCurrentDateTime(), userName, uuid);
            insertInputStatusTable(tableName);

            List<Object> listOfCurves = searchDistinctResults(CURVES_COLUMN_NAME, CURVES_TABLE_NAME);

            List<List<String>> csvData = getDataFromCSVFile(file);
            csvData = splitRowsByDateRange(csvData);
            numOfRows.set(csvData.size() > 1 ? csvData.size() - 1 : 0);

            for (int i = 1; i < csvData.size(); i++) {
                validationForMarketListSalesInvFile(csvData.get(i), i + 1, listOfCurves);
                createMarketListSalesInvEntity(csvData.get(i), entityList);
            }

            if (!listOfErrors.get().isEmpty()) {
                updateInputStatus(tableName);
                error_occurred.set(1);
                addErrorsToTable(listOfErrors.get(), obj_id);
                throw new DataMismatchedException(listOfErrors.get());
            }

            deleteValues(Collections.singleton(ZERO), tableName, ZERO);

            for (MarketListSalesInvEntity entity : entityList) {
                insertIntoMarketListSalesInv(entity, tableName);
            }

            s3FileStorageDao.storeFile(createCSV(tableName), tableName, uuid);
            updateAuditStatement(tableName, getCurrentDateTime(), uuid);
        } catch (Exception e) {
            error_occurred.set(1);
            if (listOfErrors.get().isEmpty()) {
                listOfErrors.get().add("An unexpected error occurred");
            }
            addErrorsToTable(listOfErrors.get(), obj_id);
            throw new RuntimeException(e);
        } finally {
            updateInputStatus(tableName);
        }
    }

    private void validationForMarketListSalesInvFile(List<String> record, int rowCount, List<Object> listOfCurves) {
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

        if (!isLengthEqualTo(origin, 3)) listOfErrors.get().add(lineNum + "Origin must be 3 characters.");
        if (!isLengthEqualTo(destin, 3)) listOfErrors.get().add(lineNum + "Destin must be 3 characters.");
        if (!isNumeric(flightNo)) listOfErrors.get().add(lineNum + "FlightNumber must be numeric.");

        if (!"1".equals(saleFlag) && !"0".equals(saleFlag)) {
            listOfErrors.get().add(lineNum + "saleflag must be '1' or '0'.");
        }

        if (!(isLengthEqualTo(dow, 7) && isNumeric(dow) && checkDOWDigits(dow))) {
            listOfErrors.get().add(lineNum + "DOW is not 7 digits or has a value other than '1' or '9'.");
        }

        if (!listOfCurves.contains(curveId)) {
            listOfErrors.get().add(lineNum + "CurveID is not valid.");
        }

        try {
            LocalDate formattedStartDate = LocalDate.parse(startDate, DATE_OUTPUT_FORMATTER);
            LocalDate formattedEndDate = LocalDate.parse(endDate, DATE_OUTPUT_FORMATTER);
            if (formattedEndDate.isBefore(formattedStartDate)) {
                listOfErrors.get().add(lineNum + "EndDate must be on or after StartDate.");
            }
        } catch (Exception e) {
            listOfErrors.get().add(lineNum + "Date is in an invalid format.");
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

    public void insertIntoMarketListSalesInv(MarketListSalesInvEntity entity, String collectionName) {
        try {
            MongoCollection<Document> collection = database.getCollection(collectionName.toLowerCase());

            // CWE-190: Safe parseLong with fallback to string
            Object flightNumber;
            try {
                flightNumber = Long.parseLong(entity.getFlightNumber());
            } catch (NumberFormatException e) {
                flightNumber = entity.getFlightNumber();
            }
            Object dow;
            try {
                dow = Long.parseLong(entity.getDow());
            } catch (NumberFormatException e) {
                dow = entity.getDow();
            }

            Document doc = new Document()
                    .append("Origin", entity.getOrigin())
                    .append("Destin", entity.getDestin())
                    .append("FlightNumber", flightNumber)
                    .append("PerStart", entity.getPerStart())
                    .append("PerEnd", entity.getPerEnd())
                    .append("DOW", dow)
                    .append("CurveID", entity.getCurveId())
                    .append("saleflag", entity.getSaleFlag())
                    .append("createdAt", new Date());

            collection.insertOne(doc);
        } catch (Exception e) {
            error_occurred.set(1);
            String errorMsg = "Failed to insert salesinv record for sector " +
                    entity.getOrigin() + entity.getDestin();
            listOfErrors.get().add(errorMsg);
            log.error(DATABASE_ACCESS_ERROR_MESSAGE);
            e.printStackTrace();
        }
    }

    public void uploadMarketFareAdjustments(Reader file, String tableName, String userName, Object role,
                                             String obj_id) throws DataMismatchedException {
        List<MarketListSalesInvEntity> entityList = new ArrayList<>();
        try {
//            createTable(tableName, MARKET_LIST_SALESINV_TABLE_NAME);

            String uuid = obj_id;
            insertAuditStatement(tableName, getCurrentDateTime(), userName, uuid);
            insertInputStatusTable(tableName);

            List<List<String>> csvData = getDataFromCSVFile(file);
            numOfRows.set(csvData.size() > 1 ? csvData.size() - 1 : 0);

            List<Document> deleteQueries = new ArrayList<>();
            List<Document> documentsToInsert = new ArrayList<>();

            for (int i = 1; i < csvData.size(); i++) {
                List<String> row = csvData.get(i);

                try {
                    if (row.size() < 3) {
                        listOfErrors.get().add("Line " + (i) + ": Insufficient columns (expected at least 3, found " + row.size() + ")");
                        continue;
                    }
                    String sector = row.get(0).trim();
                    String airlineCode = row.get(1).trim();
                    String adjustmentStr = row.get(2).trim();

                    // Validate empty fields
                    if (sector.isEmpty()) {
                        listOfErrors.get().add("Line " + (i) + ": sector cannot be empty");
                        continue;
                    }

                    if (airlineCode.isEmpty()) {
                        listOfErrors.get().add("Line " + (i) + ": airline_code cannot be empty");
                        continue;
                    }

                    if (adjustmentStr.isEmpty()) {
                        listOfErrors.get().add("Line " + (i) + ": adjustment cannot be empty");
                        continue;
                    }

                    // Validate adjustment value
                    if (!isValidAdjustment(adjustmentStr)) {
                        listOfErrors.get().add("Line " + (i + 1) + ": Invalid adjustment value '" + adjustmentStr + "'. Must be a positive or negative number");
                        continue;
                    }

                    double adjustment = Double.parseDouble(adjustmentStr);

                    // Create delete query document
                    Document deleteQuery = new Document()
                            .append("sector", sector)
                            .append("airlineCode", airlineCode);

                    deleteQueries.add(deleteQuery);

                    // Create MongoDB document for insertion
                    Document doc = new Document()
                            .append("sector", sector)
                            .append("airlineCode", airlineCode)
                            .append("adjustment", adjustment)
                            .append("createdAt", new Date());

                    documentsToInsert.add(doc);

                } catch (IndexOutOfBoundsException e) {
                    listOfErrors.get().add("Line " + (i) + ": Insufficient columns in CSV");
                } catch (NumberFormatException e) {
                    listOfErrors.get().add("Line " + (i) + ": Invalid number format for adjustment value");
                } catch (Exception e) {
                    listOfErrors.get().add("Line " + (i) + ": Error processing row - " + e.getMessage());
                }
            }

            if (!listOfErrors.get().isEmpty()) {
                updateInputStatus(tableName);
                error_occurred.set(1);
                addErrorsToTable(listOfErrors.get(), obj_id);
                throw new DataMismatchedException(listOfErrors.get());
            }

            deleteExistingRecords(deleteQueries, tableName);

            insertDocuments(documentsToInsert, tableName);

            s3FileStorageDao.storeFile(createCSV(tableName), tableName, uuid);
            updateAuditStatement(tableName, getCurrentDateTime(), uuid);
        } catch (Exception e) {
            error_occurred.set(1);
            if (listOfErrors.get().isEmpty()) {
                listOfErrors.get().add("An unexpected error occurred");
            }
            addErrorsToTable(listOfErrors.get(), obj_id);
            throw new RuntimeException(e);
        } finally {
            updateInputStatus(tableName);
        }
    }

    public void uploadForecastDateConfig(Reader file, String tableName, String userName, Object role,
                                             String obj_id) throws DataMismatchedException {
        List<MarketListSalesInvEntity> entityList = new ArrayList<>();
        try {
//            createTable(tableName, MARKET_LIST_SALESINV_TABLE_NAME);

            String uuid = obj_id;
            insertAuditStatement(tableName, getCurrentDateTime(), userName, uuid);
            insertInputStatusTable(tableName);

            List<List<String>> csvData = getDataFromCSVFile(file);
            numOfRows.set(csvData.size() > 1 ? csvData.size() - 1 : 0);

            List<WriteModel<Document>> bulkUpdates = new ArrayList<>();

            for (int i = 1; i < csvData.size(); i++) {
                List<String> row = csvData.get(i);

                try {
                    if (row.size() < 11) {
                        listOfErrors.get().add("Line " + (i) + ": Insufficient columns (expected at least 11, found " + row.size() + ")");
                        continue;
                    }
                    String sector = row.get(0).trim();
                    String departureDate = row.get(1).trim();
                    String lastMStartDate = row.get(3).trim();
                    String lastMEndDate = row.get(4).trim();
                    String isLastMRolling = row.get(5).trim();
                    String lastYStartDate = row.get(6).trim();
                    String lastYEndDate = row.get(7).trim();
                    String isLastYRolling = row.get(8).trim();
                    String isManual = row.get(9).trim();
                    String resetDate = row.get(10).trim();

                    // Validate empty fields
                    if (sector.isEmpty() || !isLengthEqualTo(sector, 6)) {
                        listOfErrors.get().add("Line " + (i) + ": invalid sector");
                        continue;
                    }

                    if (departureDate.isEmpty() || !isValidIsoDate(departureDate)) {
                        listOfErrors.get().add("Line " + (i) + ": invalid Departure Date, required format YYYY-MM-DD");
                        continue;
                    }

                    if (lastMStartDate.isEmpty() || !isValidIsoDate(lastMStartDate)) {
                        listOfErrors.get().add("Line " + (i) + ": invalid Last Month Start Date, required format YYYY-MM-DD");
                        continue;
                    }

                    if (lastMEndDate.isEmpty() || !isValidIsoDate(lastMEndDate)) {
                        listOfErrors.get().add("Line " + (i) + ": invalid Last Month End Date, required format YYYY-MM-DD");
                        continue;
                    }

                    if (lastYStartDate.isEmpty() || !isValidIsoDate(lastYStartDate)) {
                        listOfErrors.get().add("Line " + (i) + ": invalid Last Year Start Date, required format YYYY-MM-DD");
                        continue;
                    }

                    if (lastYEndDate.isEmpty() || !isValidIsoDate(lastYEndDate)) {
                        listOfErrors.get().add("Line " + (i) + ": invalid Last Year End Date, required format YYYY-MM-DD");
                        continue;
                    }

                    if (!resetDate.isEmpty() && !isValidIsoDate(resetDate)) {
                        listOfErrors.get().add("Line " + (i) + ": invalid Reset Date, either empty or in format YYYY-MM-DD");
                        continue;
                    }

                    if (!(isLastMRolling.equals("N") || isLastMRolling.equals("Y"))) {
                        listOfErrors.get().add("Line " + (i) + ": invalid Last Month Rolling Flag, either 'N' or 'Y'");
                        continue;
                    }
                    if (!(isLastYRolling.equals("N") || isLastYRolling.equals("Y"))) {
                        listOfErrors.get().add("Line " + (i) + ": invalid Last Year Rolling Flag, either 'N' or 'Y'");
                        continue;
                    }
                    if (!(isManual.equals("N") || isManual.equals("Y"))) {
                        listOfErrors.get().add("Line " + (i) + ": invalid Manual Flag, either 'N' or 'Y'");
                        continue;
                    }

                    // Composite key filter
                    Document filter = new Document()
                            .append("Sector", sector)
                            .append("DepartureDate", departureDate);

                    // Build update payload (NO _id, NO CreatedAt)
                    Document updateFields = new Document()
                            .append("LastMStartDate", lastMStartDate)
                            .append("LastMEndDate", lastMEndDate)
                            .append("LastMRolling", isLastMRolling)
                            .append("LastYStartDate", lastYStartDate)
                            .append("LastYEndDate", lastYEndDate)
                            .append("LastYRolling", isLastYRolling)
                            .append("ManualFlag", isManual)
                            .append("ResetDate", resetDate.isEmpty() ? null : resetDate)
                            .append("UpdatedAt", new Date());

                    // Build update document
                    Document update = new Document("$set", updateFields);

                    // Optional: upsert support
                    UpdateOptions options = new UpdateOptions().upsert(true);

                    // Add bulk update
                    bulkUpdates.add(new UpdateOneModel<>(filter, update, options));

                } catch (IndexOutOfBoundsException e) {
                    listOfErrors.get().add("Line " + (i) + ": Insufficient columns in CSV");
                } catch (NumberFormatException e) {
                    listOfErrors.get().add("Line " + (i) + ": Invalid number format for adjustment value");
                } catch (Exception e) {
                    listOfErrors.get().add("Line " + (i) + ": Error processing row - " + e.getMessage());
                }
            }

            if (!listOfErrors.get().isEmpty()) {
                updateInputStatus(tableName);
                error_occurred.set(1);
                addErrorsToTable(listOfErrors.get(), obj_id);
                throw new DataMismatchedException(listOfErrors.get());
            }

            updateRecords(bulkUpdates, tableName);

            s3FileStorageDao.storeFile(createCSV(tableName), tableName, uuid);
            updateAuditStatement(tableName, getCurrentDateTime(), uuid);
        } catch (Exception e) {
            error_occurred.set(1);
            if (listOfErrors.get().isEmpty()) {
                listOfErrors.get().add("An unexpected error occurred");
            }
            addErrorsToTable(listOfErrors.get(), obj_id);
            throw new RuntimeException(e);
        } finally {
            updateInputStatus(tableName);
        }
    }

    public void uploadActiveSectors(Reader file, String tableName, String userName, Object role,
                                         String obj_id) throws DataMismatchedException {
        try {

            insertAuditStatement(tableName, getCurrentDateTime(), userName, obj_id);
            insertInputStatusTable(tableName);

            List<List<String>> csvData = getDataFromCSVFile(file);
            numOfRows.set(csvData.size() > 1 ? csvData.size() - 1 : 0);

            List<WriteModel<Document>> bulkUpdates = new ArrayList<>();

            for (int i = 1; i < csvData.size(); i++) {
                List<String> row = csvData.get(i);

                try {
                    // CWE-125: Bounds check before accessing row elements
                    if (row.size() < 2) {
                        listOfErrors.get().add("Line " + (i) + ": Insufficient columns (expected at least 2, found " + row.size() + ")");
                        continue;
                    }
                    String sector = row.get(0).trim();
                    String forecastModel = row.get(1).trim();
                    String farecap = row.size() > 2 ? row.get(2).trim() : "NA";

                    if (sector.isEmpty() || !isLengthEqualTo(sector, 6)) {
                        listOfErrors.get().add("Line " + (i) + ": invalid sector");
                        continue;
                    }

                    Set<String> allowedModels = Set.of("LB", "ML");
                    if (!(allowedModels.contains(forecastModel))) {
                        String allowedValues = "'" + String.join("', '", allowedModels) + "'";
                        listOfErrors.get().add("Line " + (i) + ": invalid Forecast Model, allowed values: " + allowedValues);
                        continue;
                    }

                    Document filter = new Document()
                            .append("sector", sector);

                    Document updateFields = new Document()
                            .append("forecast_model", forecastModel)
                            .append("updated_at", new Date());

                    updateFields.append("farecap", farecap);

                    Document update = new Document("$set", updateFields);

                    UpdateOptions options = new UpdateOptions().upsert(true);

                    bulkUpdates.add(new UpdateOneModel<>(filter, update, options));
                } catch (IndexOutOfBoundsException e) {
                    listOfErrors.get().add("Line " + (i) + ": Insufficient columns in CSV");
                } catch (Exception e) {
                    listOfErrors.get().add("Line " + (i) + ": Error processing row - " + e.getMessage());
                }
            }

            if (!listOfErrors.get().isEmpty()) {
                updateInputStatus(tableName);
                error_occurred.set(1);
                addErrorsToTable(listOfErrors.get(), obj_id);
                throw new DataMismatchedException(listOfErrors.get());
            }

            updateRecords(bulkUpdates, tableName);

            s3FileStorageDao.storeFile(createCSV(tableName), tableName, obj_id);
            updateAuditStatement(tableName, getCurrentDateTime(), obj_id);
        } catch (Exception e) {
            error_occurred.set(1);
            if (listOfErrors.get().isEmpty()) {
                listOfErrors.get().add("An unexpected error occurred");
            }
            addErrorsToTable(listOfErrors.get(), obj_id);
            throw new RuntimeException(e);
        } finally {
            updateInputStatus(tableName);
        }
    }

    public void uploadFares(Reader file, String tableName, String userName, Object role,
                                  String objId) throws DataMismatchedException {
        try {
            insertAuditStatement(tableName, getCurrentDateTime(), userName, objId);
            insertInputStatusTable(tableName);

            List<List<String>> csvData = getDataFromCSVFile(file);
            numOfRows.set(csvData.size() > 1 ? csvData.size() - 1 : 0);

            List<String> headers = Arrays.asList(getColumnHeaders(tableName).split(COMMA));
            List<Map<String, String>> data = createListOfMap(csvData, headers);

            Validator validate = getValidator(tableName);
            String val = getAllRBDs();
            if (validate != null && !validate.isValid(data, val, listOfErrors.get())) {
                updateInputStatus(tableName);
                error_occurred.set(1);
                listOfErrors.get().add(INVALID_DATA_ERROR_MESSAGE);
                addErrorsToTable(listOfErrors.get(), objId);
                throw new DataMismatchedException(listOfErrors.get());
            }

            Map<String, Set<String>> rbdGroups = getRBDGroups();
            Map<String, Map<String, Set<String>>> fileSectorsRoutesGroups = new HashMap<>();
            Map<String, Map<String, Set<String>>> uploadedSectorRouteRbds = new HashMap<>();

            for (int i = 0; i < csvData.size(); i++) {
                List<String> row = csvData.get(i);
                try {
                    if (row.size() < 7) continue;
                    String sector = row.get(0).trim();
                    String route = row.get(3).trim();
                    String rbd = row.get(4).trim();

                    uploadedSectorRouteRbds
                        .computeIfAbsent(sector, k -> new HashMap<>())
                        .computeIfAbsent(route, k -> new HashSet<>())
                        .add(rbd);

                    String groupName = null;
                    for (Map.Entry<String, Set<String>> entry : rbdGroups.entrySet()) {
                        if (entry.getValue().contains(rbd)) {
                            groupName = entry.getKey();
                            break;
                        }
                    }

                    if (groupName != null) {
                        fileSectorsRoutesGroups.computeIfAbsent(sector, k -> new HashMap<>())
                            .computeIfAbsent(route, k -> new HashSet<>())
                            .add(groupName);
                    }
                } catch (Exception e) {
                    // Ignore parsing errors for group identification
                }
            }

            // Validate that the uploaded RBDs form complete groups
            boolean hasGroupValidationErrors = false;
            for (Map.Entry<String, Map<String, Set<String>>> sectorEntry : fileSectorsRoutesGroups.entrySet()) {
                String sector = sectorEntry.getKey();
                for (Map.Entry<String, Set<String>> routeEntry : sectorEntry.getValue().entrySet()) {
                    String route = routeEntry.getKey();
                    Set<String> expectedRbds = new HashSet<>();
                    for (String groupName : routeEntry.getValue()) {
                        expectedRbds.addAll(rbdGroups.get(groupName));
                    }
                    Set<String> actualRbds = uploadedSectorRouteRbds.get(sector).get(route);
                    
                    if (!actualRbds.equals(expectedRbds)) {
                        Set<String> missing = new HashSet<>(expectedRbds);
                        missing.removeAll(actualRbds);
                        Set<String> extra = new HashSet<>(actualRbds);
                        extra.removeAll(expectedRbds);
                        
                        String errorMsg = "Sector/Route '" + sector + route + "' validation failed.";
                        if (!missing.isEmpty()) {
                            errorMsg += " Missing RBDs for the uploaded groups: " + missing + ".";
                        }
                        if (!extra.isEmpty()) {
                            errorMsg += " Unexpected RBDs found: " + extra + ".";
                        }
                        listOfErrors.get().add(errorMsg);
                        hasGroupValidationErrors = true;
                    }
                }
            }

            if (hasGroupValidationErrors) {
                updateInputStatus(tableName);
                error_occurred.set(1);
                addErrorsToTable(listOfErrors.get(), objId);
                throw new DataMismatchedException(listOfErrors.get());
            }

            List<Bson> deleteFilters = new ArrayList<>();
            for (Map.Entry<String, Map<String, Set<String>>> sectorEntry : fileSectorsRoutesGroups.entrySet()) {
                String sector = sectorEntry.getKey();
                for (Map.Entry<String, Set<String>> routeEntry : sectorEntry.getValue().entrySet()) {
                    String route = routeEntry.getKey();
                    for (String groupName : routeEntry.getValue()) {
                        Set<String> groupRbds = rbdGroups.get(groupName);
                        if (groupRbds != null && !groupRbds.isEmpty()) {
                            deleteFilters.add(
                                Filters.and(
                                    Filters.eq("SECTOR", sector),
                                    Filters.eq("route", route),
                                    Filters.in("RBD", groupRbds)
                                )
                            );
                        }
                    }
                }
            }

            if (!deleteFilters.isEmpty()) {
                try {
                    MongoCollection<Document> collection = database.getCollection(tableName.toLowerCase());
                    Bson finalFilter = Filters.or(deleteFilters);
                    com.mongodb.client.result.DeleteResult result = collection.deleteMany(finalFilter);
                    log.info("Deleted " + result.getDeletedCount() + " existing fare records for the uploaded groups");
                } catch (Exception e) {
                    log.error("Error deleting existing fares: " + e.getMessage());
                }
            }

            List<WriteModel<Document>> bulkUpdates = new ArrayList<>();

            for (int i = 0; i < csvData.size(); i++) {
                List<String> row = csvData.get(i);

                try {
                    // CWE-125: Bounds check before accessing row elements
                    if (row.size() < 7) {
                        listOfErrors.get().add("Line " + i + ": Insufficient columns (expected at least 7, found " + row.size() + ")");
                        continue;
                    }
                    String sector   = row.get(0).trim();
                    String org      = row.get(1).trim();
                    String dst      = row.get(2).trim();
                    String route    = row.get(3).trim();
                    String rbd      = row.get(4).trim();
                    String total = row.get(5).trim();
                    String currency = row.get(6).trim();

                    Document filter = new Document().append("SECTOR", sector).append("route", route).append("RBD", rbd);

                    Document updateFields = new Document()
                            .append("Org", org)
                            .append("Dst", dst)
                            .append("TOTAL", autoConvert(total))
                            .append("currency", currency);

                    bulkUpdates.add(new UpdateOneModel<>(filter, new Document("$set", updateFields), new UpdateOptions().upsert(true)));

                } catch (IndexOutOfBoundsException e) {
                    listOfErrors.get().add("Line " + i + ": Insufficient columns in CSV");
                } catch (Exception e) {
                    listOfErrors.get().add("Line " + i + ": Error processing row - " + e.getMessage());
                }
            }

            if (!listOfErrors.get().isEmpty()) {
                updateInputStatus(tableName);
                error_occurred.set(1);
                addErrorsToTable(listOfErrors.get(), objId);
                throw new DataMismatchedException(listOfErrors.get());
            }

            updateRecords(bulkUpdates, "fares");

            s3FileStorageDao.storeFile(createCSV(tableName), tableName, objId);
            updateAuditStatement(tableName, getCurrentDateTime(), objId);

        } catch (DataMismatchedException e) {
            throw e;
        } catch (Exception e) {
            error_occurred.set(1);
            if (listOfErrors.get().isEmpty()) listOfErrors.get().add("An unexpected error occurred");
            addErrorsToTable(listOfErrors.get(), objId);
            throw new RuntimeException(e);
        } finally {
            updateInputStatus(tableName);
        }
    }

    private void deleteExistingRecords(List<Document> deleteQueries, String collectionName) {
        if (deleteQueries.isEmpty()) {
            System.out.println("No records to delete");
            return;
        }

        try {
            MongoCollection<Document> collection = database.getCollection(collectionName.toLowerCase());

            for (Document query : deleteQueries) {
                collection.deleteMany(query);
            }

            System.out.println("Deleted existing records for " + deleteQueries.size() + " sector-airline combinations");

        } catch (Exception e) {
            System.err.println("Error deleting existing records: " + e.getMessage());
            throw new RuntimeException("Failed to delete existing records", e);
        }
    }

    private boolean isValidAdjustment(String value) {
        if (value == null || value.trim().isEmpty()) {
            return false;
        }

        try {
            double num = Double.parseDouble(value);
            // Check if it's a valid number (not NaN or Infinity)
            return !Double.isNaN(num) && !Double.isInfinite(num);
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private void insertDocuments(List<Document> documents, String collectionName) {
        if (documents.isEmpty()) {
            System.out.println("No documents to insert");
            return;
        }

        try {
            MongoCollection<Document> collection = database.getCollection(collectionName.toLowerCase());
            collection.insertMany(documents);

            System.out.println("Successfully inserted " + documents.size() + " documents into " + collectionName);

        } catch (Exception e) {
            System.err.println("Error inserting documents: " + e.getMessage());
            throw new RuntimeException("Failed to insert documents into " + collectionName, e);
        }
    }

    private void updateRecords(List<WriteModel<Document>> updates, String collectionName) {
        if (updates == null || updates.isEmpty()) {
            return;
        }
        MongoCollection<Document> collection = getDbForCollection(collectionName).getCollection(collectionName);
        collection.bulkWrite(updates);
    }

    /**
     * Handles SaleInv_RBL CSV file upload.
     * CSV has a multi-row header: row 0 = group headers, row 1 = column headers (Month, Sector, Time Cluster, ..., Mon-Sun per group).
     * Only processes "Baseload" and "Recommended baseload (w 90% flown condition)" groups.
     * Updates BaseLoad and RBL_Capped fields in SaleInv_RBL collection.
     * Sets Baseload_Manual=1 or RBL_Manual=1 if the respective value changed.
     */
    
    public void uploadSaleInvRblFile(Reader file, String tableName, String userName, String obj_id) throws Exception {
        try {
            insertAuditStatement(tableName, getCurrentDateTime(), userName, obj_id);
            insertInputStatusTable(tableName);

            List<List<String>> csvData = getDataFromCSVFile(file);
            if (csvData.size() < 3) {
                throw new DataMismatchedException(List.of("SaleInv RBL CSV needs at least 2 header rows and 1 data row."));
            }

            List<String> groupHeaderRow = csvData.get(0);

            // Time band -> cluster map
            Map<String, Integer> timeBandToCluster = new HashMap<>();
            for (Document doc : database.getCollection("config_clusters").find()) {
                String tb = doc.getString("startTime") + " - " + doc.getString("endTime");
                Object cl = doc.get("cluster");
                if (cl != null) timeBandToCluster.put(tb.trim(), cl instanceof Number ? ((Number) cl).intValue() : Integer.parseInt(cl.toString().trim()));
            }

            // Find group column starts
            int baseloadStart = -1, rblCappedStart = -1;
            for (int i = 0; i < groupHeaderRow.size(); i++) {
                String h = groupHeaderRow.get(i).replace("\uFEFF", "").trim();
                if (h.equalsIgnoreCase("Baseload")) baseloadStart = i;
                else if (h.equalsIgnoreCase("Recommended baseload capped")) rblCappedStart = i;
            }
            if (baseloadStart == -1 || rblCappedStart == -1) {
                throw new DataMismatchedException(List.of("Missing required group headers: Baseload or Recommended baseload capped"));
            }

            // Collect unique months and pre-fetch existing records
            Set<String> uniqueMonths = new LinkedHashSet<>();
            for (int i = 2; i < csvData.size(); i++) {
                if (!csvData.get(i).isEmpty()) {
                    String m = csvData.get(i).get(0).replace("\uFEFF", "").trim();
                    if (!m.isEmpty()) uniqueMonths.add(m);
                }
            }

            Map<String, Document> existingMap = new HashMap<>();
            if (!uniqueMonths.isEmpty()) {
                for (Document doc : database.getCollection(SALEINV_RBL_TABLE_NAME).find(Filters.in("Month", uniqueMonths))) {
                    String key = doc.getString("Month") + "|" + doc.getString("Sector") + "|" + doc.get("TimeCluster") + "|" + doc.get("DayOfWeek");
                    existingMap.put(key, doc);
                }
                log.info("Pre-fetched {} existing records for months: {}", existingMap.size(), uniqueMonths);
            }

            // Process rows
            List<WriteModel<Document>> bulkOps = new ArrayList<>();
            for (int i = 2; i < csvData.size(); i++) {
                List<String> row = csvData.get(i);
                if (row.size() < 3) continue;

                String month = row.get(0).replace("\uFEFF", "").trim();
                String sector = row.get(1).trim();
                String timeBandStr = row.get(2).trim();
                if (month.isEmpty() || sector.isEmpty() || timeBandStr.isEmpty()) continue;

                Integer clusterNum = timeBandToCluster.get(timeBandStr);
                if (clusterNum == null) {
                    log.warn("Line {}: Unknown time band '{}'", i + 1, timeBandStr);
                    continue;
                }

                for (int d = 0; d < 7; d++) {
                    int dowIso = d + 1;
                    String blStr = (baseloadStart + d < row.size()) ? row.get(baseloadStart + d).trim() : "";
                    String rcStr = (rblCappedStart + d < row.size()) ? row.get(rblCappedStart + d).trim() : "";
                    if (blStr.isEmpty() && rcStr.isEmpty()) continue;

                    String lookupKey = month + "|" + sector + "|" + clusterNum + "|" + dowIso;
                    Document existing = existingMap.get(lookupKey);

                    Document setFields = new Document();
                    double newBL = blStr.isEmpty() ? Double.NaN : Double.parseDouble(blStr);
                    double newRC = rcStr.isEmpty() ? Double.NaN : Double.parseDouble(rcStr);

                    if (!Double.isNaN(newBL)) {
                        Object oldVal = existing != null ? existing.get("BaseLoad") : null;
                        boolean changed = oldVal == null || !oldVal.equals(newBL);
                        if (changed) {
                            setFields.append("BaseLoad", newBL);
                            setFields.append("Baseload_Manual", 1);
                        }
                    }

                    if (!Double.isNaN(newRC)) {
                        Object oldVal = existing != null ? existing.get("RBL_Capped") : null;
                        boolean changed = oldVal == null || !oldVal.equals(newRC);
                        if (changed) {
                            setFields.append("RBL_Capped", newRC);
                            setFields.append("RBL_Manual", 1);
                        }
                    }

                    if (!setFields.isEmpty()) {
                        Bson filter = Filters.and(
                                Filters.eq("Month", month), Filters.eq("Sector", sector),
                                Filters.eq("TimeCluster", clusterNum), Filters.eq("DayOfWeek", dowIso)
                        );
                        bulkOps.add(new UpdateOneModel<>(filter, new Document("$set", setFields)));
                    }
                }
            }

            if (!bulkOps.isEmpty()) {
                updateRecords(bulkOps, SALEINV_RBL_TABLE_NAME);
                log.info("SaleInv RBL: {} updates executed", bulkOps.size());
            } else {
                log.info("SaleInv RBL: no changes detected");
            }
            numOfRows.set(bulkOps.size());

            s3FileStorageDao.storeFile(createCSV(tableName), tableName, obj_id);
            updateAuditStatement(tableName, getCurrentDateTime(), obj_id);

        } catch (DataMismatchedException dme) {
            error_occurred.set(1);
            if (dme.message != null) dme.message.forEach(m -> listOfErrors.get().add(m.toString()));
            addErrorsToTable(listOfErrors.get(), obj_id);
            throw dme;
        } catch (Exception e) {
            error_occurred.set(1);
            log.error("Error processing SaleInv RBL:{}", e.getMessage());
            listOfErrors.get().add("Failed to process SaleInv RBL file");
            addErrorsToTable(listOfErrors.get(), obj_id);
            throw e;
        } finally {
            updateInputStatus(tableName);
        }
    }

}