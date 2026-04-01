package com.akasaair.queues.common.helpers;

import com.akasaair.queues.common.entity.MarketListAuditEntity;
import com.akasaair.queues.common.entity.MarketListConnAuditEntity;
import com.akasaair.queues.common.entity.MarketListConnectionsEntity;
import com.akasaair.queues.common.entity.MarketListEntity;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;
import org.springframework.beans.BeanUtils;
import software.amazon.awssdk.regions.Region;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.persistence.Column;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.akasaair.queues.common.constants.Constants.*;

@Slf4j
@Component
public class AuditHelper {

    @Value("${rm.file-path}")
    private String S3_FILE_PATH_NAME;

    @Value("${rm.s3-bucket}")
    private String S3_BUCKET_NAME;

    @Value("${RM_WEBSERVICE_AWS_ACCESS_KEY_NEW_PROD}")
    private String accessKey;

    @Value("${RM_WEBSERVICE_AWS_SECRET_KEY_NEW_PROD}")
    private String secretKey;

    @Autowired
    @Qualifier("applicationTaskExecutor")
    private TaskExecutor taskExecutor;

    private final MongoDatabase database;

    private SparkSession spark;

    public AuditHelper(MongoConnection mongoConnection) {
        this.database = mongoConnection.getDatabase();
        this.spark = null;
    }

    /**
     * Number of records to process in each pipeline batch.
     * Each batch: fetch existing from Mongo → compute delta → write to S3 → release memory.
     * At ~2 KB per entity with 50 columns, 50k records ≈ 100 MB per batch (new + existing + audit).
     */
    private static final int AUDIT_PIPELINE_BATCH_SIZE = 50_000;

    public void buildAuditLogs(List<MarketListEntity> newRecords, String userName, LocalDateTime currentDateTime, String collectionName) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {

        // Process asynchronously in pipelined batches to bound memory usage
        CompletableFuture.runAsync(() -> {
            try {
                int totalRecords = newRecords.size();
                int totalBatches = (int) Math.ceil((double) totalRecords / AUDIT_PIPELINE_BATCH_SIZE);
                log.info("Audit pipeline: {} records in {} batch(es) of {}", totalRecords, totalBatches, AUDIT_PIPELINE_BATCH_SIZE);

                for (int batchIdx = 0; batchIdx < totalBatches; batchIdx++) {
                    log.info("Audit batch {}/{}: started", batchIdx + 1, totalBatches);
                    int fromIndex = batchIdx * AUDIT_PIPELINE_BATCH_SIZE;
                    int toIndex = Math.min(fromIndex + AUDIT_PIPELINE_BATCH_SIZE, totalRecords);
                    List<MarketListEntity> batch = newRecords.subList(fromIndex, toIndex);

                    // 1. Build keys for this batch only
                    Set<String> batchKeys = batch.stream()
                            .map(record -> createCompositeKey(record.getOrigin() + record.getDestination(), record.getFlightNo(), record.getStartDate()))
                            .collect(Collectors.toSet());

                    // 2. Fetch existing records for this batch only
                    Map<String, MarketListEntity> existingMap = fetchExistingRecords(batchKeys, MarketListEntity.class, collectionName);

                    // 3. Compute delta for this batch
                    List<MarketListAuditEntity> changeMap = buildDeltaRecords(batch, existingMap, userName, currentDateTime);

                    // 4. Write this batch's delta to S3
                    if (!changeMap.isEmpty()) {
                        appendAuditsToS3(changeMap, currentDateTime, MarketListAuditEntity.class);
                    }

                    // 5. Release batch references so GC can reclaim memory
                    existingMap.clear();
                    changeMap.clear();
                    batchKeys.clear();

                    log.info("Audit batch {}/{}: completed", batchIdx + 1, totalBatches);
                }

            } catch (Throwable e) {
                log.error("Error processing audit delta: {}", e.getMessage(), e);
            }
        }, taskExecutor);

    }


    private static final Map<String, MethodHandle> ML_ENTITY_GETTER_CACHE = new ConcurrentHashMap<>();
    private static final Map<String, MethodHandle> ML_ENTITY_SETTER_CACHE = new ConcurrentHashMap<>();
    private static final Map<String, MethodHandle> ML_AUDIT_GETTER_CACHE = new ConcurrentHashMap<>();
    private static final Map<String, MethodHandle> ML_AUDIT_SETTER_CACHE = new ConcurrentHashMap<>();
    private static final Set<String> ML_AUDIT_ONLY_FIELDS =
            Set.of("userName", "sector", "date", "changedAt", "flightNo", "newRecord");

    // MethodHandle caches for MarketListConnections
    private static final Map<String, MethodHandle> CONN_ENTITY_GETTER_CACHE = new ConcurrentHashMap<>();
    private static final Map<String, MethodHandle> CONN_ENTITY_SETTER_CACHE = new ConcurrentHashMap<>();
    private static final Map<String, MethodHandle> CONN_AUDIT_GETTER_CACHE = new ConcurrentHashMap<>();
    private static final Map<String, MethodHandle> CONN_AUDIT_SETTER_CACHE = new ConcurrentHashMap<>();
    private static final Set<String> CONN_AUDIT_ONLY_FIELDS =
            Set.of("userName", "sector", "date", "changedAt", "flightNo", "newRecord");

    // Initialize MethodHandles once at class loading
    static {
        try {
            initializeMarketListMethodHandles();
            initializeConnectionMethodHandles();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize MethodHandles", e);
        }
    }


    private static void initializeMarketListMethodHandles() throws Exception {
        MethodHandles.Lookup lookup = MethodHandles.lookup();

        // MarketListEntity getters
        for (Field field : FieldUtils.getAllFields(MarketListEntity.class)) {
            field.setAccessible(true);
            ML_ENTITY_GETTER_CACHE.put(field.getName(), lookup.unreflectGetter(field));
            ML_ENTITY_SETTER_CACHE.put(field.getName(), lookup.unreflectSetter(field));
        }

        // MarketListAuditEntity getters and setters
        for (Field field : FieldUtils.getAllFields(MarketListAuditEntity.class)) {
            field.setAccessible(true);
            String fieldName = field.getName();
            ML_AUDIT_GETTER_CACHE.put(fieldName, lookup.unreflectGetter(field));
            ML_AUDIT_SETTER_CACHE.put(fieldName, lookup.unreflectSetter(field));
        }
    }

    private static void initializeConnectionMethodHandles() throws Exception {
        MethodHandles.Lookup lookup = MethodHandles.lookup();

        // MarketListConnectionsEntity getters
        for (Field field : FieldUtils.getAllFields(MarketListConnectionsEntity.class)) {
            field.setAccessible(true);
            CONN_ENTITY_GETTER_CACHE.put(field.getName(), lookup.unreflectGetter(field));
            CONN_ENTITY_SETTER_CACHE.put(field.getName(), lookup.unreflectSetter(field));
        }

        // MarketListConnAuditEntity getters and setters
        for (Field field : FieldUtils.getAllFields(MarketListConnAuditEntity.class)) {
            field.setAccessible(true);
            String fieldName = field.getName();
            CONN_AUDIT_GETTER_CACHE.put(fieldName, lookup.unreflectGetter(field));
            CONN_AUDIT_SETTER_CACHE.put(fieldName, lookup.unreflectSetter(field));
        }
    }

    private List<MarketListAuditEntity> buildDeltaRecords(
            List<MarketListEntity> newRecords,
            Map<String, MarketListEntity> existingMap,
            String userName,
            LocalDateTime currentDateTime) throws Throwable {

        List<MarketListAuditEntity> changeMap = new ArrayList<>(newRecords.size() / 10);
        String currentDateTimeStr = currentDateTime.toString();

        // Get field names once
        List<String> fieldNames = new ArrayList<>(ML_AUDIT_GETTER_CACHE.keySet());

        for (MarketListEntity newRow : newRecords) {
            String key = createCompositeKey(
                    newRow.getOrigin() + newRow.getDestination(),
                    newRow.getFlightNo(),
                    newRow.getStartDate()
            );
            MarketListEntity oldRow = existingMap.get(key);

            // Prepare audit entity (same for both new and existing records)
            MarketListAuditEntity newAudit = new MarketListAuditEntity();
            prepareNewRecordAudit(newRow, newAudit, userName, currentDateTimeStr);

            // If it's a new record, add full audit and continue
            if (oldRow == null) {
                newAudit.setNewRecord(true);
                changeMap.add(newAudit);
                continue;
            }

            // For existing records, check for changes
            boolean hasChanges = false;

            // Process fields with MethodHandles
            for (String fieldName : fieldNames) {
                boolean isNotAuditField = !ML_AUDIT_ONLY_FIELDS.contains(fieldName);

                MethodHandle auditGetter = ML_AUDIT_GETTER_CACHE.get(fieldName);
                MethodHandle auditSetter = ML_AUDIT_SETTER_CACHE.get(fieldName);

                // Get old value if exists
                Object oldVal = null;
                if (isNotAuditField && oldRow != null) {
                    MethodHandle entityGetter = ML_ENTITY_GETTER_CACHE.get(fieldName);
                    if (entityGetter != null) {
                        oldVal = entityGetter.invoke(oldRow);
                    }
                }

                // Get new value
                Object newVal = auditGetter.invoke(newAudit);

                // Compare and update
                if (newVal == null || areValuesEqual(oldVal, newVal)) {
                    auditSetter.invoke(newAudit, (Object) null);
                } else {
                    if (isNotAuditField) {
                        hasChanges = true;
                    }
                    if (newVal instanceof String && ((String) newVal).trim().isEmpty()) {
                        auditSetter.invoke(newAudit, AUDIT_EMPTY_IDENTIFIER);
                    }
                }
            }

            // Add to changeMap only if there are changes
            if (hasChanges) {
                changeMap.add(newAudit);
            }
        }

        return changeMap;
    }

    public void buildConnAuditLogs(List<MarketListConnectionsEntity> newRecords, String userName, LocalDateTime currentDateTime, String collectionName) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {

        // Process asynchronously in pipelined batches to bound memory usage
        CompletableFuture.runAsync(() -> {
            try {
                int totalRecords = newRecords.size();
                int totalBatches = (int) Math.ceil((double) totalRecords / AUDIT_PIPELINE_BATCH_SIZE);
                log.info("Connection audit pipeline: {} records in {} batch(es) of {}", totalRecords, totalBatches, AUDIT_PIPELINE_BATCH_SIZE);

                for (int batchIdx = 0; batchIdx < totalBatches; batchIdx++) {
                    log.info("Connection audit batch {}/{}: started", batchIdx + 1, totalBatches);
                    int fromIndex = batchIdx * AUDIT_PIPELINE_BATCH_SIZE;
                    int toIndex = Math.min(fromIndex + AUDIT_PIPELINE_BATCH_SIZE, totalRecords);
                    List<MarketListConnectionsEntity> batch = newRecords.subList(fromIndex, toIndex);

                    // 1. Build keys for this batch only
                    Set<String> batchKeys = batch.stream()
                            .map(record -> createCompositeKey(record.getSector1() + "-" + record.getSector2(),
                                    record.getFlight1() + "-" + record.getFlight2(), record.getStartDate()))
                            .collect(Collectors.toSet());

                    // 2. Fetch existing records for this batch only
                    Map<String, MarketListConnectionsEntity> existingMap = fetchExistingRecords(batchKeys, MarketListConnectionsEntity.class, collectionName);

                    // 3. Compute delta for this batch
                    List<MarketListConnAuditEntity> changeMap = buildConnDeltaRecords(batch, existingMap, userName, currentDateTime);

                    // 4. Write this batch's delta to S3
                    if (!changeMap.isEmpty()) {
                        appendAuditsToS3(changeMap, currentDateTime, MarketListConnAuditEntity.class);
                    }

                    // 5. Release batch references so GC can reclaim memory
                    existingMap.clear();
                    changeMap.clear();
                    batchKeys.clear();

                    log.info("Connection audit batch {}/{}: completed", batchIdx + 1, totalBatches);
                }

            } catch (Throwable e) {
                log.error("Error processing connection audit delta: {}", e.getMessage(), e);
            }
        }, taskExecutor);

    }

    private List<MarketListConnAuditEntity> buildConnDeltaRecords(
            List<MarketListConnectionsEntity> newRecords,
            Map<String, MarketListConnectionsEntity> existingMap,
            String userName,
            LocalDateTime currentDateTime) throws Throwable {

        List<MarketListConnAuditEntity> changeMap = new ArrayList<>(newRecords.size() / 10);
        String currentDateTimeStr = currentDateTime.toString();

        // Get field names once
        List<String> fieldNames = new ArrayList<>(CONN_AUDIT_GETTER_CACHE.keySet());

        for (MarketListConnectionsEntity newRow : newRecords) {
            String key = createCompositeKey(
                    newRow.getSector1() + "-" + newRow.getSector2(),
                    newRow.getFlight1() + "-" + newRow.getFlight2(),
                    newRow.getStartDate()
            );
            MarketListConnectionsEntity oldRow = existingMap.get(key);

            // Prepare audit entity (same for both new and existing records)
            MarketListConnAuditEntity newAudit = new MarketListConnAuditEntity();
            prepareNewConnRecordAudit(newRow, newAudit, userName, currentDateTimeStr);

            // If it's a new record, add full audit and continue
            if (oldRow == null) {
                newAudit.setNewRecord(true);
                changeMap.add(newAudit);
                continue;
            }

            // For existing records, check for changes
            boolean hasChanges = false;

            // Process fields with MethodHandles
            for (String fieldName : fieldNames) {
                boolean isNotAuditField = !CONN_AUDIT_ONLY_FIELDS.contains(fieldName);

                MethodHandle auditGetter = CONN_AUDIT_GETTER_CACHE.get(fieldName);
                MethodHandle auditSetter = CONN_AUDIT_SETTER_CACHE.get(fieldName);

                // Get old value if exists
                Object oldVal = null;
                if (isNotAuditField && oldRow != null) {
                    MethodHandle entityGetter = CONN_ENTITY_GETTER_CACHE.get(fieldName);
                    if (entityGetter != null) {
                        oldVal = entityGetter.invoke(oldRow);
                    }
                }

                // Get new value
                Object newVal = auditGetter.invoke(newAudit);

                // Compare and update
                if (newVal == null || areValuesEqual(oldVal, newVal)) {
                    auditSetter.invoke(newAudit, (Object) null);
                } else {
                    if (isNotAuditField) {
                        hasChanges = true;
                    }
                    if (newVal instanceof String && ((String) newVal).trim().isEmpty()) {
                        auditSetter.invoke(newAudit, AUDIT_EMPTY_IDENTIFIER);
                    }
                }
            }

            // Add to changeMap only if there are changes
            if (hasChanges) {
                changeMap.add(newAudit);
            }
        }

        return changeMap;
    }


    public <T> Map<String, T> fetchExistingRecords(
            Set<String> keys,
            Class<T> entityClass,
            String collectionName)
            throws InvocationTargetException, NoSuchMethodException,
            InstantiationException, IllegalAccessException {

        Map<String, T> resultMap = new HashMap<>(keys.size());

        if (keys == null || keys.isEmpty()) {
            return resultMap;
        }

        // Group keys by Sector (Origin + Destin)
        Map<String, List<String[]>> keysBySector = new HashMap<>();

        for (String key : keys) {
            String[] keyParts = parseCompositeKey(key);
            String sector = keyParts[0]; // already Origin+Destin or Sector1-Sector2

            keysBySector
                    .computeIfAbsent(sector, k -> new ArrayList<>())
                    .add(keyParts);
        }

        MongoCollection<Document> collection =
                database.getCollection(collectionName);

        for (Map.Entry<String, List<String[]>> entry : keysBySector.entrySet()) {

            String sector = entry.getKey();
            List<String[]> groupedKeys = entry.getValue();

            List<Bson> orFilters =
                    new ArrayList<>(groupedKeys.size());

            for (String[] keyParts : groupedKeys) {

                if (entityClass == MarketListEntity.class) {
                    orFilters.add(buildSectorGroupedMarketFilter(keyParts));
                } else if (entityClass == MarketListConnectionsEntity.class) {
                    orFilters.add(buildSectorGroupedConnectionFilter(keyParts));
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported entity type");
                }
            }

            // Sector condition pushed OUTSIDE the OR
            Bson sectorFilter = buildSectorOnlyFilter(entityClass, sector);

            Bson finalFilter = Filters.and(
                    sectorFilter,
                    Filters.or(orFilters)
            );

            collection.find(finalFilter).forEach(doc -> {
                try {
                    T entity = mapDocumentToEntity(doc, entityClass);
                    String compositeKey =
                            extractCompositeKey(entity, entityClass);
                    resultMap.put(compositeKey, entity);
                } catch (Throwable e) {
                    throw new RuntimeException("Error mapping document to entity", e);
                }
            });
        }

        return resultMap;
    }

    private Bson buildSectorOnlyFilter(Class<?> entityClass, String sector) {

        if (entityClass == MarketListEntity.class) {

            String origin = sector.substring(0, 3);
            String destin = sector.substring(3, 6);

            return Filters.and(
                    Filters.eq("Origin", origin),
                    Filters.eq("Destin", destin)
            );

        } else if (entityClass == MarketListConnectionsEntity.class) {

            String[] parts = sector.split("-");

            return Filters.and(
                    Filters.eq("Sector1", parts[0]),
                    Filters.eq("Sector2", parts[1])
            );
        }

        throw new IllegalArgumentException("Unsupported entity type");
    }

    private Bson buildSectorGroupedMarketFilter(String[] keyParts) {

        return Filters.and(
                Filters.eq("FlightNumber",
                        Integer.parseInt(keyParts[1])),
                Filters.eq("PerStart", keyParts[2])
        );
    }

    private Bson buildSectorGroupedConnectionFilter(String[] keyParts) {

        String[] flights = keyParts[1].split("-");

        return Filters.and(
                Filters.eq("Flight1",
                        Integer.parseInt(flights[0])),
                Filters.eq("Flight2",
                        Integer.parseInt(flights[1])),
                Filters.eq("PerStart", keyParts[2])
        );
    }

//    private Bson buildKeyFilter(Class<?> entityClass, String[] keyParts) {
//        if (entityClass == MarketListEntity.class) {
//
//            String origin = keyParts[0].substring(0, 3);
//            String destin = keyParts[0].substring(3, 6);
//
//            return Filters.and(
//                    Filters.eq("Origin", origin),
//                    Filters.eq("Destin", destin),
//                    Filters.eq("FlightNumber", Integer.parseInt(keyParts[1])),
//                    Filters.eq("PerStart", keyParts[2])
//            );
//
//        } else if (entityClass == MarketListConnectionsEntity.class) {
//
//            String[] sector = keyParts[0].split("-");
//            String[] flights = keyParts[1].split("-");
//
//            return Filters.and(
//                    Filters.eq("Sector1", sector[0]),
//                    Filters.eq("Sector2", sector[1]),
//                    Filters.eq("Flight1", Integer.parseInt(flights[0])),
//                    Filters.eq("Flight2", Integer.parseInt(flights[1])),
//                    Filters.eq("PerStart", keyParts[2])
//            );
//
//        } else {
//            throw new IllegalArgumentException("Unsupported entity type");
//        }
//    }


    private <T> T mapDocumentToEntity(Document doc, Class<T> entityClass) throws Throwable {
        // Create new instance
        T entity = entityClass.getDeclaredConstructor().newInstance();

        // Determine which cache to use based on entity class
        Map<String, MethodHandle> setterCache = getSetterCache(entityClass);

        // Get all fields with @Column annotation
        for (Field field : FieldUtils.getAllFields(entityClass)) {
            Column column = field.getAnnotation(Column.class);
            if (column == null) {
                continue; // Skip fields without @Column annotation
            }

            String columnName = column.name();
            Object value = doc.get(columnName);

            // Get the setter MethodHandle from cache
            MethodHandle setter = setterCache.get(field.getName());
            if (setter == null) {
                continue; // Skip if no setter in cache
            }

            try {
                if (value != null) {
                    Object convertedValue = value.toString();
                    setter.invoke(entity, convertedValue);
                } else {
                    setter.invoke(entity, (Object) null);
                }
            } catch (Throwable e) {
                throw new RuntimeException("Failed to set field: " + field.getName(), e);
            }
        }

        return entity;
    }

    /**
     * Get the appropriate setter cache based on entity class
     */
    private Map<String, MethodHandle> getSetterCache(Class<?> entityClass) {
        String className = entityClass.getSimpleName();

        if (className.equals("MarketListEntity")) {
            return ML_ENTITY_SETTER_CACHE; // Note: For entities, we might need setters too
        } else if (className.equals("MarketListAuditEntity")) {
            return ML_AUDIT_SETTER_CACHE;
        } else if (className.equals("MarketListConnectionsEntity")) {
            return CONN_ENTITY_SETTER_CACHE;
        } else if (className.equals("MarketListConnAuditEntity")) {
            return CONN_AUDIT_SETTER_CACHE;
        }

        throw new IllegalArgumentException("Unsupported entity class: " + className);
    }

    private <T> String extractCompositeKey(T entity, Class<T> entityClass) {
        if (entityClass == MarketListEntity.class) {
            MarketListEntity e = (MarketListEntity) entity;
            return createCompositeKey(
                    e.getOrigin() + e.getDestination(),
                    e.getFlightNo(),
                    e.getStartDate()
            );
        } else if (entityClass == MarketListConnectionsEntity.class) {
            MarketListConnectionsEntity e = (MarketListConnectionsEntity) entity;
            return createCompositeKey(
                    e.getSector1() + "-" + e.getSector2(),
                    e.getFlight1() + "-" + e.getFlight2(),
                    e.getStartDate()
            );
        } else {
            throw new IllegalArgumentException("Unsupported entity type: " + entityClass.getName());
        }
    }

    // Helper methods for composite key handling
    private String createCompositeKey(String sector, String flightNo, String startDate) {
        return String.join(AUDIT_KEY_DELIMITER, sector != null ? sector : "",
                flightNo != null ? flightNo : "", startDate != null ? startDate : "");
    }

    private String[] parseCompositeKey(String compositeKey) {
        return compositeKey.split("\\" + AUDIT_KEY_DELIMITER, -1); // -1 to preserve empty strings
    }

    private boolean areValuesEqual(Object val1, Object val2) {
        if (Objects.equals(val1, val2)) return true;
        if (val1 == null || val2 == null) return false;

        try {
            return Double.parseDouble(val1.toString()) == Double.parseDouble(val2.toString());
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * Prepares a MarketListAuditEntity for new record auditing by copying all fields
     * and setting audit-specific fields while clearing ignored fields.
     */
    private void prepareNewRecordAudit(MarketListEntity newRow, MarketListAuditEntity newAudit, 
                                       String userName, String currentDateTimeStr) {
        BeanUtils.copyProperties(newRow, newAudit);
        
        // Re-set audit fields
        newAudit.setDate(newRow.getStartDate().replace('/', '-'));
        newAudit.setSector(newRow.getOrigin() + newRow.getDestination());
        newAudit.setUserName(userName);
        newAudit.setChangedAt(currentDateTimeStr);
        
        // Clear only the ignored fields
        newAudit.setUuid(null);
        newAudit.setOrigin(null);
        newAudit.setDestination(null);
        newAudit.setPerType(null);
        newAudit.setStartDate(null);
        newAudit.setEndDate(null);
        newAudit.setDayOfWeek(null);
    }

    /**
     * Prepares a MarketListConnAuditEntity for new record auditing by copying all fields
     * and setting audit-specific fields while clearing ignored fields.
     */
    private void prepareNewConnRecordAudit(MarketListConnectionsEntity newRow, MarketListConnAuditEntity newAudit,
                                           String userName, String currentDateTimeStr) {
        BeanUtils.copyProperties(newRow, newAudit);
        
        // Re-set audit fields
        newAudit.setDate(newRow.getStartDate().replace('/', '-'));
        newAudit.setSector(newRow.getSector1() + "-" + newRow.getSector2());
        newAudit.setFlightNo(newRow.getFlight1() + "-" + newRow.getFlight2());
        newAudit.setUserName(userName);
        newAudit.setChangedAt(currentDateTimeStr);
        
        // Clear only the ignored fields
        newAudit.setUuid(null);
        newAudit.setSector1(null);
        newAudit.setSector2(null);
        newAudit.setFlight1(null);
        newAudit.setFlight2(null);
        newAudit.setPerType(null);
        newAudit.setStartDate(null);
        newAudit.setEndDate(null);
        newAudit.setDayOfWeek(null);
    }

    @PostConstruct
    public void initSpark() {
//        For local testing:
//        System.setProperty("os.name", "Linux");
//        System.setProperty("hadoop.home.dir", "/tmp/hadoop");
//        System.setProperty("HADOOP_USER_NAME", "aarav");
//
//        log.info("Forced OS Name: {}", System.getProperty("os.name"));
//        log.info("Java Home: {}", System.getProperty("java.home"));
//        log.info("Java Version: {}", System.getProperty("java.version"));

        spark = SparkSession.builder()
                .appName("AuditWriter")
                .master("local[2]")

                // Delta
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

                // Partitions
                .config("spark.sql.shuffle.partitions", "4")

                // S3
                .config("spark.hadoop.fs.s3a.access.key", accessKey)
                .config("spark.hadoop.fs.s3a.secret.key", secretKey)
                .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")

                // Local dirs
                .config("spark.local.dir", "/tmp/spark-temp")
                .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
                .config("spark.hadoop.hadoop.tmp.dir", "/tmp/hadoop-temp")

//                // Janino codegen
//                .config("spark.sql.codegen.wholeStage", "false")
//                .config("spark.sql.codegen.factoryMode", "NO_CODEGEN")
//
//                // Disable native IO
//                .config("spark.hadoop.io.nativeio.enabled", "false")
//                .config("spark.driver.extraJavaOptions", "-Dio.nativeio.enabled=false")
//                .config("spark.executor.extraJavaOptions", "-Dio.nativeio.enabled=false")
//
//                // Linux filesystem
//                .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")

                // Force simple auth
                .config("spark.hadoop.hadoop.security.authentication", "simple")
                .config("spark.hadoop.hadoop.security.authorization", "false")

                .config("spark.ui.enabled", "false")
                .getOrCreate();

//        spark.sparkContext().setLogLevel("ERROR");
//        System.setProperty("HADOOP_USER_NAME", "aarav");

        log.info("Spark session initialized");
    }

    @PreDestroy
    public void stopSpark() {
        if (spark != null) {
            spark.stop();
            log.info("Spark session stopped");
        }
    }

    public <T> void appendAuditsToS3(List<T> changeMap, LocalDateTime currentDateTime, Class<T> entityClass) {
        if (changeMap == null || changeMap.isEmpty()) {
            log.info("No audits to upload");
            return;
        }

        String BASE_PATH = "s3a://"+ S3_BUCKET_NAME + "/" + S3_FILE_PATH_NAME + "ml-audits/";
        String S3_PATH = BASE_PATH + "direct/";
        String className = entityClass.getSimpleName();
        if (className.equals("MarketListConnAuditEntity")) {
            S3_PATH = BASE_PATH +  "connections/";
        }

        try {
            Encoder<T> encoder = Encoders.bean(entityClass);
            Dataset<T> dataset = null;

            try {
                dataset = spark.createDataset(changeMap, encoder);
                log.info("Dataset created for write with {} rows", dataset.count());

                Dataset<Row> df = dataset.toDF();
                df = df.withColumn("changedAtDate", functions.lit(currentDateTime.toLocalDate().toString()))
                        .coalesce(1);

                df.write()
                        .format("delta")
                        .mode("append")
                        .option("mergeSchema", "true")
                        .partitionBy("changedAtDate")
                        .save(S3_PATH);
            } catch (NoClassDefFoundError e) {
                log.error("NoClassDefFoundError: {}", e.getMessage());
                log.error("Missing class: {}", e.getClass().getName());
                if (e.getCause() != null) {
                    log.error("Caused by: {}", e.getCause().getMessage());
                }

                e.printStackTrace();
                throw e;
            } catch (Exception e) {
                log.error("Unexpected error: {}", e.getMessage(), e);
                throw e;
            }

            log.info("Write to Delta successful at {}", S3_PATH);

        } catch (Exception e) {
            throw new RuntimeException("Failed writing audits to S3 Delta → " + e.getMessage(), e);
        }
    }
}
