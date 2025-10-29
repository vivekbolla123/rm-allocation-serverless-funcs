//package com.akasaair.queues.common.helpers;
//
//import com.akasaair.queues.common.entity.MarketListAuditEntity;
//import com.akasaair.queues.common.entity.MarketListConnAuditEntity;
//import com.akasaair.queues.common.entity.MarketListConnectionsEntity;
//import com.akasaair.queues.common.entity.MarketListEntity;
//import com.mongodb.client.MongoCollection;
//import com.mongodb.client.MongoDatabase;
//import com.mongodb.client.model.Filters;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.commons.lang3.reflect.FieldUtils;
//import org.bson.Document;
//import org.bson.conversions.Bson;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.core.task.TaskExecutor;
//import org.springframework.stereotype.Component;
//import org.springframework.beans.BeanUtils;
//import software.amazon.awssdk.regions.Region;
//
//import javax.persistence.Column;
//import java.lang.reflect.Field;
//import java.lang.reflect.InvocationTargetException;
//import java.sql.*;
//import java.time.LocalDateTime;
//import java.util.*;
//import java.util.concurrent.CompletableFuture;
//import java.util.stream.Collectors;
//
//import static com.akasaair.queues.common.constants.Constants.*;
//
//@Slf4j
//@Component
//public class AuditHelper {
//
//    @Value("${rm.file-path}")
//    private String S3_FILE_PATH_NAME;
//
//    @Value("${rm.s3-bucket}")
//    private String S3_BUCKET_NAME;
//
//    @Value("${RM_WEBSERVICE_AWS_ACCESS_KEY_NEW_PROD}")
//    private String accessKey;
//
//    @Value("${RM_WEBSERVICE_AWS_SECRET_KEY_NEW_PROD}")
//    private String secretKey;
//
//    @Autowired
//    private TaskExecutor taskExecutor;
//
//    private final MongoDatabase database;
//
//    public AuditHelper(MongoConnection mongoConnection) {
//        this.database = mongoConnection.getDatabase();
//    }
//
//    public void buildAuditLogs(List<MarketListEntity> newRecords, String userName, LocalDateTime currentDateTime, String collectionName) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
//        log.info("Processing the delta for audits");
//
//        Set<String> keys = newRecords.stream()
//                .map(record -> createCompositeKey(record.getOrigin() + record.getDestination(), record.getFlightNo(), record.getStartDate()))
//                .collect(Collectors.toSet());
//
//        Map<String, MarketListEntity> existingMap = fetchExistingRecords(keys, MarketListEntity.class, collectionName);
//
//        log.info("Existing records fetched. Building delta and uploading audits asynchronously");
//
//        // Process delta and upload asynchronously
//        CompletableFuture.runAsync(() -> {
//            try {
//                List<MarketListAuditEntity> changeMap = buildDeltaRecords(newRecords, existingMap, userName, currentDateTime);
//
//                appendAuditsToS3(changeMap, currentDateTime, MarketListAuditEntity.class);
//            } catch (Exception e) {
//                // Log the exception appropriately
//                log.error("Error processing audit delta: {}", e.getMessage());
//            }
//        }, taskExecutor);
//
//    }
//
//    private List<MarketListAuditEntity> buildDeltaRecords(List<MarketListEntity> newRecords,
//                                                          Map<String, MarketListEntity> existingMap,
//                                                          String userName,
//                                                          LocalDateTime currentDateTime) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, InstantiationException {
//        List<MarketListAuditEntity> changeMap = new ArrayList<>();
//        Field[] fields = FieldUtils.getAllFields(MarketListAuditEntity.class);
//
//        for (MarketListEntity newRow : newRecords) {
//            MarketListAuditEntity newAudit = new MarketListAuditEntity();
//            BeanUtils.copyProperties(newRow, newAudit);
//
//            String key = createCompositeKey(newRow.getOrigin() + newRow.getDestination(), newRow.getFlightNo(), newRow.getStartDate());
//            MarketListEntity oldRow = existingMap.get(key);
//
//            newAudit.setDate(newAudit.getStartDate().replace('/', '-'));
//            newAudit.setSector(newAudit.getOrigin() + newAudit.getDestination());
//            newAudit.setUserName(userName);
//
//            // Set unwanted fields as null
//            newAudit.setUuid(null);
//            newAudit.setOrigin(null);
//            newAudit.setDestination(null);
//            newAudit.setPerType(null);
//            newAudit.setStartDate(null);
//            newAudit.setEndDate(null);
//            newAudit.setDayOfWeek(null);
//
//            newAudit.setChangedAt(currentDateTime.toString());
//            boolean hasChanges = false;
//
//            for (Field field : fields) {
//                field.setAccessible(true);
//
//                boolean isNotAuditField = !(field.getName().equals("userName") || field.getName().equals("sector") ||
//                        field.getName().equals("date") || field.getName().equals("changedAt") ||
//                        field.getName().equals("flightNo"));
//
//                Object oldVal = null;
//                if (isNotAuditField && oldRow != null) {
//                    oldVal = field.get(oldRow);
//                }
//                Object newVal = field.get(newAudit);
//
//                if (newVal == null || areValuesEqual(oldVal, newVal)) {
//                    field.set(newAudit, null);
//                } else {
//                    if (isNotAuditField) {
//                        hasChanges = true;
//                    }
//                    if (((String) newVal).trim().isEmpty()) {
//                        field.set(newAudit, AUDIT_EMPTY_IDENTIFIER);
//                    }
//                }
//            }
//
//            if (hasChanges && oldRow != null) {
//                changeMap.add(newAudit);
//            }
//        }
//
//        return changeMap;
//    }
//
//    public void buildConnAuditLogs(List<MarketListConnectionsEntity> newRecords, String userName, LocalDateTime currentDateTime, String collectionName) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
//        log.info("Processing the delta for connection audits");
//
//        Set<String> keys = newRecords.stream()
//                .map(record -> createCompositeKey(record.getSector1() + "-" + record.getSector2(),
//                        record.getFlight1() + "-" + record.getFlight2(), record.getStartDate()))
//                .collect(Collectors.toSet());
//
//        Map<String, MarketListConnectionsEntity> existingMap = fetchExistingRecords(keys, MarketListConnectionsEntity.class, collectionName);
//
//        log.info("Existing connection records fetched. Building delta and uploading audits asynchronously");
//
//        // Process delta and upload asynchronously
//        CompletableFuture.runAsync(() -> {
//            try {
//                List<MarketListConnAuditEntity> changeMap = buildConnDeltaRecords(newRecords, existingMap, userName, currentDateTime);
//
//                appendAuditsToS3(changeMap, currentDateTime, MarketListConnAuditEntity.class);
//            } catch (Exception e) {
//                // Log the exception appropriately
//                log.error("Error processing connection audit delta: {}", e.getMessage());
//            }
//        }, taskExecutor);
//
//    }
//
//    private List<MarketListConnAuditEntity> buildConnDeltaRecords(List<MarketListConnectionsEntity> newRecords,
//                                                                  Map<String, MarketListConnectionsEntity> existingMap,
//                                                                  String userName,
//                                                                  LocalDateTime currentDateTime) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, InstantiationException {
//        List<MarketListConnAuditEntity> changeMap = new ArrayList<>();
//        Field[] fields = FieldUtils.getAllFields(MarketListConnAuditEntity.class);
//
//        for (MarketListConnectionsEntity newRow : newRecords) {
//            MarketListConnAuditEntity newAudit = new MarketListConnAuditEntity();
//            BeanUtils.copyProperties(newRow, newAudit);
//
//            String key = createCompositeKey(newRow.getSector1() + "-" + newRow.getSector2(),
//                    newRow.getFlight1() + "-" + newRow.getFlight2(), newRow.getStartDate());
//            MarketListConnectionsEntity oldRow = existingMap.get(key);
//
//            newAudit.setDate(newAudit.getStartDate().replace('/', '-'));
//            newAudit.setSector(newAudit.getSector1() + "-" + newAudit.getSector2());
//            newAudit.setFlightNo(newAudit.getFlight1() + "-" + newAudit.getFlight2());
//            newAudit.setUserName(userName);
//
//            // Set unwanted fields as null
//            newAudit.setUuid(null);
//            newAudit.setSector1(null);
//            newAudit.setSector2(null);
//            newAudit.setFlight1(null);
//            newAudit.setFlight2(null);
//            newAudit.setPerType(null);
//            newAudit.setStartDate(null);
//            newAudit.setEndDate(null);
//            newAudit.setDayOfWeek(null);
//
//            newAudit.setChangedAt(currentDateTime.toString());
//            boolean hasChanges = false;
//
//            for (Field field : fields) {
//                field.setAccessible(true);
//
//                boolean isNotAuditField = !(field.getName().equals("userName") || field.getName().equals("sector") ||
//                        field.getName().equals("date") || field.getName().equals("changedAt") ||
//                        field.getName().equals("flightNo"));
//
//                Object oldVal = null;
//                if (isNotAuditField && oldRow != null) {
//                    oldVal = field.get(oldRow);
//                }
//                Object newVal = field.get(newAudit);
//
//                if (newVal == null || areValuesEqual(oldVal, newVal)) {
//                    field.set(newAudit, null);
//                } else {
//                    if (isNotAuditField) {
//                        hasChanges = true;
//                    }
//                    if (((String) newVal).trim().isEmpty()) {
//                        field.set(newAudit, AUDIT_EMPTY_IDENTIFIER);
//                    }
//                }
//            }
//
//            if (hasChanges && oldRow != null) {
//                changeMap.add(newAudit);
//            }
//        }
//
//        return changeMap;
//    }
//
//
//    /**
//     * MongoDB implementation - fetches existing records using direct query with $or
//     * No temp collection needed - more efficient and MongoDB-native approach
//     */
//    public <T> Map<String, T> fetchExistingRecords(Set<String> keys, Class<T> entityClass, String collectionName)
//            throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
//
//        Map<String, T> resultMap = new HashMap<>();
//
//        if (keys == null || keys.isEmpty()) {
//            return resultMap;
//        }
//
//        // Build query filters directly from keys
//        List<Bson> orFilters = new ArrayList<>();
//
//        for (String key : keys) {
//            String[] keyParts = parseCompositeKey(key);
//            orFilters.add(buildKeyFilter(entityClass, keyParts));
//        }
//
//        // Single query with $or condition
//        Bson filter = Filters.or(orFilters);
//
//        MongoCollection<Document> collection = database.getCollection(collectionName);
//
//        collection.find(filter).forEach(doc -> {
//            try {
//                T entity = mapDocumentToEntity(doc, entityClass);
//                String compositeKey = extractCompositeKey(entity, entityClass);
//                resultMap.put(compositeKey, entity);
//            } catch (Exception e) {
//                throw new RuntimeException("Error mapping document to entity", e);
//            }
//        });
//
//        return resultMap;
//    }
//
//    /**
//     * Builds MongoDB filter based on entity type
//     * Replicates the SQL JOIN logic using $expr and $concat
//     */
//    private Bson buildKeyFilter(Class<?> entityClass, String[] keyParts) {
//        if (entityClass == MarketListEntity.class) {
//            // Match: CONCAT(Origin, Destin) = keyParts[0] AND FlightNumber = keyParts[1] AND PerStart = keyParts[2]
//            return Filters.and(
//                    Filters.expr(new Document("$eq", Arrays.asList(
//                            new Document("$concat", Arrays.asList("$Origin", "$Destin")),
//                            keyParts[0]
//                    ))),
//                    Filters.eq("FlightNumber", Integer.parseInt(keyParts[1])),
//                    Filters.eq("PerStart", keyParts[2])
//            );
//        } else if (entityClass == MarketListConnectionsEntity.class) {
//            // Match: CONCAT(Sector1, '-', Sector2) = keyParts[0] AND CONCAT(Flight1, '-', Flight2) = keyParts[1] AND PerStart = keyParts[2]
//            return Filters.and(
//                    Filters.expr(new Document("$eq", Arrays.asList(
//                            new Document("$concat", Arrays.asList("$Sector1", "-", "$Sector2")),
//                            keyParts[0]
//                    ))),
//                    Filters.expr(new Document("$eq", Arrays.asList(
//                            new Document("$concat", Arrays.asList(
//                                    new Document("$toString", "$Flight1"), "-", new Document("$toString", "$Flight2")
//                            )),
//                            keyParts[1]
//                    ))),
//                    Filters.eq("PerStart", keyParts[2])
//            );
//        } else {
//            throw new IllegalArgumentException("Unsupported entity type: " + entityClass.getName());
//        }
//    }
//
//    /**
//     * Maps MongoDB Document to entity using reflection
//     */
//    private <T> T mapDocumentToEntity(Document doc, Class<T> entityClass)
//            throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
//        T entity = entityClass.getDeclaredConstructor().newInstance();
//
//        for (Field field : entityClass.getDeclaredFields()) {
//            Column column = field.getAnnotation(Column.class);
//            if (column == null) {
//                continue; // skip if not annotated with @Column
//            }
//
//            String columnName = column.name();
//            Object value = doc.get(columnName);
//
//            field.setAccessible(true);
//            try {
//                // Convert to String if needed (adjust based on your entity field types)
//                if (value != null) {
//                    field.set(entity, value.toString());
//                } else {
//                    field.set(entity, null);
//                }
//            } catch (IllegalAccessException e) {
//                throw new RuntimeException("Failed to set field: " + field.getName(), e);
//            }
//        }
//
//        return entity;
//    }
//
//    private <T> String extractCompositeKey(T entity, Class<T> entityClass) {
//        if (entityClass == MarketListEntity.class) {
//            MarketListEntity e = (MarketListEntity) entity;
//            return createCompositeKey(
//                    e.getOrigin() + e.getDestination(),
//                    e.getFlightNo(),
//                    e.getStartDate()
//            );
//        } else if (entityClass == MarketListConnectionsEntity.class) {
//            MarketListConnectionsEntity e = (MarketListConnectionsEntity) entity;
//            return createCompositeKey(
//                    e.getSector1() + "-" + e.getSector2(),
//                    e.getFlight1() + "-" + e.getFlight2(),
//                    e.getStartDate()
//            );
//        } else {
//            throw new IllegalArgumentException("Unsupported entity type: " + entityClass.getName());
//        }
//    }
//
//    // Helper methods for composite key handling
//    private String createCompositeKey(String sector, String flightNo, String startDate) {
//        return String.join(AUDIT_KEY_DELIMITER, sector != null ? sector : "",
//                flightNo != null ? flightNo : "", startDate != null ? startDate : "");
//    }
//
//    private String[] parseCompositeKey(String compositeKey) {
//        return compositeKey.split("\\" + AUDIT_KEY_DELIMITER, -1); // -1 to preserve empty strings
//    }
//
//    private boolean areValuesEqual(Object val1, Object val2) {
//        if (Objects.equals(val1, val2)) return true;
//        if (val1 == null || val2 == null) return false;
//
//        try {
//            return Double.parseDouble(val1.toString()) == Double.parseDouble(val2.toString());
//        } catch (NumberFormatException e) {
//            return false;
//        }
//    }
//
//    public <T> void appendAuditsToS3(List<T> changeMap, LocalDateTime currentDateTime, Class<T> entityClass) {
//        if (changeMap == null || changeMap.isEmpty()) {
//            log.info("No audits to upload");
//            return;
//        }
//
//        String basePath = "s3://" + S3_BUCKET_NAME + "/" + S3_FILE_PATH_NAME + "audits/";
//        String changedAt = currentDateTime.toString().split("T")[0];
//        String fileName = changedAt + ".csv";
//
//        try (Connection conn = getS3DuckDBConnection()) {
//            // Process each partition
//            for (T change : changeMap) {
//                if (change == null) continue;
//
//                String sector = (String) entityClass.getMethod("getSector").invoke(change);
//                String flightNo = (String) entityClass.getMethod("getFlightNo").invoke(change);
//                String date = (String) entityClass.getMethod("getDate").invoke(change);
//                String changedAtDateTime = (String) entityClass.getMethod("getChangedAt").invoke(change);
//                String userName = (String) entityClass.getMethod("getUserName").invoke(change);
//
//                String partitionPath = String.format("sector=%s/flightNo=%s/date=%s/",
//                        sector, flightNo, date);
//                String specificFilePath = basePath + partitionPath + fileName;
//                String tableName = "audit_" + sector.replace("-", "") + "_" +
//                        flightNo.replace("-", "") + "_" +
//                        date.replace('-', '_');
//
//                // Extract columns from new changes
//                Set<String> newColumns = new HashSet<>();
//                Field[] fields = FieldUtils.getAllFields(entityClass);
//                for (Field field : fields) {
//                    field.setAccessible(true);
//                    try {
//                        if (field.get(change) != null) {
//                            newColumns.add(field.getName());
//                        }
//                    } catch (IllegalAccessException e) {
//                        log.error("Error accessing field: " + field.getName());
//                    }
//                }
//                newColumns.add("changeType");
//
//                // Get schema from existing file (if any)
//                Set<String> existingColumns = new HashSet<>();
//                try (Statement schemaStmt = conn.createStatement()) {
//                    String describeQuery = String.format("DESCRIBE SELECT * FROM read_csv('%s')",
//                            specificFilePath);
//                    ResultSet rs = schemaStmt.executeQuery(describeQuery);
//                    while (rs.next()) {
//                        existingColumns.add(rs.getString("column_name"));
//                    }
//                } catch (SQLException e) {
//                    // No existing file found
//                }
//
//                // Combine schemas
//                Set<String> allColumns = new HashSet<>(newColumns);
//                allColumns.addAll(existingColumns);
//                if (allColumns.isEmpty()) {
//                    continue;
//                }
//
//                // Create table for combined data
//                StringBuilder createTableSQL = new StringBuilder("CREATE TEMPORARY TABLE " + tableName + " (");
//                List<String> columnDefs = new ArrayList<>();
//                for (String column : allColumns) {
//                    columnDefs.add("\"" + column + "\" VARCHAR");
//                }
//                createTableSQL.append(String.join(", ", columnDefs)).append(")");
//                SqlHelperFunctions.execute(conn, createTableSQL.toString(), null);
//
//                // Insert new changes
//                List<String> columns = new ArrayList<>();
//                List<Object> values = new ArrayList<>();
//                for (Field field : fields) {
//                    field.setAccessible(true);
//                    String fieldName = field.getName();
//                    try {
//                        Object value = field.get(change);
//                        if (value != null) {
//                            columns.add(fieldName);
//                            values.add(value.toString());
//                        }
//                    } catch (IllegalAccessException e) {
//                        log.error("Error accessing field: " + fieldName);
//                    }
//                }
//                columns.add("changeType");
//                values.add(CHANGE_TYPE_MARKETLIST);
//
//                if (columns.size() > 1) {
//                    StringBuilder insertSQL = new StringBuilder("INSERT INTO " + tableName + " (\"");
//                    insertSQL.append(String.join("\", \"", columns));
//                    insertSQL.append("\") VALUES (");
//                    List<String> placeholders = new ArrayList<>();
//                    for (int i = 0; i < values.size(); i++) {
//                        placeholders.add("?");
//                    }
//
//                    insertSQL.append(String.join(", ", placeholders));
//                    insertSQL.append(")");
//                    SqlHelperFunctions.execute(conn, insertSQL.toString(), values);
//                }
//
//                // Insert existing data if available
//                if (!existingColumns.isEmpty()) {
//                    String readExistingSQL = String.format("INSERT INTO %s (\"%s\") SELECT \"%s\" FROM read_csv(?)",
//                            tableName, String.join("\", \"", existingColumns),
//                            String.join("\", \"", existingColumns));
//
//                    List<Object> readParams = List.of(specificFilePath);
//                    SqlHelperFunctions.execute(conn, readExistingSQL, readParams);
//                }
//
//                // Copy to S3
//                String destination = basePath + partitionPath + changedAt + ".csv";
//                String copySQL = String.format("COPY %s TO '%s' (FORMAT CSV, HEADER TRUE, OVERWRITE_OR_IGNORE)",
//                        tableName, destination);
//
//                SqlHelperFunctions.execute(conn, copySQL, null);
//
//                String dropSQL = "DROP TABLE IF EXISTS " + tableName;
//                SqlHelperFunctions.execute(conn, dropSQL, null);
//            }
//            log.info("Audits uploaded successfully");
//
//        } catch (Exception e) {
//            log.error("Error uploading audits: " + e.getMessage());
//        }
//    }
//
//    private Connection getS3DuckDBConnection() throws SQLException {
//        String region = Region.AP_SOUTH_1.toString();
//        Properties connectionProps = new Properties();
//        connectionProps.put("threads", "8");
//
//        Connection connection = DriverManager.getConnection("jdbc:duckdb:", connectionProps);
//
//        try (Statement stmt = connection.createStatement()) {
//            // Load httpfs extension
//            stmt.execute("INSTALL httpfs");
//            stmt.execute("LOAD httpfs");
//        } catch (SQLException e) {
//            connection.close();
//            throw e;
//        }
//
//        connection.close();
//
//        Properties s3ConnectionProps = new Properties();
//        s3ConnectionProps.put("threads", "8");
//        s3ConnectionProps.put("s3_use_ssl", "true");
//        s3ConnectionProps.put("s3_access_key_id", accessKey);
//        s3ConnectionProps.put("s3_secret_access_key", secretKey);
//        s3ConnectionProps.put("s3_session_token", "");
//        s3ConnectionProps.put("s3_region", region);
//        s3ConnectionProps.put("s3_endpoint", "s3." + region + ".amazonaws.com");
//        s3ConnectionProps.put("s3_url_style", "path");
//
//        return DriverManager.getConnection("jdbc:duckdb:", s3ConnectionProps);
//    }
//}
//
//@Slf4j
//class SqlHelperFunctions {
//
//    public static void execute(Connection connection, String query, List<Object> params) {
//        PreparedStatement pstmt = null;
//
//        try {
//            pstmt = connection.prepareStatement(query);
//
//            if (params != null && !params.isEmpty()) {
//                for (int i = 0; i < params.size(); i++) {
//                    setParameter(pstmt, i + 1, params.get(i));
//                }
//            }
//
//            pstmt.execute();
//
//        } catch (SQLException e) {
//            log.error("Error executing update: {}", e.getMessage());
//        } finally {
//            try {
//                if (pstmt != null) pstmt.close();
//            } catch (SQLException ex) {
//                log.error("Error closing statement: {}", ex.getMessage());
//            }
//        }
//    }
//
//    private static void setParameter(PreparedStatement pstmt, int index, Object value) throws SQLException {
//        if (value == null) {
//            pstmt.setNull(index, Types.NULL);
//        } else if (value instanceof String) {
//            pstmt.setString(index, (String) value);
//        } else if (value instanceof Integer) {
//            pstmt.setInt(index, (Integer) value);
//        } else if (value instanceof Long) {
//            pstmt.setLong(index, (Long) value);
//        } else if (value instanceof Double) {
//            pstmt.setDouble(index, (Double) value);
//        } else if (value instanceof Float) {
//            pstmt.setFloat(index, (Float) value);
//        } else if (value instanceof Boolean) {
//            pstmt.setBoolean(index, (Boolean) value);
//        } else if (value instanceof java.sql.Date) {
//            pstmt.setDate(index, (java.sql.Date) value);
//        } else if (value instanceof Timestamp) {
//            pstmt.setTimestamp(index, (Timestamp) value);
//        } else if (value instanceof Byte) {
//            pstmt.setInt(index, ((Byte) value).intValue());
//        } else if (value instanceof byte[]) {
//            pstmt.setBytes(index, (byte[]) value);
//        } else {
//            // For other types
//            pstmt.setObject(index, value);
//        }
//    }
//}