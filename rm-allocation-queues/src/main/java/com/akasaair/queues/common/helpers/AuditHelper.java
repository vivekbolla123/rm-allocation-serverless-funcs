package com.akasaair.queues.common.helpers;

import com.akasaair.queues.common.entity.MarketListAuditEntity;
import com.akasaair.queues.common.entity.MarketListConnAuditEntity;
import com.akasaair.queues.common.entity.MarketListConnectionsEntity;
import com.akasaair.queues.common.entity.MarketListEntity;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.scheduling.annotation.Async;
import software.amazon.awssdk.regions.Region;

import javax.persistence.Column;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import org.springframework.beans.BeanUtils;

import static com.akasaair.queues.common.constants.Constants.*;
import static com.akasaair.queues.common.constants.QueryConstants.INSERT_INTO_ML_AUDIT_LOGS;

public class AuditHelper {

    private final Connection connection;

    public AuditHelper(Connection connection) {
        this.connection = connection;
    }

    public void buildAuditLogs(List<MarketListEntity> newRecords, String userName, LocalDateTime currentDateTime, String tableName) throws IllegalAccessException, SQLException, InvocationTargetException, NoSuchMethodException, InstantiationException {
//        String changedAt = currentDateTime.toString().split("T")[0];

        // Extract keys from newRecords
        Set<String> keys = newRecords.stream()
                .map(record -> createCompositeKey(record.getOrigin() + record.getDestination(), record.getFlightNo(), record.getStartDate()))
                .collect(Collectors.toSet());

        // Fetch existing records
        Map<String, MarketListEntity> existingMap = fetchExistingRecords(keys, MarketListEntity.class, tableName);
        List<MarketListAuditEntity> changeMap = new ArrayList<>();

        Field[] fields = FieldUtils.getAllFields(MarketListAuditEntity.class);

        for (MarketListEntity newRow : newRecords) {
            MarketListAuditEntity newAudit = new MarketListAuditEntity();
            BeanUtils.copyProperties(newRow, newAudit);

            String key = createCompositeKey(newRow.getOrigin() + newRow.getDestination(), newRow.getFlightNo(), newRow.getStartDate());
            MarketListEntity oldRow = existingMap.get(key);

            newAudit.setDate(newAudit.getStartDate().replace('/', '-'));
            newAudit.setSector(newAudit.getOrigin() + newAudit.getDestination());
            newAudit.setUserName(userName);

            // Set unwanted fields as null
            newAudit.setUuid(null);
            newAudit.setOrigin(null);
            newAudit.setDestination(null);
            newAudit.setPerType(null);
            newAudit.setStartDate(null);
            newAudit.setEndDate(null);
            newAudit.setDayOfWeek(null);

            newAudit.setChangedAt(currentDateTime.toString());
            boolean hasChanges = false;
            for (Field field : fields) {
                field.setAccessible(true);

                boolean isNotAuditField = !(field.getName().equals("userName") || field.getName().equals("sector") ||
                        field.getName().equals("date") || field.getName().equals("changedAt") ||
                        field.getName().equals("flightNo"));

                Object oldVal = null;
                if (isNotAuditField && oldRow != null) {
                    oldVal = field.get(oldRow);
                }
                Object newVal = field.get(newAudit);

                if (newVal == null || areValuesEqual(oldVal, newVal)) {
                    field.set(newAudit, null);
                } else {
                    if (isNotAuditField) {
                        hasChanges = true;
                    }
                    if (((String) newVal).trim().isEmpty()){
                        field.set(newAudit, AUDIT_EMPTY_IDENTIFIER);
                    }
                }
            }
            if (hasChanges && oldRow != null){
                changeMap.add(newAudit);
            }
        }
        appendAuditsToS3(changeMap, currentDateTime, MarketListAuditEntity.class);
    }

    public void buildConnAuditLogs(List<MarketListConnectionsEntity> newRecords, String userName, LocalDateTime currentDateTime, String tableName) throws IllegalAccessException, SQLException, InvocationTargetException, NoSuchMethodException, InstantiationException {
        // Extract keys from newRecords
        Set<String> keys = newRecords.stream()
                .map(record -> createCompositeKey(record.getSector1() + "-" + record.getSector2(),
                        record.getFlight1() + "-" + record.getFlight2(), record.getStartDate()))
                .collect(Collectors.toSet());

        // Fetch existing records
        Map<String, MarketListConnectionsEntity> existingMap = fetchExistingRecords(keys, MarketListConnectionsEntity.class, tableName);
        List<MarketListConnAuditEntity> changeMap = new ArrayList<>();

        Field[] fields = FieldUtils.getAllFields(MarketListConnAuditEntity.class);

        for (MarketListConnectionsEntity newRow : newRecords) {
            MarketListConnAuditEntity newAudit = new MarketListConnAuditEntity();
            BeanUtils.copyProperties(newRow, newAudit);

            String key = createCompositeKey(newRow.getSector1() + "-" + newRow.getSector2(),
                    newRow.getFlight1() + "-" + newRow.getFlight2(), newRow.getStartDate());
            MarketListConnectionsEntity oldRow = existingMap.get(key);

            newAudit.setDate(newAudit.getStartDate().replace('/', '-'));
            newAudit.setSector(newAudit.getSector1() + "-" + newAudit.getSector2());
            newAudit.setFlightNo(newAudit.getFlight1() + "-" + newAudit.getFlight2());
            newAudit.setUserName(userName);

            // Set unwanted fields as null
            newAudit.setUuid(null);
            newAudit.setSector1(null);
            newAudit.setSector2(null);
            newAudit.setFlight1(null);
            newAudit.setFlight2(null);
            newAudit.setPerType(null);
            newAudit.setStartDate(null);
            newAudit.setEndDate(null);
            newAudit.setDayOfWeek(null);

            newAudit.setChangedAt(currentDateTime.toString());
            boolean hasChanges = false;
            for (Field field : fields) {
                field.setAccessible(true);

                boolean isNotAuditField = !(field.getName().equals("userName") || field.getName().equals("sector") ||
                        field.getName().equals("date") || field.getName().equals("changedAt") ||
                        field.getName().equals("flightNo"));

                Object oldVal = null;
                if (isNotAuditField && oldRow != null) {
                    oldVal = field.get(oldRow);
                }
                Object newVal = field.get(newAudit);

                if (newVal == null || areValuesEqual(oldVal, newVal)) {
                    field.set(newAudit, null);
                } else {
                    if (isNotAuditField) {
                        hasChanges = true;
                    }
                    if (((String) newVal).trim().isEmpty()){
                        field.set(newAudit, AUDIT_EMPTY_IDENTIFIER);
                    }
                }
            }
            if (hasChanges && oldRow != null){
                changeMap.add(newAudit);
            }
        }
        appendAuditsToS3(changeMap, currentDateTime, MarketListConnAuditEntity.class);
    }

    public <T> Map<String, T> fetchExistingRecords(Set<String> keys, Class<T> entityClass, String tableName) throws SQLException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        Map<String, T> resultMap = new HashMap<>();

        if (keys == null || keys.isEmpty()) {
            return resultMap; // nothing to fetch
        }

        // Extract individual components from composite keys for SQL IN clause
        List<String[]> keyComponents = keys.stream()
                .map(this::parseCompositeKey)
                .toList();

        // Build placeholders like: (?,?,?), (?,?,?), ...
        String placeholders = keyComponents.stream()
                .map(k -> "(?, ?, ?)")
                .collect(Collectors.joining(", "));

        String sql = "";
        if (entityClass == MarketListEntity.class) {
            sql = "SELECT * FROM "+ tableName +" WHERE (concat(Origin, Destin), FlightNumber, PerStart) IN (" + placeholders + ")";
        } else if (entityClass == MarketListConnectionsEntity.class) {
            sql = "SELECT * FROM "+ tableName +" WHERE (concat(Sector1,'-', Sector2), concat(Flight1,'-', Flight2), PerStart) IN (" + placeholders + ")";
        } else {
            throw new IllegalArgumentException("Unsupported entity type: " + entityClass.getName());
        }

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            int index = 1;
            for (String[] keyParts : keyComponents) {
                pstmt.setString(index++, keyParts[0]); // sector
                pstmt.setString(index++, keyParts[1]); // flightNo
                pstmt.setString(index++, keyParts[2]); // startDate
            }

            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    T entity = mapRowToEntity(rs, entityClass);

                    String key;
                    if (entityClass == MarketListEntity.class) {
                        key = createCompositeKey(
                                ((MarketListEntity) entity).getOrigin() + ((MarketListEntity) entity).getDestination(),
                                ((MarketListEntity) entity).getFlightNo(),
                                ((MarketListEntity) entity).getStartDate()
                        );
                    } else if (entityClass == MarketListConnectionsEntity.class) {
                        key = createCompositeKey(
                                ((MarketListConnectionsEntity) entity).getSector1() + "-" + ((MarketListConnectionsEntity) entity).getSector2(),
                                ((MarketListConnectionsEntity) entity).getFlight1() + "-" + ((MarketListConnectionsEntity) entity).getFlight2(),
                                ((MarketListConnectionsEntity) entity).getStartDate()
                        );
                    } else {
                        throw new IllegalArgumentException("Unsupported entity type: " + entityClass.getName());
                    }
                    resultMap.put(key, entity);
                }
            }
        }

        return resultMap;
    }

    private static <T> T mapRowToEntity(ResultSet rs, Class<T> entityClass) throws SQLException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        T entity = entityClass.getDeclaredConstructor().newInstance();

        for (Field field : entityClass.getDeclaredFields()) {
            Column column = field.getAnnotation(Column.class);
            if (column == null) {
                continue; // skip if not annotated with @Column
            }

            String columnName = column.name();
            String value = rs.getString(columnName);

            field.setAccessible(true);
            try {
                field.set(entity, value);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Failed to set field: " + field.getName(), e);
            }
        }

        return entity;
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

    @Async
    public <T> String appendAuditsToS3(List<T> changeMap, LocalDateTime currentDateTime, Class<T> entityClass) {
        if (changeMap == null || changeMap.isEmpty()) {
            return "No changes to process";
        }

        String basePath = "s3://" + S3_BUCKET_NAME + "/" + S3_FILE_PATH_NAME + "audits/";
        String changedAt = currentDateTime.toString().split("T")[0];
        String fileName = changedAt + ".csv";

        try (Connection conn = getS3DuckDBConnection()) {
            // Process each partition
            for (T change : changeMap) {
                if (change == null) continue;

                String sector = (String) entityClass.getMethod("getSector").invoke(change);
                String flightNo = (String) entityClass.getMethod("getFlightNo").invoke(change);
                String date = (String) entityClass.getMethod("getDate").invoke(change);
                String changedAtDateTime = (String) entityClass.getMethod("getChangedAt").invoke(change);
                String userName = (String) entityClass.getMethod("getUserName").invoke(change);

//                MarketListAuditEntity sampleChange = change;
                String partitionPath = String.format("sector=%s/flightNo=%s/date=%s/",
                        sector, flightNo, date);
                String specificFilePath = basePath + partitionPath + fileName;
                String tableName = "audit_" + sector.replace("-", "") + "_" +
                        flightNo.replace("-", "") + "_" +
                        date.replace('-', '_');

                // Extract columns from new changes
                Set<String> newColumns = new HashSet<>();
                Field[] fields = FieldUtils.getAllFields(entityClass);
                for (Field field : fields) {
                    field.setAccessible(true);
                    try {
                        if (field.get(change) != null) {
                            newColumns.add(field.getName());
                        }
                    } catch (IllegalAccessException e) {
                        System.err.println("Error accessing field: " + field.getName());
                    }
                }
                newColumns.add("changeType");

                // Get schema from existing file (if any)
                Set<String> existingColumns = new HashSet<>();
                try (Statement schemaStmt = conn.createStatement()) {
                    String describeQuery = String.format("DESCRIBE SELECT * FROM read_csv('%s')",
                            specificFilePath);
                    ResultSet rs = schemaStmt.executeQuery(describeQuery);
                    while (rs.next()) {
                        existingColumns.add(rs.getString("column_name"));
                    }
                } catch (SQLException e) {
//                    log.info("No existing file found at {}: {}", specificFilePath, e.getMessage());
                }

                // Combine schemas
                Set<String> allColumns = new HashSet<>(newColumns);
                allColumns.addAll(existingColumns);
                if (allColumns.isEmpty()) {
                    continue;
                }

                // Create table for combined data
                StringBuilder createTableSQL = new StringBuilder("CREATE TEMPORARY TABLE " + tableName + " (");
                List<String> columnDefs = new ArrayList<>();
                for (String column : allColumns) {
                    columnDefs.add("\"" + column + "\" VARCHAR");
                }
                createTableSQL.append(String.join(", ", columnDefs)).append(")");
                SqlHelperFunctions.execute(conn, createTableSQL.toString(), null);

                // Insert new changes
                List<String> columns = new ArrayList<>();
                List<Object> values = new ArrayList<>();
                for (Field field : fields) {
                    field.setAccessible(true);
                    String fieldName = field.getName();
                    try {
                        Object value = field.get(change);
                        if (value != null) {
                            columns.add(fieldName);
                            values.add(value.toString());
                        }
                    } catch (IllegalAccessException e) {
                        System.err.println("Error accessing field: " + fieldName);
                    }
                }
                columns.add("changeType");
                values.add(CHANGE_TYPE_MARKETLIST);

                if (columns.size() > 1) {
                    StringBuilder insertSQL = new StringBuilder("INSERT INTO " + tableName + " (\"");
                    insertSQL.append(String.join("\", \"", columns));
                    insertSQL.append("\") VALUES (");
                    List<String> placeholders = new ArrayList<>();
                    for (int i = 0; i < values.size(); i++) {
                        placeholders.add("?");
                    }

                    insertSQL.append(String.join(", ", placeholders));
                    insertSQL.append(")");
                    SqlHelperFunctions.execute(conn, insertSQL.toString(), values);
                }

                // Insert existing data if available
                if (!existingColumns.isEmpty()) {
                    String readExistingSQL = String.format("INSERT INTO %s (\"%s\") SELECT \"%s\" FROM read_csv(?)",
                            tableName, String.join("\", \"", existingColumns),
                            String.join("\", \"", existingColumns));

                    List<Object> readParams = List.of(specificFilePath);
                    SqlHelperFunctions.execute(conn, readExistingSQL, readParams);
                }

                // Copy to S3
                String destination = basePath + partitionPath + changedAt + ".csv";
                String copySQL = String.format("COPY %s TO '%s' (FORMAT CSV, HEADER TRUE, OVERWRITE_OR_IGNORE)",
                        tableName, destination);

//                log.info("Writing to S3 using query: {}", copySQL);
                SqlHelperFunctions.execute(conn, copySQL, null);

                String dropSQL = "DROP TABLE IF EXISTS " + tableName;
                SqlHelperFunctions.execute(conn, dropSQL, null);

                insertIntoMlAuditLogs(sector, flightNo, date, userName, changedAtDateTime);
            }
            return "Success";
        } catch (Exception e) {
            System.err.println("Error processing CSV append: " + e.getMessage());
            return "Error: " + e.getMessage();
        }
    }

    private Connection getS3DuckDBConnection() throws SQLException {
        String region = Region.AP_SOUTH_1.toString();

        Properties connectionProps = new Properties();
        connectionProps.put("s3_use_ssl", "true");
        connectionProps.put("threads", 8);
        connectionProps.put("s3_access_key_id", System.getenv("AWS_ACCESS_KEY_ID"));
        connectionProps.put("s3_secret_access_key", System.getenv("AWS_SECRET_ACCESS_KEY"));
        connectionProps.put("s3_region", region);
        connectionProps.put("s3_endpoint", "s3." + region + ".amazonaws.com");
        connectionProps.put("s3_url_style", "path");

        return DriverManager.getConnection("jdbc:duckdb:", connectionProps);
    }

    public void insertIntoMlAuditLogs(String sector, String flightNo, String date, String userName, String changedAtDateTime) {
        try {
            List<Object> params = Arrays.asList(
                    sector,
                    LocalDate.parse(date, DATE_OUTPUT_FORMATTER_3).toString(),
                    flightNo,
                    userName,
                    changedAtDateTime
            );

            SqlHelperFunctions.execute(connection, INSERT_INTO_ML_AUDIT_LOGS(), params);

        } catch (Exception e) {
            String errorMsg = "Failed to insert audit log for sector " + sector +
                    "_" + flightNo +
                    "_" + date +
                    "_" + userName;
            System.out.println(DATABASE_ACCESS_ERROR_MESSAGE + ": " + errorMsg);
        }
    }
}


class SqlHelperFunctions {

    public static void execute(Connection connection, String query, List<Object> params) {
        PreparedStatement pstmt = null;

        try {
            pstmt = connection.prepareStatement(query);

            if (params != null && !params.isEmpty()) {
                for (int i = 0; i < params.size(); i++) {
                    setParameter(pstmt, i + 1, params.get(i));
                }
            }

            pstmt.execute();

        } catch (SQLException e) {
            System.err.println("Error executing update: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                if (pstmt != null) pstmt.close();
            } catch (SQLException ex) {
                System.err.println("Error closing statement: " + ex.getMessage());
                ex.printStackTrace();
            }
        }
    }

    private static void setParameter(PreparedStatement pstmt, int index, Object value) throws SQLException {
        if (value == null) {
            pstmt.setNull(index, Types.NULL);
        } else if (value instanceof String) {
            pstmt.setString(index, (String) value);
        } else if (value instanceof Integer) {
            pstmt.setInt(index, (Integer) value);
        } else if (value instanceof Long) {
            pstmt.setLong(index, (Long) value);
        } else if (value instanceof Double) {
            pstmt.setDouble(index, (Double) value);
        } else if (value instanceof Float) {
            pstmt.setFloat(index, (Float) value);
        } else if (value instanceof Boolean) {
            pstmt.setBoolean(index, (Boolean) value);
        } else if (value instanceof Date) {
            pstmt.setDate(index, (Date) value);
        } else if (value instanceof Timestamp) {
            pstmt.setTimestamp(index, (Timestamp) value);
        } else if (value instanceof Byte) {
            pstmt.setInt(index, ((Byte) value).intValue());
        } else if (value instanceof byte[]) {
            pstmt.setBytes(index, (byte[]) value);
        } else {
            // For other types
            pstmt.setObject(index, value);
        }
    }
}