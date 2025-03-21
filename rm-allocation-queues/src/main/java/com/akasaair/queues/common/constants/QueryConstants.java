package com.akasaair.queues.common.constants;

import org.springframework.stereotype.Component;

import static com.akasaair.queues.common.constants.Constants.*;

@Component
public class QueryConstants {

    public static String SELECT_MARKET_LIST(String tableName) {
        return "SELECT UUID FROM " + tableName + " where " +
                " Origin = ? and Destin = ? and analystName = ? and PerStart = ? and FlightNumber = ? ";
    }

    public static String INSERT_INTO_MARKET_LIST_CONNECTIONS(String tableName) {
        return "INSERT INTO " + tableName +
                " (Sector1,Sector2,Flight1,Flight2,Pertype,PerStart,PerEnd,DOW,Price_Strategy,Discount_Value," +
                "FirstRBDAlloc,OtherRBDAlloc,B2BBackstop,B2CBackstop,B2BFactor,SkippingFactor,analystName," +
                "UUID,Outbound_stop,Outbound_duration,Currency,fareAnchor,Offset,DiscountFlag) " +
                "VALUES " +
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ";
    }

    public static String SELECT_MARKET_LIST_CONNECTIONS(String tableName) {
        return "SELECT UUID FROM " + tableName + " where " +
                " Sector1 = ? and Sector2 = ? and analystName = ? ";
    }

    public static String INSERT_INTO_MARKET_LIST(String tableName) {
        return "INSERT INTO " + tableName +
                " (Origin, Destin, FlightNumber, PerType, PerStart, PerEnd, DOW, hardAnchor, plfThreshold, TimeWindowStart, "
                +
                "TimeWindowEnd, CurveID, CarrExlusionB2B, CarrExlusionB2C, flightExclusionB2B, flightExclusionB2C, fareAnchor, fareOffset, FirstRBDAlloc, OtherRBDAlloc, "
                +
                "B2BBackstop, B2CBackstop, B2BFactor, obSeats, obFare, SkippingFactor, DaySpan, AutoTimeRangeFlag, " +
                "analystName, openingFares, OverBooking, rbdPushFlag, profileFares, B2CTolerance, B2BTolerance, distressInventoryFlag, seriesBlock, autoGroup, autoBackstopFlag,tbfFlag, UUID) "
                +

                "VALUES " +
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ;";
    }

    public static String GET_ROLE_PERMISSION_LIST(Object role) {
        return "SELECT " +
                "DISTINCT p.Permission AS permissions " +
                "FROM config_permissions p " +
                "JOIN ( " +
                "SELECT crp.Role, GROUP_CONCAT(permissions.permission ORDER BY permissions.position) AS concatenated_string "
                +
                "FROM config_role_permission crp " +
                "CROSS JOIN JSON_TABLE( " +
                "JSON_UNQUOTE(JSON_EXTRACT(crp.PermissionID, '$.permissions[*]')), " +
                "'$[*]' COLUMNS (permission INT PATH '$', position FOR ORDINALITY) " +
                ") AS permissions " +
                "GROUP BY crp.Role " +
                ") AS crp ON FIND_IN_SET(p.PermissionID, crp.concatenated_string) " +
                " WHERE crp.Role IN (" + role + ")";
    }

    public static String GET_STATION_MASTER_LIST() {
        return "SELECT station FROM config_station_master where station in (?,?) and route = ? ";
    }

    public static String GET_PARAMETER_VALUES() {
        return "SELECT parameterValue FROM rm_parameter_values WHERE parameterKey = ?";
    }

    public static String GET_COLUMN_HEADERS() {
        return "SELECT columns FROM config_column_names WHERE tableName = ?";
    }

    public static String GET_CURRENT_INPUT_STATUS() {
        return "SELECT * FROM inputs_status where name = ? and is_running = '1'";
    }

    public static String INSERT_FILE_UPLOAD() {
        return "INSERT INTO file_upload_audit (tableName, curr_version, start_time, userName) VALUE (?,?,?,?)";
    }

    public static String DELETE_TABLE(String tableName) {
        return "drop table " + tableName;
    }

    public static String GET_INPUT_STATUS_COUNT() {
        return "SELECT count(*) as count FROM inputs_status where name = ?";
    }

    public static String UPDATE_INPUT_STATUS(int value, String tableName) {
        return "UPDATE inputs_status SET is_running = b'" + value + "' WHERE name = '" + tableName + "'";
    }

    public static String COPY_DATA_INTO_MAIN_TABLE(String tableName, String tempTableName) {
        return "insert into " + tableName + " select * from " + tempTableName;
    }

    public static String INSERT_INPUT_STATUS() {
        return "INSERT INTO inputs_status (is_running,name) VALUES (b'1',?)";
    }

    public static String UPDATE_FILE_UPLOAD() {
        return "UPDATE file_upload_audit SET end_time = ? WHERE curr_version = ? AND tableName = ?";
    }

    public static String GET_CURRENT_VERSION_COUNT() {
        return "SELECT count(*) as count FROM currentVersion where tableName = ?";
    }

    public static String UPDATE_CURRENT_VERSION() {
        return "UPDATE currentVersion SET curr_version = ? WHERE tableName = ?";
    }

    public static String INSERT_CURRENT_VERSION() {
        return "INSERT INTO currentVersion (curr_version,tableName) VALUES (?,?)";
    }

    public static String GET_STRATEGY_GRID_TIME(int limit, String value) {
        String condition = " ";
        if (value != null) {
            condition = " WHERE strategy = '" + value + "'";
        }
        StringBuilder query = new StringBuilder()
                .append("SELECT channel, strategy, ")
                .append("'Time' AS Decision, ")
                .append("case when s.ndo_band = n.ndo_band then n.ndo_band end as Band, ");

        for (int i = 0; i < limit; i++) {
            query.append(" MAX(IF(dplf_band = ").append(i).append(", time_range, NULL)) AS '").append(i).append("' ");
            if (i < limit - 1) {
                query.append(",");
            }
        }
        query.append(" FROM d1_d2_strategies s INNER JOIN ndo_bands n ON n.ndo_band=s.ndo_band ")
                .append(condition)
                .append(" GROUP BY strategy, Band, channel ");
        return query.toString();
    }

    public static String GET_STRATEGY_GRID_CRITERIA(int limit, String value) {
        String condition = " ";
        if (value != null) {
            condition = " WHERE strategy = '" + value + "'";
        }
        StringBuilder query = new StringBuilder()
                .append("SELECT channel, strategy, ")
                .append("'Criteria' AS Decision, ")
                .append("case when s.ndo_band = n.ndo_band then n.ndo_band end as Band, ");

        for (int i = 0; i < limit; i++) {
            query.append(" MAX(IF(dplf_band = ").append(i).append(", criteria, NULL)) AS '").append(i).append("' ");
            if (i < limit - 1) {
                query.append(",");
            }
        }

        query.append(" FROM d1_d2_strategies s INNER JOIN ndo_bands n ON n.ndo_band=s.ndo_band ")
                .append(condition)
                .append(" GROUP BY strategy, Band, channel ");
        return query.toString();
    }

    public static String GET_STRATEGY_GRID_OFFSET(int limit, String value) {
        String condition = " ";
        if (value != null) {
            condition = " WHERE strategy = '" + value + "'";
        }
        StringBuilder query = new StringBuilder()
                .append("SELECT channel, strategy, ")
                .append("'Offset' AS Decision, ")
                .append("case when s.ndo_band = n.ndo_band then n.ndo_band end as Band, ");

        for (int i = 0; i < limit; i++) {
            query.append(" MAX(IF(dplf_band = ").append(i).append(", offset, NULL)) AS '").append(i).append("' ");
            if (i < limit - 1) {
                query.append(",");
            }
        }
        query.append(" FROM d1_d2_strategies s INNER JOIN ndo_bands n ON n.ndo_band=s.ndo_band ")
                .append(condition)
                .append(" GROUP BY strategy, Band, channel ");
        return query.toString();
    }

    public static String GET_STRATEGY_GRID(int limit, String value) {
        return GET_STRATEGY_GRID_TIME(limit, value) +
                "UNION ALL " +
                GET_STRATEGY_GRID_CRITERIA(limit, value) +
                "UNION ALL " +
                GET_STRATEGY_GRID_OFFSET(limit, value) +
                " ORDER BY Strategy, channel, Decision, Band ";
    }

    public static String INPUT_FILE_UPLOAD_INSERT() {
        return "SELECT tableName, role, username, name, init_timestamp, route_for_adhoc " +
                "FROM inputs_file_upload " +
                "WHERE obj_id = ?";
    }

    public static String INPUT_FILE_UPLOAD_UPDATE() {
        return "UPDATE inputs_file_upload " +
                "SET status= ?, inprogress_timestamp = ? " +
                "WHERE obj_id = ?";
    }

    public static String INPUT_FILE_UPLOAD_TIME_TAKEN_UPDATE() {
        return "UPDATE inputs_file_upload " +
                "SET status= ?, done_timestamp = ?, total_rows= ?, total_time_taken=? " +
                "WHERE obj_id = ?";
    }

    public static String INPUT_FILE_UPLOAD_ERROR_OCCURRED() {
        return "UPDATE inputs_file_upload " +
                "SET errors = ? " +
                "WHERE obj_id = ?";
    }

    public static String COUNT_OF_RECORDS(String tableName) {
        return "SELECT COUNT(*) AS count FROM " + tableName;
    }

    public static String GET_USER_SECTOR_MAP(String user) {
        return "SELECT " + user + " as user from config_user_sector_map where sector= ?";
    }

}
