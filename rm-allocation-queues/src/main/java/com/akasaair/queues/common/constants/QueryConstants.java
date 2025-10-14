package com.akasaair.queues.common.constants;

import org.springframework.stereotype.Component;
import org.bson.Document;
import org.bson.conversions.Bson;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import static com.akasaair.queues.common.constants.Constants.*;

@Component
public class QueryConstants {

    // SELECT_MARKET_LIST
    public static Bson SELECT_MARKET_LIST_FILTER() {
        // Usage: Pass origin, destin, analystName, perStart, flightNumber as parameters
        // return Filters.and(
        //     Filters.eq("Origin", origin),
        //     Filters.eq("Destin", destin),
        //     Filters.eq("analystName", analystName),
        //     Filters.eq("PerStart", perStart),
        //     Filters.eq("FlightNumber", flightNumber)
        // );
        return null; // To be used with parameters in actual implementation
    }

    public static Document SELECT_MARKET_LIST_PROJECTION() {
        return new Document("UUID", 1).append("_id", 0);
    }

    // INSERT_INTO_MARKET_LIST_CONNECTIONS
    public static Document INSERT_INTO_MARKET_LIST_CONNECTIONS_DOC(
            String sector1, String sector2, String flight1, String flight2,
            String pertype, String perStart, String perEnd, String dow,
            String priceStrategy, String discountValue, String firstRBDAlloc,
            String otherRBDAlloc, String b2bBackstop, String b2cBackstop,
            String b2bFactor, String skippingFactor, String analystName,
            String uuid, String outboundStop, String outboundDuration,
            String carrierExclusionB2B, String carrierExclusionB2C,
            String currency, String fareAnchor, String offset,
            String discountFlag, String sosFlag) {

        return new Document("Sector1", sector1)
                .append("Sector2", sector2)
                .append("Flight1", flight1)
                .append("Flight2", flight2)
                .append("Pertype", pertype)
                .append("PerStart", perStart)
                .append("PerEnd", perEnd)
                .append("DOW", dow)
                .append("Price_Strategy", priceStrategy)
                .append("Discount_Value", discountValue)
                .append("FirstRBDAlloc", firstRBDAlloc)
                .append("OtherRBDAlloc", otherRBDAlloc)
                .append("B2BBackstop", b2bBackstop)
                .append("B2CBackstop", b2cBackstop)
                .append("B2BFactor", b2bFactor)
                .append("SkippingFactor", skippingFactor)
                .append("analystName", analystName)
                .append("UUID", uuid)
                .append("Outbound_stop", outboundStop)
                .append("Outbound_duration", outboundDuration)
                .append("CarrierExclusionB2B", carrierExclusionB2B)
                .append("CarrierExclusionB2C", carrierExclusionB2C)
                .append("Currency", currency)
                .append("fareAnchor", fareAnchor)
                .append("Offset", offset)
                .append("DiscountFlag", discountFlag)
                .append("sosFlag", sosFlag);
    }

    // SELECT_MARKET_LIST_CONNECTIONS
    public static Bson SELECT_MARKET_LIST_CONNECTIONS_FILTER(
            String sector1, String sector2, String analystName) {
        return Filters.and(
                Filters.eq("Sector1", sector1),
                Filters.eq("Sector2", sector2),
                Filters.eq("analystName", analystName)
        );
    }

    // INSERT_INTO_MARKET_LIST
    public static Document INSERT_INTO_MARKET_LIST_DOC(
            String origin, String destin, String flightNumber, String perType,
            String perStart, String perEnd, String dow, String hardAnchor,
            String plfThreshold, String timeWindowStart, String timeWindowEnd,
            String curveID, String carrExlusionB2B, String carrExlusionB2C,
            String flightExclusionB2B, String flightExclusionB2C,
            String fareAnchor, String fareOffset, String firstRBDAlloc,
            String otherRBDAlloc, String b2bBackstop, String b2cBackstop,
            String b2bFactor, String obSeats, String obFare,
            String skippingFactor, String daySpan, String autoTimeRangeFlag,
            String analystName, String openingFares, String overBooking,
            String rbdPushFlag, String profileFares, String b2cTolerance,
            String b2bTolerance, String distressInventoryFlag,
            String seriesBlock, String autoGroup, String autoBackstopFlag,
            String tbfFlag, String cugFlag, String mktFareFlag, String uuid,
            String directFlag, String tbfEnhFlag, String defaultMlFlag,
            String b2bStrategy, String outboundStop, String marketfareUpsell,
            String forecastAllocFlag) {

        return new Document("Origin", origin)
                .append("Destin", destin)
                .append("FlightNumber", flightNumber)
                .append("PerType", perType)
                .append("PerStart", perStart)
                .append("PerEnd", perEnd)
                .append("DOW", dow)
                .append("hardAnchor", hardAnchor)
                .append("plfThreshold", plfThreshold)
                .append("TimeWindowStart", timeWindowStart)
                .append("TimeWindowEnd", timeWindowEnd)
                .append("CurveID", curveID)
                .append("CarrExlusionB2B", carrExlusionB2B)
                .append("CarrExlusionB2C", carrExlusionB2C)
                .append("flightExclusionB2B", flightExclusionB2B)
                .append("flightExclusionB2C", flightExclusionB2C)
                .append("fareAnchor", fareAnchor)
                .append("fareOffset", fareOffset)
                .append("FirstRBDAlloc", firstRBDAlloc)
                .append("OtherRBDAlloc", otherRBDAlloc)
                .append("B2BBackstop", b2bBackstop)
                .append("B2CBackstop", b2cBackstop)
                .append("B2BFactor", b2bFactor)
                .append("obSeats", obSeats)
                .append("obFare", obFare)
                .append("SkippingFactor", skippingFactor)
                .append("DaySpan", daySpan)
                .append("AutoTimeRangeFlag", autoTimeRangeFlag)
                .append("analystName", analystName)
                .append("openingFares", openingFares)
                .append("OverBooking", overBooking)
                .append("rbdPushFlag", rbdPushFlag)
                .append("profileFares", profileFares)
                .append("B2CTolerance", b2cTolerance)
                .append("B2BTolerance", b2bTolerance)
                .append("distressInventoryFlag", distressInventoryFlag)
                .append("seriesBlock", seriesBlock)
                .append("autoGroup", autoGroup)
                .append("autoBackstopFlag", autoBackstopFlag)
                .append("tbfFlag", tbfFlag)
                .append("CugFlag", cugFlag)
                .append("MktFareFlag", mktFareFlag)
                .append("UUID", uuid)
                .append("directFlag", directFlag)
                .append("TBFEnhFlag", tbfEnhFlag)
                .append("defaultMlFlag", defaultMlFlag)
                .append("b2bStrategy", b2bStrategy)
                .append("Outbound_Stop", outboundStop)
                .append("Marketfare_Upsell", marketfareUpsell)
                .append("forecastAllocFlag", forecastAllocFlag);
    }

    // GET_ROLE_PERMISSION_LIST - Complex aggregation with $lookup
    public static List<Bson> GET_ROLE_PERMISSION_LIST_PIPELINE(List<String> roles) {
        // This requires aggregation pipeline with $lookup (JOIN equivalent)
        // Original SQL uses CROSS JOIN with JSON_TABLE and GROUP_CONCAT
        // MongoDB equivalent uses $lookup and $unwind

        return Arrays.asList(
                Aggregates.match(Filters.in("Role", roles)),
                Aggregates.lookup("config_permissions", "PermissionID", "PermissionID", "permissions"),
                Aggregates.unwind("$permissions"),
                Aggregates.project(Projections.fields(
                        Projections.include("permissions.Permission")
                )),
                Aggregates.group(null,
                        Accumulators.addToSet("permissions", "$permissions.Permission"))
        );
    }

    // GET_STATION_MASTER_LIST
    public static Bson GET_STATION_MASTER_LIST_FILTER(String station1, String station2, String route) {
        return Filters.and(
                Filters.in("station", Arrays.asList(station1, station2)),
                Filters.eq("route", route)
        );
    }

    public static Document GET_STATION_MASTER_LIST_PROJECTION() {
        return new Document("station", 1).append("_id", 0);
    }

    // GET_PARAMETER_VALUES
    public static Bson GET_PARAMETER_VALUES_FILTER(String parameterKey) {
        return Filters.eq("parameterKey", parameterKey);
    }

    public static Document GET_PARAMETER_VALUES_PROJECTION() {
        return new Document("parameterValue", 1).append("_id", 0);
    }

    // GET_COLUMN_HEADERS
    public static Bson GET_COLUMN_HEADERS_FILTER(String tableName) {
        return Filters.eq("tableName", tableName);
    }

    public static Document GET_COLUMN_HEADERS_PROJECTION() {
        return new Document("columns", 1).append("_id", 0);
    }

    // GET_CURRENT_INPUT_STATUS
    public static Bson GET_CURRENT_INPUT_STATUS_FILTER(String name) {
        return Filters.and(
                Filters.eq("name", name),
                Filters.eq("is_running", "1")
        );
    }

    // INSERT_FILE_UPLOAD
    public static Document INSERT_FILE_UPLOAD_DOC(
            String tableName, String currVersion,
            String startTime, String userName) {
        return new Document("tableName", tableName)
                .append("curr_version", currVersion)
                .append("start_time", startTime)
                .append("userName", userName);
    }

    // DELETE_TABLE - In MongoDB, this would be dropping a collection
    public static String DELETE_COLLECTION(String collectionName) {
        // Use: database.getCollection(collectionName).drop();
        return collectionName;
    }

    // GET_INPUT_STATUS_COUNT
    public static Bson GET_INPUT_STATUS_COUNT_FILTER(String name) {
        return Filters.eq("name", name);
    }

    // UPDATE_INPUT_STATUS
    public static Bson UPDATE_INPUT_STATUS_FILTER(String tableName) {
        return Filters.eq("name", tableName);
    }

    public static Bson UPDATE_INPUT_STATUS_UPDATE(int value) {
        return Updates.set("is_running", String.valueOf(value));
    }

    // COPY_DATA_INTO_MAIN_TABLE
    // In MongoDB, this would be done programmatically by reading from temp collection
    // and inserting into main collection, or using aggregation $out/$merge
    public static String COPY_DATA_INTO_MAIN_COLLECTION(String collectionName, String tempCollectionName) {
        // Use aggregation:
        // database.getCollection(tempCollectionName).aggregate(
        //     Arrays.asList(Aggregates.out(collectionName))
        // )
        return collectionName;
    }

    // INSERT_INPUT_STATUS
    public static Document INSERT_INPUT_STATUS_DOC(String name) {
        return new Document("is_running", "1")
                .append("name", name);
    }

    // UPDATE_FILE_UPLOAD
    public static Bson UPDATE_FILE_UPLOAD_FILTER(String currVersion, String tableName) {
        return Filters.and(
                Filters.eq("curr_version", currVersion),
                Filters.eq("tableName", tableName)
        );
    }

    public static Bson UPDATE_FILE_UPLOAD_UPDATE(String endTime) {
        return Updates.set("end_time", endTime);
    }

    // GET_CURRENT_VERSION_COUNT
    public static Bson GET_CURRENT_VERSION_COUNT_FILTER(String tableName) {
        return Filters.eq("tableName", tableName);
    }

    // UPDATE_CURRENT_VERSION
    public static Bson UPDATE_CURRENT_VERSION_FILTER(String tableName) {
        return Filters.eq("tableName", tableName);
    }

    public static Bson UPDATE_CURRENT_VERSION_UPDATE(String currVersion) {
        return Updates.set("curr_version", currVersion);
    }

    // INSERT_CURRENT_VERSION
    public static Document INSERT_CURRENT_VERSION_DOC(String currVersion, String tableName) {
        return new Document("curr_version", currVersion)
                .append("tableName", tableName);
    }

    // GET_STRATEGY_GRID_TIME - Complex aggregation with pivot
    public static List<Bson> GET_STRATEGY_GRID_TIME_PIPELINE(int limit, String value) {
        List<Bson> pipeline = new ArrayList<>();

        // Match stage (WHERE clause)
        if (value != null) {
            pipeline.add(Aggregates.match(Filters.eq("strategy", value)));
        }

        // Lookup to join with ndo_bands
        pipeline.add(Aggregates.lookup("ndo_bands", "ndo_band", "ndo_band", "ndo_data"));
        pipeline.add(Aggregates.unwind("$ndo_data"));

        // Group and create pivot-like structure
        Document groupFields = new Document("channel", "$channel")
                .append("strategy", "$strategy")
                .append("Band", "$ndo_band");

        Document groupAccumulators = new Document("_id", groupFields)
                .append("Decision", new Document("$literal", "Time"));

        // Add dynamic fields for each dplf_band (0 to limit-1)
        for (int i = 0; i < limit; i++) {
            groupAccumulators.append(String.valueOf(i),
                    new Document("$max",
                            new Document("$cond", Arrays.asList(
                                    new Document("$eq", Arrays.asList("$dplf_band", i)),
                                    "$time_range",
                                    null
                            ))
                    )
            );
        }

        pipeline.add(new Document("$group", groupAccumulators));

        // Project to flatten structure
        Document projectFields = new Document("_id", 0)
                .append("channel", "$_id.channel")
                .append("strategy", "$_id.strategy")
                .append("Decision", "$Decision")
                .append("Band", "$_id.Band");

        for (int i = 0; i < limit; i++) {
            projectFields.append(String.valueOf(i), "$" + i);
        }

        pipeline.add(Aggregates.project(projectFields));

        return pipeline;
    }

    // GET_STRATEGY_GRID_CRITERIA
    public static List<Bson> GET_STRATEGY_GRID_CRITERIA_PIPELINE(int limit, String value) {
        List<Bson> pipeline = new ArrayList<>();

        if (value != null) {
            pipeline.add(Aggregates.match(Filters.eq("strategy", value)));
        }

        pipeline.add(Aggregates.lookup("ndo_bands", "ndo_band", "ndo_band", "ndo_data"));
        pipeline.add(Aggregates.unwind("$ndo_data"));

        Document groupFields = new Document("channel", "$channel")
                .append("strategy", "$strategy")
                .append("Band", "$ndo_band");

        Document groupAccumulators = new Document("_id", groupFields)
                .append("Decision", new Document("$literal", "Criteria"));

        for (int i = 0; i < limit; i++) {
            groupAccumulators.append(String.valueOf(i),
                    new Document("$max",
                            new Document("$cond", Arrays.asList(
                                    new Document("$eq", Arrays.asList("$dplf_band", i)),
                                    "$criteria",
                                    null
                            ))
                    )
            );
        }

        pipeline.add(new Document("$group", groupAccumulators));

        Document projectFields = new Document("_id", 0)
                .append("channel", "$_id.channel")
                .append("strategy", "$_id.strategy")
                .append("Decision", "$Decision")
                .append("Band", "$_id.Band");

        for (int i = 0; i < limit; i++) {
            projectFields.append(String.valueOf(i), "$" + i);
        }

        pipeline.add(Aggregates.project(projectFields));

        return pipeline;
    }

    // GET_STRATEGY_GRID_OFFSET
    public static List<Bson> GET_STRATEGY_GRID_OFFSET_PIPELINE(int limit, String value) {
        List<Bson> pipeline = new ArrayList<>();

        if (value != null) {
            pipeline.add(Aggregates.match(Filters.eq("strategy", value)));
        }

        pipeline.add(Aggregates.lookup("ndo_bands", "ndo_band", "ndo_band", "ndo_data"));
        pipeline.add(Aggregates.unwind("$ndo_data"));

        Document groupFields = new Document("channel", "$channel")
                .append("strategy", "$strategy")
                .append("Band", "$ndo_band");

        Document groupAccumulators = new Document("_id", groupFields)
                .append("Decision", new Document("$literal", "Offset"));

        for (int i = 0; i < limit; i++) {
            groupAccumulators.append(String.valueOf(i),
                    new Document("$max",
                            new Document("$cond", Arrays.asList(
                                    new Document("$eq", Arrays.asList("$dplf_band", i)),
                                    "$offset",
                                    null
                            ))
                    )
            );
        }

        pipeline.add(new Document("$group", groupAccumulators));

        Document projectFields = new Document("_id", 0)
                .append("channel", "$_id.channel")
                .append("strategy", "$_id.strategy")
                .append("Decision", "$Decision")
                .append("Band", "$_id.Band");

        for (int i = 0; i < limit; i++) {
            projectFields.append(String.valueOf(i), "$" + i);
        }

        pipeline.add(Aggregates.project(projectFields));

        return pipeline;
    }

    // GET_STRATEGY_GRID - UNION ALL equivalent using $unionWith
    public static List<Bson> GET_STRATEGY_GRID_PIPELINE(int limit, String value) {
        // MongoDB 4.4+ supports $unionWith for UNION ALL equivalent
        List<Bson> pipeline = new ArrayList<>();

        // First get time data
        pipeline.addAll(GET_STRATEGY_GRID_TIME_PIPELINE(limit, value));

        // Union with criteria data
        pipeline.add(new Document("$unionWith",
                new Document("coll", "d1_d2_strategies")
                        .append("pipeline", GET_STRATEGY_GRID_CRITERIA_PIPELINE(limit, value))
        ));

        // Union with offset data
        pipeline.add(new Document("$unionWith",
                new Document("coll", "d1_d2_strategies")
                        .append("pipeline", GET_STRATEGY_GRID_OFFSET_PIPELINE(limit, value))
        ));

        // Sort the combined results
        pipeline.add(Aggregates.sort(
                Sorts.orderBy(
                        Sorts.ascending("strategy"),
                        Sorts.ascending("channel"),
                        Sorts.ascending("Decision"),
                        Sorts.ascending("Band")
                )
        ));

        return pipeline;
    }

    // GET_AUTO_B2B_FACTOR_GRID
    public static List<Bson> GET_AUTO_B2B_FACTOR_GRID_PIPELINE(int limit) {
        List<Bson> pipeline = new ArrayList<>();

        Document groupFields = new Document("name", "$name")
                .append("ndo_band", "$ndo_band")
                .append("forecast_band", "$forecast_band");

        Document groupAccumulators = new Document("_id", groupFields);

        for (int i = 0; i < limit; i++) {
            groupAccumulators.append(String.valueOf(i),
                    new Document("$max",
                            new Document("$cond", Arrays.asList(
                                    new Document("$eq", Arrays.asList("$dplf_band", i)),
                                    "$b2b_factor",
                                    null
                            ))
                    )
            );
        }

        pipeline.add(new Document("$group", groupAccumulators));

        Document projectFields = new Document("_id", 0)
                .append("name", "$_id.name")
                .append("ndo_band", "$_id.ndo_band")
                .append("forecast_band", "$_id.forecast_band");

        for (int i = 0; i < limit; i++) {
            projectFields.append(String.valueOf(i), "$" + i);
        }

        pipeline.add(Aggregates.project(projectFields));

        return pipeline;
    }

    // INPUT_FILE_UPLOAD_INSERT
    public static Bson INPUT_FILE_UPLOAD_INSERT_FILTER(String objId) {
        return Filters.eq("obj_id", objId);
    }

    public static Document INPUT_FILE_UPLOAD_INSERT_PROJECTION() {
        return new Document("tableName", 1)
                .append("role", 1)
                .append("username", 1)
                .append("name", 1)
                .append("init_timestamp", 1)
                .append("route_for_adhoc", 1)
                .append("_id", 0);
    }

    // INPUT_FILE_UPLOAD_UPDATE
    public static Bson INPUT_FILE_UPLOAD_UPDATE_FILTER(String objId) {
        return Filters.eq("obj_id", objId);
    }

    public static Bson INPUT_FILE_UPLOAD_UPDATE_UPDATE(String status, String inprogressTimestamp) {
        return Updates.combine(
                Updates.set("status", status),
                Updates.set("inprogress_timestamp", inprogressTimestamp)
        );
    }

    // INPUT_FILE_UPLOAD_TIME_TAKEN_UPDATE
    public static Bson INPUT_FILE_UPLOAD_TIME_TAKEN_UPDATE_UPDATE(
            String status, String doneTimestamp, int totalRows, String totalTimeTaken) {
        return Updates.combine(
                Updates.set("status", status),
                Updates.set("done_timestamp", doneTimestamp),
                Updates.set("total_rows", totalRows),
                Updates.set("total_time_taken", totalTimeTaken)
        );
    }

    // INPUT_FILE_UPLOAD_ERROR_OCCURRED
    public static Bson INPUT_FILE_UPLOAD_ERROR_OCCURRED_UPDATE(String errors) {
        return Updates.set("errors", errors);
    }

    // COUNT_OF_RECORDS - Use collection.countDocuments()
    public static long COUNT_OF_RECORDS(String collectionName) {
        // Use: database.getCollection(collectionName).countDocuments();
        return 0; // Placeholder
    }

    // GET_USER_SECTOR_MAP
    public static Bson GET_USER_SECTOR_MAP_FILTER(String user, String sector) {
        return Filters.eq("sector", sector);
    }

    public static Document GET_USER_SECTOR_MAP_PROJECTION(String user) {
        return new Document(user, 1).append("_id", 0);
    }

    // INSERT_INTO_MARKET_LIST_SALESINV
    public static Document INSERT_INTO_MARKET_LIST_SALESINV_DOC(
            String origin, String destin, String flightNumber,
            String perStart, String perEnd, String dow,
            String curveID, String saleflag) {
        return new Document("Origin", origin)
                .append("Destin", destin)
                .append("FlightNumber", flightNumber)
                .append("PerStart", perStart)
                .append("PerEnd", perEnd)
                .append("DOW", dow)
                .append("CurveID", curveID)
                .append("saleflag", saleflag);
    }

    // INSERT_INTO_ML_AUDIT_LOGS
    public static Document INSERT_INTO_ML_AUDIT_LOGS_DOC(
            String sector, String date, String flightNo,
            String userName, String changedAt) {
        return new Document("sector", sector)
                .append("date", date)
                .append("flightNo", flightNo)
                .append("userName", userName)
                .append("changedAt", changedAt);
    }

    // For INSERT IGNORE behavior, use updateOne with upsert option
    public static Bson INSERT_INTO_ML_AUDIT_LOGS_FILTER(
            String sector, String date, String flightNo,
            String userName, String changedAt) {
        return Filters.and(
                Filters.eq("sector", sector),
                Filters.eq("date", date),
                Filters.eq("flightNo", flightNo),
                Filters.eq("userName", userName),
                Filters.eq("changedAt", changedAt)
        );
    }
}
