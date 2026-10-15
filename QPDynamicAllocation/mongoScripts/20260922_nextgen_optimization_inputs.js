/**
 * MongoDB Configuration & Seed Script for NextGen AU Optimization & Competition Response
 * Database: qp_dw_rmalloc
 *
 * Operations included:
 * 1. Collection creations for comp_response_grid, comp_response_plf_bands, comp_fare_recalibration, summary_nextgen, inventory_optimization, rm_parameter_values
 * 2. Seed data for comp_response_plf_bands (6 bands: <80% to 100+%)
 * 3. Seed data for comp_response_grid (48 DEFAULT and DEFAULT_AU grid rules)
 * 4. Seed data for comp_fare_recalibration (default sector offsets)
 * 5. Update config_column_names for market_list (all 3 flavors) and all NextGen tables
 * 6. Update config_inputs for UI screen display and permissions
 * 7. Update NextGen default values for all records in market_list, market_list_international, and market_list_adhoc_* (excluding collections containing 'connection')
 * 8. Seed/Update rm_parameter_values with DEFAULT_MARKET_LIST including NextGen columns
 */

db = db.getSiblingDB('qp_dw_rmalloc');

// ==========================================
// 1. Create Collections
// ==========================================
var collectionsToCreate = [
    'comp_response_grid',
    'comp_response_plf_bands',
    'comp_fare_recalibration',
    'summary_nextgen',
    'inventory_optimization',
    'rm_parameter_values'
];

collectionsToCreate.forEach(function (colName) {
    if (!db.getCollectionNames().includes(colName)) {
        db.createCollection(colName);
        print("Created collection: " + colName);
    }
});

// ==========================================
// 2. Seed comp_response_plf_bands
// ==========================================
var defaultPlfBands = [
    { "id": 0, "band_name": "<80%", "start": -9999.0, "end": 80.0 },
    { "id": 1, "band_name": "80-85%", "start": 80.0, "end": 85.0 },
    { "id": 2, "band_name": "85-90%", "start": 85.0, "end": 90.0 },
    { "id": 3, "band_name": "90-95%", "start": 90.0, "end": 95.0 },
    { "id": 4, "band_name": "95-100%", "start": 95.0, "end": 100.0 },
    { "id": 5, "band_name": "100+%", "start": 100.0, "end": 9999.0 }
];

if (db.comp_response_plf_bands.countDocuments({}) === 0) {
    db.comp_response_plf_bands.insertMany(defaultPlfBands);
    print("Seeded default rows for comp_response_plf_bands");
} else {
    // Ensure both id and plf_band exist on existing documents
    db.comp_response_plf_bands.find({ id: { $exists: false }, plf_band: { $exists: true } }).forEach(function(doc) {
        db.comp_response_plf_bands.updateOne({ _id: doc._id }, { $set: { id: doc.plf_band } });
    });
    db.comp_response_plf_bands.find({ plf_band: { $exists: false }, id: { $exists: true } }).forEach(function(doc) {
        db.comp_response_plf_bands.updateOne({ _id: doc._id }, { $set: { plf_band: doc.id } });
    });
    // Remove threshold_factor from existing documents in comp_response_plf_bands
    db.comp_response_plf_bands.updateMany({}, { $unset: { "threshold_factor": "" } });
}

// ==========================================
// 3. Seed comp_response_grid (48 DEFAULT_AU + 48 DEFAULT = 96 rules)
// ==========================================
var defaultAuGridRules = [
    { "grid_name": "DEFAULT_AU", "ndo_band": 0, "plf_band": 0, "time_range": "FR", "criteria": "Min", "response_rule": "Min (2A, 0.9C)", "tolerance": 0.7 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 0, "plf_band": 1, "time_range": "ExTR", "criteria": "Min", "response_rule": "Min (2A, 0.95C)", "tolerance": 0.8 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 0, "plf_band": 2, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, 0.95C)", "tolerance": 0.9 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 0, "plf_band": 3, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2.5A, C)", "tolerance": 0.95 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 0, "plf_band": 4, "time_range": "TR", "criteria": "Min", "response_rule": "Min (3A, C)", "tolerance": 1.0 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 0, "plf_band": 5, "time_range": "TR", "criteria": "Min", "response_rule": "Min (3A, 1.2C)", "tolerance": 1.0 },

    { "grid_name": "DEFAULT_AU", "ndo_band": 1, "plf_band": 0, "time_range": "FR", "criteria": "Min", "response_rule": "Min (2A, 0.9C)", "tolerance": 0.7 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 1, "plf_band": 1, "time_range": "ExTR", "criteria": "Min", "response_rule": "Min (2A, 0.95C)", "tolerance": 0.8 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 1, "plf_band": 2, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, 0.95C)", "tolerance": 0.9 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 1, "plf_band": 3, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2.5A, C)", "tolerance": 0.95 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 1, "plf_band": 4, "time_range": "TR", "criteria": "Min", "response_rule": "Min (3A, C)", "tolerance": 1.0 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 1, "plf_band": 5, "time_range": "TR", "criteria": "Min", "response_rule": "Min (3A, 1.2C)", "tolerance": 1.0 },

    { "grid_name": "DEFAULT_AU", "ndo_band": 2, "plf_band": 0, "time_range": "FR", "criteria": "Min", "response_rule": "Min (2A, 0.9C)", "tolerance": 0.7 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 2, "plf_band": 1, "time_range": "ExTR", "criteria": "Min", "response_rule": "Min (2A, 0.95C)", "tolerance": 0.8 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 2, "plf_band": 2, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, C)", "tolerance": 0.9 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 2, "plf_band": 3, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2.25A, C)", "tolerance": 0.95 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 2, "plf_band": 4, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2.5A, C)", "tolerance": 1.0 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 2, "plf_band": 5, "time_range": "TR", "criteria": "Min", "response_rule": "Min (3A, 1.2C)", "tolerance": 1.0 },

    { "grid_name": "DEFAULT_AU", "ndo_band": 3, "plf_band": 0, "time_range": "FR", "criteria": "Min", "response_rule": "Min (2A, 0.9C)", "tolerance": 0.7 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 3, "plf_band": 1, "time_range": "ExTR", "criteria": "Min", "response_rule": "Min (2A, 0.95C)", "tolerance": 0.8 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 3, "plf_band": 2, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, C)", "tolerance": 0.9 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 3, "plf_band": 3, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2.25A, C)", "tolerance": 0.95 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 3, "plf_band": 4, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2.5A, C)", "tolerance": 1.0 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 3, "plf_band": 5, "time_range": "TR", "criteria": "Min", "response_rule": "Min (3A, 1.2C)", "tolerance": 1.0 },

    { "grid_name": "DEFAULT_AU", "ndo_band": 4, "plf_band": 0, "time_range": "FR", "criteria": "Min", "response_rule": "Min (2A, 0.9C)", "tolerance": 0.7 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 4, "plf_band": 1, "time_range": "ExTR", "criteria": "Min", "response_rule": "Min (2A, 0.95C)", "tolerance": 0.8 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 4, "plf_band": 2, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, C)", "tolerance": 0.9 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 4, "plf_band": 3, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2.25A, C)", "tolerance": 0.95 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 4, "plf_band": 4, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2.5A, C)", "tolerance": 1.0 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 4, "plf_band": 5, "time_range": "TR", "criteria": "Min", "response_rule": "Min (3A, 1.2C)", "tolerance": 1.0 },

    { "grid_name": "DEFAULT_AU", "ndo_band": 5, "plf_band": 0, "time_range": "FR", "criteria": "Min", "response_rule": "Min (2A, 0.9C)", "tolerance": 0.7 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 5, "plf_band": 1, "time_range": "ExTR", "criteria": "Min", "response_rule": "Min (2A, 0.9C)", "tolerance": 0.8 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 5, "plf_band": 2, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, C)", "tolerance": 0.9 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 5, "plf_band": 3, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, C)", "tolerance": 0.95 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 5, "plf_band": 4, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2.5A, C)", "tolerance": 1.0 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 5, "plf_band": 5, "time_range": "TR", "criteria": "Min", "response_rule": "Min (3A, 1.2C)", "tolerance": 1.0 },

    { "grid_name": "DEFAULT_AU", "ndo_band": 6, "plf_band": 0, "time_range": "FR", "criteria": "Min", "response_rule": "Min (2A, 0.85C)", "tolerance": 0.7 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 6, "plf_band": 1, "time_range": "ExTR", "criteria": "Min", "response_rule": "Min (2A, 0.9C)", "tolerance": 0.8 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 6, "plf_band": 2, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, 0.95C)", "tolerance": 0.9 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 6, "plf_band": 3, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, C)", "tolerance": 0.95 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 6, "plf_band": 4, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2.5A, C)", "tolerance": 1.0 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 6, "plf_band": 5, "time_range": "TR", "criteria": "Min", "response_rule": "Min (3A, 1.2C)", "tolerance": 1.0 },

    { "grid_name": "DEFAULT_AU", "ndo_band": 7, "plf_band": 0, "time_range": "FR", "criteria": "Min", "response_rule": "Min (2A, 0.85C)", "tolerance": 0.7 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 7, "plf_band": 1, "time_range": "ExTR", "criteria": "Min", "response_rule": "Min (2A, 0.9C)", "tolerance": 0.8 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 7, "plf_band": 2, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, 0.95C)", "tolerance": 0.9 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 7, "plf_band": 3, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, C)", "tolerance": 0.95 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 7, "plf_band": 4, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2.5A, C)", "tolerance": 1.0 },
    { "grid_name": "DEFAULT_AU", "ndo_band": 7, "plf_band": 5, "time_range": "TR", "criteria": "Min", "response_rule": "Min (3A, 1.2C)", "tolerance": 1.0 }
];

var defaultGridRules = defaultAuGridRules.map(function (r) {
    var copy = Object.assign({}, r);
    copy.grid_name = "DEFAULT";
    copy.tolerance = 1.0;
    return copy;
});

var allGridRules = defaultAuGridRules.concat(defaultGridRules);

db.comp_response_grid.deleteMany({ "grid_name": { $in: ["DEFAULT", "DEFAULT_AU"] } });
db.comp_response_grid.insertMany(allGridRules);
print("Seeded 96 default rows for comp_response_grid (48 DEFAULT_AU and 48 DEFAULT)");

// ==========================================
// 4. Seed comp_fare_recalibration
// ==========================================
var defaultRecalibration = [
    { "Sector": "BOMDEL", "IX": 2000, "6E": 0, "SG": 0, "AI": 0 },
    { "Sector": "DELGOX", "IX": 0, "6E": 0, "SG": 0, "AI": 0 },
    { "Sector": "BLRGAU", "IX": 0, "6E": 0, "SG": 0, "AI": 0 }
];

if (db.comp_fare_recalibration.countDocuments({}) === 0) {
    db.comp_fare_recalibration.insertMany(defaultRecalibration);
    print("Seeded default rows for comp_fare_recalibration");
}

// ==========================================
// 5. Update config_column_names
// ==========================================
var marketListColumns = "Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,CugFlag,MktFareFlag,directFlag,TBFEnhFlag,defaultMlFlag,b2bStrategy,Outbound_Stop,Marketfare_Upsell,forecastAllocFlag,Spoilage_Flag,CanAdjustedForecast,market_station_mapping,afForecastAllocFlag,ProfileFareUpsell,incrementedProfileFareFlag,auto_tolerance_flag,buy_up_factor_NG,connection_fare_flag_NG,additional_connection_duration_NG,CompetitionResponseGrid_NG,nextgen_flag_NG,competition_response_flag_NG,PromoFlag_NG,analystName";

var nextgenSummaryColumns = "Origin,Destin,FltNum,DepDate,Capacity,NG_FirstRBD,Predicted_Flown_Revised,MarketFare_Selected,MarketFare_Min,MarketFare_Max,Grid_Name,Rule_Applied,nextgen_flag,competition_response_flag,buy_up_factor,RunId,Id,NG_Historical_current_booked,NG_LF_Forecast,NG_AF_Forecast,NG_Opening_fare,NG_Strategy_fare,NG_Anchor_fare,analystName,status,allocationStatus";

var inventoryOptimizationColumns = "sector,date,DOW,STD,flight,NDO,capacity,Total_Bookings,fare,fare_All_in,oal_b2c_min,oal_b2c_min_All_in,anchor_fare,anchor_fare_All_in,strategy_fare,strategy_fare_All_in,opening fare,opening fare_All_in,Promo_Flag,mu_bar_orig,mu,AU,P_eq,mu_bar_2,mu_2,AU_2,AU_Revised,predicted_pickup,Predicted_Flown,Predicted_pickup_Revised,Predicted_Flown_Revised,LF_Forecast_Revised,AF_Forecast,Distance,ASKM,RPKM,RASK_Forecast,Current_Revenue,Expected_Revenue,Total_Expected_Revenue,Predicted_Revenue,SnapshotDate,Tagging,FareBucket,RunId";

db.config_column_names.deleteMany({
    "tableName": {
        $in: [
            "market_list",
            "market_list_international",
            "market_list_s3",
            "comp_response_grid",
            "comp_response_plf_bands",
            "comp_fare_recalibration",
            "summary_nextgen",
            "inventory_optimization"
        ]
    }
});

db.config_column_names.insertMany([
    { "tableName": "market_list", "columns": marketListColumns },
    { "tableName": "market_list_international", "columns": marketListColumns },
    { "tableName": "market_list_s3", "columns": marketListColumns },
    { "tableName": "comp_response_grid", "columns": "grid_name,ndo_band,plf_band,time_range,criteria,response_rule,tolerance" },
    { "tableName": "comp_response_plf_bands", "columns": "id,band_name,start,end" },
    { "tableName": "comp_fare_recalibration", "columns": "Sector,IX,6E,SG,AI" },
    { "tableName": "summary_nextgen", "columns": nextgenSummaryColumns },
    { "tableName": "inventory_optimization", "columns": inventoryOptimizationColumns }
]);
print("Updated config_column_names for market_list and NextGen tables (including inventory_optimization)");

// ==========================================
// 6. Update config_inputs (UI Input Screen Configuration)
// ==========================================
db.config_inputs.deleteMany({
    "tableName": {
        $in: [
            "comp_response_grid",
            "comp_response_plf_bands",
            "comp_fare_recalibration",
            "inventory_optimization"
        ]
    }
});

db.config_inputs.insertMany([
    {
        "tableName": "comp_response_grid",
        "value": "comp_response_grid",
        "title": "Competition Response Grid",
        "view_permission": "view_strategy",
        "upload_permission": "update_strategy",
        "message": "Upload Competition Response Grid configuration CSV file",
        "route": null,
        "order": 50
    },
    {
        "tableName": "comp_response_plf_bands",
        "value": "comp_response_plf_bands",
        "title": "Competition Response PLF Bands",
        "view_permission": "view_forecast_bands",
        "upload_permission": "update_forecast_bands",
        "message": "Upload Competition Response PLF Bands configuration CSV file",
        "route": null,
        "order": 51
    },
    {
        "tableName": "comp_fare_recalibration",
        "value": "comp_fare_recalibration",
        "title": "Competition Fare Recalibration",
        "view_permission": "view_market_fare_adjustments",
        "upload_permission": "update_market_fare_adjustments",
        "message": "Upload Competition Fare Recalibration configuration CSV file",
        "route": null,
        "order": 52
    }
]);

print("Successfully configured NextGen tables in config_inputs and config_column_names.");

// ==========================================
// 7. Update NextGen Default Columns in Market List Collections
// ==========================================
var nextGenMarketListDefaults = {
    "nextgen_flag_NG": 0,
    "competition_response_flag_NG": 1,
    "PromoFlag_NG": 0,
    "connection_fare_flag_NG": 0,
    "buy_up_factor_NG": 0,
    "additional_connection_duration_NG": 0,
    "CompetitionResponseGrid_NG": "DEFAULT_AU"
};

var allCollections = db.getCollectionNames();
var targetMarketListCollections = ['market_list', 'market_list_international'];

allCollections.forEach(function (colName) {
    if (colName.startsWith('market_list_adhoc')) {
        var lower = colName.toLowerCase();
        if (!lower.includes('connection')) {
            if (!targetMarketListCollections.includes(colName)) {
                targetMarketListCollections.push(colName);
            }
        }
    }
});

print("Target market list collections to update: " + JSON.stringify(targetMarketListCollections));

targetMarketListCollections.forEach(function (colName) {
    if (db.getCollectionNames().includes(colName)) {
        var updateResult = db.getCollection(colName).updateMany(
            {},
            {
                $set: nextGenMarketListDefaults
            }
        );
        print("Updated " + colName + ": matched " + updateResult.matchedCount + ", modified " + updateResult.modifiedCount);
    }
});

// ==========================================
// 8. Update rm_parameter_values (DEFAULT_MARKET_LIST)
// ==========================================
var defaultMarketListConfig = {
    "Origin": "ABC",
    "Destin": "XYZ",
    "PerStart": "mm/dd/yyyy",
    "PerEnd": "mm/dd/yyyy",
    "FlightNumber": 1234,
    "analystName": "System",
    "DOW": 9999999,
    "PerType": "D",
    "TimeWindowStart": "00:05",
    "TimeWindowEnd": "23:55",
    "CurveID": "Default-MM-30%",
    "CarrExlusionB2C": "SG",
    "CarrExlusionB2B": "SG",
    "flightExclusionB2C": "",
    "flightExclusionB2B": "",
    "fareAnchor": "Default-Metro_Med",
    "hardAnchor": "",
    "plfThreshold": "",
    "fareOffset": "",
    "FirstRBDAlloc": 5,
    "OtherRBDAlloc": 5,
    "SkippingFactor": 10,
    "B2BTolerance": "0",
    "B2CTolerance": "100",
    "B2BBackstop": 3500,
    "B2CBackstop": 3500,
    "B2BFactor": 0,
    "obSeats": "",
    "obFare": "",
    "DaySpan": 0,
    "AutoTimeRangeFlag": 0,
    "openingFares": 1,
    "OverBooking": 1,
    "profileFares": 1,
    "rbdPushFlag": 0,
    "distressInventoryFlag": 0,
    "seriesBlock": 0,
    "autoGroup": 0,
    "autoBackstopFlag": 0,
    "tbfFlag": 1,
    "CugFlag": 0,
    "MktFareFlag": 1,
    "directFlag": 1,
    "TBFEnhFlag": 0,
    "UUID": "uuid",
    "createdAt": "createdAt",
    "defaultMlFlag": 1,
    "b2bStrategy": "",
    "Outbound_Stop": 0,
    "Marketfare_Upsell": 0,
    "forecastAllocFlag": 0,
    "Spoilage_Flag": 0,
    "CanAdjustedForecast": 0,
    "market_station_mapping": 0,
    "afForecastAllocFlag": 0,
    "incrementedProfileFareFlag": 0,
    "ProfileFareUpsell": 0,
    "auto_tolerance_flag": 0,
    "buy_up_factor_NG": 0,
    "connection_fare_flag_NG": 0,
    "additional_connection_duration_NG": 0,
    "CompetitionResponseGrid_NG": "DEFAULT_AU",
    "nextgen_flag_NG": 0,
    "competition_response_flag_NG": 1,
    "PromoFlag_NG": 0
};

var defaultMarketListParamValue = JSON.stringify(defaultMarketListConfig);

var targetObjectId = (typeof ObjectId !== 'undefined') ? ObjectId("6a5088fed08bce60a96181ea") : "6a5088fed08bce60a96181ea";

var existingDefaultMl = db.rm_parameter_values.findOne({
    $or: [
        { "parameterKey": "DEFAULT_MARKET_LIST" },
        { "_id": targetObjectId }
    ]
});

if (existingDefaultMl) {
    if (existingDefaultMl.parameterValue) {
        try {
            var parsed = JSON.parse(existingDefaultMl.parameterValue);
            parsed.buy_up_factor_NG = 0;
            parsed.connection_fare_flag_NG = 0;
            parsed.additional_connection_duration_NG = 0;
            parsed.CompetitionResponseGrid_NG = "DEFAULT_AU";
            parsed.nextgen_flag_NG = 0;
            parsed.competition_response_flag_NG = 1;
            parsed.PromoFlag_NG = 0;
            defaultMarketListParamValue = JSON.stringify(parsed);
        } catch (e) {
            defaultMarketListParamValue = JSON.stringify(defaultMarketListConfig);
        }
    }
    db.rm_parameter_values.updateOne(
        { "_id": existingDefaultMl._id },
        {
            $set: {
                "feature": "misc",
                "parameterKey": "DEFAULT_MARKET_LIST",
                "parameterValue": defaultMarketListParamValue
            }
        }
    );
    print("Updated DEFAULT_MARKET_LIST in rm_parameter_values");
} else {
    db.rm_parameter_values.insertOne({
        "_id": targetObjectId,
        "feature": "misc",
        "parameterKey": "DEFAULT_MARKET_LIST",
        "parameterValue": defaultMarketListParamValue
    });
    print("Inserted DEFAULT_MARKET_LIST in rm_parameter_values");
}

print("Successfully updated market list collections and rm_parameter_values with NextGen defaults.");

