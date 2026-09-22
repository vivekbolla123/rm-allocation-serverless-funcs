/**
 * MongoDB Configuration & Seed Script for NextGen AU Optimization & Competition Response
 * Database: qp_dw_rmalloc
 *
 * Operations included:
 * 1. Collection creations for comp_response_grid, comp_response_plf_bands, comp_fare_recalibration, summary_nextgen, inventory_optimization
 * 2. Seed data for comp_response_plf_bands (6 bands: <80% to 100+%)
 * 3. Seed data for comp_response_grid (48 DEFAULT grid rules)
 * 4. Seed data for comp_fare_recalibration (default sector offsets)
 * 5. Update config_column_names for market_list (all 3 flavors) and all NextGen tables
 * 6. Update config_inputs for UI screen display and permissions
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
    'inventory_optimization'
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
}

// ==========================================
// 3. Seed comp_response_grid (48 DEFAULT combinations)
// ==========================================
var defaultGridRules = [
    { "grid_name": "DEFAULT", "ndo_band": 0, "plf_band": 0, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, 0.9C)" },
    { "grid_name": "DEFAULT", "ndo_band": 0, "plf_band": 1, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, 0.95C)" },
    { "grid_name": "DEFAULT", "ndo_band": 0, "plf_band": 2, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, 0.95C)" },
    { "grid_name": "DEFAULT", "ndo_band": 0, "plf_band": 3, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2.5A, C)" },
    { "grid_name": "DEFAULT", "ndo_band": 0, "plf_band": 4, "time_range": "TR", "criteria": "Min", "response_rule": "Min (3A, C)" },
    { "grid_name": "DEFAULT", "ndo_band": 0, "plf_band": 5, "time_range": "TR", "criteria": "Med", "response_rule": "Min (3A, C)" },

    { "grid_name": "DEFAULT", "ndo_band": 1, "plf_band": 0, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, 0.9C)" },
    { "grid_name": "DEFAULT", "ndo_band": 1, "plf_band": 1, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, 0.95C)" },
    { "grid_name": "DEFAULT", "ndo_band": 1, "plf_band": 2, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, 0.95C)" },
    { "grid_name": "DEFAULT", "ndo_band": 1, "plf_band": 3, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2.5A, C)" },
    { "grid_name": "DEFAULT", "ndo_band": 1, "plf_band": 4, "time_range": "TR", "criteria": "Min", "response_rule": "Min (3A, C)" },
    { "grid_name": "DEFAULT", "ndo_band": 1, "plf_band": 5, "time_range": "TR", "criteria": "Med", "response_rule": "Min (3A, C)" },

    { "grid_name": "DEFAULT", "ndo_band": 2, "plf_band": 0, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, 0.9C)" },
    { "grid_name": "DEFAULT", "ndo_band": 2, "plf_band": 1, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, 0.95C)" },
    { "grid_name": "DEFAULT", "ndo_band": 2, "plf_band": 2, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, C)" },
    { "grid_name": "DEFAULT", "ndo_band": 2, "plf_band": 3, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2.25A, C)" },
    { "grid_name": "DEFAULT", "ndo_band": 2, "plf_band": 4, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2.5A, C)" },
    { "grid_name": "DEFAULT", "ndo_band": 2, "plf_band": 5, "time_range": "TR", "criteria": "Med", "response_rule": "Min (3A, C)" },

    { "grid_name": "DEFAULT", "ndo_band": 3, "plf_band": 0, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, 0.9C)" },
    { "grid_name": "DEFAULT", "ndo_band": 3, "plf_band": 1, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, 0.95C)" },
    { "grid_name": "DEFAULT", "ndo_band": 3, "plf_band": 2, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, C)" },
    { "grid_name": "DEFAULT", "ndo_band": 3, "plf_band": 3, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2.25A, C)" },
    { "grid_name": "DEFAULT", "ndo_band": 3, "plf_band": 4, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2.5A, C)" },
    { "grid_name": "DEFAULT", "ndo_band": 3, "plf_band": 5, "time_range": "TR", "criteria": "Med", "response_rule": "Min (3A, C)" },

    { "grid_name": "DEFAULT", "ndo_band": 4, "plf_band": 0, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, 0.9C)" },
    { "grid_name": "DEFAULT", "ndo_band": 4, "plf_band": 1, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, 0.95C)" },
    { "grid_name": "DEFAULT", "ndo_band": 4, "plf_band": 2, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, C)" },
    { "grid_name": "DEFAULT", "ndo_band": 4, "plf_band": 3, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2.25A, C)" },
    { "grid_name": "DEFAULT", "ndo_band": 4, "plf_band": 4, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2.5A, C)" },
    { "grid_name": "DEFAULT", "ndo_band": 4, "plf_band": 5, "time_range": "TR", "criteria": "Med", "response_rule": "Min (3A, C)" },

    { "grid_name": "DEFAULT", "ndo_band": 5, "plf_band": 0, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, 0.9C)" },
    { "grid_name": "DEFAULT", "ndo_band": 5, "plf_band": 1, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, 0.9C)" },
    { "grid_name": "DEFAULT", "ndo_band": 5, "plf_band": 2, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, C)" },
    { "grid_name": "DEFAULT", "ndo_band": 5, "plf_band": 3, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, C)" },
    { "grid_name": "DEFAULT", "ndo_band": 5, "plf_band": 4, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2.5A, C)" },
    { "grid_name": "DEFAULT", "ndo_band": 5, "plf_band": 5, "time_range": "TR", "criteria": "Med", "response_rule": "Min (3A, C)" },

    { "grid_name": "DEFAULT", "ndo_band": 6, "plf_band": 0, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, 0.85C)" },
    { "grid_name": "DEFAULT", "ndo_band": 6, "plf_band": 1, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, 0.9C)" },
    { "grid_name": "DEFAULT", "ndo_band": 6, "plf_band": 2, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, 0.95C)" },
    { "grid_name": "DEFAULT", "ndo_band": 6, "plf_band": 3, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, C)" },
    { "grid_name": "DEFAULT", "ndo_band": 6, "plf_band": 4, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2.5A, C)" },
    { "grid_name": "DEFAULT", "ndo_band": 6, "plf_band": 5, "time_range": "TR", "criteria": "Med", "response_rule": "Min (3A, C)" },

    { "grid_name": "DEFAULT", "ndo_band": 7, "plf_band": 0, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, 0.85C)" },
    { "grid_name": "DEFAULT", "ndo_band": 7, "plf_band": 1, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, 0.9C)" },
    { "grid_name": "DEFAULT", "ndo_band": 7, "plf_band": 2, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, 0.95C)" },
    { "grid_name": "DEFAULT", "ndo_band": 7, "plf_band": 3, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2A, C)" },
    { "grid_name": "DEFAULT", "ndo_band": 7, "plf_band": 4, "time_range": "TR", "criteria": "Min", "response_rule": "Min (2.5A, C)" },
    { "grid_name": "DEFAULT", "ndo_band": 7, "plf_band": 5, "time_range": "TR", "criteria": "Med", "response_rule": "Min (3A, C)" }
];

if (db.comp_response_grid.countDocuments({}) === 0) {
    db.comp_response_grid.insertMany(defaultGridRules);
    print("Seeded default rows for comp_response_grid");
}

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

var nextgenSummaryColumns = "Origin,Destin,FltNum,DepDate,Capacity,NG_FirstRBD,Predicted_Flown_Revised,MarketFare_Selected,MarketFare_Min,MarketFare_Max,Grid_Name,Rule_Applied,nextgen_flag,competition_response_flag,buy_up_factor,RunId,Id,NG_Historical_current_booked,NG_LF_Forecast,NG_AF_Forecast,NG_Opening_fare,Strategy_fare,Anchor_fare,analystName,status,allocationStatus";

var inventoryOptimizationColumns = "sector,date,DOW,STD,flight,NDO,capacity,Total_Bookings,fare,oal_b2c_min,anchor_fare,strategy_fare,Promo_Flag,mu_bar_orig,mu,AU,P_eq,mu_bar_2,mu_2,AU_2,AU_Revised,predicted_pickup,Predicted_Flown,Predicted_pickup_Revised,Predicted_Flown_Revised,LF_Forecast_Revised,AF_Forecast,Distance,ASKM,RPKM,RASK_Forecast,Current_Revenue,Expected_Revenue,Total_Expected_Revenue,Predicted_Revenue,SnapshotDate,Tagging,FareBucket";

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
    { "tableName": "comp_response_grid", "columns": "grid_name,ndo_band,plf_band,time_range,criteria,response_rule" },
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
