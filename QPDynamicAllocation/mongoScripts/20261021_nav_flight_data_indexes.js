/**
 * MongoDB index setup for the Navitaire bookload cache.
 * Database:   qp_rm_master
 * Collection: nav_flight_data  (written by rm-allocation-scheduled-jobs
 *             navitaireCache.py, module key "navitaire_cache")
 *
 * NOTE: navitaireCache.py rebuilds these same indexes on its staging
 * collection on every run (it swaps staging -> live via renameCollection), so
 * indexes are maintained automatically in steady state. This script is for
 * initial / manual setup and verification.
 *
 * Indexes:
 *   1. flight_key (unique)  - upsert key; one doc per flight.
 *   2. (departure_date, sector) - range + sector lookups (reports, prefetch).
 *   3. sector               - sector-only lookups.
 *   4. departure_date       - date-window scans / pruning.
 *
 * Also ensures the companion metadata collection exists.
 */

db = db.getSiblingDB('qp_rm_master');

var COLLECTION = 'nav_flight_data';
var META_COLLECTION = 'nav_flight_data_meta';

// Ensure collections exist (createIndex would create nav_flight_data anyway,
// but this keeps intent explicit and mirrors existing scripts).
[COLLECTION, META_COLLECTION].forEach(function (colName) {
    if (!db.getCollectionNames().includes(colName)) {
        db.createCollection(colName);
        print("Created collection: qp_rm_master." + colName);
    }
});

var indexes = [
    { spec: { "flight_key": 1 }, options: { name: "uq_flight_key", unique: true } },
    { spec: { "departure_date": 1, "sector": 1 }, options: { name: "ix_departure_date_sector" } },
    { spec: { "sector": 1 }, options: { name: "ix_sector" } },
    { spec: { "departure_date": 1 }, options: { name: "ix_departure_date" } }
];

indexes.forEach(function (idx) {
    try {
        var created = db.getCollection(COLLECTION).createIndex(idx.spec, idx.options);
        print("Ensured index on " + COLLECTION + ": " + created +
              " (" + JSON.stringify(idx.spec) + ")");
    } catch (e) {
        print("Failed to create index " + JSON.stringify(idx.spec) +
              " on " + COLLECTION + ": " + e);
    }
});

print("Done: indexes ensured on qp_rm_master." + COLLECTION);
