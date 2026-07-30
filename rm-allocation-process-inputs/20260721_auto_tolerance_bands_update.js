db = db.getSiblingDB('rm_database');

db.auto_tolerance_ndo_config.drop();
db.auto_tolerance_ndo_config.insertMany([
  { ndo_min: 0, ndo_max: 7, n_value: 0.1 },
  { ndo_min: 8, ndo_max: 14, n_value: 0.1 },
  { ndo_min: 15, ndo_max: 21, n_value: 0.1 },
  { ndo_min: 22, ndo_max: 45, n_value: 4 },
  { ndo_min: 46, ndo_max: 90, n_value: 4 },
  { ndo_min: 91, ndo_max: 120, n_value: 5 },
]);

db.auto_tolerance_delta_bands.drop();
db.auto_tolerance_delta_bands.insertMany([
  { id: 1, band_name: "Negative", min_value: -9999, max_value: -3 },
  { id: 2, band_name: "No Change", min_value: -2, max_value: 2 },
  { id: 3, band_name: "Positive", min_value: 3, max_value: 9999 }
]);

const dplf_bands = [
  "(-9999,-15)",
  "(-15,-8)",
  "(-8,-2)",
  "(-2,5)",
  "(5,10)",
  "(10,15)",
  "(15,9999)"
];

const delta_bands = [1, 2, 3];

db.auto_tolerance_grid_values.drop();
let id = 0;
let gridRows = [];
for (let dplf of dplf_bands) {
  for (let delta of delta_bands) {
    gridRows.push({
      dplf_band_id: id.toString(),
      dplf_band_name: dplf,
      delta_band_id: delta,
      g_value: 0
    });
    id++;
  }
}
db.auto_tolerance_grid_values.insertMany(gridRows);
