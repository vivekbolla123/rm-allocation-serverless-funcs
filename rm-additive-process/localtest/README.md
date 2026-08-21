# Additive Pickup Model — local test flow

Runs `rm-additive-process` end to end on a laptop.

Every input is a CSV in `inputs/`. Those CSVs are loaded into a **local MySQL**
(wherever the model reads MySQL) and a **local MongoDB** (wherever the model reads
Mongo). The real model then runs against them, and the weighted results are
written back to local Mongo and to CSV in `outputs/`.

`inputs/` and `outputs/` are the only files the flow writes — no intermediate
artefacts, no parquet cache — and both are **committed to the repo**: the inputs
are the fixture, the outputs are the reference to diff against.

Nothing touches AWS, SSM, S3, or any production database. No production file is
modified — every local difference lives in a subclass in this folder.

---

## Contents

1. [Prerequisites](#1-prerequisites)
2. [First run](#2-first-run)
3. [The flow, end to end](#3-the-flow-end-to-end)
4. [Inputs](#4-inputs)
5. [The model pipeline](#5-the-model-pipeline)
6. [Outputs](#6-outputs)
7. [Everyday workflows](#7-everyday-workflows)
8. [CLI reference](#8-cli-reference)
9. [Configuration](#9-configuration)
10. [How it works internally](#10-how-it-works-internally)
11. [Editing the mock data by hand](#11-editing-the-mock-data-by-hand)
12. [Troubleshooting](#12-troubleshooting)

---

## 1. Prerequisites

| Need | Why | Check |
|---|---|---|
| Python 3.10 with the repo's `requirements.txt` | pandas, numpy, scipy, pymongo, sqlalchemy, pymysql | `python -c "import pandas, scipy, pymongo, sqlalchemy, pymysql"` |
| A local **MongoDB** | required — all Mongo reads and every output go here | `net start MongoDB` |
| A local **MySQL** | optional — needed only for `--esb mysql` | see [MySQL](#mysql) below |

MongoDB is not optional; MySQL is. Without MySQL, run with `--esb csv` and the
three ESB reads are served from the input CSVs with pandas instead.

### MySQL

The flow uses **your existing local MySQL**. It does not install, initialise, or
manage a server. Defaults are `root` on `127.0.0.1:3306` with an empty password —
override with the `ADDITIVE_LOCAL_MYSQL_*` env vars in
[Configuration](#9-configuration).

Seeding creates the schema `qp_dl_navdata_local` and three `QP_DL_*` tables inside
it, and truncates them on every `--seed`. Nothing outside that schema is touched.

---

## 2. First run

```powershell
cd C:\RM\rm-allocation-serverless-funcs\rm-additive-process

# if your MySQL needs credentials
$env:ADDITIVE_LOCAL_MYSQL_USER     = 'root'
$env:ADDITIVE_LOCAL_MYSQL_PASSWORD = 'yourpassword'

python -m localtest.run_local --generate --seed --run
```

No MySQL running? This works immediately:

```powershell
python -m localtest.run_local --generate --seed --run --esb csv
```

Roughly ten seconds per sector, two sectors by default. When it finishes you
should see:

```
status            : SUCCESS
sectors succeeded : ['BOMDEL', 'DELBOM']
sectors failed    : []
output fare_bucket_weighted           1672 docs
output flight_level_weighted            88 docs
output inventory_optimization         1760 docs
mongo  local_qp_dw_rmalloc.add_model_fare_bucket_level_weighted       3344 docs
mongo  local_qp_dw_rmalloc.add_model_flight_level_weighted             176 docs
mongo  local_qp_dw_rmalloc.add_model_inventory_optimization           3520 docs
csv    ...\localtest\outputs
```

Then read one flight's numbers back out:

```powershell
python -m localtest.inspect_outputs --sector BOMDEL --flight 1101 --ndo 18
```

### Checking a run is actually good

`SUCCESS` only means no sector threw. Look for these too:

* `sectors failed : []` — a sector can fail on its own and the run still reports
  `PARTIAL`.
* `Computed 19 fare bucket entries` — if this is 1, the fare ladder collapsed and
  every downstream number is meaningless.
* `Flight matching complete: N future flights matched, 0 skipped` — a large
  `skipped` count means not enough history cleared `MIN_FLIGHT_MATCHES`.
* Non-zero doc counts for all three output datasets.
* Pick a flight with `future_flight_NDO` above 10 and confirm
  `weighted_pax_count` is non-zero. Flights departing today are legitimately zero
  — no pickup window is left.
* On the AU ladder for that flight, `ProtectionLimit` should be non-decreasing as
  fares fall and `AU` non-increasing, with exactly one row flagged
  `IsOpeningFare`. `inspect_outputs` prints it.

---

## 3. The flow, end to end

Three independent stages. You normally generate and seed once, then re-run as
often as you like.

```
                    ┌──────────────────────────────────────────┐
  --generate        │  generate_inputs.py                      │
                    │  deterministic mock data from one seed   │
                    └───────────────────┬──────────────────────┘
                                        v
                              localtest/inputs/*.csv
                                        │
                    ┌───────────────────┴──────────────────────┐
  --seed            │  seed.py                                 │
                    └──────┬─────────────────────────┬─────────┘
                           v                         v
        ┌──────────────────────────┐   ┌──────────────────────────────┐
        │ local MySQL              │   │ local MongoDB                │
        │ qp_dl_navdata_local      │   │ local_qp_dw_rmalloc          │
        │  QP_DL_FLOWNDATAPAX      │   │  fares                       │
        │  QP_DL_BOOKEDDATAPAX     │   │  rm_parameter_values         │
        │  QP_DL_InventoryLeg      │   │ local_qp_rm_master           │
        │                          │   │  active_sectors, tax_mast_*, │
        │                          │   │  config_sector_demand_mult., │
        │                          │   │  config_quarter_wtp          │
        └────────────┬─────────────┘   └───────────────┬──────────────┘
                     │                                 │
                     └────────────┬────────────────────┘
                                  v
                    ┌──────────────────────────────────────────┐
  --run             │  LocalAdditiveProcessor                  │
                    │  = AdditivePickupModelProcessor          │
                    │    (production, mode='PROD')             │
                    │    + LocalAdditiveCache                  │
                    │  steps 1-16, one sector at a time        │
                    └──────┬─────────────────────────┬─────────┘
                           v                         v
        ┌──────────────────────────┐   ┌──────────────────────────────┐
        │ outputs/                 │   │ local MongoDB                │
        │  fare_bucket_weighted_   │   │  add_model_fare_bucket_      │
        │    <SECTOR>.csv          │   │    level_weighted            │
        │  flight_level_weighted_  │   │  add_model_flight_level_     │
        │    <SECTOR>.csv          │   │    weighted                  │
        │  inventory_optimization_ │   │  add_model_inventory_        │
        │    <SECTOR>.csv          │   │    optimization              │
        │                          │   │  additive_model_run_audit    │
        └──────────────────────────┘   └──────────────────────────────┘
```

What `--run` does, in order:

1. Resolves the sector list — `--sectors` if given, otherwise
   `local_qp_rm_master.active_sectors`.
2. Fetches run-scoped master data once: tax masters and pricing constants from
   Mongo.
3. For each sector, runs steps 1-16 (see [section 5](#5-the-model-pipeline)) and
   times it.
4. Writes a per-sector row into `additive_model_run_audit`, then clears the
   in-memory batch state and moves to the next sector.
5. Stamps the audit document terminal once every expected sector has reported.

Sectors are processed one at a time — exactly as production does — so per-sector
timings are meaningful. Results are identical to batch processing because every
step groups by `Sector`.

There is no parquet cache, so every run is a full refetch from MySQL (or from the
CSVs under `--esb csv`). That is the same behaviour production gets with
`FORCE_BACKFILL` set: nothing carries over between runs, so a run can never show
you stale numbers.

---

## 4. Inputs

All in `localtest/inputs/`. `--generate` writes them; you can also hand-edit or
replace any of them and re-seed.

### Into MySQL — schema `qp_dl_navdata_local`

The production SQL in `helpers/db_helpers/additive_esb_helper.py` runs **verbatim**
against these three tables. Only the data is local.

| CSV | Table | Feeds |
|---|---|---|
| `hist_flown_data.csv` | `QP_DL_FLOWNDATAPAX` | historical flown pax — Query 1 |
| `inventory_leg.csv` | `QP_DL_InventoryLeg` | STD lookup and forward schedule — Queries 2 and 3 |
| `booked_data.csv` | `QP_DL_BOOKEDDATAPAX` | forward bookings — Query 3 |

Key columns, since these are the ones worth getting right when editing by hand:

* `hist_flown_data.csv` — `Origin`, `Destination`, `DepartureDate`,
  `BookingDate`, `FlightNumber`, `InventoryLegID`, `PNR`, `RBD`, `FareBasis`,
  `FB_Net`, `YQ_Pro`, `RCS`, `CUTE`, `DiscountedBaseFare_Pro`,
  `AdjustedCapacity`, `Distance`
* `inventory_leg.csv` — `InventoryLegID` (unique), `DepartureStation`,
  `ArrivalStation`, `STD` (full datetime), `FlightNumber`, `AdjustedCapacity`,
  `Capacity`, `DepartureDate`, `CarrierCode`, `Status`
* `booked_data.csv` — `InventoryLegID`, `DepartureDate`, `BookingDate`, `PNR`,
  `RBD`, `FareBasis`, and the same fare components

### Into MongoDB

| CSV | Collection | Feeds |
|---|---|---|
| `fares.csv` | `local_qp_dw_rmalloc.fares` | step 1, GP fare buckets |
| `rm_parameter_values.csv` | `local_qp_dw_rmalloc.rm_parameter_values` | DUF / GST / RCS pricing constants |
| `active_sectors.csv` | `local_qp_rm_master.active_sectors` | sector list when `--sectors` is omitted |
| `tax_mast_dom.csv` | `local_qp_rm_master.tax_mast_dom` | all-in → airline-component conversion |
| `tax_mast_intl.csv` | `local_qp_rm_master.tax_mast_intl` | same, international |
| `demand_multiplier.csv` | `local_qp_rm_master.config_sector_demand_multiplier` | step 13 |
| `wtp.csv` | `local_qp_rm_master.config_quarter_wtp` | step 14 |

Two formats to be careful with:

* `demand_multiplier.csv` — `DepartureDate` is parsed **day-first**
  (`21/08/2026`). Columns: `DepartureDate`, `Sector`, `FlightNumber`,
  `Demand_Multiplier` (e.g. `10%`, `-5%`).
* `wtp.csv` — `Sector`, `Year`, `Month` (`Aug` or `8`) **or** `Quarter`
  (`Q3`), `WTP Factor` (e.g. `3%`).

Mongo database names are prefixed `local_` so seeding can never clobber a local
dump of the real `qp_dw_rmalloc` / `qp_rm_master`. Override with
`ADDITIVE_LOCAL_MONGO_DB` / `ADDITIVE_LOCAL_MONGO_MASTER_DB` if you want the
production names.

### Optional CSV stand-ins for computed steps

Two steps can be fed from a CSV instead of being computed. Both need the file to
exist **and** the flag to be passed, so the real logic runs by default.

| CSV | Replaces | Flag |
|---|---|---|
| `fare_buckets.csv` | step 1 — GP fare bucket build | `--use-csv-fare-buckets` |
| `tagging.csv` | step 4 — Z-score lean/peak tagging | `--use-csv-tagging` |

`--dump-derived` writes the computed versions of both back into `inputs/`, so you
have a correctly-shaped starting point to edit:

```powershell
python -m localtest.run_local --run --dump-derived
# edit inputs/tagging.csv
python -m localtest.run_local --run --use-csv-tagging
```

Columns: `fare_buckets.csv` is `Sector,FareBasis` (e.g. `BOMDEL,7643-8929` or
`BOMDEL,28617+`); `tagging.csv` is
`Sector,DepartureDate,FlightNumber,Time Cluster,Tagging`.

---

## 5. The model pipeline

What runs per sector, and which input each step depends on. Useful for narrowing
down where a wrong number came from.

| Step | What it does | Depends on |
|---|---|---|
| 1 | GP fare buckets, 19 per sector | Mongo `fares`, tax masters, pricing constants |
| 2 | ESB fetch: historical flown, STD lookup, forward scan | the three `QP_DL_*` tables |
| 3 | Derive flown / lean-peak source / booked / future flights | step 2 |
| 4 | Historical lean/peak tagging (Z-score on flight RASK) | step 3 |
| 5 | Future tagging (50th percentile of the prev-month window) | step 3 |
| 6 | Combine flown + booked, assign fare buckets | steps 1, 3 |
| 7 | Pivot pax by NDO 0..180 | steps 4, 5, 6 |
| 8 | Future flights tagged and expanded by fare bucket | steps 1, 5 |
| 9 | Flight matching: Sector, DOW, Tagging, STD window | steps 7, 8 |
| 10 | Join matched history at fare bucket level | step 9 |
| 11 | Predictions — mean **and** weighted | step 10 |
| 12 | Remaining-RBD distribution | step 11, non-required-RBD bookings |
| 13 | Demand multiplier → demand-adjusted prediction | `config_sector_demand_multiplier` |
| 14 | WTP fare-bucket redistribution | `config_quarter_wtp` |
| 15 | Inventory optimization (EMSR-b protection limits, AU) | step 14 |
| 16 | Save | → Mongo + `outputs/*.csv` |

The model computes both a **normal** (mean) and a **weighted** (recency-decayed,
`alpha = 0.99`) branch through steps 11-14. Only the weighted branch is persisted
in step 16; the normal branch is computed and then discarded.

Step 15 is the one place this flow diverges from production logic: it rebuilds the
AU ladder from the weighted branch instead of the normal one, so that all three
persisted datasets are consistent. See
[section 10](#step-15-is-driven-by-the-weighted-branch-locally).

The run log names every step as it goes, with row counts, which is usually enough
to locate a problem without extra artefacts:

```
Computed 19 fare bucket entries across 1 sectors
[Query 1] 40592 rows, 1 sectors (from 2025-08-01)
Freshly tagged 1460 historical flights
Combined data prepared: 43947 rows
Flight matching complete: 88 future flights matched, 0 skipped
Predictions complete (alpha=0.99) — weighted fare bucket: 1672, flight: 88
[WTP] redistributed 44 flight(s) with a non-zero factor
```

`--verbose` adds debug detail.

---

## 6. Outputs

**Only the weighted branch is persisted.** Six CSVs for two sectors, and three
Mongo collections. Nothing else is written to disk.

| Dataset | Mongo collection | CSV |
|---|---|---|
| flight level, weighted | `add_model_flight_level_weighted` | `flight_level_weighted_<SECTOR>.csv` |
| fare bucket level, weighted | `add_model_fare_bucket_level_weighted` | `fare_bucket_weighted_<SECTOR>.csv` |
| inventory optimization (AUs) | `add_model_inventory_optimization` | `inventory_optimization_<SECTOR>.csv` |

Plus the run audit in `local_qp_dw_rmalloc.additive_model_run_audit` — the same
collection production uses.

The CSVs are committed, so `git diff` after a run tells you exactly what your
change did to the numbers.

### File and document shape

* No date in the CSV filename. A re-run for the same sector overwrites the file.
  The snapshot date is still a column (`SnapshotDate`), because it is half of the
  idempotency key.
* The 0..180 pickup-curve columns are dropped from both the CSV and the Mongo
  documents — 181 numeric fields is not what you read or query. `--full-csv`
  writes an extra `<dataset>_<sector>_full.csv` that keeps them;
  `--mongo-include-ndo` keeps them in Mongo.
* Writes are idempotent per `(SnapshotDate, Sector)`: re-running a sector
  replaces that slice rather than appending, mirroring how the S3 partition is
  overwritten in production. `--reset-outputs` drops every `add_model_*`
  collection outright.

### Columns — `flight_level_weighted` (28)

| Group | Columns |
|---|---|
| identity | `SnapshotDate`, `Sector`, `DepartureDate`, `DOW`, `STD`, `FlightNumber`, `Capacity`, `Tagging`, `future_flight_NDO` |
| current state | `Current_Bookings`, `Current_Revenue` |
| prediction | `weighted_pax_count`, `weighted_pax_std_square` |
| benchmark | `Matched_Current_Bookings`, `Weighted_Matched_Current_Bookings` |
| step 12 | `Remaining_pax_count`, `Final_pax_count`, `Final_Total_Bookings`, `predicted_pax_count_final` |
| step 13 | `Demand Multiplier`, `Demand Multiplier Number`, `New predicted_pax_count_final`, `Fare Bucket Ratio`, `New Final_pax_count`, `New Final Total Bookings` |
| derived | `Load_Factor` |
| step 15 carry-over | `OpeningFareBucket`, `OpeningFareMidpoint` |

### Columns — `fare_bucket_weighted` (33)

Same identity and current-state columns, plus:

| Group | Columns |
|---|---|
| bucket | `FareBucket`, `FareBucket_Lower`, `FareBucket_Upper` |
| prediction | `weighted_pax_count`, `weighted_pax_std` |
| step 12 | `Remaining_pax_count`, `Total_Predicted_pax_count`, `FareBasis_Prediction_ratio`, `Remaining_pax_distribution`, `Final_pax_count`, `Predicted_Total_Bookings` |
| step 13 | `predicted_pax_count_final`, `Demand Multiplier`, `Demand Multiplier Number`, `New predicted_pax_count_final`, `Fare Bucket Ratio`, `New Final_pax_count`, `New Final Total Bookings` |
| step 14 | `WTP_Factor`, `WTP_pax_count` |

### Columns — `inventory_optimization` (40)

Every column from the weighted fare-bucket frame, plus the EMSR-b results:

| Column | Meaning |
|---|---|
| `FareMidpoint` | midpoint of the bucket; open-ended top bucket uses `lower x 1.10` |
| `ProtectionLimit` | seats protected for this bucket and everything above it, monotonically nested and capped at `Capacity` |
| `AU` | authorization level = `Capacity - ProtectionLimit` of the next-higher bucket; the top bucket gets full `Capacity` |
| `RBD` | only set on the promo row (`W`); `None` on filed buckets, because the model has no RBD mapping at this stage |
| `IsOpeningFare` | the lowest filed bucket whose `AU` still exceeds current bookings |
| `OpeningFareBucket`, `OpeningFareMidpoint` | that bucket, repeated on every row of the flight |

There is one extra row per flight beyond the 19 buckets: the **promo** row, at
`PROMO_FARE_RATIO = 0.80` of the lowest filed fare, RBD `W`. Hence 20 rows per
flight (1760 = 88 flights x 20).

The opening fare is determined **ignoring** promo inventory, which is why a
flight can show `IsOpeningFare` on a filed bucket while the promo row still
carries a non-zero `AU`.

### Reading the numbers

Follow one flight's pax count through the pipeline:

```
weighted_pax_count            step 11  weighted mean pickup over the open NDO window
+ Remaining_pax_distribution  step 12  this bucket's share of non-required-RBD pax
= Final_pax_count
x Demand Multiplier Number    step 13  (100 + analyst %) / 100
= New Final_pax_count
→ WTP_pax_count               step 14  redistributed across buckets; flight total preserved
```

At flight level, `Final_Total_Bookings = Current_Bookings + Final_pax_count` and
`Load_Factor = Final_Total_Bookings / Capacity`.

The AU ladder does **not** sit at the end of that chain. It is built from
`weighted_pax_count` — the raw step-11 prediction — so the remaining-RBD
distribution, the demand multiplier and the WTP redistribution do not feed the
protection limits. That is how production behaves too (with the normal branch);
see [section 10](#step-15-is-driven-by-the-weighted-branch-locally).

```powershell
python -m localtest.inspect_outputs --sector BOMDEL --flight 1101 --ndo 18
```

prints the fare-bucket table, the flight-level row and the full AU ladder for one
flight, which is the quickest way to check all three outputs agree.

---

## 7. Everyday workflows

**Iterate on model logic** — generate and seed once, then just re-run:

```powershell
python -m localtest.run_local --generate --seed        # once
python -m localtest.run_local --run --sectors BOMDEL   # repeat
```

**See what a change did to the numbers** — the committed outputs are the
baseline, so just re-run and diff. Keep the anchor pinned so the inputs do not
move under you:

```powershell
python -m localtest.run_local --generate --seed --anchor 2026-08-21   # once
git add localtest/inputs localtest/outputs                            # baseline

# make your change
python -m localtest.run_local --run
git diff --stat localtest/outputs
git diff localtest/outputs/flight_level_weighted_BOMDEL.csv
```

If the diff is empty, your change was behaviour-preserving. If it is not, the
diff is the review.

**Test a specific tagging or fare ladder** — dump, edit, replay:

```powershell
python -m localtest.run_local --run --dump-derived
# edit inputs/fare_buckets.csv
python -m localtest.run_local --run --use-csv-fare-buckets
```

**Reproduce a data-shaped bug** — hand-write the CSV, re-seed, run:

```powershell
# edit inputs/hist_flown_data.csv
python -m localtest.run_local --seed --run --sectors BOMDEL
```

**Start completely clean:**

```powershell
python -m localtest.run_local --generate --seed --run --reset-outputs
```

There is no cache to clear — every run already refetches everything.

---

## 8. CLI reference

### `run_local`

```
python -m localtest.run_local [stages] [options]
```

**Stages** — at least one is required.

| Flag | Effect |
|---|---|
| `--generate` | (re)generate the mock input CSVs |
| `--seed` | load the input CSVs into local MySQL + MongoDB |
| `--run` | run the model |

**Mock data shape** — used with `--generate`.

| Flag | Default | Notes |
|---|---|---|
| `--hist-days` | `400` | keep at 400+; the tagging windows are 365 days |
| `--future-days` | `21` | forward schedule; raise toward 180 for the full horizon |
| `--mean-bookings` | `26` | average bookings per flight |
| `--seed-value` | `20260821` | RNG seed |
| `--anchor` | today | `YYYY-MM-DD`; pins the snapshot date |

**Run options**

| Flag | Notes |
|---|---|
| `--sectors A B` | sectors to process; omit to read `active_sectors` from Mongo |
| `--esb auto\|mysql\|csv` | where the ESB reads come from; `auto` prefers MySQL |
| `--analyst NAME` | recorded in the audit as `triggered_by` |
| `--run-id ID` | otherwise `local-<random>` |
| `--reset-outputs` | drop every `add_model_*` collection before running |
| `--full-csv` | also write `*_full.csv` with the 0..180 curve |
| `--mongo-include-ndo` | keep the 0..180 curve in the Mongo documents |
| `--use-csv-fare-buckets` | read step 1 from `inputs/fare_buckets.csv` |
| `--use-csv-tagging` | read step 4 from `inputs/tagging.csv` |
| `--dump-derived` | write computed fare buckets + tagging back into `inputs/` |
| `--verbose` | debug logging |

### `inspect_outputs`

```powershell
python -m localtest.inspect_outputs [--sector BOMDEL] [--flight 1101] [--ndo 18]
```

Prints one flight's weighted fare-bucket ladder and its flight-level row, read
straight from Mongo. `--ndo` is `future_flight_NDO`, i.e. days from the snapshot
date to departure.

### `generate_inputs`

```powershell
python -m localtest.generate_inputs [--hist-days N] [--future-days N] ...
```

The generator on its own, if you want to regenerate CSVs without touching the
databases.

---

## 9. Configuration

Every setting has a local default and an env-var override.

| Env var | Default |
|---|---|
| `ADDITIVE_LOCAL_MYSQL_HOST` | `127.0.0.1` |
| `ADDITIVE_LOCAL_MYSQL_PORT` | `3306` |
| `ADDITIVE_LOCAL_MYSQL_USER` | `root` |
| `ADDITIVE_LOCAL_MYSQL_PASSWORD` | *(empty)* |
| `ADDITIVE_LOCAL_MYSQL_SCHEMA` | `qp_dl_navdata_local` |
| `ADDITIVE_LOCAL_MONGO_HOST` | `127.0.0.1` |
| `ADDITIVE_LOCAL_MONGO_PORT` | `27017` |
| `ADDITIVE_LOCAL_MONGO_USER` | *(empty — no auth)* |
| `ADDITIVE_LOCAL_MONGO_PASSWORD` | *(empty)* |
| `ADDITIVE_LOCAL_MONGO_AUTH_SOURCE` | `admin` |
| `ADDITIVE_LOCAL_MONGO_DB` | `local_qp_dw_rmalloc` |
| `ADDITIVE_LOCAL_MONGO_MASTER_DB` | `local_qp_rm_master` |

The resolved configuration is printed at the top of every run, so you can always
see what it actually connected to.

---

## 10. How it works internally

### Files

```
localtest/
  bootstrap.py         sys.path wiring + local stand-in for `configsettings`
  config.py            LocalConfig: connections and paths from env vars
  schema.py            CSV -> table/collection registry, MySQL DDL, output map
  generate_inputs.py   deterministic mock CSV generator
  seed.py              CSV -> local MySQL + local MongoDB
  local_helpers.py     LocalAdditiveCache (Mongo/CSV sink), LocalCsvEsbHelper
  local_processor.py   LocalAdditiveProcessor
  run_local.py         CLI entry point
  inspect_outputs.py   read one flight's weighted results back out of Mongo
  inputs/              mock input CSVs   (committed — the fixture)
  outputs/             result CSVs       (committed — the reference)
```

### `configsettings` has to be faked before anything imports the model

`rm-allocation-configurations/configsettings.py` calls AWS SSM **at import time**.
Importing it locally would fail, and if it succeeded it would pull production
credentials. So `bootstrap.py` builds a plain module with the same attribute names
from `LocalConfig` and registers it in `sys.modules['configsettings']` before
`additive_model` is imported. This is why `bootstrap()` must run first, and why
`run_local.py` imports the model lazily inside functions rather than at module
level.

### The local run uses `mode='PROD'`, not `mode='TEST'`

The model's own `TEST` mode reads the demand multiplier and WTP from hard-coded
absolute CSV paths on one developer's machine, skips the run audit, and takes a
TEST-only save branch. None of that is the code that ships.

So the local flow runs `mode='PROD'` and swaps the *cache* instead.
`LocalAdditiveCache` overrides exactly two things:

* every parquet-cache read and write becomes a no-op, so no cache files are
  produced. An empty read makes the ESB helper take its full-backfill branch and
  makes tagging take its cold-start branch — which is what a local run wants
  every time.
* `write_partitioned_s3` — the single hook the PROD save path uses for every
  output dataset — writes to local Mongo and CSV, and skips any dataset absent
  from `OUTPUT_COLLECTIONS`.

Everything else follows the production path: Mongo-backed demand-multiplier and
WTP reads, the audit write, the same column sets, the same partitioning.
Intermediate artefacts are suppressed because PROD suppresses them — the local
flow inherits that rather than overriding it.

`LocalAdditiveProcessor` adds the step-15 change described next, plus the two
optional CSV stand-ins for steps 1 and 4.

### Step 15 is driven by the weighted branch locally

Production builds the EMSR-b ladder from the **normal** fare-bucket frame:

```python
# additive_model.py:1993
optimized_df = self._compute_inventory_optimization(result_merged_df)

# additive_model.py:1548
opt_df = compute_inventory_optimization_df(bucket_df,
                                           pax_col='predicted_pax_count',
                                           std_col='predicted_pax_std', ...)
```

This flow persists only the weighted branch, so `LocalAdditiveProcessor`
recomputes step 15 from the weighted frame with `pax_col='weighted_pax_count'`
and `std_col='weighted_pax_std'`. **The AUs here therefore differ from production
by design** — if you are validating AU behaviour against a production run, that is
the difference to account for.

Mechanically: `_apply_wtp_redistribution` is called twice per sector with
identical arguments, once per branch, so the override identifies the weighted
frame by the presence of `weighted_pax_count` and stashes it for step 15 to use.
The stash is cleared at step 1, which is where each sector's batch begins.

Note what does *not* work: handing the weighted frame to the production call
unchanged. `compute_flight_inventory_optimization` falls back to
`Final_pax_count` when `pax_col` is missing and sets the std column to `0.0`, so
you would get a ladder built on the wrong demand with every sigma at zero — no
error, just wrong protection limits.

To go back to production behaviour, delete the `_compute_inventory_optimization`
and `_apply_wtp_redistribution` overrides in `local_processor.py`.

### Persisting more output datasets

Add the dataset name to `OUTPUT_COLLECTIONS` in `schema.py`. Three of the five
names the model emits are already there; the two not persisted are
`flight_level_normal` and `fare_bucket_normal`.

### The `csv` ESB backend

`--esb csv` skips MySQL entirely: `LocalCsvEsbHelper` rebuilds the three fetch
results (`hist_raw_df`, `future_flight_master`, `forward_raw_df`) from `inputs/`
with pandas.

It is a **reimplementation** of the SQL and can drift from it. Treat `--esb mysql`
as the reference and use `csv` for fast iteration when you are changing model
logic rather than the queries.

### `InventoryLegID` is a primary key on purpose

The production forward query selects `fm.AdjustedCapacity` while grouping on
`fm.Capacity`. MySQL 8 only accepts that under `ONLY_FULL_GROUP_BY` when the
grouped set is functionally dependent on a primary key, so the DDL in
`schema.py` declares `InventoryLegID` as the PK of `QP_DL_InventoryLeg`.

### Known quirk in the production frame

The step-15 inventory-optimization frame carries two `FareBucket` columns:
`compute_flight_inventory_optimization` copies `FareBasis` into `FareBucket`, and
`_save_results` then renames `FareBasis` → `FareBucket` as well. The local writer
collapses them and logs a warning rather than dropping one silently. This affects
the production S3 write too.

---

## 11. Editing the mock data by hand

The generator (`generate_inputs.py`) mocks two sectors, four flights a day each,
spread across four STD time clusters, over 400 days of history. That shape is not
arbitrary — it is the minimum that satisfies the model's real preconditions:

* the 365-day rolling windows used by historical and future tagging;
* enough spread in flight-level RASK for the Z-score tagging to separate flights
  into Lean / Shoulder / Peak / Super Peak;
* at least `MIN_FLIGHT_MATCHES = 13` same-DOW, same-tagging historical flights
  for every future flight.

Shrink it and the run will still succeed while quietly producing nothing.

### Fare spaces

The single easiest thing to get wrong:

```
fares.csv TOTAL      all-in fare
airline component    (TOTAL - other_taxes) / (1 + GST)   <- fare buckets live here
booking Fare         DiscountedBaseFare_Pro + YQ_Pro + RCS + CUTE
                     i.e. already airline-component space
```

`other_taxes` for a domestic sector is `ASF + PSF + UDF_DF + DUF(destination)`,
read from `tax_mast_dom.csv` and `PRICING_DUF_SECTORS` in
`rm_parameter_values.csv`. If the booking fares in `hist_flown_data.csv` fall
outside the bucket ladder built from `fares.csv`, every booking is dropped as
unbucketed and the run produces nothing while still reporting `SUCCESS`.

The lowest bucket's lower bound is relaxed by 20% at assignment time, which gives
a little headroom at the bottom but not at the top.

### `FareBasis` is load-bearing

* 5th character `T` switches the fare calculation to `FB_Net` instead of
  `DiscountedBaseFare_Pro`.
* Containing `SOL` excludes the booking from pax counts and from RASK-based
  tagging.

The generator uses `QPDRB<RBD>` (5th character `B`) and appends `SOL` to about 2%
of bookings.

### RBDs

* `Constants.FARE_BUCKET_RBDS` — the RBDs `fares.csv` must cover to build the
  ladder.
* `Constants.REQUIRED_RBDS` — bookings in these RBDs go through the fare-bucket
  pivot.
* Anything else (the generator uses `W4`, `W5`, `W6`, `G8`, `S`, `GA`, `GB`, `L`
  for about 12% of bookings) feeds step 12, the remaining-RBD distribution. With
  no such bookings, `Remaining_pax_count` is zero everywhere.

### Generator knobs

```powershell
python -m localtest.run_local --generate `
    --hist-days 400 --future-days 21 --mean-bookings 26 `
    --seed-value 20260821 --anchor 2026-08-21
```

* `--hist-days` — 400 or more. Shorter starves both tagging windows.
* `--future-days` — days of forward schedule. 21 keeps a run near ten seconds per
  sector; raise toward 180 for the production horizon, at proportional cost.
* `--mean-bookings` — the default 26 against a 189-seat capacity is a
  deliberately thin load that keeps runs fast; the load factors it produces are
  **not** realistic. Use around 90 when you want output that looks like a real
  flight.
* `--anchor` — pins the snapshot date, so runs on different days are comparable.
* `--seed-value` — same seed and anchor gives byte-identical CSVs.

---

## 12. Troubleshooting

**`MongoDB is not reachable`** — start it (`net start MongoDB`) or set
`ADDITIVE_LOCAL_MONGO_HOST` / `_PORT`.

**`MySQL is not reachable`** — start your server, fix the
`ADDITIVE_LOCAL_MYSQL_*` values, or use `--esb csv`.

**`Host '127.0.0.1' is not allowed to connect`** — your grant is
`root@localhost` and the server runs with `--skip-name-resolve`, so `127.0.0.1`
never resolves back to `localhost`. Add a `root@'127.0.0.1'` grant, or set
`ADDITIVE_LOCAL_MYSQL_HOST=localhost`.

**`The real configsettings is already imported`** — something imported the model
before `bootstrap()` ran. Import the model lazily, inside a function.

**`missing input CSV: ...`** — run `--generate` first, or drop your own file at
that path.

**`No fare data found in qp_dw_rmalloc.fares`** — Mongo was not seeded, or
`--sectors` names a sector with no rows in `fares.csv`.

**`No matched results … batch produced no output`** — not enough history to clear
`MIN_FLIGHT_MATCHES = 13`. Raise `--hist-days`, or check the `skipped` count in
the flight-matching log line.

**Run reports SUCCESS but the outputs are empty** — almost always the fare-bucket
mismatch described in [section 11](#fare-spaces). The tell is in the log: the
`Combined data prepared: N rows` count is healthy but `Matched result dataset`
is tiny or absent, because the bookings never got a `FareBasis`.

**Only one fare bucket computed** — `fares.csv` has fewer than two distinct
`TOTAL` values for that sector, so the GP ladder collapses.

**Predictions are all zero for a flight** — expected when
`future_flight_NDO = 0`; the flight departs today and there is no pickup window
left. Check a flight further out.

**`WTP_pax_count` equals `New Final_pax_count`** — the WTP factor for that
sector-month is 0%, or `wtp.csv` has no row covering it.

**Duplicate `FareBucket` column warning** — expected, see
[section 10](#known-quirk-in-the-production-frame).

**`git diff` shows changes in outputs/ after a no-op run** — the snapshot date
moved. Re-generate with `--anchor` pinned to the date the committed baseline was
built with (`SnapshotDate` in any output row).
