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

Roughly 45 seconds for both sectors. When it finishes you should see:

```
status            : SUCCESS
sectors succeeded : ['BOMDEL', 'DELBOM']
sectors failed    : []
output fare_bucket_weighted           4880 docs
output flight_level_weighted           244 docs
mongo  local_qp_dw_rmalloc.add_model_fare_bucket_level_weighted       9760 docs
mongo  local_qp_dw_rmalloc.add_model_flight_level_weighted             488 docs
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
        │ outputs/  (2 files,      │   │ local MongoDB                │
        │            all sectors)  │   │  add_model_fare_bucket_      │
        │  fare_bucket_weighted    │   │    level_weighted            │
        │    .csv                  │   │  add_model_flight_level_     │
        │  flight_level_weighted   │   │    weighted                  │
        │    .csv                  │   │  additive_model_run_audit    │
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
| 16 | Save | → 2 Mongo collections + 2 CSVs |

The model computes both a **normal** (mean) and a **weighted** (recency-decayed,
`alpha = 0.99`) branch through steps 11-14. Only the weighted branch is persisted
in step 16; the normal branch is computed and then discarded.

Step 15 is the one place this flow diverges from production logic: it rebuilds the
AU ladder from the weighted branch instead of the normal one, so both persisted
datasets are consistent. See
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

**Two files, both covering every sector.** Only the weighted branch is
persisted; the normal branch is computed and discarded. Nothing else is written
to disk.

| Dataset | Mongo collection | CSV |
|---|---|---|
| flight level, weighted | `add_model_flight_level_weighted` | `flight_level_weighted.csv` |
| fare bucket level, weighted, **including the EMSR-b AU ladder** | `add_model_fare_bucket_level_weighted` | `fare_bucket_weighted.csv` |

Plus the run audit in `local_qp_dw_rmalloc.additive_model_run_audit` — the same
collection production uses.

The EMSR-b results are **not** a third dataset. The optimization frame is the
weighted fare-bucket frame plus the optimization columns and one promo row per
flight — same grain, superset of columns — so it *is* the fare-bucket output and
the pre-optimization copy is dropped. You will see this in the log:

```
[local-out] fare_bucket_weighted: superseded by the optimized frame, skipped
[local-out] inventory_optimization: persisted as fare_bucket_weighted
```

The mapping lives in `schema.py` (`DATASET_ALIASES`, `SUPERSEDED_DATASETS`).

Sectors are processed one at a time, so the first sector truncates each CSV and
later sectors append. That is why there is no sector in the filename.

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

### Columns — `fare_bucket_weighted`, the EMSR-b part (9 of the 42)

The fare-bucket file carries the columns listed above plus the EMSR-b inputs and
results, and **20 rows per flight**: 19 filed buckets and one promo row.

| Column | Meaning |
|---|---|
| `OptimizationDemand` | the per-bucket mean demand the ladder was built from: the pipeline's **final** demand, i.e. `WTP_pax_count` |
| `OptimizationDemandStd` | the sigma used, scaled from `weighted_pax_std` by the same per-bucket ratio so the coefficient of variation is held constant |
| `FareMidpoint` | midpoint of the bucket; open-ended top bucket uses `lower x 1.10` |
| `ProtectionLimit` | `ceil(Mu_Bar + z * Sigma_Bar)`, floored at 0 and monotonically nested. **Not** capped at `Capacity` — it is the demand the class wants held back, and `AU` is floored at 0 regardless |
| `AU` | authorization level = `Capacity - ProtectionLimit` of the next-higher bucket; the top bucket gets full `Capacity` |
| `RBD` | only set on the promo row (`W`); `None` on filed buckets, because the model has no RBD mapping at this stage |
| `IsOpeningFare` | the anchor fare: the lowest filed bucket whose `AU` still exceeds current bookings |
| `OpeningFareBucket`, `OpeningFareMidpoint` | that bucket, repeated on every row of the flight |

`OptimizationDemand` and `OptimizationDemandStd` exist so the ladder is auditable:
you can recompute every protection limit from those two columns plus
`FareMidpoint`, `Capacity` and `Constants.EMSR_SIGMA_FACTOR` / `BUY_UP_FACTOR`.

One quirk to be aware of on the **promo** row: `OptimizationDemand` and
`OptimizationDemandStd` are correctly zeroed, but the other demand columns carried
over from the fare-bucket frame (`WTP_pax_count`, `Final_pax_count`, …) still hold
the lowest filed bucket's values, because the promo row is built as a copy of it.
Read the promo row's demand from `OptimizationDemand` only.

The **promo** row sits at `PROMO_FARE_RATIO = 0.80` of the lowest filed fare on
RBD `W`, which is why there are 20 rows per flight rather than 19
(4880 = 244 flights x 20 per sector).

`AU` near `Capacity` on most rows is not a bug — it means there is little
predicted pickup to protect against. See
[the AU ladder needs demand](#the-au-ladder-needs-demand).

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

The AU ladder sits at the **end** of that chain: step 15 optimises on
`WTP_pax_count`, surfaced as `OptimizationDemand`. So the remaining-RBD
distribution, the analyst demand multiplier and the WTP redistribution all feed
the protection limits.

That was not always true — until recently step 15 was fed the raw step-11
prediction, which meant steps 12-14 had no effect on inventory at all. See
[step 15 and the BRD](#step-15-and-the-brd).

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
| `--future-days` | `60` | forward schedule; drives how much pickup is left to protect |
| `--load-factor` | `0.45` | bookings per flight relative to that flight's seats |
| `--capacity` | *(mixed)* | force one seat count on every flight; default is the 189 / 174 fleet |
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
python -m localtest.inspect_outputs [--sector BOMDEL] [--flight 1101] [--ndo 56]
```

For one flight, read straight from Mongo: the demand chain through steps 11-14,
the EMSR-b AU ladder, and the flight-level row. `--ndo` is `future_flight_NDO`,
i.e. days from the snapshot date to departure.

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
  inputs/              mock input CSVs      (committed — the fixture)
  outputs/             2 result CSVs        (committed — the reference)
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

Production hands step 15 the **normal** fare-bucket frame
(`additive_model.py`: `self._compute_inventory_optimization(result_merged_df)`).
This flow persists only the weighted branch, so `LocalAdditiveProcessor`
substitutes the weighted frame and the weighted base columns:

```python
super()._compute_inventory_optimization(source,
                                        base_col='weighted_pax_count',
                                        base_std_col='weighted_pax_std')
```

Demand selection, sigma scaling and the buy-up factor are all inherited unchanged
from production. **The AUs here therefore differ from production by design** —
production uses `predicted_pax_count` / `predicted_pax_std` as the sigma base. If
you are validating AU behaviour against a production run, that is the difference
to account for.

Mechanically: `_apply_wtp_redistribution` is called twice per sector with
identical arguments, once per branch, so the override identifies the weighted
frame by the presence of `weighted_pax_count` and stashes it for step 15. The
stash is cleared at step 1, which is where each sector's batch begins.

To go back to production behaviour, delete the `_compute_inventory_optimization`
and `_apply_wtp_redistribution` overrides in `local_processor.py`.

### Step 15 and the BRD

The EMSR-b implementation was checked line by line against the BRD by
transcribing the spec into a reference implementation and diffing across all 244
flights: `Mu_Bar`, `Sigma_Bar` capped at `0.25 * Mu_Bar`,
`F_j = SUM(Mu_i * f_i) / Mu_Bar`, `Prob_j`, `z = Phi^-1(Prob_j)`,
`y = ceil(...)` floored at 0, `AU_j = Capacity - y_(j-1)`, `AU_1 = Capacity`,
promo at 0.80 of the lowest filed fare on RBD `W`, and the anchor-fare rule all
match exactly.

Three gaps were found and fixed in production code:

**1. The buy-up factor did not exist.** The BRD requires the `Prob_j` right-hand
side multiplied by `1 / (1 - alpha)`, alpha initialised to 0. There was no such
parameter. Now `Constants.BUY_UP_FACTOR` (default `0.0`) is threaded through
`compute_inventory_optimization_df` and `compute_flight_inventory_optimization`
as `buy_up_factor`. At 0 the ladder is unchanged; raising it lifts every
probability, hence z, hence the protection limits, so more seats are held for
higher fares:

| alpha | max protection | lowest AU | buckets at AU 0 | anchor fare |
|---|---|---|---|---|
| 0.00 | 257 | 0 | 10 | 10418-12146 |
| 0.10 | 273 | 0 | 10 | 10418-12146 |
| 0.25 | 367 | 0 | 10 | 10418-12146 |
| 0.50 | 482 | 0 | 11 | 12146-13447 |

(one flight, demand scaled to 220 pax on 40 seats; every row cross-checked
against the reference implementation)

Nothing computes alpha from the final PLF forecast yet — the BRD describes it as
"based on final PLF forecast". That derivation is still to be built; the
parameter is in place for it.

**2. Step 15 was fed the wrong demand.** It used `predicted_pax_count`, the raw
step-11 prediction, so the remaining-RBD distribution, the demand multiplier and
the WTP redistribution had no effect on protection limits or AUs — the analyst's
demand multiplier moved the forecast but not the inventory. It now uses the most
processed demand available (`WTP_pax_count`, falling back through
`New Final_pax_count` and `Final_pax_count`), surfaced as `OptimizationDemand`.

On the local fixture that is a 13% demand increase and moves 27% of AUs; mean AU
36.88 -> 36.58, mean protection 3.56 -> 3.91. The effect is larger on real data
with realistic load factors and non-zero demand multipliers.

Sigma needed a decision, because only the step-11 prediction has a standard
deviation — steps 12-14 are deterministic transforms of the mean and carry no
variance of their own. Sigma is scaled by the same per-bucket ratio, holding the
coefficient of variation constant, and reported as `OptimizationDemandStd`. This
is second-order: `Sigma_Bar` is clipped to `EMSR_SIGMA_FACTOR * Mu_Bar` on about
82% of buckets, so the cap usually decides the uncertainty term anyway.

**3. `ProtectionLimit` was capped at `Capacity`.** The BRD has no such ceiling.
The cap never changed an `AU` — `AU = max(0, Capacity - y)` floors at 0 either
way — but it made the reported column understate protection on busy flights, and
it bound on 59 of 60 flights once demand was realistic. Removed.

One rule the code adds that the BRD does not mention is retained: monotonic
nesting, `y_j = max(y_j, y_(j-1))`. Tested across load factors from 7 to 320 pax
it never bound once, so the natural ladder is already monotonic.

### Persisting more output datasets

Add the dataset name to `OUTPUT_COLLECTIONS` in `schema.py`. The two the model
emits that are not persisted are `flight_level_normal` and `fare_bucket_normal`.

To split the EMSR-b ladder back out into its own dataset, remove
`inventory_optimization` from `DATASET_ALIASES` and `fare_bucket_weighted` from
`SUPERSEDED_DATASETS`, then give both an entry in `OUTPUT_COLLECTIONS`.

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
  for every future flight;
* a mixed fleet — 189 and 174 seats alternating by flight — so nothing silently
  assumes a single capacity.

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
    --hist-days 400 --future-days 60 --load-factor 0.45 `
    --seed-value 20260821 --anchor 2026-08-21
```

* `--hist-days` — 400 or more. Shorter starves both tagging windows.
* `--future-days` — days of forward schedule. Also decides how much pickup is
  left to protect, so it drives whether the AU ladder means anything: see
  [the AU ladder needs demand](#the-au-ladder-needs-demand). 60 costs about a
  minute for both sectors; raise toward 180 for the production horizon.
* `--load-factor` — bookings per flight as a fraction of that flight's seats.
  **This is the setting that decides the size and runtime of the whole fixture**,
  because the input is one row per booked pax: at 0.45 on the 189/174 fleet
  `hist_flown_data.csv` is about 28 MB and 257k rows. Halve it and the input
  roughly halves.
* `--capacity` — force a single seat count on every flight. The default is the
  mixed fleet defined in `SECTORS`: 189 and 174 seats, alternating by flight, so
  the fixture exercises non-uniform capacity.
* `--anchor` — pins the snapshot date, so runs on different days are comparable.
* `--seed-value` — same seed and anchor gives byte-identical CSVs.

### The AU ladder needs demand

Step 15 protects seats against **predicted pickup relative to capacity**, so the
AU ladder is only interesting when that ratio is non-trivial. Two settings
control it, and getting either wrong produces a ladder where every `AU` equals
`Capacity` — which looks broken but is correct behaviour for a flight with no
demand to protect against.

**Load factor.** `weighted_pax_count` is the pickup for one fare bucket over the
remaining window, so it is a small number by construction. At a 14% load factor
on 189 seats the whole flight's pickup is around 1% of capacity, protection rounds
to 0-4 seats, and every `AU` pins to capacity. At the default 0.45 the same flight
descends properly.

**Forward horizon.** Predicted pickup only covers `NDO <= future_flight_NDO`, so
a flight departing tomorrow has almost nothing left to protect while one 120 days
out has nearly its whole load. With the generator's booking curve about 65% of
bookings are already in by 21 days out but only 17% by 120 days out, so the AU
ladder gets steeper the further out you look.

At the defaults, a 189-seat flight 56 days out gives:

```
   FareBucket   FareMidpoint   Demand   Protection    AU
       28617+       31478.70   0.0000            0   189
  18217-21187       19702.00   1.9569            2   189
  13447-15656       14551.50   3.7311            8   184
  10418-12146       11282.00   4.0643           15   179
    7643-8929        8286.00   3.4671           23   170
    5014-5578        5296.00   2.2265           34   158
    3241-3621        3431.00   2.8127           47   146
    2046-2302        2174.00   1.0201           58   133   <- anchor fare
        Promo        1739.20   0.0000            0   131
```

`AU` reaching 0 requires predicted remaining demand to exceed capacity, i.e. a
far-out, heavily-booked flight. It is not something to expect near departure.

This is worth internalising because it is a property of the model, not of the
mock: **AU is least restrictive near departure and most restrictive far out**,
since protection is computed against future demand only.

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

**Every `AU` equals `Capacity`** — not a bug. There is not enough predicted
pickup relative to capacity for EMSR-b to protect anything. Raise
`--load-factor` or `--future-days`; see
[the AU ladder needs demand](#the-au-ladder-needs-demand).

**`RBD` is empty on the AU ladder** — production behaviour. Only the promo row
carries an RBD (`W`); the model has no bucket-to-RBD mapping at step 15, so if you
need RBDs against AUs that mapping has to come from elsewhere.

**`WTP_pax_count` equals `New Final_pax_count`** — the WTP factor for that
sector-month is 0%, or `wtp.csv` has no row covering it.

**Duplicate `FareBucket` column warning** — expected, see
[section 10](#known-quirk-in-the-production-frame).

**`git diff` shows changes in outputs/ after a no-op run** — the snapshot date
moved. Re-generate with `--anchor` pinned to the date the committed baseline was
built with (`SnapshotDate` in any output row).
