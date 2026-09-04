# Additive Pickup Model — Technical Flow Document

## Overview

The Additive Pickup Model forecasts additional bookings expected on future flights at both **Fare Bucket** and **Flight** level. It uses historical NDO (Number of Days Out) booking patterns from similar flights to predict how many more passengers will book on a given future flight before departure.

The raw forecast is then passed through two adjustment layers before saving: a **Demand Multiplier** (Step 13) that scales each flight's prediction for expected month-level demand, and a **WTP fare-bucket redistribution** (Step 14) that reshapes the within-flight fare-bucket split for expected price inflation/deflation.

The model also defines **Inventory Optimization** (EMSR-b protection limits / AU) and **Market Fare Integration** (RateGain OAL/QP), but these are **not currently wired into the run** — see *Deferred Steps*.

**Execution model (new).** The pipeline no longer runs as a single monolithic Glue job over all sectors. It is split into two Lambdas plus a fetch-only Glue job, coordinated through a per-run **audit document** in MongoDB:

- **`rm-additive-prefetch`** — the planner / load balancer. Reads the active-sector list and the *previous* run's per-sector timings, packs sectors into balanced bins, and fans them out over SQS. Routes sectors that have no cache yet (new / backfill) to the Glue fetch-only job instead of a process bin.
- **`rm-additive-process`** — the worker. Consumes one bin from SQS, runs the full forecast pipeline (Steps 1–15) for those sectors, writes each sector's processing time to the audit doc, and writes results to S3.
- **`rm-additive-fetch` (Glue, fetch-only)** — for new sectors / backfills only: pulls the full historical window into the S3 cache with **no** processing, so the next daily `process` run treats them as ordinary cached sectors.

The forecast math (Steps 1–15) is identical to the previous single-job version — only orchestration, storage, and audit changed.

---

## Execution Architecture (Prefetch → Process → Audit)

```
                         ┌──────────────────────────────┐
                         │  EventBridge (daily cron)     │
                         └───────────────┬──────────────┘
                                         ▼
                         ┌──────────────────────────────┐
                         │  rm-additive-prefetch (Lambda)│
                         │  • read active_sectors        │
                         │  • read prior run sector_stats│  ◄── audit doc (last run)
                         │  • split cached vs new sectors│
                         │  • LPT bin-pack cached sectors│   (the load balancer)
                         │  • write NEW run skeleton doc │  ──► audit doc (this run)
                         └───────┬───────────────┬───────┘
                new sectors      │               │      cached sectors (balanced bins)
             (backfill needed)   ▼               ▼
              ┌───────────────────────┐   ┌──────────────── SQS ────────────────┐
              │ rm-additive-fetch     │   │ bin 0 | bin 1 | bin 2 | ... | bin k  │
              │ (Glue, fetch-only)    │   └───┬──────┬──────┬───────────┬────────┘
              │ • full history → S3   │       ▼      ▼      ▼           ▼
              │   cache, NO processing│   ┌────────┐┌────────┐      ┌────────┐
              └───────────┬───────────┘   │process ││process │ ...  │process │  (parallel
                          │               │Lambda 0││Lambda 1│      │Lambda k│   invocations)
                          │               └───┬────┘└───┬────┘      └───┬────┘
                          │                   │  per-sector timing +    │
                          │                   │  results                │
                 S3 cache (ready next run)    ▼                         ▼
                          │        ┌───────────────────────┐  ┌──────────────────┐
                          └───────►│ S3 datasets (output)   │  │ audit doc (Mongo)│
                                   │ snapshot_date=/sector= │  │ sector_stats{}   │
                                   └───────────────────────┘  │ sectors_completed│
                                                              │ status           │
                                                              └──────────────────┘
```

**Why the split.** A single Lambda can't process ~50 international sectors within the 15-minute limit, and a monolithic Glue job serialises everything. Fanning balanced bins across ~10 parallel `process` Lambdas brings a full daily run to **under ~5 minutes** while keeping each invocation well inside its timeout. The prefetch owns *planning* (who does what), the process Lambdas own *work* (the forecast), and the audit doc is the shared ledger that (a) lets bins coordinate completion without a central orchestrator and (b) feeds next run's load balancing.

**Message contract (prefetch → SQS → process).** One SQS message per bin:

```json
{
  "modules": {"additive_process": 1},
  "overrides": {
    "sectors": ["AUHBLR", "BOMDXB", "..."],
    "run_id": "2026-07-03T02:00:05Z-ab12",
    "batch_index": 3,
    "batch_total": 10,
    "sectors_expected": 48,
    "mode": "PROD"
  }
}
```

The shared `lambda_function.py` handler routes a nested `overrides` key wholesale even for SQS events, so no handler change is needed beyond registering `additive_process` in `module_map`.

Each SQS message carries **one sector** (SQS batch size 5, concurrency 60). Messages are processed **independently** in the handler — a per-message failure redelivers only that message.

**Mapped sectors (N:1 fallback donor — own-data-first).** Many thin/no-history sectors may map to a single donor (e.g. `NMIDEL, NMIBLR, JAIBLR → BOMDEL`, configured in `qp_rm_master.config_additive_mapped_sectors`). The flow is two-phase and rides the same 1-sector-per-message queue:

1. **Prefetch** dispatches the **donor** as a normal message, tagging it with the dependent list. Dependents are **not** dispatched here.
   ```json
   {"modules": {"additive_process": 1},
    "overrides": {"sectors": ["BOMDEL"],
                  "dependent_sectors": ["NMIDEL", "NMIBLR", "JAIBLR"],
                  "run_id": "…", "sectors_expected": 48, "mode": "PROD"}}
   ```
2. **The donor caches its history, then fans out immediately.** As soon as the donor finishes **Step 2** (its history is written to the warm cache), the worker **enqueues each dependent** as its own 1-sector message carrying `mapped_sector`. Crucially this happens **before** the donor runs Steps 3–15, so dependents execute **in parallel** with the donor's own processing rather than waiting for it to finish (`_enqueue_dependents_after_cache`, one-shot per invocation via the `_deps_enqueued` guard):
   ```json
   {"modules": {"additive_process": 1},
    "overrides": {"sectors": ["JAIBLR"], "mapped_sector": "BOMDEL", "run_id": "…", "mode": "PROD"}}
   ```
3. **Each dependent runs** on its own invocation. It fetches its own (possibly thin) history **plus** the donor's, but the donor's history is **NOT relabelled** — it keeps its own `Sector` label through tagging and matching. This is the **own-data-first** contract:
   - **Own history is tried first.** The dependent walks all 7 own-sector tagging levels and all own-sector flight-matching levels against **its own** flown history. If it clears the `MIN_FLIGHT_MATCHES` threshold, the donor is **never consulted** and the output is identical to a standalone run of that sector.
   - **Donor is a fallback only.** Only when all 7 own levels fail to reach the threshold does the dependent fall to the donor's tagging levels (8–14) and the donor's flight-matching ladder.
   - **Relabel happens at merge time, per winning key only.** When the donor wins a match for a given future flight, exactly those matched donor rows are relabelled to the dependent `Sector` right before the Step 10 / Step 12 joins (tracked in `self._donor_won = {future_key: (donor, dependent)}`). So join keys line up without erasing provenance for the own-data path.
   - **Donor history is bucketed with the dependent's fare buckets** (Step 6 / Step 7) so it can be pooled and matched, but it stays labelled `Sector=donor` until a merge-time relabel. The donor's **future** flights are stripped (`_restrict_future_frames_to`) so no donor output leaks.

   **Fail-fast:** a dependent with **no QP fares** to build fare buckets is failed directly (cannot forecast without its own buckets); a message that declares `mapped_sector` but resolves **no donor** is failed directly (mapping is required, no silent standalone fallback).

`sectors_expected` counts donors + plain + **dependents** (dependents produce output and write their own audit stat), so the run's `audit_finalize` only closes once every dependent has also completed. Excluding mapped sectors, normal sectors behave exactly as before — and a data-rich mapped sector now also behaves exactly like a standalone run, because the donor is only reached on genuine own-data starvation.

---

## Code Structure (Process Lambda)

`rm-additive-process` is split into four files. The processor holds the forecast
math; all DB, SQL, cache, and audit work lives in helpers behind a shared cache.

| File | Responsibility |
|------|----------------|
| `lambda_function.py` | Handler. Builds injected connections (ESB engine, Mongo `qp_dw_rmalloc` + `qp_rm_master`, S3) once at module load, merges an SQS batch of 1-sector messages into one bin, and dispatches via `module_map` → `AdditivePickupModelProcessor(...).run()`. |
| `additive_model.py` | `AdditivePickupModelProcessor` — orchestration (`run` / `_run_batch`) plus all forecast math (Steps 1–15). No direct DB/cache code; it delegates. |
| `helpers/db_helpers/additive_esb_helper.py` | `AdditiveEsbHelper` — all ESB SQL (historical incremental, STD lookup, forward scan), writing results into the shared cache. |
| `helpers/db_helpers/additive_mongo_helper.py` | `AdditiveMongoHelper` — all Mongo I/O: active sectors, tax master, raw fares, demand multiplier, WTP, and audit upsert/finalize. |
| `helpers/utils/common_utils.py` | Pure helpers (tagging / date / fare-bound parsing), `DatabaseConfig` (table names), and the **`AdditiveCache`** class. |

**`AdditiveCache` is the keystone.** It is constructed once with the run mode
and the injected S3 client, then shared *by reference* with both helpers. It owns
two things: the run-scoped in-memory dict `cache.data` (tax master, fares, derived
frames) and the parquet cache. In **TEST** it reads/writes local parquet; in
**PROD** it reads/writes S3 sector/month partitions. Because the same object is
shared, a value the Mongo helper writes (e.g. tax master) is immediately visible
to the ESB helper and the processor — so **test mode and caching behave
identically in every file**.

Wiring in `__init__`:

```python
self.cache = AdditiveCache(self.mode, self.s3_client, logger)
self.esb_helper = AdditiveEsbHelper(self.esb_engine, logger).bind_context(self.cache)
self.mongo_helper = AdditiveMongoHelper(self.db, self.master_db, logger).bind_context(self.cache, self.mode)
self.mongo_helper.set_run(run_id, sectors_expected, analyst, batch_index, snapshot_date)
```

### Tunable Constants

The model's logic constants live in `Constants.py` and are mirrored into `ConstantsUtils` (`constants_utils.py`), which also exposes DB overrides via `rm_parameter_values` (all DB parameter values are strings, so each is cast on read). The structural sets (`NDO_RANGE`, `REQUIRED_RBDS`, `FARE_BUCKET_RBDS`) are static; the following are the calculation constants intended for tuning:

| Constant | Default | Cast | Used in |
|----------|:-------:|:----:|---------|
| `MIN_FLIGHT_MATCHES` | 13 | `int` | Step 9 — flown-pool match threshold |
| `WEIGHT_ALPHA` | 0.99 | `float` | Steps 11/12 — recency decay |
| `WEIGHT_FUTURE_X` | 1 | `int` | Steps 11/12 — weight exponent for not-yet-departed history |
| `GP_FARE_BUCKET_POINTS` | 19 | `int` | Step 1 — GP boundary points (upper bound on bucket count) |
| `FARE_BUCKET_LOWER_FACTOR` | 0.8 | `float` | Step 6 — lowest bucket's lower-bound scale |
| `FUTURE_TAG_MIN_ENTRIES` | 12 | `int` | Step 5 — min rows for a tagging level to be usable |
| `TIME_BAND_HOURS` | 2 | `int` | Step 9 — STD band on the DOW-Tagging ladder level |
| `PREDICTION_HORIZON_DAYS` | 180 | `int` | Step 8 — forecast horizon |
| `PARQUET_ROW_THRESHOLD` | 250000 | `int` | storage chunking |

---

## Data Sources

| Source | Type | Connection | Purpose |
|--------|------|------------|---------|
| `QP_DW_NAVFACT.FLOWNDATAPAX` | MySQL (DW) | Historical flown passenger data | NDO pickup patterns + revenue for RASK |
| `QP_DW_NAVFACT.BOOKEDDATAPAX` | MySQL (DW) | Forward booked passenger data | Current bookings on future flights |
| `QP_DW_NAVFACT.flight_master` | MySQL (DW) | Flight inventory/schedule | STD, Capacity, FlightNumber, Sector, Status |
| `qp_rm_master.active_sectors` | MongoDB | Active sector configuration | Which sectors to process |
| `qp_dw_rmalloc.fares` | MongoDB | QP airline fare components | Fare bucket computation via GP |
| `qp_rm_master.market_fares_rg` | MongoDB | RateGain market fares | OAL/QP competitor pricing |
| `RM_DW_DATA.*` | MySQL (DW) | Output tables | Final predictions storage |

| `qp_dw_rmalloc.additive_model_run_audit` | MongoDB | Per-run audit ledger | Per-sector timing (load balancer), run completion tracking |
| `qp_rm_master.config_sector_demand_multiplier` | MongoDB | Flight-level demand multiplier | Step 13 — scales each flight's prediction (%) |
| `qp_rm_master.config_quarter_wtp` | MongoDB | Sector × Month-Year WTP factor | Step 14 — price inflation/deflation redistribution (%) |

**Note (updated for the Lambda split).** SQL **reads** go through the injected **ESB engine** (`rdEsbConn`); there are **no DW writes** anymore — forecast output is written to **S3 only**, partitioned `snapshot_date=/sector=`. All connections (ESB engine, DW engine, S3, and the three Mongo DBs) are **injected via `params`** by `lambda_function.py`; the module creates no engines and reads no SSM. The `flight_master.Sector` column uses slash format (`BOM/DEL`), while downstream code uses concatenated format (`BOMDEL`).

### Key Column Formats (ESB — post-migration)

| Column | Type | Format | Example | Purpose |
|--------|------|--------|---------|---------|
| `DepartDate` / `DepartureDate` (flight_master) | bigint | YYYYMMDD | `20260703` | Fast date range filtering via integer index |
| `BookingDateUTC` | bigint | YYYYMMDD | `20250204` | Fast booking date filtering |
| `STDTime` | bigint | 1HHMMSS | `1083000` = 08:30:00 | Leading `1` prefix avoids loss of leading zeros for early morning times |
| `Distance` | decimal | — | `1148.0000` | Available directly on pax table (no CityPair join needed) |
| `Sector` (flight_master) | varchar | ORG/DST | `BOM/DEL` | Pre-computed on flight_master; converted to `BOMDEL` in code |
| `AdjustedCapacity` (pax table) | int | — | `189` | Available directly on pax table (no JOIN to flight_master needed for capacity) |

---

## Consolidated SQL Queries

The model uses an **incremental caching strategy** with 3 queries:

### Query 1: Historical Flown Data (incremental with parquet cache)

**Table:** `FLOWNDATAPAX` only (NO JOIN to flight_master — avoids 100M+ row scan)

**First run:** Fetches full 365 days (~6 min for 1 sector, ~20 min for 10 sectors)  
**Subsequent runs:** Fetches only yesterday's data from cache (~1-2 seconds)

**What it fetches:**
- Per-PNR booking records: Sector (via CONCAT(Origin,Destination)), DepartureDate, NDO, FlightNumber, RBD, FareBasis
- Fare calculation: `FB_Net` (when 5th char of FareBasis = 'T') or `DiscountedBaseFare_Pro` + `YQ_Pro` + `RCS` + `CUTE`
- Pax Count (excluding SOL bookings)
- Revenue per flight (for RASK lean/peak tagging)
- Capacity (`AdjustedCapacity` from pax table directly) and Distance (from pax table directly)
- InventoryLegID (for STD lookup in Query 2)

**Filters (using bigint indexes for performance):**
- `DepartDate BETWEEN 20250707 AND 20260706` (bigint index)
- `BookingDateUTC >= 20250123` (lower bound — 545 days back)
- `(Origin = 'BOM' AND Destination = 'DEL') OR ...` (individual indexed columns)
- `FlightNumber < 6000` (direct int comparison)
- `AdjustedCapacity > 0`

**Cache behavior:**
- Saved to `additive_model_cache/hist_flown_data.parquet`
- On re-run: loads cache, fetches only new day, appends, prunes data > 365 days old

### Query 2: STD Lookup (incremental)

**Table:** `flight_master` — lightweight lookup by InventoryLegID

**What it fetches:** `InventoryLegID` → `STDTime` mapping

**Process:** Only fetches STD for InventoryLegIDs not already in the STD cache. Batched in groups of 5000 IDs.

**Cache:** `additive_model_cache/std_lookup.parquet`

**Note:** No `IsActive = 1` filter — historical (departed) flights have `IsActive = 0` but we still need their STD.

### Query 3: Forward Data (always fresh — not cached)

**Tables:** `flight_master` LEFT JOIN `BOOKEDDATAPAX` (on InventoryLegID)

**What it fetches:**
- Future inventory legs: Sector (via CONCAT(DepartureStation, ArrivalStation)), DepartureDate, STD, FlightNumber, Capacity
- Per-PNR booked data: NDO, RBD, FareBasis, Fare, Pax Count
- Aggregated "Booked Pax" count (bookings where `BookingDateUTC < today`)

**Filters:**
- `flight_master.DepartureDate BETWEEN 20260707 AND 20270102` (bigint)
- `flight_master.Sector IN ('BOM/DEL', 'DEL/BOM', ...)` (slash format)
- `flight_master.CarrierCode = 'QP'`, `IsActive = 1`
- `Capacity > 0`, `FlightNumber < 6000`

**Why not cached:** Forward bookings change every day — must always be fresh.

**Data window:** Departure dates span 365 days, but booking dates can go back up to **545 days** (365 + 180 NDO) — the BookingDateUTC lower bound handles this.

---

## Step-by-Step Pipeline Flow

### Step 0: Receive Sector Bin (from Prefetch)

**Source:** `overrides['sectors']` — the bin assigned to this `process` invocation by `rm-additive-prefetch`.

The worker no longer fetches the full active-sector list; it processes exactly the sectors handed to it. Fallbacks:
- **TEST mode** → `additive_constants.TEST_SECTORS`.
- **Standalone run with no bin** (manual/backfill single invocation) → reads `qp_rm_master.active_sectors` directly.

The bin is then processed **one sector at a time** (batch of 1) so each sector's wall-clock time can be measured for the audit / load balancer. Results are identical to batch processing because every downstream step groups by `Sector`.

**Output:** `self.active_sectors` = the assigned bin, e.g. `['AUHBLR', 'BOMDXB', 'DELDXB', ...]`

---

### Step 1: Fetch Fare Bucket Configuration

**Source:** MongoDB `qp_dw_rmalloc.fares`

Computes sector-level fare buckets dynamically from airline fare components using geometric progression (GP).

**Process:**
1. Fetch fares for active sectors, required RBDs only, exclude `TOTAL = 1`
2. For each sector:
   - Get sorted unique fare values (TOTAL column)
   - G1 = min fare, G19 = max fare
   - Compute common ratio: `R = (G19/G1)^(1/(N-1))` where `N = GP_FARE_BUCKET_POINTS` (default **19**)
   - Generate `N` GP values: `G_i = G1 × R^(i-1)`
   - Match each GP value to closest actual fare component (**deduplicate consecutive matches**)
   - Form buckets: consecutive ranges + 1 open-ended top bucket

**Example DataFrame (`farebasis_df`) for sector BOMAMD:**

| Sector | FareBasis |
|--------|-----------|
| BOMAMD | 1620-1911 |
| BOMAMD | 1911-2145 |
| BOMAMD | 2145-2645 |
| BOMAMD | 2645-2865 |
| BOMAMD | ... |
| BOMAMD | 21685-25088 |
| BOMAMD | 25088+ |

**Up to 19 fare buckets per sector** (18 lower-upper ranges + 1 open-ended). The count is an **upper bound**, not fixed: because consecutive GP points that map to the **same** actual fare are de-duplicated, a sector whose real fares are **clustered** ends with **fewer** buckets (e.g. BOMDEL collapses to 16). Each sector's bucket set is computed **independently from its own fares** — so a donor's bucket count never changes a dependent's, since the dependent uses its own QP fares for its own buckets.

---

### Step 2: Consolidated DB Fetch (Incremental, sector/month cache)

**Storage model.** Caches are parquet, partitioned by **sector and month**:

```
_cache/hist_flown/sector=BOMDEL/month=2025-07.parquet ... month=2026-07.parquet
_cache/tagging/  sector=BOMDEL/month=2025-07.parquet ...
_cache/std_lookup.parquet                       (single global file, leg-ID keyed)
```

TEST writes these under a local directory; PROD writes them to S3 (`_cache/`
prefix). The layout is identical in both — only the destination differs — so a
local test exercises the same paths as prod.

**Read** — `_read_sector_cache` lists every `month=` partition for the batch's
sectors and concatenates them, reconstructing the full retained window.

**Write** — `_write_sector_cache(kind, df, months=…)` rewrites ONLY the month
partitions named in `months`. This is what makes the daily run cheap: it
touches one file, not the whole year. `months=None` means "rewrite every month
present" and is used only for a backfill or the month-start re-tag.

**Daily one-day update** (cached sector, normal day):
1. read all month partitions → full-year `cached_df`
2. fetch **yesterday only** from SQL
3. `touched_months = {month(yesterday)}`
4. drop yesterday from `cached_df`, concat the fresh rows, apply retention
5. write back **only** `sector=X/month=<current>.parquet` — the other months are untouched

**Force backfill** (`FORCE_BACKFILL=true`) — skips the cache read (every sector
treated as new → full-window query `hist_start … yesterday`), and writes with
`months=None` so every month partition is rebuilt. Use on the first run and
after any change to the fare/revenue definition or the NDO window. Requires a
clean cache directory/prefix first, since it overwrites in place rather than
deleting.

**Custom / CDP month refresh** (`REFRESH_MONTHS=N` or an explicit list) — a PNR
cancelled long after departure corrupts an older month that the daily path
never revisits. This refetches the chosen months for the batch's sectors and
rewrites exactly those partitions. `N` = last N calendar months; clamped to the
retained window. Tags for refreshed months are corrected at the next
month-start re-tag.

**Retention** — the floor is the first day of the current month, one year back
(`min(month_start − 1yr, month_start − 365d)`), so it moves only at month
rollover. Whole month partitions below the floor are deleted; the floor month
itself is rewritten if it lost rows.

**Original incremental description follows:**


Uses a **3-query strategy** with local parquet caching:

| Query | Target | First Run | Daily Run |
|-------|--------|-----------|-----------|
| 1 | `FLOWNDATAPAX` (no JOIN) | Full 365-day scan (~6-20 min) | Yesterday only (~1-2 sec) |
| 2 | `flight_master` STD lookup | All unique leg IDs | Only new leg IDs (~instant) |
| 3 | `flight_master` + `BOOKEDDATAPAX` | Always fresh | Always fresh (~2-3 sec) |

**Cache files (in `additive_model_cache/`):**
- `hist_flown_data.parquet` — rolling 365-day historical pax data
- `std_lookup.parquet` — InventoryLegID → STDTime mapping

**Daily run behavior:**
1. Load hist cache from parquet (~1 sec)
2. Fetch yesterday's new data from DB (~1-2 sec)
3. Append, deduplicate by departure date, prune >365 days, save cache
4. Fetch STD for any new InventoryLegIDs not in STD cache
5. Fetch fresh forward data (always)

**Post-processing (in Python, not SQL):**
- `STDTime` (1HHMMSS bigint) → Python `time` objects via `_int_to_time()`
- `DOW` derived from `DepartureDate.dt.day_name()`
- NDO 0-180 filtering applied in-memory

---

### Step 3: Derive Datasets from Cached Data

Four DataFrames are derived in-memory (no additional DB calls):

#### 3a. Flown Data (for NDO Pickup Model)

| Sector | DepartureDate | DOW | NDO | FlightNumber | STD | PNR | RBD | Fare | Pax Count |
|--------|--------------|-----|-----|--------------|------|-----|-----|------|-----------|
| BOMDEL | 2025-07-10 | Thursday | 45 | 1234 | 08:20:00 | ABC123 | Q2 | 4500 | 2 |
| BOMDEL | 2025-07-10 | Thursday | 12 | 1234 | 08:20:00 | DEF456 | T1 | 3200 | 1 |

#### 3b. Lean/Peak Source (for RASK calculation)

Aggregated at flight level (sum of revenue, max capacity, first distance):

| Sector | DepartureDate | FlightNumber | STD_time | Revenue | Capacity | Distance |
|--------|--------------|--------------|----------|---------|----------|----------|
| BOMDEL | 2025-07-10 | 1234 | 08:20:00 | 850000 | 189 | 1148 |
| BOMDEL | 2025-07-11 | 1234 | 08:20:00 | 720000 | 189 | 1148 |

#### 3c. Booked Data (forward NDO pickup)

Same structure as Flown Data but from `BOOKEDDATAPAX` for departures today to +179 days.

#### 3d. Future Flights (inventory with current bookings)

| Sector | DepartureDate | STD | FlightNumber | Capacity | Total Bookings |
|--------|--------------|------|--------------|----------|----------------|
| BOMDEL | 2026-07-15 | 08:20:00 | 1234 | 189 | 142 |
| BOMDEL | 2026-07-16 | 08:20:00 | 1234 | 189 | 98 |

---

### Step 4: Compute Historical Lean/Peak Tagging

**Purpose:** Classify each historical flight as Lean, Shoulder, Peak, or Super Peak based on its RASK performance relative to its Sector-TimeCluster average.

**Calculation Flow:**

1. Assign Time Cluster to each flight based on STD:
   - 02:30–07:30, 07:30–11:00, 11:00–14:30, 14:30–18:00, 18:00–21:00, 21:00–02:30

2. Calculate flight-level RASK:
   ```
   RASK = Revenue / (Capacity × Distance)
   ```

3. Calculate Sector-TimeCluster level RASK:
   ```
   TC_RASK = Total_TC_Revenue / (Total_TC_Capacity × Distance)
   ```

4. Calculate Standard Deviation at Sector-TimeCluster level:
   ```
   STD = sqrt( Σ(TC_RASK - RASK_i)² / (N - 1) )
   ```

5. Calculate Z-Score:
   ```
   Z_Score = (RASK - TC_RASK) / STD
   ```

6. Remove outliers (|Z| > 6)

7. Apply tagging:
   | Z-Score Range | Tag |
   |---------------|-----|
   | Z < -0.5 | Lean |
   | -0.5 ≤ Z < 1 | Shoulder |
   | 1 ≤ Z < 2 | Peak |
   | Z ≥ 2 | Super Peak |

**Example Output (`historical_tagging`):**

| Sector | DepartureDate | FlightNumber | Time Cluster | Tagging |
|--------|--------------|--------------|--------------|---------|
| BOMDEL | 2025-07-10 | 1234 | 7:30-11:00 | Shoulder |
| BOMDEL | 2025-12-25 | 1234 | 7:30-11:00 | Peak |
| BOMDEL | 2025-02-05 | 1234 | 7:30-11:00 | Lean |

---

### Step 5: Compute Future Lean/Peak Tagging

**Purpose:** Assign lean/peak tags to future flights (which have no RASK data) using the 50th percentile of historical tagging grouped by Sector, DOW, Quarter, Time Cluster.

**Process:**

1. Add DOW, Quarter columns to historical tagging
2. Sort by Sector, DOW, Quarter, Time Cluster, Tagging (descending: Super Peak → Lean)
3. Select middle (50th percentile) tag per group

**Example: Group = (BOMDEL, Monday, AMJ, 7:30-11:00)**

Sorted tags: `[Shoulder, Shoulder, Lean, Lean, Lean]`  
Middle index: `(5-1)//2 = 2` → picks `Lean`

**Output (level `q_dow_tc` — the finest of the 7 frames):**

| Sector | DOW | Quarter | Time Cluster | Tagging |
|--------|-----|---------|--------------|---------|
| BOMDEL | Monday | AMJ | 7:30-11:00 | Lean |
| BOMDEL | Monday | AMJ | 14:30-18:00 | Shoulder |
| BOMDEL | Tuesday | OND | 7:30-11:00 | Peak |

**Fallback ladder (7 own-sector levels, + 7 donor levels = 14).** `_compute_future_tagging` now returns a **dict of 7** progressively coarser 50th-percentile frames (`_future_tag_levels`), and `_fill_nan_tagging` applies them in order until every future flight has a tag. Each level drops any group with fewer than **`FUTURE_TAG_MIN_ENTRIES` (12)** rows, so a sparsely-supported fine group falls through to a better-supported coarser level rather than producing a low-confidence tag.

Own-sector levels (1–7), coarsest last:

| # | Level key | Grouping keys | Example |
|---|-----------|---------------|---------|
| 1 | `q_dow_tc` | Sector, Quarter, DOW, Time Cluster | (BOMDEL, AMJ, Monday, 7:30-11:00) → Lean |
| 2 | `q_dow`    | Sector, Quarter, DOW | (BOMDEL, AMJ, Monday) → Shoulder |
| 3 | `q_tc`     | Sector, Quarter, Time Cluster | (BOMDEL, AMJ, 7:30-11:00) → Shoulder |
| 4 | `q`        | Sector, Quarter | (BOMDEL, AMJ) → Shoulder |
| 5 | `dow_tc`   | Sector, DOW, Time Cluster | (BOMDEL, Monday, 7:30-11:00) → Shoulder |
| 6 | `dow`      | Sector, DOW | (BOMDEL, Monday) → Shoulder |
| 7 | `sector`   | Sector | (BOMDEL) → Shoulder |

**Mapped-sector levels (8–14).** Only for a dependent (mapped) sector whose own 7 levels still leave NaNs: `_fill_nan_tagging` maps the dependent's `Sector` to its donor (`_MP_Sector`) and re-applies the **same 7 grouping shapes** against the donor's tagging frames. A normal sector never enters this branch (`if self.mapped_sectors and df['Tagging'].isna().any()`). This is the tagging half of the **own-data-first** contract: the donor's tags are used only after the dependent's own tags are exhausted.

---

### Step 6: Prepare Combined Data with Fare Buckets

**Purpose:** Combine flown + booked data, filter SOL bookings, and assign each booking to a fare bucket based on average fare per PNR.

**Process:**

1. Concatenate `flown_df` and `booked_df`
2. Filter: `Pax Count > 0` (removes SOL-only rows)
3. Calculate `AvgFare = Fare / Pax Count`
4. For each row, find the fare bucket where `Lower ≤ AvgFare < Upper`
5. Lowest fare bucket's lower bound is scaled by `FARE_BUCKET_LOWER_FACTOR` (default **0.8**, i.e. −20%) to catch near-boundary fares (per BRD)
6. **Donor rows (mapped sectors):** a donor history row (`Sector=donor`) has no buckets of its own, so it is bucketed using the **dependent's** ranges (via the inverted `{donor: dependent}` map). The row keeps `Sector=donor`; only its `FareBasis` comes from the dependent's ladder, so it can be pooled and matched while provenance is preserved until a merge-time relabel.

**Example: Sector BOMAMD, AvgFare = 3500**

Fare buckets: `[1620-1911, 1911-2145, ..., 3000-4000, 4000-5000, ...]`  
→ Falls into `3000-4000`

**Example Output (`combined_df` — subset of columns):**

| Sector | DepartureDate | NDO | FlightNumber | RBD | Fare | Pax Count | FareBasis |
|--------|--------------|-----|--------------|-----|------|-----------|-----------|
| BOMDEL | 2025-07-10 | 45 | 1234 | Q2 | 7200 | 2 | 3000-4000 |
| BOMDEL | 2025-07-10 | 12 | 1234 | T1 | 3200 | 1 | 3000-4000 |
| BOMDEL | 2025-07-10 | 5 | 1234 | V2 | 1800 | 1 | 1620-1911 |

---

### Step 7: Pivot NDO Data

**Purpose:** Create a matrix where each row is a (Flight, FareBucket) combination and columns are NDO 0-180 with pax count values. This is the core data structure for the additive pickup calculation.

**Process:**

1. Split into `main_df` (required RBDs + valid fare bucket) and `remaining_df` (everything else)
2. Pivot `main_df`:
   - Index: Sector, DepartureDate, DOW, FlightNumber, STD, FareBasis
   - Columns: NDO (0 to 180)
   - Values: sum(Pax Count)
3. Impute missing fare buckets per flight (add zero rows for buckets with no bookings)
4. Fill zeros for NDOs where booking date is already past
5. Add Time Cluster, merge historical tagging, fill NaN tags with future tagging

**Example Output (`pivot_ndo` — showing NDO columns 0-5 only):**

| Sector | DepartureDate | DOW | FlightNumber | STD | FareBasis | 0 | 1 | 2 | 3 | 4 | 5 | ... | Tagging |
|--------|--------------|-----|--------------|------|-----------|---|---|---|---|---|---|-----|---------|
| BOMDEL | 2025-07-10 | Thu | 1234 | 08:20 | 3000-4000 | 3 | 2 | 1 | 4 | 2 | 0 | ... | Shoulder |
| BOMDEL | 2025-07-10 | Thu | 1234 | 08:20 | 4000-5000 | 1 | 0 | 2 | 1 | 1 | 3 | ... | Shoulder |
| BOMDEL | 2025-07-10 | Thu | 1234 | 08:20 | 1620-1911 | 0 | 0 | 0 | 0 | 0 | 0 | ... | Shoulder |

**Interpretation:** Flight BOMDEL-1234 on 2025-07-10 received 3 pax in fare bucket 3000-4000 at NDO 0 (day of departure), 2 pax at NDO 1, etc.

---

### Step 8: Prepare Future Flights with Tagging

**Purpose:** Attach lean/peak tagging and fare bucket expansion to future flights.

**Process:**

1. Filter to prediction horizon (180 days)
2. Add Quarter, DOW, Time Cluster columns
3. Merge tagging from Step 5 (with fallback)
4. Cross-join with fare buckets per sector (expand each flight to N fare bucket rows)
5. Create Key column: `Sector_DepartureDate_FlightNumber`

**Example Output (`future_df`):**

| Sector | DepartureDate | STD | FlightNumber | Capacity | Total Bookings | DOW | Quarter | Time Cluster | Tagging | Key |
|--------|--------------|------|--------------|----------|----------------|-----|---------|--------------|---------|-----|
| BOMDEL | 2026-08-15 | 08:20 | 1234 | 189 | 98 | Saturday | JAS | 7:30-11:00 | Shoulder | BOMDEL_2026-08-15_1234 |

**Example Output (`future_df_expanded` — one row per fare bucket):**

| Sector | DepartureDate | FlightNumber | Capacity | Total Bookings | Tagging | FareBasis | Key |
|--------|--------------|--------------|----------|----------------|---------|-----------|-----|
| BOMDEL | 2026-08-15 | 1234 | 189 | 98 | Shoulder | 1620-1911 | BOMDEL_2026-08-15_1234 |
| BOMDEL | 2026-08-15 | 1234 | 189 | 98 | Shoulder | 1911-2145 | BOMDEL_2026-08-15_1234 |
| BOMDEL | 2026-08-15 | 1234 | 189 | 98 | Shoulder | 3000-4000 | BOMDEL_2026-08-15_1234 |

---

### Step 9: Flight Matching

**Purpose:** For each future flight, find historically similar flights to use as the basis for predicting additional bookings.

**Flown vs unflown split.** The historical pool is split **once** into `flown` (`DepartureDate < current_date`, complete NDO curves) and `unflown` (`DepartureDate >= current_date`, incomplete forward curves). The `MIN_FLIGHT_MATCHES` threshold is evaluated **only against the flown pool** — this is what decides the winning group. The `unflown` pool is never counted toward the minimum.

**The winning group is chosen from FLOWN, then applied to BOTH pools.** Once a group definition clears the flown threshold, that **same** group (sector, tag-set, DOW, time-band) is applied to both the flown and unflown pools **with no minimum**, and the two key-lists are concatenated. So future flights inherit the historical winner's group without themselves needing to satisfy the threshold.

**Matching ladder (own-sector, in order).** For each future flight, `_ladder` yields group definitions coarsest-last; the first to clear `MIN_FLIGHT_MATCHES` on the flown pool wins:
1. Sector + DOW + Tagging within **±`TIME_BAND_HOURS` (2h)** of STD
2. Sector + Tagging (any DOW, any STD)
3. Sector + **±1 tagging window** (any DOW/STD), then ±2, ±3 … until the window covers every tag
4. Sector + **all taggings** (final own-sector fallback)

**Minimum matches required:** 13 (`MIN_FLIGHT_MATCHES`), on the flown pool only.

**Mapped-sector (donor) ladder.** If **all** own-sector levels fail to reach the threshold **and** the sector has a donor, matching falls to the donor with a shorter ladder — Sector(donor) + DOW + Tagging (±2h), then Sector(donor) + Tagging. When the donor wins, the future key is recorded in `self._donor_won` so Steps 10/12 relabel exactly those donor rows to the dependent at merge time. A normal sector never enters this branch (`donor is None`).

**Skipped flights.** If neither the own-sector ladder nor (if applicable) the donor ladder reaches the threshold, the flight is **skipped** (absent from `flight_matches`) — no low-confidence prediction.

STD time-band uses a midnight-wrap-safe distance (`diff > 720 → 1440 − diff`).

**Example:**

Future flight: `BOMDEL_2026-08-15_1234` (Saturday, Shoulder, STD 08:20)

Ladder walk on the **flown** pool:
- Level 1: Sector = BOMDEL, DOW = Saturday, Tagging = Shoulder, STD within ±2h of 08:20 → [06:20, 10:20] → **18 flown matches ≥ 13 → wins here**.
- The winning group is then also applied to the `unflown` pool (no minimum); flown + unflown keys are concatenated and stored.

If Level 1 had returned < 13 flown, the walk would continue (drop DOW, then widen the tagging window, then all-tags), and only after all own-sector levels fail would a donor (if configured) be tried.

**Output (`flight_matches` dict):**

```python
{
    'BOMDEL_2026-08-15_1234': ['BOMDEL_2025-08-09_1234', 'BOMDEL_2025-08-02_1234', 
                                'BOMDEL_2025-07-26_1234', ...],  # 18 keys
    'BOMDEL_2026-08-16_1234': ['BOMDEL_2025-08-10_1234', ...],  # 13+ keys
}
```

**Summary CSV (TEST mode):**

| FutureKey | MatchCount |
|-----------|------------|
| BOMDEL_2026-08-15_1234 | 18 |
| BOMDEL_2026-08-16_1234 | 22 |
| DELBOM_2026-08-15_5678 | 14 |

---

### Step 10: Build Matched Result

**Purpose:** For each future flight key, join its matched historical flights' NDO pivot data at the fare bucket level.

**Process:**

1. For each `future_key` → get list of `matched_hist_keys`
2. Get future flight's expanded rows (per fare bucket)
3. Get historical flights' pivot_ndo rows for matched keys (DOW dropped from the frame)
4. Merge on: **Sector, Tagging, FareBasis** (DOW is *not* a merge key — it was already applied as a preference during matching in Step 9, so forcing it again here would over-restrict and produce noisy single-flight joins)
5. **Donor fallback relabel (mapped sectors only).** If this future key was won by its donor (`self._donor_won`), the matched historical rows carry `Sector=donor`; they are relabelled to the dependent `Sector` for that key only, immediately before the merge, so the `Sector` join aligns. Keys won on the dependent's own history are untouched.

**Example:**

Future: `BOMDEL_2026-08-15_1234`, FareBasis `3000-4000`  
Matched historical: 18 flights → each contributes its NDO row for FareBasis `3000-4000`

**Result DataFrame (one row per future-flight × fare-bucket × matched-historical-flight):**

| Sector | DepartureDate_future | FlightNumber_future | FareBasis | DepartureDate_hist | FlightNumber_hist | 0 | 1 | 2 | ... | 180 |
|--------|---------------------|--------------------|-----------|--------------------|-------------------|---|---|---|-----|-----|
| BOMDEL | 2026-08-15 | 1234 | 3000-4000 | 2025-08-09 | 1234 | 3 | 2 | 1 | ... | 0 |
| BOMDEL | 2026-08-15 | 1234 | 3000-4000 | 2025-08-02 | 1234 | 2 | 1 | 3 | ... | 0 |
| BOMDEL | 2026-08-15 | 1234 | 3000-4000 | 2025-07-26 | 1234 | 4 | 3 | 2 | ... | 0 |

---

### Step 11: Calculate Predictions

**Purpose:** Compute mean and weighted-mean predicted additional pax count at fare bucket and flight level.

#### Normal Mean

Group by (Sector, DepartureDate_future, DOW, STD_future, FlightNumber_future, Capacity, Total Bookings, Tagging, FareBasis) → take **mean** of each NDO column across all matched historical rows.

#### Weighted Mean

Apply exponential decay weight based on how old the historical flight is:
```
Weight = 0.99 ^ |DepartureDate_hist - CurrentDate|_days
```
Future-dated records get weight = 1.

Weighted mean = sum(NDO × Weight) / sum(Weight)

#### Predicted Pax Count

For a future flight departing in `N` days, sum the mean NDO values for NDO 0 through N:
```
predicted_pax_count = Σ mean(NDO_i) for i = 0 to N
```

#### Standard Deviation

```
predicted_pax_std = sqrt( Σ std(NDO_i)² for i = 0 to N )
```

**Example — Fare Bucket Level (`result_merged_df`):**

| Sector | DepartureDate_future | FlightNumber_future | FareBasis | future_flight_NDO | predicted_pax_count | predicted_pax_std |
|--------|---------------------|--------------------|-----------|--------------------|--------------------|--------------------|
| BOMDEL | 2026-08-15 | 1234 | 3000-4000 | 43 | 12.5 | 3.2 |
| BOMDEL | 2026-08-15 | 1234 | 4000-5000 | 43 | 8.3 | 2.1 |
| BOMDEL | 2026-08-15 | 1234 | 5000-8000 | 43 | 5.7 | 1.8 |

**Example — Flight Level (`result_final_df`):**

| Sector | DepartureDate_future | FlightNumber_future | Capacity | Total Bookings | predicted_pax_count | predicted_pax_std_square |
|--------|---------------------|--------------------|---------:|---------------:|--------------------:|------------------------:|
| BOMDEL | 2026-08-15 | 1234 | 189 | 98 | 26.5 | 4.3 |

---

### Step 12: Remaining RBD Distribution

**Purpose:** Account for bookings from non-required RBDs (e.g., group/staff/other fare types) that weren't captured in the fare-bucket-level model.

**Process:**

1. Filter `remaining_df` to only non-required RBDs
2. Pivot remaining data by NDO (same structure, no fare bucket dimension)
3. Keep **flown flights only** (`DepartureDate < current_date`) — future flights have incomplete curves and would understate the remaining pax
4. For each future flight, gather the matched historical flights (same `flight_matches` dict), joining on **Sector only** (DOW dropped — matching on DOW here left as few as one flight and produced noisy single-flight estimates)
5. Sum remaining pax for NDOs ≤ future_flight_NDO per matched flight, then take the **recency-weighted mean** across matches using the same decay as Step 11 (`weight = 0.99 ^ x`, `x = current_date − DepartureDate_hist`, future-dated → `x = 1`):
   ```
   remaining_pax_count = Σ(weight × remaining_raw) / Σ(weight)
   ```
   > **Performance note.** The per-row NDO sum (`remaining_raw`) is **vectorised**: instead of a Python `apply(axis=1)` looping over 181 NDO columns per row, a `(1, n_ndo)` column-index vector is broadcast against the `(rows, 1)` NDO-limit column to build a mask, and the masked matrix is summed in NumPy/C. This is behaviour-preserving (identical output to the old loop) but replaces the main hotspot that made Step 12 slow on the exploded match frame — the step also runs twice (normal + weighted), so the exploded frame is large.
   > **Donor fallback.** For a future flight matched via its donor (`_donor_won`), the matched donor rows carry `Sector=donor`; they are relabelled to the dependent `Sector` for that key just before the `Sector`-keyed join, mirroring Step 10.
6. Distribute proportionally across fare buckets:

```
FareBasis_Prediction_ratio = predicted_pax_count_bucket / Total_Predicted_pax_count_flight
Remaining_pax_distribution = ratio × remaining_pax_count_flight
Final_pax_count = predicted_pax_count + Remaining_pax_distribution
```

**Example:**

Flight BOMDEL_2026-08-15_1234:
- Total predicted (required RBDs): 26.5 pax
- Remaining pax (staff/group etc.): 4.2 pax

Fare bucket `3000-4000` predicted 12.5 out of 26.5 → ratio = 0.472  
Remaining distribution for this bucket: 0.472 × 4.2 = 1.98 pax  
Final: 12.5 + 1.98 = 14.48 pax

**Flight Level Final:**

```
Predicted Total Bookings = Total Bookings + predicted_pax_count
Final Prediction Total Bookings = Predicted Total Bookings + remaining_pax_count
```

| Sector | FlightNumber | Total Bookings | predicted_pax_count | remaining_pax_count | Final Prediction Total Bookings |
|--------|-------------|:--------------:|:-------------------:|:-------------------:|:-------------------------------:|
| BOMDEL | 1234 | 98 | 26.5 | 4.2 | 128.7 |

---

### Step 13: Demand Multiplier (Demand Adjusted Prediction)

**Purpose:** Scale each flight's final prediction up or down by an analyst-supplied, **flight-level** demand signal for the future month.

**Input:** `qp_rm_master.config_sector_demand_multiplier` (PROD) / `Demand_multiplier.csv` (TEST), keyed **(DepartureDate, Sector, FlightNumber)**, value a percentage (e.g. `-30%`). Missing key → **0%** (no change).

**Formula (flight level):**

```
DAP = Final Prediction × (100 + Demand Multiplier) / 100
```

Because the multiplier is flight-level, every fare bucket scales by the same factor, so the bucket-level result equals the BRD's proportional redistribution:

```
bucket DAP = (bucket Final / flight Final) × flight DAP = bucket Final × (100 + DM)/100
```

**Behaviour:** `DAP` is **added alongside** `Final_pax_count` (the pre-adjustment value is kept for audit). Applied to all four frames (flight / fare-bucket × normal / weighted). New columns: `Demand_Multiplier`, `DAP`, and `DAP_Total_Bookings` (flight frames). Default 0% leaves the prediction unchanged.

---

### Step 14: WTP Fare-Bucket Redistribution (price inflation/deflation)

**Purpose:** Redistribute the **demand-adjusted** predictions (DAP) *within* each flight's fare-bucket ladder to reflect expected price inflation/deflation in the departure month. This changes only the bucket split — the flight total is preserved.

**Input:** `qp_rm_master.config_quarter_wtp` (PROD) / `WTP.csv` (TEST), keyed **(Sector, Month, Year)** — the loader also accepts **(Sector, Quarter, Year)**, expanding a quarter to its three months. Value a percentage: `+X%` inflation, `-X%` deflation, `0%` no-op.

**Rule (2-term):** treat each bucket's DAP as a uniform density `PaxPerUnit = pax / (Upper − Lower)` (the open top bucket uses the previous bucket's width), rescale the bucket bounds by `1 / (1 + WTP%)`, and re-integrate:

- **Inflation** (factor > 1): each bucket pulls demand from the **lower / previous** bucket.
- **Deflation** (factor < 1): each bucket pulls from the **higher / forward** bucket; the **first** bucket absorbs the bottom leftover and the **top** bucket takes the residual, so the flight total is conserved exactly.

Terms are **signed** (a term goes negative when a bucket is narrower than the shift) — that negative is what conserves the total, so it is never clamped.

**Output columns added (fare-bucket frames only):** `WTP_Factor`, `WTP_pax_count`. Flight-level frames are untouched.

> **Accuracy note.** The 2-term formula is exact when the price shift is smaller than one bucket width. At larger factors (±20 %+) the window crosses more than one bucket and the result approximates — but the flight total is still conserved via the residual.

---

### Worked Example (Steps 13 → 14)

Flight **BOMDEL 1234**, departing **Nov 2026**. After Step 12 its fare-bucket
predictions (`Final_pax_count`) sum to a flight prediction of **45**.

**Step 13 — Demand Multiplier.** `config_sector_demand_multiplier` has
`(2026-11-14, BOMDEL, 1234) = +20%` → factor `1.2`, applied flight-level and
pushed to each bucket by the same factor (`DAP_flight = 45 × 1.2 = 54`):

| Fare bucket | Final_pax_count | DAP (×1.2) |
|-------------|:---------------:|:----------:|
| 3000–4000 | 20 | 24 |
| 4000–5000 | 15 | 18 |
| 5000–6500 | 8 | 9.6 |
| 6500+ | 2 | 2.4 |
| **Flight** | **45** | **54** |

**Step 14 — WTP (+10% inflation).** Redistribute the **DAP** column. Density
`PPU = DAP / width` (the open 6500+ bucket uses the previous width, 1500); bounds
rescaled by `1/1.1`. Inflation pulls each bucket's demand from the **lower**
bucket, so the cheapest bucket loses and the higher buckets gain — total unchanged:

| Fare bucket | DAP | PPU | WTP_pax_count |
|-------------|:---:|:------:|:-------------:|
| 3000–4000 | 24 | 0.0240 | 15.27 |
| 4000–5000 | 18 | 0.0180 | 18.55 |
| 5000–6500 | 9.6 | 0.0064 | 14.00 |
| 6500+ | 2.4 | 0.0016 | 6.18 |
| **Flight** | **54** | | **54.00 ✓** |

Middle-bucket term, worked out (4000–5000):
`(4000 − 4000/1.1)·0.024 + (5000/1.1 − 4000)·0.018 = 8.73 + 9.82 = 18.55`.

**Deflation reverses direction.** The same flight at **−10%** (factor 0.9) pulls
from the **higher** bucket — the cheapest bucket gains, the top loses, still summing to 54:

| Fare bucket | DAP | WTP_pax_count (−10%) |
|-------------|:---:|:--------------------:|
| 3000–4000 | 24 | 32.00 |
| 4000–5000 | 18 | 13.56 |
| 5000–6500 | 9.6 | 7.20 |
| 6500+ | 2.4 | 1.24 |
| **Flight** | **54** | **54.00 ✓** |

---

### Step 15: Save Results

**Weighted-only storage.** The model computes both a normal-mean and a weighted-mean branch internally (the weighted branch reuses the normal frames through Steps 12–14), but **only the weighted outputs are persisted**. The normal-path CSVs, the per-frame `*_full_*.parquet` debug dumps, and the (always-`None`) `inventory_optimization` dataset are no longer written.

**TEST mode outputs (CSV files in `additive_model_output/`):**

| File | Content |
|------|---------|
| `flight_level_weighted_*.csv` | Flight-level predictions (weighted mean) |
| `fare_bucket_weighted_*.csv` | Fare-bucket-level predictions (weighted mean) |
| `intermediate_*.csv` / `.parquet` | Per-step intermediate outputs (still written for debugging) |

**PROD mode outputs (S3 only — DW writes removed):**
- Partitioned Parquet datasets in S3 under `{env}/additive_model/datasets/{dataset}/snapshot_date=YYYY-MM-DD/sector=XXX/data.parquet` for **only** `flight_level_weighted` and `fare_bucket_weighted`. These are the **full** frames (all columns incl. the pivoted NDO 0–180 curve).
- The fare-bucket dataset carries the Step 13/14 columns `Demand_Multiplier`, `DAP`, `WTP_Factor`, `WTP_pax_count`; the flight-level dataset carries `Demand_Multiplier`, `DAP`, `DAP_Total_Bookings`. The demand-adjusted, price-redistributed `WTP_pax_count` is the headline fare-bucket number.
- Each sector writes its own partition independently, so parallel `process` Lambdas never conflict.
- Per-sector timing + run status written to `qp_dw_rmalloc.additive_model_run_audit` (see **Audit Lifecycle & Load Balancer**).

---

## Deferred Steps (defined, not currently wired)

> **Status:** the model *defines* these two steps, but `run()` currently passes `optimized_df=None`, so **no market-fare or inventory dataset is produced today**. They are documented here because they're part of the model's design and are expected to slot in between Step 14 (WTP) and Step 15 (Save), feeding `optimized_df` into the save.

### Market Fare Integration

**Purpose:** Attach competitor (OAL) and own (QP) minimum/maximum fares from RateGain data for pricing context.

**Process:**

1. Fetch RateGain fare data from MongoDB (pre-processed)
2. Split by airline: OAL (other airlines) vs QP (Akasa)
3. Split by channel: B2B (Travelboutiqueonline) vs B2C (others)
4. For each future flight, find fares matching:
   - Same Sector + Same DepartureDate
   - STD within expanding time windows (±2h, ±4h, ..., ±48h)
5. Compute min and max fare for each combination

**Output columns added:**

| Column | Description |
|--------|-------------|
| oal_b2b_min | Lowest OAL fare in B2B channel within time window |
| oal_b2b_max | Highest OAL fare in B2B channel within time window |
| oal_b2c_min | Lowest OAL fare in B2C channel |
| oal_b2c_max | Highest OAL fare in B2C channel |
| qp_b2b_min | Lowest QP fare in B2B channel |
| qp_b2b_max | Highest QP fare in B2B channel |
| qp_b2c_min | Lowest QP fare in B2C channel |
| qp_b2c_max | Highest QP fare in B2C channel |

---

### Inventory Optimization (EMSR-b)

**Purpose:** Calculate protection limits and authorization levels (AU) for each fare class using Expected Marginal Seat Revenue (EMSR-b) methodology, add Promo Fare (W RBD) at 80% of lowest filed fare, and determine Opening Fare without considering promo inventory.

**Process (per flight, fare classes sorted highest fare → lowest):**

For fare class `j` (where class 1 is highest fare, up to class `n` lowest filed fare):

1. **Aggregated mean demand:**
   ```
   μ_bar = μ_1 + μ_2 + ... + μ_j
   ```

2. **Aggregated standard deviation:**
   ```
   σ_bar = min(sqrt(σ_1² + σ_2² + ... + σ_j²), Factor × μ_bar)   (default Factor = 0.25)
   ```

3. **Weighted average fare for aggregated class:**
   ```
   F_j = Σ(μ_i × fare_midpoint_i) / μ_bar
   ```

4. **Next lower fare:**
   - For `j < n`: `f_{j+1}` is the midpoint of next lower filed class `j+1`.
   - For `j = n` (lowest filed class): `f_{n+1} = f_promo = 0.80 × f_n`.

5. **Probability:**
   ```
   P_j = 1 - (f_{j+1} / F_j)
   ```

6. **Z-score from inverse normal:**
   ```
   z = Φ⁻¹(P_j)
   ```

7. **Protection limit:**
   ```
   y_j = min(Capacity, max(0, ⌈μ_bar + z × σ_bar⌉))
   ```

8. **Authorization (AU):**
   ```
   AU_1 = Capacity                          (highest class)
   AU_j = max(0, Capacity - Protection_{j-1})    (class 2..n)
   AU_promo = max(0, Capacity - Protection_n)    (Promo class n+1)
   ```

9. **Promo Fare (W RBD):**
   - Unfiled promo tier with fare midpoint `0.80 × f_n`, RBD `'W'`, `ProtectionLimit = 0`, and `AU = AU_promo`.

10. **Opening Fare Determination:**
   - Evaluates lowest filed fare class where `AU > Current_Bookings` without consideration of Promo inventory.

**Example (Flight BOMDEL-1234, Capacity=189, Current_Bookings=50):**

| FareBucket | FareMidpoint | predicted_pax | predicted_std | ProtectionLimit | AU | OpeningFareBucket | IsOpeningFare | RBD |
|------------|:------------:|:-------------:|:-------------:|:---------------:|:---:|:-----------------:|:-------------:|:---:|
| 25088+ | 27597 | 2.1 | 1.0 | 2 | 189 | 1620-1911 | False | |
| 21685-25088 | 23387 | 3.5 | 1.5 | 5 | 187 | 1620-1911 | False | |
| 15000-21685 | 18343 | 5.2 | 2.0 | 11 | 184 | 1620-1911 | False | |
| 8000-15000 | 11500 | 8.7 | 2.5 | 21 | 178 | 1620-1911 | False | |
| 5000-8000 | 6500 | 12.5 | 3.2 | 36 | 168 | 1620-1911 | False | |
| 3000-4000 | 3500 | 15.8 | 3.8 | 54 | 153 | 1620-1911 | False | |
| 1620-1911 | 1766 | 18.2 | 4.1 | 73 | 135 | 1620-1911 | True | |
| Promo | 1412.8 | 0.0 | 0.0 | 0 | 116 | 1620-1911 | False | W |

---

---

## Edge Cases (handled)

| # | Edge Case | Handling |
|---|-----------|----------|
| 1 | New sector, no history | NaN tagging fails the boolean candidate mask; 0 candidates → flight skipped cleanly, no exception |
| 2 | Fewer than 13 flown matches | Own-sector ladder walks Sector+DOW+Tagging(±2h) → Sector+Tagging → ±N tag-window → all-tags (all on **FLOWN** only). For a mapped sector, a donor ladder is tried only after all own levels fail. If still < 13, flight is **skipped** |
| 3 | Missing fare-bucket config | `_fetch_fare_buckets()` raises; **for a dependent** the batch also fails fast if it has no dependent-sector buckets — the sector fails cleanly (not the whole run) rather than guessing |
| 4 | Completely new flight number | Matched on Sector/DOW/Tagging/time, not flight number — handled |
| 5 | Single-sample time cluster | `Count = 1` → STD = NaN → Z = NaN → dropped from tagging |
| 6 | Capacity change (aircraft swap) | `max` capacity per flight; RASK uses the flight's own capacity |
| 7 | STD wrap-around midnight | `diff > 720 min → 1440 − diff`; 23:55 and 02:00 correctly ~2h apart |
| 8 | NDO = 0 (same-day) | Valid; only the open NDO window is summed |
| 9 | SOL / staff bookings | Excluded from pax counts and revenue via the FareBasis CASE |
| 10 | Open booking window vs closed | Closed NDOs fill 0, open NDOs stay NaN — the zero-fill rule in Step 7 |
| 11 | Zero-booking future flight | Kept via the flight universe taken before the NDO filter; Total Bookings = 0 |
| 12 | M ≤ 1 in weighted std | NaN — dispersion not estimable from one observation |
| 13 | Late cancellation (post-departure) | CDP-driven month refresh rewrites the affected month partition(s) — see Storage |
| 14 | Not-yet-flown flight in match pool | The `unflown` pool never counts toward `MIN_FLIGHT_MATCHES` (threshold is flown-only); it only inherits the flown winner's group. Remaining-RBD (Step 12) filters to `DepartureDate < current_date` |
| 15 | Fallback-tagged history in match pool | Excluded — only flights with a genuine Z-score (historical) tag join the candidate pool via the `_hist_tagged` flag; future-tagging-derived tags don't leak into matching |
| 16 | Datetime/object dtype mismatch on merge keys | `_derive_future_flights` coerces `DepartureDate` to `datetime64` and `STD` (time objects) to string before the schedule↔bookings merge — pandas 1.x can silently change dtypes during `groupby(dropna=False)` |
| 17 | Sparse future-tagging group (< 12 rows) | `FUTURE_TAG_MIN_ENTRIES` drops any level-group below the floor so the fill falls through to a coarser, better-supported level rather than emitting a low-confidence tag |
| 18 | Data-rich mapped (dependent) sector | Own history clears the threshold at levels 1–7, so the donor is **never consulted** — output is identical to a standalone run. Donor is reached only on genuine own-data starvation |
| 19 | Dependent with no mapped donor | If a message declares `mapped_sector` but no donor resolves, the sector is **failed fast** — never silently run as a no-donor standalone |
| 20 | Donor-won match, Sector join mismatch | Donor rows keep `Sector=donor` through matching; they are relabelled to the dependent **per winning key** just before the Step 10/12 `Sector` joins (`_donor_won`), so join keys align without erasing own-data provenance |
| 21 | Donor fares in a different range than dependent buckets | Donor history is bucketed with the **dependent's** ranges (inverted `{donor: dependent}` map) in Steps 6/7, so a donor fare outside the dependent's outer bounds still lands in the nearest open/edge bucket rather than being dropped |
| 22 | Donor fails after dependents already enqueued | Dependents are fanned out right after the donor **caches** (Step 2), before Steps 3–15. A later donor-processing failure does **not** un-enqueue dependents; each dependent runs on the already-warm cache independently. (Trade-off of the parallel fan-out: dependents no longer wait for full donor success.) |
| 23 | Duplicate dependent fan-out on retry | `_enqueue_dependents_after_cache` is one-shot per invocation via `_deps_enqueued`; the post-loop call is idempotent, so an SQS redelivery of the donor message will re-enqueue (by design, since the first enqueue may not have been durably sent) but a single invocation never double-sends |
| 24 | All GP points map to one fare | `_compute_gp_fare_buckets` returns a single open-ended `"{fare}+"` bucket when < 2 distinct matched fares remain after dedup — no crash, but the sector effectively has one bucket |
| 25 | Empty remaining-RBD match set | If no future key has any matched remaining-RBD history, Step 12 returns the pre-distribution frames unchanged (`remaining_pax_count` defaults to 0) instead of erroring |
| 26 | Weighted branch shares normal frames | Steps 12–14 run twice (normal + weighted); `_donor_won` is reset per **batch** (not per branch) so the same donor-relabel map correctly applies to both branches within a sector |

## Known Limitations

| # | Limitation | Detail |
|---|-----------|--------|
| 1 | No demand unconstraining | Historical data is constrained (accepted bookings); sold-out low fares understate true demand |
| 2 | O(N×M) flight matching | Still a per-future-flight ladder walk over the flown pool; the remaining-RBD NDO sum is now vectorised (Step 12), removing the previous main hotspot |
| 3 | Single global decay α = 0.99 | Not fitted per sector/season against realised bookings |
| 4 | No day-of-year seasonality | DOW + Quarter + Tagging capture most of it; specific holidays are not modelled explicitly |
| 5 | Coarse time bands | ~4.5h bands treat 07:31 and 10:59 as identical |
| 6 | No connecting-pax modelling | Leg-level only; feed traffic not modelled |
| 7 | Remaining-RBD proportionality | Assumes remaining pax split across buckets like required-RBD pax |
| 8 | Market fares / EMSR-b not wired | Defined but `run()` passes `optimized_df=None` — forecast is produced but no inventory decision (see *Deferred Steps*). The active post-remaining steps are now Demand Multiplier (13) + WTP (14). |
| 9 | Normal-distribution assumption | EMSR-b assumes normal demand; small samples make the approximation rough (negative binomial would fit better) |

---

## Architecture Summary

```
┌──────────────────┐     ┌──────────────────────────────┐
│ MongoDB           │     │ MySQL (ESB — read only)       │
│ • active_sectors  │     │ • QP_DL_FLOWNDATAPAX          │
│ • fares (GP)      │     │ • QP_DL_BOOKEDDATAPAX         │
│ • tax master      │     │ • QP_DL_InventoryLeg (STD)    │
│ • run_audit ◄──┐  │     └───────────────┬──────────────┘
└────────┬───────┼──┘                     │
         │       │ (prior sector_stats)   │ (injected ESB engine)
         ▼       │                        │
┌─────────────────────────┐               │
│ rm-additive-prefetch     │              │
│ • bin-pack (LPT)         │              │
│ • route new→Glue fetch   │              │
│ • write run skeleton doc │              │
└──────┬─────────────┬─────┘              │
   new │             │ cached bins (SQS)  │
sectors▼             ▼                    ▼
┌───────────┐  ┌──────────────────────────────────────────────┐
│ Glue      │  │  rm-additive-process  (× k parallel bins)      │
│ fetch-only│  │  Step 1  Fare Buckets (GP, qp_dw_rmalloc.fares)│
│ → S3 cache│  │  Step 2  Incremental fetch (1-day) + S3 cache  │
└─────┬─────┘  │  Step 3  Derive frames                         │
      │        │  Step 4  Historical Lean/Peak (Z-score RASK)   │
      │ cache  │  Step 5  Future Lean/Peak (50th pct)           │
      │ ready  │  Step 6  Combine + fare-bucket assign          │
      └───────►│  Step 7  Pivot NDO (0-180)                     │
   (next run)  │  Step 8  Future flights + tagging              │
               │  Step 9  Match: own ladder → donor fallback    │
               │  Step 10 Matched result (donor relabel@merge)  │
               │  Step 11 Predictions (mean + weighted, α=0.99) │
               │  Step 12 Remaining-RBD (vectorised NDO sum)    │
               │  Step 13 Demand Multiplier (flight-level %)    │
               │  Step 14 WTP redistribution (price infl/defl)  │
               │  Step 15 Save                                  │
               └───────┬───────────────────────────┬────────────┘
                       │ results                    │ per-sector timing
                       ▼                            ▼
              ┌───────────────────────┐   ┌──────────────────────┐
              │ S3 datasets (PROD)     │   │ run_audit doc (Mongo)│
              │ snapshot_date=/sector= │   │ • sector_stats{}     │
              │  — or —                │   │ • sectors_completed  │
              │ CSV/parquet (TEST)     │   │ • status (finalized) │
              └───────────────────────┘   └──────────────────────┘
```

### Performance Summary

Per-sector cost is unchanged; the win is **parallelism**. The prefetch balances bins so total wall-clock ≈ the heaviest bin, not the sum of all sectors.

| Scenario | Path | Wall-clock |
|----------|------|-----------|
| New sector (first appearance) | Glue fetch-only backfill (365d) → S3 cache | ~6 min / sector (one-off) |
| First *process* run of a cached sector | 1-day incremental + full pipeline | per-sector baseline recorded to audit |
| Daily run, 1 bin (~5 cached sectors) | 1-day incremental + pipeline, sector-by-sector | ~3–5 min |
| **Daily run, ~50 sectors, ~10 balanced bins** | fan-out over parallel `process` Lambdas | **~4–5 min total** |

Because bins are balanced on the *previous* run's measured times, the makespan (slowest bin) stays close to `total_cost / num_bins` rather than being dragged out by one heavy sector landing with several other heavy ones.

---

## Audit Lifecycle & Load Balancer

The audit collection `qp_dw_rmalloc.additive_model_run_audit` holds **one document per run**. It does double duty: a completion ledger for the fan-out, and the cost model the next run balances against.

### Document shape

```json
{
  "run_id": "2026-07-03T02:00:05Z-ab12",
  "module": "additive_pickup_model",
  "env": "prod",
  "snapshot_date": "2026-07-03",
  "status": "IN_PROGRESS",          // → SUCCESS | PARTIAL when finalized
  "sectors_expected": 48,           // set by prefetch (authoritative)
  "sectors_completed": 12,          // atomically $inc'd by each process Lambda
  "batches_seen": [0, 3, 7],        // which bins have reported
  "start_time": "…", "end_time": null,
  "sector_stats": {
    "AUHBLR": {"seconds": 42.1, "status": "SUCCESS"},
    "BOMDXB": {"seconds": 55.7, "status": "SUCCESS"},
    "DELDXB": {"seconds": 39.4, "status": "FAILED", "error": "…"}
  }
}
```

### Who writes what, and when

1. **Prefetch (once, up front)** writes the **skeleton**: `run_id`, `snapshot_date`, `status: IN_PROGRESS`, `sectors_expected`, `start_time`, and the planned bins. `sectors_expected` is the number of sectors routed to `process` bins (new sectors going to the Glue fetch-only path are **not** counted — they produce no forecast this run).
2. **Each process Lambda**, per sector, does one atomic upsert:
   - `$set sector_stats.<SECTOR>` = `{seconds, status[, error]}` — disjoint key paths, so parallel bins never collide;
   - `$inc sectors_completed` by 1;
   - `$addToSet batches_seen` its `batch_index`.
3. **Finalization — "last one turns off the lights."** After a Lambda finishes its bin it re-reads the doc; if `sectors_completed >= sectors_expected`, it flips `status` to `SUCCESS` (or `PARTIAL` if any sector failed) and stamps `end_time`. The update is **guarded on `status: IN_PROGRESS`**, so only the first finisher wins and the write is idempotent — no central orchestrator, no Step Functions required.

### First run vs subsequent runs

**First run (no history).**
- There is no prior `sector_stats`, so the load balancer has no measured cost to pack against.
- Also, on a cold system every sector is *new* (no S3 cache) → all sectors route to the **Glue fetch-only backfill**, not to `process` bins. So run 1 is dominated by backfill and produces **no forecast**; its purpose is to populate the cache.
- Bin-packing therefore has nothing to balance on run 1. When the first *process* run happens, prefetch falls back to **equal-count bins** (round-robin N sectors/bin) — or, if you prefer a cheap proxy, the number of cached months per sector. The times measured on that run become the cost model for the next.

**Subsequent runs.**
- Prefetch reads the previous run's `sector_stats.<sector>.seconds` and uses them as per-sector cost.
- Sectors that gained a cache since last time are now packed into `process` bins; any newly-added active sectors with no cache are split off to the Glue fetch-only path (with SQS back-pressure) and join the process bins the following run.
- To avoid one anomalous slow day skewing the plan, smooth the cost with an EWMA across the last few runs (e.g. `cost = 0.7·last + 0.3·prev_ewma`) rather than the raw last value. A sector with a missing/failed prior time gets a conservative high default so it isn't packed next to other heavy sectors.

### The load balancer (bin-packing) — algorithm

The goal is **min-makespan multiprocessor scheduling**: assign sectors to bins so the slowest bin is as light as possible. Prefetch uses the classic **Longest-Processing-Time-first (LPT)** greedy heuristic (within ~4/3 of optimal):

1. **Cost each sector** from audit history (EWMA of `seconds`); new/unknown sectors get a high default and are usually diverted to Glue fetch-only instead.
2. **Choose bin count** `k = clamp( ceil(total_cost / TARGET_BIN_SECONDS), 1, MAX_PARALLEL )`, with `TARGET_BIN_SECONDS` set below the Lambda timeout (headroom for cold start + the fetch). A single sector's cost must fit one Lambda timeout; if not, that sector is flagged for its own bin / backfill path.
3. **Sort** sectors by cost **descending**.
4. **Greedy assign**: place each sector into the **currently-lightest bin** (min-heap keyed on bin load). This is the LPT step — big sectors placed first, small ones fill the gaps.
5. **Emit** one SQS message per bin with `sectors`, `run_id`, `batch_index`, `batch_total`, and `sectors_expected = Σ bin sizes`.

Example: sectors with costs `[55, 42, 40, 39, 20, 12, 10, 8]` into 3 bins →
`bin0 = 55 + 12 = 67`, `bin1 = 42 + 20 + 8 = 70`, `bin2 = 40 + 39 = 79 … ` then `10` goes to the lightest (bin0 → 77). Final loads ≈ `77 / 70 / 79` — makespan 79 vs a naive round-robin that could exceed 100.

## Edge Case Deep Dive: Insufficient History & Irregular Schedules

### Case 1: Sector with Irregular Schedule (e.g., DELIXB flies only Tue/Thu/Fri)

**What happens step by step:**

| Step | Behavior |
|------|----------|
| Historical Data (Query 1) | Only fetches data for days the flight actually operated → ~156 flights/year (3 days × 52 weeks) instead of 365 |
| Lean/Peak Tagging (Step 4) | RASK computed only on actual departure days. Each DOW gets its own tag pool (only Tue, Thu, Fri have entries) |
| Future Tagging (Step 5) | 50th percentile computed per DOW. Mon/Wed/Sat/Sun have NO tagging entries for DELIXB |
| Future Flights (Step 8) | If a future DELIXB flight is scheduled on Tuesday → tagging found. If scheduled on Monday (unusual) → NaN tagging after both fallback passes |
| Flight Matching (Step 9) | Matches on **DOW** first, against FLOWN flights only. A Tuesday future flight only looks for departed Tuesday flights. With ~52 Tuesdays in 365 days, after Tagging and STD filtering, typically 13-40 matches are found (else the Sector+Tagging DOW-drop fallback applies) |
| Predictions (Step 11) | Normal prediction proceeds. No degradation in quality for the operating days |

**Key insight:** The DOW-based matching naturally handles irregular schedules. The model doesn't assume a flight operates daily — it only matches against days the flight actually flew.

**Potential issue:** If a flight is newly extended to a day it never flew (e.g., DELIXB adds Saturday service), the DOW level finds zero Saturday matches. The ladder then **drops DOW** (Sector+Tagging), widens the tagging window, and finally tries all-tags — so a Saturday flight can still match against other-DOW DELIXB flights of the same tag. Only if the whole own-sector ladder (and any donor ladder) stays under 13 flown matches is the flight skipped.
- **Mitigation:** Over a few weeks, Saturday data accumulates in the cache and the finer DOW level starts winning.

---

### Case 2: Brand New Sector with No Historical Data but Future Bookings

**Example:** `BLRCCU` (Bangalore-Kolkata) is introduced on July 15, with bookings already made via forward sales.

**Trace through the pipeline:**

| Step | What Happens | Result |
|------|-------------|--------|
| Step 0 | `active_sectors` includes BLRCCU | ✓ Sector is processed |
| Step 1 (Fare Buckets) | Fares fetched from MongoDB for BLRCCU | ✓ Fare buckets computed (fares exist because RBDs are configured) |
| Query 1 (Historical) | Looks for FLOWNDATAPAX with Origin=BLR, Dest=CCU | **Empty** — no departed flights yet |
| Query 3 (Forward) | Looks for flight_master BLRCCU with bookings | ✓ Finds future flights + existing bookings |
| Step 3a (Flown Data) | No rows for BLRCCU | Empty for this sector |
| Step 3c (Booked Data) | Forward bookings exist | ✓ Has booked data |
| Step 3d (Future Flights) | flight_master has scheduled flights | ✓ Future flights with Total Bookings |
| Step 4 (Historical Tagging) | No historical flights → no RASK → no tags | Empty for BLRCCU |
| Step 5 (Future Tagging) | No historical tags to compute 50th percentile | The 7 tagging-level frames have no BLRCCU rows (unless BLRCCU is mapped to a donor, whose levels 8–14 would supply tags) |
| Step 6 (Combined Data) | Booked data contributes to combined_df | ✓ But only forward bookings (no flown) |
| Step 7 (Pivot NDO) | Pivot includes booked data for BLRCCU | ✓ Has NDO values (from forward bookings) |
| Step 7 (Tagging Merge) | Historical tagging merge → NaN | NaN |
| Step 7 (Fill NaN) | Future tagging lookup → no BLRCCU entries → still NaN | **Tagging stays NaN** |
| Step 8 (Future Flights) | Tagging merge → NaN | Future flights have NaN Tagging |
| Step 9 (Matching) | Boolean mask on Sector/DOW/Tagging over FLOWN flights returns 0 rows (NaN tag matches nothing; no departed BLRCCU flights) | **< 13 matches → flight skipped** |
| Step 10 (Matched Result) | No matches → skipped | No rows for BLRCCU |
| Step 11 (Predictions) | No matched data → no predictions | **BLRCCU absent from output** |

**End result:** The new sector produces **zero predictions**. Future flights exist and have bookings, but the model cannot forecast additional pickups because there's no historical pattern to learn from.

**When does it start working?**
- After the first flight departs (becomes flown data in the cache)
- Specifically: needs ≥13 departed flights on the same DOW with the same Tagging level (or ≥13 after the Sector+Tagging DOW-drop fallback)
- Practically: **~4-7 weeks** after launch

**Partial data scenario (first few weeks):**
- Early weeks: fewer than 13 flown matches per group → flight is **skipped** (no low-confidence prediction is emitted; the model does not predict below the 13-match floor)
- As data accumulates: once a Sector+DOW+Tagging (or Sector+Tagging) group reaches 13 flown matches, predictions begin appearing for those flights
- Steady state: most combinations saturated → full predictions

---

### Case 3: Flight Number Change (Same Route, New Flight Number)

If BOMDEL flight 1234 is renumbered to 5678:
- Historical data has `FlightNumber = 1234`
- Future flights have `FlightNumber = 5678`
- Key format: `BOMDEL_2026-08-15_5678` (future) vs `BOMDEL_2025-08-09_1234` (hist)

**Does matching still work?** YES — flight matching does NOT match on FlightNumber. It matches on:
1. Sector (BOMDEL = BOMDEL ✓)
2. DOW (Saturday = Saturday ✓)
3. Tagging (Shoulder = Shoulder ✓)
4. STD window (within ±2h ✓)

The Key column is only used to INDEX rows in pivot_ndo for retrieval after matching. The matching itself is Sector+DOW+Tagging+STD based. So renumbering a flight has **no impact** on the model.

---

### Summary Table: Edge Case Outcomes

| Scenario | Predictions Produced? | Time to Resolve | Workaround |
|----------|:---------------------:|:---------------:|------------|
| Irregular schedule (Tue/Thu/Fri only) | ✓ Yes (on operating days) | N/A — works immediately | None needed |
| New day added to existing route | ✗ No (for new day only) | 5-8 weeks | None — wait for data |
| Brand new sector, no history | ✗ No (unless mapped) | 4-7 weeks | **Now wired:** map it to a donor in `config_additive_mapped_sectors` and it borrows the donor's flown curves via the own-data-first fallback while producing output under its own Sector |
| Brand new flight number, same route | ✓ Yes | Immediate | None needed |
| Sector with very thin history (<13 flown matches) | ✗ No (unless mapped) | Ongoing | Below the 13-match floor the own-sector ladder (drop DOW → widen tagging → all-tags) may lift it over the floor; failing that, a configured donor supplies the fallback, else the flight is skipped |
| Historical tagging all one type (e.g., always "Lean") | ✓ Yes | N/A | All future flights tagged "Lean" → matches found within same tag |
| Future flight on a DOW that never operated | ✗ No | 5+ weeks | Sector-level fallback tagging attempted, but matching still needs DOW-specific history |