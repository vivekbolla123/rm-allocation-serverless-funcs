# Exhaustive Guide: Feature Flags & Functions in `rm-allocation-process-inputs`

This document provides a highly granular, bit-by-bit breakdown of every feature flag, control parameter, and major function within the `rm-allocation-process-inputs` module. 

---

## 1. Feature Flags & Operational Parameters

The input payload (usually retrieved from the `market_list` or passed directly via SQS) contains numerous configuration strings/ints that act as feature flags. These dictate the branching logic throughout the allocation process.

### Routing & Initialization Flags
*   **`saleinvflag` (String "1" or "0")**
    *   **Location:** `lambda_function.py`, `runAlloc_SalesInv.py`
    *   **Purpose:** The master bypass switch. If set to `"1"`, the engine abandons all standard B2B/B2C revenue management allocations. It strictly delegates processing to `DynAllocSalesInv`, which exclusively calculates W-class (distress/promotional) inventory limits, ignoring high-yield jumps.
*   **`route_type` (String "direct" or "connection")**
    *   **Location:** `lambda_function.py`
    *   **Purpose:** Determines whether to load the standard direct flight allocation engine (`DynAlloc`) or the connecting flights engine (`DynAllocConnections`).

### Fare & Pricing Flags
*   **`MktFareFlag` (String "1" or "0")**
    *   **Location:** `allocations.py`, `determine_rbd.py`
    *   **Purpose:** When enabled (`"1"`), commands the system to utilize the `MarketFareService` to fetch live competitor or market-anchored fares from the database/API rather than relying solely on static historical curves.
*   **`Autobackstopflag` / `backstop`**
    *   **Location:** `determine_rbd.py`
    *   **Purpose:** Dictates whether the system applies a rigid, pre-defined Minimum Selling Price (backstop). If auto is enabled, it may compute a sliding backstop based on remaining NDO (Number of Days Out).
*   **`hard_anchor_flag` / `hardAnchor`**
    *   **Location:** `determine_rbd.py` (`apply_hard_anchor_cap`)
    *   **Purpose:** Forces the algorithm to respect a hard floor/ceiling. If the ML forecast or heuristics suggest a fare of $50, but the `hardAnchor` is MIN $70, this flag forces the output to $70.
*   **`is_spoilage_reduction_active`**
    *   **Location:** `allocations.py` (`b2CAllocation`), general overrides
    *   **Purpose:** Activates defensive "spoilage" logic near departure. If flights are heavily empty close to departure, this flag allows the pricing to purposefully drop below standard tolerance limits to stimulate any last-minute volume (minimizing wasted seats).

### Allocation & Capacity Overrides
*   **`OverBooking` / `overbookingflag` / `b2boverbookingflag`**
    *   **Location:** `runAlloc.py` (`canOverbook`), `allocations.py`
    *   **Purpose:** If `"1"`, authorizes the engine to output total allowable allocations that exceed the physical capacity (Lid) of the aircraft to account for expected no-shows. B2B has a separate flag to ensure agent bulk bookings aren't overbooked aggressively.
*   **`rbdPushFlag`**
    *   **Location:** `allocations.py` (`b2CAllocations`)
    *   **Purpose:** A dual-pass execution flag. If enabled, the system computes the allocation normally, and then runs a *second pass* recalculation (`currParams.rbdPushFlag = 1`) mathematically designed to "push down" available units from higher RBDs into slightly lower RBDs to stimulate conversion when demand is sluggish.
*   **`AutoTimeRangeFlag`**
    *   **Location:** `runAlloc.py` (`autoTimeRange`, `find_time_range`)
    *   **Purpose:** Instead of treating all flights in a day equally, this flag dynamically categorizes the flight's departure time (e.g., Morning Peak, Midday Trough) to apply unique time-of-day offsets to the anchor fare.
*   **`distressInventoryFlag` / `seriesBlock`**
    *   **Location:** `runAlloc.py`
    *   **Purpose:** Controls whether distress buckets (L and P classes) are artificially restricted to protect standard yield.

### Specialized Group Booking Flags
These flags carve out virtual capacity from the plane before standard consumers (B2C) can book them, protecting inventory for specialized channels.

*   **`AutoGroup` ("1" or "0")**
    *   **Function:** `allocations.py` -> `group_quote_alloc()`
    *   **Purpose:** Evaluates historical group block averages. If enabled, it subtracts expected group capacity from `adjustedCapacity` before computing B2C seats.
*   **`TbfFlag` & `tbfenhflag` ("1" or "0")**
    *   **Function:** `allocations.py` -> `tbf_allocation()`
    *   **Purpose:** "To Be Flown" feature. If active, specifically models and blocks inventory for high-value corporate/agency recurring trips. `tbfenhflag` is the "enhanced" version allowing aggressive blocks if current TBF bookings are low but historically high.
*   **`CugFlag` & `cug_mkt_flag` ("1" or "0")**
    *   **Function:** `allocations.py` -> `cug_allocation()`
    *   **Purpose:** "Closed User Group" (e.g., specific partnered portals). Triggers highly conditional logic: Only allocates W-series RBDs to CUG if NDO is between 10-30 AND the forecasted PLF is under 90%.

---

## 2. Deep Dive: `allocations.py` Core Functions

`allocations.py` holds the `RBDAllocations` class—the mathematical heart of the system.

### A. The Consumer Engine (`b2CAllocations` & `b2CAllocation`)
*   **Flow:** This function receives the exact entry price determined by `determine_rbd.py`. It loops over the sorted B2C RBD ladder (highest price to lowest).
*   **Logic:** It uses `dPLF` (Delta Passenger Load Factor) and `bookedDeficit` (how far behind the target booking curve we are). It distributes `remainingCapacity` downwards. If sales are strong, it allocates 0 to cheap RBDs. If weak, it flows capacity down the ladder.
*   **Helper: `getIncrementalValue`:** Calculates the step-cost of moving down one RBD. Used to decide if allowing a cheaper seat hurts overall yield too much.
*   **Helper: `jumpingAllocation`:** Aggressive yield protection. If a certain booking threshold is crossed rapidly, this stops sequential cascading and "jumps" to close 2 or 3 lower RBDs simultaneously.

### B. Near-Term Yield Protection (`d3d4Calculations` & `d3d4_forecast_calculations`)
*   **Purpose:** Specifically engineered for "D-3" and "D-4" (3 to 4 days out from departure).
*   **Logic:** As departure nears, business travelers book. This function overrides standard cascading to heavily restrict cheaper RBDs, relying on the `imp_forecast` (imputed machine learning forecast) to confidently hold out for higher-paying late-bookers.

### C. The B2B Engine (`b2BAllocation`)
*   **Purpose:** Allocates Agent / Corporate portal seats.
*   **Logic:** Inherently tied to the B2C result. It takes `b2cAllocationsUnits`, applies a `b2bSkipFlag` or `B2B_thresholdflag`, and distributes capacity to the B2B RBD ladder. Includes logic (`close_b2b`) to completely zero-out B2B availability if consumer demand alone is forecasted to fill the plane at a higher yield.

---

## 3. Deep Dive: `determine_rbd.py` Core Functions

The `DetermineRBD` class figures out the optimal opening price BEFORE `allocations.py` decides how many seats to sell at that price.

### A. `determineRBDToOpen` (The Orchestrator)
*   **Inputs:** `anchorFare` (base system fare), historical `last7avg`, real-time `BookingsInHour`, and `obfare` (Overbooking fare limit).
*   **Purpose:** Generates the final baseline `price` the system should start selling at.

### B. `final_fare_anchor_forecast` (The Resolver)
*   **Purpose:** Often, the heuristic strategies (`strategyFare`), manual profiles (`profileFare`), and machine learning forecasts (`imp_forecast`) conflict.
*   **Logic:** 
    *   Calls `is_greater` to see if ML forecast is drastically higher than manual strategy.
    *   Evaluates `get_imp_fcs_diff` to see if the forecast has shifted significantly over the last 6 hours.
    *   Picks the optimal fare source (ML vs. Strategy) based on highest confidence.

### C. The Safety Nets (`apply_tolerance_logic` & `apply_hard_anchor_cap`)
*   **`apply_tolerance_logic`:** Ensures that sequentially running batches don't cause the fare to jump $100 -> $50 -> $120. If `tolerance_per` is 15%, it caps the intra-day delta.
*   **`calculate_overbooking_fare_and_status`:** If `overBookingCount` > 0 (plane is physically full), standard pricing rules are thrown out. This function sets a massive upsell multiplier to ensure only the highest yielding desperate passengers buy the overbooked seats.

---

## 4. Deep Dive: Execution Managers (`runAlloc.py` & Variants)

These modules instantiate the environment, pull the data, and execute the math components listed above.

### A. `runAlloc.py` (`DynAlloc`)
*   **`runDynamicAllocationSingleFlight`:** The God-function for a single flight.
    *   **Fetches context:** `currBookedLoad`, `currAdjustedCapacity`.
    *   **Target Calculation:** Uses `getStartOfDayValues` to calculate what the booked load *should* be at this distinct hour (`tgtAtCurrentHour`).
    *   **Execution:** Calls `determineRBDToOpen`, then feeds the result into `RBDAllocations.b2CAllocations`.
*   **Formatting Outputs:**
    *   `updateNavitaire` (Direct API patch format builder)
    *   `updateNavSFTP` (Flat CSV format builder)

### B. `runAlloc_Connections.py` (`DynAllocConnections`)
*   **`allocateAUforflights`:** Specialized logic for pairs of flights (e.g., DEL-BOM + BOM-GOI).
    *   It calculates the booked load for *both* legs independently. 
    *   It determines if selling a cheap through-connection cannibalizes a high-paying direct passenger on either leg.
*   **`_apply_direct_flight_upsell`:** If Leg 1 is empty but Leg 2 is completely full, this aggressively raises the connection price so only a very lucrative connection passenger can displace a direct passenger on Leg 2.

### C. `runAlloc_SalesInv.py` (`DynAllocSalesInv`)
*   **`runSalesInventoryAllocationSingleMarket`:** 
    *   Bypasses `determine_rbd.py` completely.
    *   Looks exclusively at `distress_pax_count` and passes directly to `RBDAllocations.sales_inv_allocation()`.
    *   Extremely lightweight and designed to be run rapidly when only promotional inventory needs adjustment without touching premium cabins.
