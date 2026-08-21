"""
localtest/generate_inputs.py
=============================================================================
Deterministic mock-data generator for the local additive-model flow.

Writes one CSV per dataset into `localtest/inputs/`. Everything is derived from
a single seed and a single anchor date, so two runs on the same day produce
byte-identical files.

The data is shaped to satisfy the real model's preconditions rather than to look
like production traffic:

  * ~400 days of history, because historical tagging uses a rolling 365-day
    window and future tagging re-derives tags on the trailing 365 days ending
    on the last day of the previous month.
  * a load factor and a forward horizon that leave enough predicted pickup for
    step 15 to build a non-degenerate AU ladder — see DEFAULT_LOAD_FACTOR below.
  * 4 flights per day per sector, each parked in a different STD time cluster,
    so the Z-score tagging has a populated (Sector, Time Cluster) group with
    real day-to-day variance.
  * Enough same-DOW / same-tagging history to clear MIN_FLIGHT_MATCHES (13).
  * Booking fares that land inside the GP fare-bucket ladder built from the
    mocked `fares` collection, after the all-in -> airline-component conversion.

Fare spaces, which are easy to get wrong:

    fares.csv `TOTAL`     all-in fare (what the pricing engine publishes)
    airline component     (TOTAL - other_taxes) / (1 + GST)   <- buckets live here
    booking `Fare`        DiscountedBaseFare_Pro + YQ_Pro + RCS + CUTE
                          i.e. already airline-component space

`other_taxes` for a domestic sector is ASF + PSF + UDF_DF + DUF(destination),
so the tax master and PRICING_DUF_SECTORS below have to agree with the booking
fare range or every fare falls outside the ladder and the run yields nothing.
"""

import argparse
import json
import os
from datetime import date, datetime, time, timedelta

import numpy as np
import pandas as pd

# =============================================================================
# Fixed characteristics of the mocked network
# =============================================================================

# Seats per flight, mixed fleet: 189 (737-800 / A321) and 174 (A320).
# Set per flight so the fixture exercises a non-uniform capacity, which matters
# because protection limits and AUs are driven by demand *relative to capacity*.
# `--capacity` overrides every flight with one value.
#
# Load is expressed as a load factor rather than an absolute booking count, so it
# stays meaningful across the two capacities. It is also the knob that decides
# whether the EMSR-b ladder in step 15 is interesting or degenerate: predicted
# pickup is only the remaining window (NDO <= future_flight_NDO), so a thin load
# leaves almost nothing to protect and every AU pins to capacity.
DEFAULT_LOAD_FACTOR = 0.45

SECTORS = {
    'BOMDEL': {
        'origin': 'BOM', 'destination': 'DEL', 'distance': 1148,
        # (flight number, STD, seats)
        'flights': [(1101, time(6, 30), 189), (1103, time(9, 15), 174),
                    (1105, time(13, 20), 189), (1107, time(19, 45), 174)],
        'demand_index': 1.00,
    },
    'DELBOM': {
        'origin': 'DEL', 'destination': 'BOM', 'distance': 1148,
        'flights': [(1102, time(7, 5), 189), (1104, time(10, 30), 174),
                    (1106, time(14, 0), 189), (1108, time(20, 15), 174)],
        'demand_index': 0.94,
    },
}

# Domestic tax master. GST is a percentage here (the helper divides by 100).
TAX_MAST_DOM = [
    {'Code': 'BOM', 'GST': 5, 'RCS': 0, 'CUTE': 30, 'ASF': 236, 'PSF': 0, 'UDF_DF': 150},
    {'Code': 'DEL', 'GST': 5, 'RCS': 0, 'CUTE': 30, 'ASF': 236, 'PSF': 0, 'UDF_DF': 130},
]

# One unrelated international row so the collection exists. Nothing in SECTORS
# may appear here: `is_domestic` is decided by absence from the intl master.
TAX_MAST_INTL = [
    {'Sectors': 'BOMKWI', 'GST': 0, 'CUTE': 30, 'ASF': 236, 'PSF_ADF': 0, 'UDF': 0,
     'PSC_PZ': 0, 'RCS_CUTE': 0, 'RCS': 0,
     'Tax1': 0, 'Tax2': 0, 'Tax3': 0, 'Tax4': 0, 'Tax5': 0},
]

PRICING_DUF_SECTORS = {'BOM': '89', 'DEL': '66'}
PRICING_GST_SECTORS = []      # empty -> GST applies everywhere
PRICING_RCS_SECTORS = []

# Per-pax pass-throughs baked into every booking's Fare.
YQ_PRO = 299.0
RCS = 0.0
CUTE = 30.0

# All-in fare ladder published per RBD, geometric from FARE_MIN to FARE_MAX.
FARE_ALLIN_MIN = 2600
FARE_ALLIN_MAX = 30500

# Booking fares (airline-component space) are drawn from this range.
BOOKING_FARE_MIN = 2100
BOOKING_FARE_MAX = 26500

# RBDs that do NOT appear in Constants.REQUIRED_RBDS, so they feed step 12
# (remaining-RBD distribution) instead of the fare-bucket pivot.
NON_REQUIRED_RBDS = ['W4', 'W5', 'W6', 'G8', 'S', 'GA', 'GB', 'L']
NON_REQUIRED_SHARE = 0.12
SOL_SHARE = 0.02              # SOL bookings are excluded from pax counts / RASK


# =============================================================================
# Helpers
# =============================================================================

def _geometric_ladder(low, high, n):
    ratio = (high / low) ** (1.0 / (n - 1))
    return [int(round(low * ratio ** i)) for i in range(n)]


def _booked_fraction(ndo):
    """Share of a flight's final bookings already taken at `ndo` days out.

    Monotonically decreasing in NDO; ~0.08 at 180 days, 1.0 at departure.
    Used to decide how many forward bookings a future flight already has.
    """
    ndo = max(0, min(180, int(ndo)))
    return float(np.clip(1.0 - (ndo / 180.0) ** 0.45, 0.05, 1.0))


def _sample_ndos(rng, n):
    """NDO values for `n` bookings on one flight, weighted toward departure."""
    raw = rng.beta(1.35, 3.1, size=n) * 180.0
    return np.clip(np.round(raw), 0, 180).astype(int)


def _sample_fares(rng, n, demand_index):
    """Booking fares in airline-component space, log-uniform then nudged by demand."""
    lo, hi = np.log(BOOKING_FARE_MIN), np.log(BOOKING_FARE_MAX)
    fares = np.exp(rng.uniform(lo, lo + (hi - lo) * 0.86, size=n))
    fares = fares * (0.9 + 0.2 * demand_index)
    return np.clip(np.round(fares, 2), BOOKING_FARE_MIN, BOOKING_FARE_MAX)


def _flight_load(rng, dep_date, demand_index, load_factor, capacity):
    """Bookings a flight ends up with — DOW + seasonal + noise around the target
    load factor.

    The spread is what gives the Z-score tagging something to separate; a flat
    load would put every flight in one band.
    """
    dow_factor = {0: 1.02, 1: 0.92, 2: 0.90, 3: 0.97, 4: 1.16, 5: 1.05, 6: 1.14}
    seasonal = 1.0 + 0.18 * np.sin(2 * np.pi * (dep_date.timetuple().tm_yday / 365.0))
    noise = rng.normal(1.0, 0.17)
    load = (capacity * load_factor
            * dow_factor[dep_date.weekday()] * seasonal * demand_index * noise)
    return int(np.clip(round(load), 4, capacity))


def _fare_basis(rbd, is_sol):
    """5th character must not be 'T' (that branch reads FB_Net instead of the
    discounted base fare), and 'SOL' marks a non-revenue booking."""
    return f'QPDRB{rbd}SOL' if is_sol else f'QPDRB{rbd}'


# =============================================================================
# Generators
# =============================================================================

def build_inventory_legs(hist_start, future_end, capacity_override=None):
    """One row per (sector, date, flight) across the whole span.

    Capacity comes from the per-flight seat count in SECTORS unless
    `capacity_override` forces a single value on every flight.
    """
    rows = []
    leg_id = 900_000_000
    n_days = (future_end - hist_start).days + 1
    for sector, meta in SECTORS.items():
        for offset in range(n_days):
            dep = hist_start + timedelta(days=offset)
            for flight_number, std, seats in meta['flights']:
                seats = capacity_override or seats
                leg_id += 1
                rows.append({
                    'InventoryLegID': leg_id,
                    'DepartureStation': meta['origin'],
                    'ArrivalStation': meta['destination'],
                    'STD': datetime.combine(dep, std).strftime('%Y-%m-%d %H:%M:%S'),
                    'FlightNumber': flight_number,
                    'AdjustedCapacity': seats,
                    'Capacity': seats,
                    'DepartureDate': dep.isoformat(),
                    'CarrierCode': 'QP',
                    'Status': 1,
                })
    return pd.DataFrame(rows)


def build_bookings(legs_df, today, rng, load_factor, bucket_rbds):
    """Split every leg's bookings into flown (past) and forward (future) rows.

    Flown rows carry their own capacity/distance because the production flown
    query reads them from QP_DL_FLOWNDATAPAX, not from the flight master.
    Forward rows are joined to the flight master on InventoryLegID, so they only
    need the booking attributes.
    """
    flown, booked = [], []
    pnr_seq = 0

    leg_meta = {(r.DepartureStation + r.ArrivalStation): SECTORS[r.DepartureStation +
                                                                r.ArrivalStation]
                for r in legs_df.itertuples()}

    for row in legs_df.itertuples():
        sector = row.DepartureStation + row.ArrivalStation
        meta = leg_meta[sector]
        dep_date = date.fromisoformat(row.DepartureDate)
        is_future = dep_date >= today

        capacity = int(row.AdjustedCapacity)
        total_load = _flight_load(rng, dep_date, meta['demand_index'], load_factor,
                                  capacity)

        if is_future:
            # Only the bookings already made (BookingDate < today) exist yet.
            ndo_now = (dep_date - today).days
            n = int(round(total_load * _booked_fraction(ndo_now)))
            if n <= 0:
                continue
            # NDO strictly greater than today's NDO => BookingDate strictly before today.
            ndos = np.clip(_sample_ndos(rng, n), ndo_now + 1, 180)
        else:
            n = total_load
            ndos = _sample_ndos(rng, n)

        fares = _sample_fares(rng, n, meta['demand_index'])
        is_sol = rng.random(n) < SOL_SHARE
        is_non_req = (~is_sol) & (rng.random(n) < NON_REQUIRED_SHARE)

        req_rbds = rng.choice(bucket_rbds, size=n)
        nonreq_rbds = rng.choice(NON_REQUIRED_RBDS, size=n)

        for i in range(n):
            pnr_seq += 1
            rbd = nonreq_rbds[i] if is_non_req[i] else req_rbds[i]
            fare = float(fares[i])
            base = round(fare - YQ_PRO - RCS - CUTE, 2)
            booking_date = dep_date - timedelta(days=int(ndos[i]))
            record = {
                'InventoryLegID': row.InventoryLegID,
                'DepartureDate': dep_date.isoformat(),
                'BookingDate': booking_date.isoformat(),
                'PNR': f'P{pnr_seq:07d}',
                'RBD': rbd,
                'FareBasis': _fare_basis(rbd, bool(is_sol[i])),
                'FB_Net': base,
                'YQ_Pro': YQ_PRO,
                'RCS': RCS,
                'CUTE': CUTE,
                'DiscountedBaseFare_Pro': base,
            }
            if is_future:
                booked.append(record)
            else:
                flown.append(dict(record,
                                  Origin=meta['origin'],
                                  Destination=meta['destination'],
                                  FlightNumber=row.FlightNumber,
                                  AdjustedCapacity=capacity,
                                  Distance=meta['distance']))

    flown_cols = ['Origin', 'Destination', 'DepartureDate', 'BookingDate', 'FlightNumber',
                  'InventoryLegID', 'PNR', 'RBD', 'FareBasis', 'FB_Net', 'YQ_Pro', 'RCS',
                  'CUTE', 'DiscountedBaseFare_Pro', 'AdjustedCapacity', 'Distance']
    booked_cols = ['InventoryLegID', 'DepartureDate', 'BookingDate', 'PNR', 'RBD',
                   'FareBasis', 'FB_Net', 'YQ_Pro', 'RCS', 'CUTE', 'DiscountedBaseFare_Pro']

    return (pd.DataFrame(flown, columns=flown_cols),
            pd.DataFrame(booked, columns=booked_cols))


def build_fares(bucket_rbds):
    """All-in QP fares, one per (sector, RBD), on a geometric ladder.

    The GP bucketing in step 1 needs a spread of distinct values per sector to
    resolve 19 buckets — a short or clustered ladder collapses them.
    """
    ladder = _geometric_ladder(FARE_ALLIN_MIN, FARE_ALLIN_MAX, len(bucket_rbds))
    rows = []
    for sector in SECTORS:
        for rbd, total in zip(bucket_rbds, ladder):
            rows.append({'SECTOR': sector, 'RBD': rbd, 'TOTAL': total})
    return pd.DataFrame(rows)


def build_demand_multiplier(today, future_end, rng):
    """Flight-level analyst override. DepartureDate is written day-first because
    the production normaliser parses it with dayfirst=True."""
    pattern = [10, -5, 0, 15, -10, 5]
    rows, i = [], 0
    n_days = (future_end - today).days + 1
    for sector, meta in SECTORS.items():
        for offset in range(0, n_days, 3):
            dep = today + timedelta(days=offset)
            for flight_number, _std, _seats in meta['flights'][:2]:
                rows.append({
                    'DepartureDate': dep.strftime('%d/%m/%Y'),
                    'Sector': sector,
                    'FlightNumber': flight_number,
                    'Demand_Multiplier': f'{pattern[i % len(pattern)]}%',
                })
                i += 1
    return pd.DataFrame(rows)


def build_wtp(today, future_end):
    """Sector x month willingness-to-pay factor covering the forecast horizon."""
    months = pd.period_range(pd.Timestamp(today).to_period('M'),
                             pd.Timestamp(future_end).to_period('M'), freq='M')
    pattern = [3, 0, -4, 6]
    rows = []
    for sector in SECTORS:
        for i, period in enumerate(months):
            rows.append({
                'Sector': sector,
                'Year': period.year,
                'Month': period.strftime('%b'),
                'WTP Factor': f'{pattern[i % len(pattern)]}%',
            })
    return pd.DataFrame(rows)


def build_rm_parameter_values():
    return pd.DataFrame([
        {'parameterKey': 'PRICING_DUF_SECTORS',
         'parameterValue': json.dumps(PRICING_DUF_SECTORS)},
        {'parameterKey': 'PRICING_GST_SECTORS',
         'parameterValue': json.dumps(PRICING_GST_SECTORS)},
        {'parameterKey': 'PRICING_RCS_SECTORS',
         'parameterValue': json.dumps(PRICING_RCS_SECTORS)},
    ])


# =============================================================================
# Entry point
# =============================================================================

def generate(inputs_dir, hist_days=400, future_days=60,
             load_factor=DEFAULT_LOAD_FACTOR, capacity=None,
             seed=20260821, anchor=None, logger=None):
    """Write every input CSV and return {filename: row count}."""
    # Imported here so bootstrap() has already installed the local configsettings.
    from Constants import FARE_BUCKET_RBDS

    log = (logger.info if logger else print)
    os.makedirs(inputs_dir, exist_ok=True)

    today = anchor or date.today()
    hist_start = today - timedelta(days=hist_days)
    future_end = today + timedelta(days=future_days)
    rng = np.random.default_rng(seed)
    bucket_rbds = list(FARE_BUCKET_RBDS)

    seats = sorted({capacity} if capacity else
                   {s for m in SECTORS.values() for _, _, s in m['flights']})
    log(f"[generate] anchor={today} history={hist_start}..{today - timedelta(days=1)} "
        f"forward={today}..{future_end}")
    log(f"[generate] capacities={seats} load_factor={load_factor:.0%} "
        f"(~{', '.join(str(round(s * load_factor)) for s in seats)} pax per flight)")

    legs = build_inventory_legs(hist_start, future_end, capacity)
    flown, booked = build_bookings(legs, today, rng, load_factor, bucket_rbds)

    frames = {
        'inventory_leg.csv': legs,
        'hist_flown_data.csv': flown,
        'booked_data.csv': booked,
        'fares.csv': build_fares(bucket_rbds),
        'active_sectors.csv': pd.DataFrame({'sector': list(SECTORS)}),
        'tax_mast_dom.csv': pd.DataFrame(TAX_MAST_DOM),
        'tax_mast_intl.csv': pd.DataFrame(TAX_MAST_INTL),
        'rm_parameter_values.csv': build_rm_parameter_values(),
        'demand_multiplier.csv': build_demand_multiplier(today, future_end, rng),
        'wtp.csv': build_wtp(today, future_end),
    }

    counts = {}
    for name, df in frames.items():
        path = os.path.join(inputs_dir, name)
        df.to_csv(path, index=False)
        counts[name] = len(df)
        log(f"[generate] {name:<26} {len(df):>8} rows")

    return counts


def main():
    from localtest.bootstrap import bootstrap, ensure_localtest_importable
    ensure_localtest_importable()
    cfg = bootstrap()

    ap = argparse.ArgumentParser(description='Generate mock CSV inputs for the local '
                                             'additive-model flow.')
    ap.add_argument('--hist-days', type=int, default=400,
                    help='days of history to generate (>=400 keeps the 365-day '
                         'tagging windows fully populated)')
    ap.add_argument('--future-days', type=int, default=60,
                    help='days of forward schedule (raise toward 180 for a full '
                         'horizon; runtime grows with it)')
    ap.add_argument('--load-factor', type=float, default=DEFAULT_LOAD_FACTOR,
                    help='target load factor; sets bookings per flight relative to '
                         'that flight\'s seats, and drives how much pickup is left '
                         'for the AU ladder to protect')
    ap.add_argument('--capacity', type=int, default=None,
                    help='force one seat count on every flight (default: the mixed '
                         '189 / 174 fleet in SECTORS)')
    ap.add_argument('--seed', type=int, default=20260821)
    ap.add_argument('--anchor', default=None, help='YYYY-MM-DD; defaults to today')
    args = ap.parse_args()

    anchor = date.fromisoformat(args.anchor) if args.anchor else None
    generate(cfg.inputs_dir, hist_days=args.hist_days, future_days=args.future_days,
             load_factor=args.load_factor, capacity=args.capacity,
             seed=args.seed, anchor=anchor)


if __name__ == '__main__':
    main()
