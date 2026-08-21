"""
localtest/inspect_outputs.py
=============================================================================
Reads one flight's results back out of local MongoDB so a run can be sanity
checked without opening a spreadsheet.

There are only two output datasets, and the EMSR-b AU ladder rides along on the
fare-bucket one, so everything below comes from two documents' worth of query.

    python -m localtest.inspect_outputs
    python -m localtest.inspect_outputs --sector DELBOM --flight 1104 --ndo 25
"""

import argparse


def _fmt(value, width=14, places=4):
    if isinstance(value, float):
        return f'{value:>{width}.{places}f}'
    return f'{str(value):>{width}}'


def _table(rows, columns, widths=None):
    widths = widths or [14] * len(columns)
    print(''.join(f'{c[:w - 1]:>{w}}' for c, w in zip(columns, widths)))
    for row in rows:
        print(''.join(_fmt(row.get(c), w) for c, w in zip(columns, widths)))


def main(argv=None):
    from localtest.bootstrap import bootstrap, ensure_localtest_importable
    ensure_localtest_importable()
    cfg = bootstrap()

    ap = argparse.ArgumentParser()
    ap.add_argument('--sector', default='BOMDEL')
    ap.add_argument('--flight', type=int, default=1101)
    ap.add_argument('--ndo', type=int, default=56,
                    help='future_flight_NDO of the flight to inspect')
    args = ap.parse_args(argv)

    client = cfg.mongo_client()
    try:
        db = client[cfg.mongo_db]
        key = {'Sector': args.sector, 'FlightNumber': args.flight,
               'future_flight_NDO': args.ndo}
        print(f'{cfg.mongo_db}  {args.sector} QP{args.flight} at NDO {args.ndo}')

        buckets = list(db.add_model_fare_bucket_level_weighted.find(key, {'_id': 0})
                       .sort('FareMidpoint', -1))
        flight = db.add_model_flight_level_weighted.find_one(key, {'_id': 0}) or {}

        if not buckets:
            print('\n  no fare-bucket rows for that key')
        else:
            # ---- demand chain, steps 11 -> 14 ----------------------------
            print('\n--- demand through the pipeline (fare bucket level, weighted) ---')
            _table(sorted(buckets, key=lambda r: r.get('FareBucket_Lower') or 0),
                   ['FareBucket', 'weighted_pax_count', 'Remaining_pax_distribution',
                    'Final_pax_count', 'Demand Multiplier', 'New Final_pax_count',
                    'WTP_Factor', 'WTP_pax_count'])
            filed = [r for r in buckets if r.get('FareBucket') != 'Promo']
            print('  totals  Final=%.3f  DemandAdjusted=%.3f  WTP=%.3f' % (
                sum(r.get('Final_pax_count') or 0 for r in filed),
                sum(r.get('New Final_pax_count') or 0 for r in filed),
                sum(r.get('WTP_pax_count') or 0 for r in filed)))

            # ---- EMSR-b ladder -------------------------------------------
            print('\n--- EMSR-b AU ladder (highest fare first) ---')
            _table(buckets,
                   ['FareBucket', 'FareMidpoint', 'OptimizationDemand',
                    'OptimizationDemandStd', 'ProtectionLimit', 'AU', 'RBD',
                    'IsOpeningFare'],
                   [14, 14, 20, 22, 16, 8, 8, 14])

        # ---- flight level -----------------------------------------------
        print('\n--- flight level (weighted) ---')
        if not flight:
            print('  no row for that key')
        for field in ['SnapshotDate', 'DepartureDate', 'DOW', 'STD', 'Capacity',
                      'Current_Bookings', 'Tagging', 'weighted_pax_count',
                      'weighted_pax_std_square', 'Weighted_Matched_Current_Bookings',
                      'Remaining_pax_count', 'Final_pax_count', 'Final_Total_Bookings',
                      'Load_Factor', 'Demand Multiplier',
                      'New predicted_pax_count_final', 'New Final Total Bookings',
                      'OpeningFareBucket', 'OpeningFareMidpoint']:
            print(f'  {field:<36}{_fmt(flight.get(field), 18)}')
    finally:
        client.close()


if __name__ == '__main__':
    main()
