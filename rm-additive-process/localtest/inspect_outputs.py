"""
localtest/inspect_outputs.py
=============================================================================
Reads one flight's weighted results and its AU ladder back out of local MongoDB
so a run can be sanity checked without opening a spreadsheet.

    python -m localtest.inspect_outputs
    python -m localtest.inspect_outputs --sector DELBOM --flight 1104 --ndo 25
"""

import argparse


def _fmt(value, width=14, places=4):
    if isinstance(value, float):
        return f'{value:>{width}.{places}f}'
    return f'{str(value):>{width}}'


def main(argv=None):
    from localtest.bootstrap import bootstrap, ensure_localtest_importable
    ensure_localtest_importable()
    cfg = bootstrap()

    ap = argparse.ArgumentParser()
    ap.add_argument('--sector', default='BOMDEL')
    ap.add_argument('--flight', type=int, default=1101)
    ap.add_argument('--ndo', type=int, default=18,
                    help='future_flight_NDO of the flight to inspect')
    args = ap.parse_args(argv)

    client = cfg.mongo_client()
    try:
        db = client[cfg.mongo_db]
        key = {'Sector': args.sector, 'FlightNumber': args.flight,
               'future_flight_NDO': args.ndo}
        print(f'{cfg.mongo_db}  {args.sector} QP{args.flight} at NDO {args.ndo}')

        # ---- fare bucket level, weighted --------------------------------
        cols = ['FareBucket', 'weighted_pax_count', 'Remaining_pax_distribution',
                'Final_pax_count', 'Demand Multiplier', 'New Final_pax_count',
                'WTP_Factor', 'WTP_pax_count']
        rows = list(db.add_model_fare_bucket_level_weighted.find(key, {'_id': 0})
                    .sort('FareBucket_Lower', 1))
        print('\n--- fare bucket level (weighted) ---')
        if not rows:
            print('  no rows for that key')
        else:
            print(''.join(f'{c[:12]:>14}' for c in cols))
            for row in rows:
                print(''.join(_fmt(row.get(c)) for c in cols))
            print('  totals  Final=%.3f  DemandAdjusted=%.3f  WTP=%.3f' % (
                sum(r.get('Final_pax_count') or 0 for r in rows),
                sum(r.get('New Final_pax_count') or 0 for r in rows),
                sum(r.get('WTP_pax_count') or 0 for r in rows)))

        # ---- flight level, weighted --------------------------------------
        print('\n--- flight level (weighted) ---')
        flight = db.add_model_flight_level_weighted.find_one(key, {'_id': 0}) or {}
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

        # ---- EMSR-b ladder: protection limits and AUs ---------------------
        print('\n--- inventory optimization (EMSR-b, weighted branch) ---')
        ladder = list(db.add_model_inventory_optimization.find(key, {'_id': 0})
                      .sort('FareMidpoint', -1))
        if not ladder:
            print('  no rows for that key')
        else:
            head = ('FareBucket', 'FareMidpoint', 'pax', 'Protection', 'AU', 'RBD',
                    'Opening')
            print(''.join(f'{h:>14}' for h in head))
            for row in ladder:
                print(''.join([
                    _fmt(row.get('FareBucket')),
                    _fmt(row.get('FareMidpoint'), places=2),
                    _fmt(row.get('weighted_pax_count'), places=3),
                    _fmt(row.get('ProtectionLimit')),
                    _fmt(row.get('AU')),
                    _fmt(row.get('RBD')),
                    _fmt(row.get('IsOpeningFare'))]))
    finally:
        client.close()


if __name__ == '__main__':
    main()
