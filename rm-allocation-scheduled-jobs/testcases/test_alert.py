"""
Test Scheduled Run Failure Alert

=============================================================================
USAGE
=============================================================================

python test_alert.py --test-hour "04:00"
python test_alert.py --test-hour "17:00" --test-date "2026-03-11"

Arguments:
  --test-hour HH:MM     Hour to simulate (UTC), required
  --test-date YYYY-MM-DD  Date to simulate (optional, defaults to today)

=============================================================================
MONITORED RUNS
=============================================================================

CUG Run:
  Cron: 8:50 AM IST (03:20 UTC)
  Test: python test_alert.py --test-hour "04:00"
  Params: DTD 10-30, Type: Scheduled, market_list, Domestic

Schedule30:
  Cron: 10:00 PM IST (16:30 UTC)
  Test: python test_alert.py --test-hour "17:00"
  Params: DTD 0-365, Type: Scheduled_Thirty, market_list_thirty, Domestic

Other EventBridge runs:
  Test: python test_alert.py --test-hour "<hour after run>"

=============================================================================
TEST SCENARIO 1: SUCCESS (No Alerts)
=============================================================================

Ensure the run exists in allocation_run_audit AND its summary exists in
allocation_summary_report with failures < total.

  python test_alert.py --test-hour "04:00"

Expected: "Run method completed." with 0 monitored failed runs, 0 missing runs

=============================================================================
TEST SCENARIO 2: RUN NOT COMPLETED (process-inputs or summary failed)
=============================================================================

The run exists in allocation_run_audit but has no summary report.

Steps:
  1. Find the run:
     db.allocation_run_audit.findOne({
       type: "Scheduled", dtd_start: 10, dtd_end: 30,
       start_time: {$gte: "2026-03-12 03:00:00", $lt: "2026-03-12 04:00:00"}
     })

  2. Backup its summary:
     var backup = db.allocation_summary_report.findOne({run_id: "<RUN_ID>"})
     db.allocation_summary_report_backup.insertOne(backup)

  3. Delete the summary:
     db.allocation_summary_report.deleteOne({run_id: "<RUN_ID>"})

  4. Run test:
     python test_alert.py --test-hour "04:00"

  5. Expected: Email with subject "RM Alert: Scheduled Run Not Completed"

  6. Restore:
     var backup = db.allocation_summary_report_backup.findOne({run_id: "<RUN_ID>"})
     delete backup._id
     db.allocation_summary_report.insertOne(backup)
     db.allocation_summary_report_backup.deleteOne({run_id: "<RUN_ID>"})

=============================================================================
TEST SCENARIO 3: RUN NOT STARTED (fetch-inputs failed)
=============================================================================

The run does NOT exist in allocation_run_audit at all.

Steps:
  1. Find the run:
     db.allocation_run_audit.findOne({
       type: "Scheduled", dtd_start: 10, dtd_end: 30,
       start_time: {$gte: "2026-03-12 03:00:00", $lt: "2026-03-12 04:00:00"}
     })

  2. Temporarily rename its type:
     db.allocation_run_audit.updateOne(
       {run_id: "<RUN_ID>"},
       {$set: {type: "Scheduled_BACKUP"}}
     )

  3. Run test:
     python test_alert.py --test-hour "04:00"

  4. Expected: Email with subject "RM Alert: Scheduled Run Not Started"

  5. Restore:
     db.allocation_run_audit.updateOne(
       {run_id: "<RUN_ID>"},
       {$set: {type: "Scheduled"}}
     )

=============================================================================
TEST SCENARIO 4: SCHEDULE30 FAILURE (using --test-date for past runs)
=============================================================================

Steps:
  1. Find a Schedule30 run:
     db.allocation_run_audit.findOne({
       type: "Scheduled_Thirty",
       start_time: {$gte: "2026-03-11 16:00:00", $lt: "2026-03-11 17:00:00"}
     })

  2. Backup and delete its summary (same as Scenario 2)

  3. Run test with date:
     python test_alert.py --test-hour "17:00" --test-date "2026-03-11"

  4. Expected: Email with subject "RM Alert: Scheduled Run Not Completed"

  5. Restore the summary

=============================================================================
"""

import sys
import logging
import argparse
from pymongo import MongoClient

import configsettings
from constants_utils import ConstantsUtils
from scheduled_run_fail_alert import ScheduledRunFailureAlert


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    return logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description='Test scheduled run failure alerts',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        '--test-hour',
        type=str,
        required=True,
        help='Hour to simulate in HH:MM UTC format (e.g., "04:00" for CUG, "17:00" for Schedule30)'
    )

    parser.add_argument(
        '--test-date',
        type=str,
        help='Date to simulate in YYYY-MM-DD format (e.g., "2026-03-11"). Defaults to today.'
    )

    args = parser.parse_args()

    logger = setup_logging()

    print("=" * 80)
    print("SCHEDULED RUN FAILURE ALERT - TEST MODE")
    print("=" * 80)
    print(f"Test Hour (UTC): {args.test_hour}")
    if args.test_date:
        print(f"Test Date: {args.test_date}")

    try:
        hour_val = int(args.test_hour.split(':')[0])
        prev_hour = (hour_val - 1) % 24
        print(f"Checking runs between: {prev_hour:02d}:00 - {hour_val:02d}:00 UTC")
    except:
        pass

    print("=" * 80)
    print()

    mongoconn = MongoClient("mongodb://localhost:27017")

    params = {
        'mongoconn': mongoconn,
        'navconn': None,
        'cache_client': None,
    }

    overrides = {
        'test_mode': True,
        'test_hour': args.test_hour,
    }

    if args.test_date:
        overrides['test_date'] = args.test_date

    class MockContext:
        function_name = "test-scheduled-run-fail-alert"
        aws_request_id = "test-request-id"

    context = MockContext()

    try:
        alert_checker = ScheduledRunFailureAlert(logger, params, overrides, context)
        alert_checker.run()

        print()
        print("=" * 80)
        print("TEST COMPLETED")
        print("=" * 80)

    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
