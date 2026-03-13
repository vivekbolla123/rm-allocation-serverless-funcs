"""
Test Scheduled Run Failure Alert

This script tests the alert system by simulating different time windows.

IMPORTANT: This script does NOT delete any database records. 
You must manually delete/modify records to simulate failures.

=============================================================================
TESTING GUIDE
=============================================================================

MONITORED RUNS:
---------------
1. CUG Run: 8:50 AM IST (03:20 UTC) - DTD 10-30, Type: Scheduled
2. Schedule30: 10:00 PM IST (16:30 UTC) - Type: Scheduled_Thirty
3. Other EventBridge scheduled runs

=============================================================================
TEST SCENARIO 1: SUCCESS (No Alerts)
=============================================================================

This tests that the system correctly identifies successful runs.

Steps:
1. Ensure a run exists in the database with its summary report
2. Run the test for that time window
3. Expected: No alert emails sent

Example - Test CUG Success:
---------------------------
# Verify run exists in DB:
db.allocation_run_audit.findOne({
  type: "Scheduled",
  dtd_start: 10,
  dtd_end: 30,
  start_time: {$gte: "2026-03-12 03:00:00", $lt: "2026-03-12 04:00:00"}
})

# Verify summary exists:
db.allocation_summary_report.findOne({
  run_id: "558728d5-ade3-4851-a1c8-530fcd2b4e0d"
})

# Run test:
python test_alert.py --test-hour "04:00"

# Expected output:
# "Run method completed." (no alerts sent)

=============================================================================
TEST SCENARIO 2: FAILURE (Alert Sent)
=============================================================================

This tests that the system detects failed runs and sends alerts.

Steps:
1. Find a run in the database
2. MANUALLY delete its summary report from allocation_summary_report
3. Run the test for that time window
4. Expected: Alert email sent
5. MANUALLY restore the summary report

Example - Test CUG Failure:
---------------------------
# Step 1: Find the run
db.allocation_run_audit.findOne({
  type: "Scheduled",
  dtd_start: 10,
  dtd_end: 30,
  start_time: {$gte: "2026-03-12 03:00:00", $lt: "2026-03-12 04:00:00"}
})
# Result: run_id = "558728d5-ade3-4851-a1c8-530fcd2b4e0d"

# Step 2: Backup the summary (IMPORTANT!)
const backup = db.allocation_summary_report.findOne({
  run_id: "558728d5-ade3-4851-a1c8-530fcd2b4e0d"
})
db.allocation_summary_report_backup.insertOne(backup)

# Step 3: Delete the summary to simulate failure
db.allocation_summary_report.deleteOne({
  run_id: "558728d5-ade3-4851-a1c8-530fcd2b4e0d"
})

# Step 4: Run test
python test_alert.py --test-hour "04:00"

# Expected output:
# "Sent Alert mail for failed runs"
# You should receive an email with the failed run details

# Step 5: Restore the summary
const backup = db.allocation_summary_report_backup.findOne({
  run_id: "558728d5-ade3-4851-a1c8-530fcd2b4e0d"
})
db.allocation_summary_report.insertOne(backup)
db.allocation_summary_report_backup.deleteOne({
  run_id: "558728d5-ade3-4851-a1c8-530fcd2b4e0d"
})

=============================================================================
TEST TIMES FOR DIFFERENT RUNS
=============================================================================

CUG Run (8:50 AM IST / 3:20 AM UTC):
  Test Hour: "04:00" (checks 03:00-04:00 UTC window)
  Command: python test_alert.py --test-hour "04:00"

Schedule30 (10:00 PM IST / 4:30 PM UTC):
  Test Hour: "17:00" (checks 16:00-17:00 UTC window)
  Command: python test_alert.py --test-hour "17:00"

Other Scheduled Runs:
  Use the hour AFTER the run time
  Example: Run at 14:30 UTC → Test Hour: "15:00"

=============================================================================
USAGE
=============================================================================

python test_alert.py --test-hour "04:00"

Arguments:
  --test-hour HH:MM   Hour to simulate (UTC format, e.g., "04:00", "17:00")

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
        help='Date to simulate in YYYY-MM-DD format (e.g., "2026-03-11"). If not provided, uses current date.'
    )
    
    args = parser.parse_args()
    
    logger = setup_logging()
    
    print("=" * 80)
    print("SCHEDULED RUN FAILURE ALERT - TEST MODE")
    print("=" * 80)
    print(f"Test Hour (UTC): {args.test_hour}")
    if args.test_date:
        print(f"Test Date: {args.test_date}")
    
    # Calculate window
    try:
        hour_val = int(args.test_hour.split(':')[0])
        prev_hour = (hour_val - 1) % 24
        print(f"Checking runs between: {prev_hour:02d}:00 - {hour_val:02d}:00 UTC")
    except:
        pass
    
    print("=" * 80)
    print()
    
    # Setup MongoDB connection
    mongoconn = MongoClient("mongodb://localhost:27017")
    
    params = {
        'mongoconn': mongoconn,
        'navconn': None,
        'cache_client': None,
    }
    
    overrides = {
        'test_mode': True,
        'test_hour': args.test_hour
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
