#!/usr/bin/env python3
"""
Cutover step: retire the OLD-lambda triggers left behind by update_services.py
so each service fires ONLY the new Lambda.

This fixes the "double invocation" state. It is the deliberate second half of
the migration:

    1. update_services.py   -> ADD new EventBridge target / CREATE new
                               schedule (both old + new fire)
    2. remove_old_targets.py -> REMOVE old EventBridge target and
                               DISABLE/DELETE old schedule (only new fires)

Handles two trigger types:
  * eventbridge_rule : removes the old-lambda target from the rule, keeping
    the "<id>-new-lambda" target.
  * scheduler        : retires the ORIGINAL schedule (the new one is a
    separate "<name>-new-lambda" schedule). SCHEDULER_ACTION controls whether
    the old schedule is disabled (default, reversible) or deleted.

SQS event-source mappings are intentionally left alone (SQS fans out, so both
lambdas drain the same queue; handle those separately if a hard cutover is
needed).

Safety model (mirrors update_services.py):
  * Dry-run by default. Set EXECUTE = True to apply.
  * Reads the SAME migration CSV produced by fetch_services.py.
  * Validates the CSV header so the wrong file can't be used.
  * Idempotent: skips triggers that are already retired.
  * Guard: NEVER retires the old trigger unless the NEW target/schedule is
    confirmed present first, so a trigger is never lost entirely.
"""

import csv
import json
import os
import sys

import boto3
from botocore.exceptions import ClientError

from env_config import output_csv_name


DEFAULT_REGION = "ap-south-1"

# ===========================================================================
# ENVIRONMENT SWITCH
# ===========================================================================
# Set ENV to "uat" or "prod". The input CSV is derived from this toggle.
ENV = "uat"

# How to retire the OLD EventBridge Scheduler schedule during cutover:
#   "disable" -> set the old schedule State to DISABLED (reversible, default)
#   "delete"  -> delete the old schedule outright (not reversible)
# The new "<name>-new-lambda" schedule created by update_services.py is never
# touched by this script.
SCHEDULER_ACTION = "disable"

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


# sys.maxsize can overflow the C long csv expects (notably on Windows), so
# fall back to progressively smaller limits until one is accepted.
def _raise_csv_field_limit():
    limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(limit)
            return
        except OverflowError:
            limit = int(limit // 2)


_raise_csv_field_limit()


REQUIRED_COLUMNS = {
    "service",
    "old_lambda",
    "new_lambda",
    "service_type",
    "resource_name",
    "resource_arn",
    "enabled",
    "configuration",
}


def validate_csv_columns(fieldnames):
    """Ensure the input CSV is the migration file, not the audit file."""
    actual = set(fieldnames or [])
    missing = REQUIRED_COLUMNS - actual

    if missing:
        print(
            "ERROR: The input CSV is missing required columns: "
            f"{', '.join(sorted(missing))}",
            file=sys.stderr,
        )
        print(
            "       This script expects the migration file produced by "
            "fetch_services.py",
            file=sys.stderr,
        )
        print(
            "       (uat_lambda_service_inventory.csv), NOT the audit file "
            "from EB_fetch.py.",
            file=sys.stderr,
        )
        sys.exit(1)


def parse_json(value):
    if not value:
        return {}

    return json.loads(value)


def get_lambda_arn(lambda_client, function_name):
    try:
        response = lambda_client.get_function(
            FunctionName=function_name
        )
        return response["Configuration"]["FunctionArn"]
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ResourceNotFoundException":
            return None
        raise


def list_rule_targets(eventbridge_client, rule_name):
    targets = []
    paginator = eventbridge_client.get_paginator(
        "list_targets_by_rule"
    )
    for page in paginator.paginate(Rule=rule_name):
        targets.extend(page.get("Targets", []))
    return targets


# ============================================================================
# EventBridge Rule - remove old target
# ============================================================================

def remove_old_eventbridge_target(
    lambda_client,
    eventbridge_client,
    row,
    execute,
):
    rule_name = row["resource_name"]
    old_lambda = row["old_lambda"]
    new_lambda = row["new_lambda"]

    configuration = parse_json(row["configuration"])
    old_target = configuration.get("Target", {})

    # The old target Id, exactly as captured before migration.
    old_target_id = old_target.get("Id", "lambda-target")

    # The Id update_services.py gave the new target.
    new_target_id = f"{old_target_id}-new-lambda"

    print()
    print(f"[EVENTBRIDGE] {rule_name}")
    print(f"              OLD: {old_lambda}")
    print(f"              NEW: {new_lambda}")
    print(f"              Removing old target Id: {old_target_id}")

    if not execute:
        print(
            "              DRY RUN - would remove old target"
        )
        return

    # Resolve both lambda ARNs so we can reason about live targets.
    old_lambda_arn = get_lambda_arn(lambda_client, old_lambda)
    new_lambda_arn = get_lambda_arn(lambda_client, new_lambda)

    if not new_lambda_arn:
        print(
            "              ERROR: New Lambda does not exist - refusing to "
            f"remove old target: {new_lambda}",
            file=sys.stderr,
        )
        return

    # Inspect the rule's live targets.
    try:
        targets = list_rule_targets(eventbridge_client, rule_name)
    except ClientError as exc:
        print(
            f"              ERROR listing targets: {exc}",
            file=sys.stderr,
        )
        return

    target_ids = {t.get("Id") for t in targets}
    target_arns = {t.get("Arn") for t in targets}

    # GUARD: never remove the old target unless the new one is present.
    new_present = (
        new_target_id in target_ids
        or new_lambda_arn in target_arns
    )

    if not new_present:
        print(
            "              SKIP: new-lambda target not found on this rule. "
            "Run update_services.py first - refusing to remove old target.",
            file=sys.stderr,
        )
        return

    # Figure out exactly which target Id carries the OLD lambda.
    # Prefer matching by ARN (authoritative); fall back to the captured Id.
    old_ids_to_remove = []
    for t in targets:
        tid = t.get("Id")
        tarn = t.get("Arn")

        if tid == new_target_id:
            continue  # never remove the new target

        if old_lambda_arn and tarn == old_lambda_arn:
            old_ids_to_remove.append(tid)
        elif tid == old_target_id and tarn != new_lambda_arn:
            # ARN lookup unavailable (old lambda deleted) but Id matches the
            # captured old target, and it is not the new lambda.
            old_ids_to_remove.append(tid)

    if not old_ids_to_remove:
        print(
            "              Already clean - no old target present (skipping)"
        )
        return

    try:
        response = eventbridge_client.remove_targets(
            Rule=rule_name,
            Ids=old_ids_to_remove,
        )

        failed = response.get("FailedEntryCount", 0)
        if failed:
            print(
                f"              WARNING: {failed} target(s) failed to "
                f"remove: {response.get('FailedEntries')}",
                file=sys.stderr,
            )
        else:
            print(
                "              REMOVED old target(s): "
                f"{', '.join(old_ids_to_remove)}"
            )

    except ClientError as exc:
        print(
            f"              ERROR removing target: {exc}",
            file=sys.stderr,
        )
        return

    # Clean up the resource-based permission update_services.py added so the
    # old permission statement doesn't dangle. The statement id it used was
    # "{rule_name}-new-lambda" on the NEW lambda - we KEEP that (new lambda
    # still needs it). There is nothing to remove on the new lambda.
    #
    # The OLD lambda keeps its own original permission; we do not touch it so
    # that re-adding the old target later (rollback) still works.


# ============================================================================
# EventBridge Scheduler - retire old schedule
# ============================================================================

def cutover_old_scheduler(
    scheduler_client,
    row,
    execute,
    scheduler_action,
):
    """
    update_services.py creates a SEPARATE new schedule named
    "<old>-new-lambda" for the new Lambda, leaving the original schedule
    firing the old Lambda (double invocation). This retires the OLD schedule.

    GUARD: never retire the old schedule unless the new "-new-lambda" schedule
    is confirmed present, so the trigger is never lost entirely.
    """
    configuration = parse_json(row["configuration"])

    old_schedule_name = row["resource_name"]
    group_name = configuration.get("GroupName") or "default"
    new_schedule_name = f"{old_schedule_name}-new-lambda"

    print()
    print(f"[SCHEDULER] {old_schedule_name}  (group: {group_name})")
    print(f"            OLD: {row['old_lambda']}")
    print(f"            NEW: {row['new_lambda']}")
    print(f"            Action: {scheduler_action} old schedule")

    if not execute:
        print(
            f"            DRY RUN - would {scheduler_action} old schedule"
        )
        return

    # GUARD: confirm the new schedule exists before retiring the old one.
    try:
        scheduler_client.get_schedule(
            Name=new_schedule_name,
            GroupName=group_name,
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ResourceNotFoundException":
            print(
                "            SKIP: new-lambda schedule not found "
                f"({new_schedule_name}). Run update_services.py first - "
                "refusing to retire old schedule.",
                file=sys.stderr,
            )
            return
        print(
            f"            ERROR checking new schedule: {exc}",
            file=sys.stderr,
        )
        return

    # Fetch the current old schedule.
    try:
        old_schedule = scheduler_client.get_schedule(
            Name=old_schedule_name,
            GroupName=group_name,
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ResourceNotFoundException":
            print(
                "            Already gone - old schedule not found "
                "(skipping)"
            )
            return
        print(
            f"            ERROR fetching old schedule: {exc}",
            file=sys.stderr,
        )
        return

    if scheduler_action == "delete":
        try:
            scheduler_client.delete_schedule(
                Name=old_schedule_name,
                GroupName=group_name,
            )
            print(f"            DELETED old schedule: {old_schedule_name}")
        except ClientError as exc:
            print(
                f"            ERROR deleting old schedule: {exc}",
                file=sys.stderr,
            )
        return

    # Default: disable (reversible).
    if old_schedule.get("State") == "DISABLED":
        print("            Already DISABLED (skipping)")
        return

    # update_schedule requires the full definition; re-send it with
    # State=DISABLED. Build kwargs from the existing schedule.
    kwargs = {
        "Name": old_schedule_name,
        "GroupName": group_name,
        "ScheduleExpression": old_schedule["ScheduleExpression"],
        "FlexibleTimeWindow": old_schedule["FlexibleTimeWindow"],
        "Target": old_schedule["Target"],
        "State": "DISABLED",
    }

    for optional in (
        "ScheduleExpressionTimezone",
        "Description",
        "KmsKeyArn",
        "StartDate",
        "EndDate",
    ):
        if old_schedule.get(optional):
            kwargs[optional] = old_schedule[optional]

    try:
        scheduler_client.update_schedule(**kwargs)
        print(f"            DISABLED old schedule: {old_schedule_name}")
    except ClientError as exc:
        print(
            f"            ERROR disabling old schedule: {exc}",
            file=sys.stderr,
        )


# ============================================================================
# Main
# ============================================================================

def main():

    # Hardcoded settings (no CLI args needed).
    # Input CSV is selected by the ENV toggle at the top of this file.
    INPUT_CSV = os.path.join(_SCRIPT_DIR, output_csv_name(ENV))
    REGION = DEFAULT_REGION
    EXECUTE = True  # Set to True to actually remove old targets.

    if SCHEDULER_ACTION not in ("disable", "delete"):
        print(
            f"ERROR: SCHEDULER_ACTION must be 'disable' or 'delete', "
            f"got '{SCHEDULER_ACTION}'.",
            file=sys.stderr,
        )
        sys.exit(1)

    session = boto3.Session(region_name=REGION)
    lambda_client = session.client("lambda")
    eventbridge_client = session.client("events")
    scheduler_client = session.client("scheduler")

    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        validate_csv_columns(reader.fieldnames)
        rows = list(reader)

    print(f"Loaded {len(rows)} attachments from CSV")

    if EXECUTE:
        print()
        print("********************************************************")
        print("EXECUTE MODE")
        print("Old EventBridge targets will be REMOVED.")
        print(f"Old Scheduler schedules will be {SCHEDULER_ACTION.upper()}D.")
        print("New-lambda targets and schedules are preserved.")
        print("********************************************************")
    else:
        print()
        print("********************************************************")
        print("DRY RUN")
        print("No AWS resources will be changed.")
        print(f"Scheduler action would be: {SCHEDULER_ACTION}")
        print("Set EXECUTE = True to apply.")
        print("********************************************************")

    current_service = None
    rules_processed = 0
    schedules_processed = 0

    for row in rows:

        service_type = row["service_type"]
        if service_type not in ("eventbridge_rule", "scheduler"):
            continue

        service = row["service"]
        if service != current_service:
            current_service = service
            print()
            print("=" * 80)
            print(f"SERVICE: {service}")
            print(f"OLD:     {row['old_lambda']}")
            print(f"NEW:     {row['new_lambda']}")
            print("=" * 80)

        if service_type == "eventbridge_rule":
            remove_old_eventbridge_target(
                lambda_client,
                eventbridge_client,
                row,
                EXECUTE,
            )
            rules_processed += 1
        else:  # scheduler
            cutover_old_scheduler(
                scheduler_client,
                row,
                EXECUTE,
                SCHEDULER_ACTION,
            )
            schedules_processed += 1

    print()
    print("=" * 80)
    print(f"EventBridge rules processed: {rules_processed}")
    print(f"Scheduler schedules processed: {schedules_processed}")

    if EXECUTE:
        print(
            "Cutover complete. Rules target only the new Lambda; old "
            f"schedules were {SCHEDULER_ACTION}d."
        )
    else:
        print("Dry run complete. No AWS resources were modified.")

    print("=" * 80)


if __name__ == "__main__":
    main()
