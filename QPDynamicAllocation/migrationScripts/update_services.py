#!/usr/bin/env python3

import csv
import json
import sys
from datetime import datetime

import boto3
from botocore.exceptions import ClientError


DEFAULT_REGION = "ap-south-1"

# ===========================================================================
# ENVIRONMENT SWITCH
# ===========================================================================
# Set ENV to "uat" or "prod". The input CSV is derived from this toggle.
ENV = "uat"

import os  # noqa: E402

from env_config import output_csv_name  # noqa: E402

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# The `configuration` column holds full JSON blobs (EventBridge targets with
# InputTransformer, DeadLetterConfig, etc.) that can exceed the csv module's
# default field size limit (131072 chars). Raise it so DictReader never trips.
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


# The columns update_services.py needs. These are produced by
# fetch_services.py (uat_lambda_service_inventory.csv). The audit file
# produced by EB_fetch.py (uat_eventbridge_scheduler_inventory.csv) has a
# different schema and must NOT be used here.
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
            "from EB_fetch.py",
            file=sys.stderr,
        )
        print(
            "       (uat_eventbridge_scheduler_inventory.csv).",
            file=sys.stderr,
        )
        sys.exit(1)


def parse_enabled(value):
    """Interpret the CSV 'enabled' cell (string or bool) as a bool."""
    if isinstance(value, bool):
        return value

    return str(value).strip().lower() == "true"


def parse_json(value):
    if not value:
        return {}

    return json.loads(value)


def get_lambda_arn(
    lambda_client,
    function_name,
):
    try:
        response = lambda_client.get_function(
            FunctionName=function_name
        )

        return response[
            "Configuration"
        ]["FunctionArn"]

    except ClientError as exc:

        if (
            exc.response["Error"]["Code"]
            == "ResourceNotFoundException"
        ):
            return None

        raise


# ============================================================================
# SQS
# ============================================================================

def migrate_sqs(
    lambda_client,
    row,
    execute,
):
    new_lambda = row["new_lambda"]
    queue_arn = row["resource_arn"]

    configuration = parse_json(
        row["configuration"]
    )

    print()
    print(
        f"[SQS] {queue_arn}"
    )

    print(
        f"      OLD: {row['old_lambda']}"
    )

    print(
        f"      NEW: {new_lambda}"
    )

    if not execute:
        print(
            "      DRY RUN - would create "
            "event source mapping"
        )
        return

    # Check if mapping already exists.
    paginator = lambda_client.get_paginator(
        "list_event_source_mappings"
    )

    for page in paginator.paginate(
        FunctionName=new_lambda,
        EventSourceArn=queue_arn,
    ):

        for mapping in page.get(
            "EventSourceMappings",
            [],
        ):

            print(
                "      Already exists:"
                f" {mapping['UUID']}"
            )

            return

    kwargs = {
        "FunctionName": new_lambda,
        "EventSourceArn": queue_arn,
        "Enabled": parse_enabled(row["enabled"]),
    }

    if configuration.get(
        "BatchSize"
    ) is not None:

        kwargs["BatchSize"] = configuration[
            "BatchSize"
        ]

    if configuration.get(
        "MaximumBatchingWindowInSeconds"
    ) is not None:

        kwargs[
            "MaximumBatchingWindowInSeconds"
        ] = configuration[
            "MaximumBatchingWindowInSeconds"
        ]

    if configuration.get(
        "FunctionResponseTypes"
    ):

        kwargs[
            "FunctionResponseTypes"
        ] = configuration[
            "FunctionResponseTypes"
        ]

    if configuration.get(
        "FilterCriteria"
    ):

        kwargs[
            "FilterCriteria"
        ] = configuration[
            "FilterCriteria"
        ]

    if configuration.get(
        "ScalingConfig"
    ):

        kwargs[
            "ScalingConfig"
        ] = configuration[
            "ScalingConfig"
        ]

    for parameter in [
        "BisectBatchOnFunctionError",
        "MaximumRecordAgeInSeconds",
        "MaximumRetryAttempts",
        "ParallelizationFactor",
        "TumblingWindowInSeconds",
    ]:

        if configuration.get(parameter) is not None:

            kwargs[parameter] = configuration[
                parameter
            ]

    try:

        response = (
            lambda_client
            .create_event_source_mapping(
                **kwargs
            )
        )

        print(
            "      CREATED:"
            f" {response['UUID']}"
        )

    except ClientError as exc:

        print(
            "      ERROR:"
            f" {exc}",
            file=sys.stderr,
        )


# ============================================================================
# EventBridge Rule
# ============================================================================

def migrate_eventbridge_rule(
    lambda_client,
    eventbridge_client,
    row,
    execute,
):
    rule_name = row["resource_name"]
    new_lambda = row["new_lambda"]

    configuration = parse_json(
        row["configuration"]
    )

    old_target = configuration[
        "Target"
    ]

    print()
    print(
        f"[EVENTBRIDGE] {rule_name}"
    )

    print(
        f"              OLD: {row['old_lambda']}"
    )

    print(
        f"              NEW: {new_lambda}"
    )

    if not execute:
        print(
            "              DRY RUN - would add "
            "second target"
        )
        return

    new_lambda_arn = get_lambda_arn(
        lambda_client,
        new_lambda,
    )

    if not new_lambda_arn:

        print(
            f"              ERROR: New Lambda does "
            f"not exist: {new_lambda}",
            file=sys.stderr,
        )

        return

    # Create a different target ID.
    old_target_id = old_target.get(
        "Id",
        "lambda-target",
    )

    new_target_id = (
        f"{old_target_id}-new-lambda"
    )

    # Check existing targets first.
    paginator = eventbridge_client.get_paginator(
        "list_targets_by_rule"
    )

    existing_targets = []

    for page in paginator.paginate(
        Rule=rule_name
    ):

        existing_targets.extend(
            page.get("Targets", [])
        )

    # Don't create duplicate target.
    for target in existing_targets:

        if (
            target.get("Arn")
            == new_lambda_arn
        ):

            print(
                "              Target already "
                "exists - skipping"
            )

            return

    new_target = dict(
        old_target
    )

    new_target["Id"] = new_target_id
    new_target["Arn"] = new_lambda_arn

    # EventBridge PutTargets adds/updates the target
    # without removing the existing target.
    try:

        eventbridge_client.put_targets(
            Rule=rule_name,
            Targets=[
                new_target
            ],
        )

        print(
            "              ADDED target:"
            f" {new_target_id}"
        )

    except ClientError as exc:

        print(
            "              ERROR adding target:"
            f" {exc}",
            file=sys.stderr,
        )

        return

    # Add Lambda permission.
    statement_id = (
        f"{rule_name}-new-lambda"
    )

    try:

        lambda_client.add_permission(
            FunctionName=new_lambda,
            StatementId=statement_id,
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=row["resource_arn"],
        )

        print(
            "              Added Lambda "
            "invoke permission"
        )

    except ClientError as exc:

        error_code = exc.response[
            "Error"
        ]["Code"]

        if (
            error_code
            == "ResourceConflictException"
        ):

            print(
                "              Lambda permission "
                "already exists"
            )

        else:

            print(
                "              WARNING: Could not "
                "add Lambda permission:"
                f" {exc}",
                file=sys.stderr,
            )


# ============================================================================
# EventBridge Scheduler
# ============================================================================

def migrate_scheduler(
    lambda_client,
    scheduler_client,
    row,
    execute,
):
    old_schedule_name = row[
        "resource_name"
    ]

    new_lambda = row[
        "new_lambda"
    ]

    configuration = parse_json(
        row["configuration"]
    )

    old_target = configuration[
        "Target"
    ]

    group_name = (
        configuration.get(
            "GroupName"
        )
        or "default"
    )

    # IMPORTANT:
    #
    # We DO NOT modify the existing schedule.
    #
    # We create a second schedule for the new Lambda.

    new_schedule_name = (
        f"{old_schedule_name}-new-lambda"
    )

    print()
    print(
        f"[SCHEDULER] {old_schedule_name}"
    )

    print(
        f"             NEW SCHEDULE: "
        f"{new_schedule_name}"
    )

    print(
        f"             NEW LAMBDA: "
        f"{new_lambda}"
    )

    if not execute:

        print(
            "             DRY RUN - would create "
            "new schedule"
        )

        return

    new_lambda_arn = get_lambda_arn(
        lambda_client,
        new_lambda,
    )

    if not new_lambda_arn:

        print(
            f"             ERROR: New Lambda does "
            f"not exist: {new_lambda}",
            file=sys.stderr,
        )

        return

    new_target = dict(
        old_target
    )

    new_target["Arn"] = new_lambda_arn

    # create_schedule requires both of these; bail out cleanly rather than
    # letting the API raise if the captured config somehow lacks them.
    if not configuration.get("ScheduleExpression"):
        print(
            "             ERROR: captured config has no ScheduleExpression "
            "- skipping",
            file=sys.stderr,
        )
        return

    if not configuration.get("FlexibleTimeWindow"):
        print(
            "             ERROR: captured config has no FlexibleTimeWindow "
            "- skipping",
            file=sys.stderr,
        )
        return

    # Check whether the new schedule already exists.
    try:

        scheduler_client.get_schedule(
            Name=new_schedule_name,
            GroupName=group_name,
        )

        print(
            "             Schedule already exists - "
            "skipping"
        )

        return

    except ClientError as exc:

        if (
            exc.response["Error"]["Code"]
            != "ResourceNotFoundException"
        ):

            print(
                f"             ERROR checking "
                f"schedule: {exc}",
                file=sys.stderr,
            )

            return

    kwargs = {
        "Name": new_schedule_name,
        "GroupName": group_name,
        "ScheduleExpression": configuration[
            "ScheduleExpression"
        ],
        "FlexibleTimeWindow": configuration[
            "FlexibleTimeWindow"
        ],
        "Target": new_target,
        "State": configuration.get(
            "State",
            "ENABLED",
        ),
    }

    if configuration.get(
        "ScheduleExpressionTimezone"
    ):

        kwargs[
            "ScheduleExpressionTimezone"
        ] = configuration[
            "ScheduleExpressionTimezone"
        ]

    if configuration.get(
        "Description"
    ):

        kwargs[
            "Description"
        ] = (
            f"{configuration['Description']} "
            "(new Lambda)"
        )

    if configuration.get(
        "KmsKeyArn"
    ):

        kwargs[
            "KmsKeyArn"
        ] = configuration[
            "KmsKeyArn"
        ]

    if configuration.get(
        "StartDate"
    ):

        kwargs[
            "StartDate"
        ] = datetime.fromisoformat(
            configuration["StartDate"]
        )

    if configuration.get(
        "EndDate"
    ):

        kwargs[
            "EndDate"
        ] = datetime.fromisoformat(
            configuration["EndDate"]
        )

    try:

        scheduler_client.create_schedule(
            **kwargs
        )

        print(
            "             CREATED:"
            f" {new_schedule_name}"
        )

    except ClientError as exc:

        print(
            "             ERROR creating "
            f"schedule: {exc}",
            file=sys.stderr,
        )

        return

    # Scheduler invokes Lambda using the target RoleArn.
    #
    # The execution role must have lambda:InvokeFunction
    # permission for the NEW Lambda ARN.
    #
    # We do not modify the existing schedule or its role.

    print(
        "             NOTE: Verify that the "
        "Scheduler execution role"
    )

    print(
        f"                   allows "
        f"lambda:InvokeFunction on "
        f"{new_lambda_arn}"
    )


# ============================================================================
# Main
# ============================================================================

def main():

    # ── Hardcoded settings (no CLI args needed) ──
    # Input CSV is selected by the ENV toggle at the top of this file.
    INPUT_CSV = os.path.join(_SCRIPT_DIR, output_csv_name(ENV))
    REGION = DEFAULT_REGION
    EXECUTE = True  # Set to False for dry-run

    session = boto3.Session(
        region_name=REGION
    )

    lambda_client = session.client(
        "lambda"
    )

    eventbridge_client = session.client(
        "events"
    )

    scheduler_client = session.client(
        "scheduler"
    )

    with open(
        INPUT_CSV,
        newline="",
        encoding="utf-8",
    ) as f:

        reader = csv.DictReader(f)
        validate_csv_columns(reader.fieldnames)
        rows = list(reader)

    print(
        f"Loaded {len(rows)} "
        f"attachments from CSV"
    )

    if EXECUTE:

        print()
        print(
            "********************************************************"
        )
        print(
            "EXECUTE MODE"
        )
        print(
            "Only new attachments will be created."
        )
        print(
            "NO existing attachments will be deleted."
        )
        print(
            "********************************************************"
        )

    else:

        print()
        print(
            "********************************************************"
        )
        print(
            "DRY RUN"
        )
        print(
            "No AWS resources will be changed."
        )
        print(
            "Use --execute to create the attachments."
        )
        print(
            "********************************************************"
        )

    current_service = None

    for row in rows:

        service = row[
            "service"
        ]

        if service != current_service:

            current_service = service

            print()
            print("=" * 80)
            print(
                f"SERVICE: {service}"
            )
            print(
                f"OLD:     {row['old_lambda']}"
            )
            print(
                f"NEW:     {row['new_lambda']}"
            )
            print("=" * 80)

        service_type = row[
            "service_type"
        ]

        if service_type == "sqs":

            migrate_sqs(
                lambda_client,
                row,
                EXECUTE,
            )

        elif (
            service_type
            == "eventbridge_rule"
        ):

            migrate_eventbridge_rule(
                lambda_client,
                eventbridge_client,
                row,
                EXECUTE,
            )

        elif (
            service_type
            == "scheduler"
        ):

            migrate_scheduler(
                lambda_client,
                scheduler_client,
                row,
                EXECUTE,
            )

        else:

            print(
                f"WARNING: Unknown service type: "
                f"{service_type}",
                file=sys.stderr,
            )

    print()
    print("=" * 80)

    if EXECUTE:

        print(
            "Migration complete."
        )

        print(
            "Existing Lambda services were "
            "NOT deleted or modified."
        )

    else:

        print(
            "Dry run complete."
        )

        print(
            "No AWS resources were modified."
        )

    print("=" * 80)


if __name__ == "__main__":
    main()
