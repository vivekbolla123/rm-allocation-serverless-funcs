#!/usr/bin/env python3

import argparse
import csv
import json
import sys

import boto3
from botocore.exceptions import ClientError


DEFAULT_REGION = "ap-south-1"

# service name -> old Lambda / new Lambda
LAMBDA_MAP = {
    "rm-additive-process": {
        "old_lambda": "uat-rm-additive-process",
        "new_lambda": "uat-rm-lambda-rm-additive-process",
    },
    "rm-additive-prefetch": {
        "old_lambda": "uat-rm-additive-prefetch",
        "new_lambda": "uat-rm-lambda-rm-additive-prefetch",
    },
    "rm-allocation-fetch-inputs": {
        "old_lambda": "uat-rm-allocation-serverless-funcs-rm-allocation-fetch-inputs",
        "new_lambda": "uat-rm-lambda-rm-allocation-fetch-inputs",
    },
    "rm-allocation-prefetch-inputs": {
        "old_lambda": "uat-rm-allocation-serverless-funcs-rm-allocation-prefetch-inputs",
        "new_lambda": "uat-rm-lambda-rm-allocation-prefetch-inputs",
    },
    "rm-allocation-process-inputs": {
        "old_lambda": "uat-rm-allocation-serverless-funcs-rm-allocation-process-inputs",
        "new_lambda": "uat-rm-lambda-rm-allocation-process-inputs",
    },
    "rm-allocation-scheduled-jobs": {
        "old_lambda": "uat-rm-allocation-serverless-funcs-rm-allocation-scheduled-jobs",
        "new_lambda": "uat-rm-lambda-rm-allocation-scheduled-jobs",
    },
    "rm-allocation-update-rbd": {
        "old_lambda": "uat-rm-allocation-serverless-funcs-rm-allocation-update-rbd",
        "new_lambda": "uat-rm-lambda-rm-allocation-update-rbd",
    },
    "rm-allocation-update-rbd-ftp": {
        "old_lambda": "uat-rm-allocation-serverless-funcs-rm-allocation-update-rbd-ftp",
        "new_lambda": "uat-rm-lambda-rm-allocation-update-rbd-ftp",
    },
    "rm-pricing-triggers": {
        "old_lambda": "uat-rm-allocation-serverless-funcs-rm-pricing-triggers",
        "new_lambda": "uat-rm-lambda-rm-pricing-triggers",
    },
}


def json_dump(value):
    if value is None:
        return ""

    return json.dumps(
        value,
        separators=(",", ":"),
        sort_keys=True,
    )


def get_lambda_arn(lambda_client, function_name):
    response = lambda_client.get_function(
        FunctionName=function_name
    )

    return response["Configuration"]["FunctionArn"]


# ---------------------------------------------------------------------------
# SQS
# ---------------------------------------------------------------------------

def fetch_sqs_mappings(
    lambda_client,
    old_lambda,
    new_lambda,
):
    rows = []

    paginator = lambda_client.get_paginator(
        "list_event_source_mappings"
    )

    for page in paginator.paginate(
        FunctionName=old_lambda
    ):
        for mapping in page.get("EventSourceMappings", []):

            event_source_arn = mapping.get(
                "EventSourceArn",
                "",
            )

            configuration = {
                "BatchSize": mapping.get("BatchSize"),
                "MaximumBatchingWindowInSeconds": mapping.get(
                    "MaximumBatchingWindowInSeconds"
                ),
                "FunctionResponseTypes": mapping.get(
                    "FunctionResponseTypes"
                ),
                "FilterCriteria": mapping.get(
                    "FilterCriteria"
                ),
                "ScalingConfig": mapping.get(
                    "ScalingConfig"
                ),
                "BisectBatchOnFunctionError": mapping.get(
                    "BisectBatchOnFunctionError"
                ),
                "MaximumRecordAgeInSeconds": mapping.get(
                    "MaximumRecordAgeInSeconds"
                ),
                "MaximumRetryAttempts": mapping.get(
                    "MaximumRetryAttempts"
                ),
                "ParallelizationFactor": mapping.get(
                    "ParallelizationFactor"
                ),
                "TumblingWindowInSeconds": mapping.get(
                    "TumblingWindowInSeconds"
                ),
            }

            rows.append({
                "environment": "uat",
                "service": old_lambda,
                "old_lambda": old_lambda,
                "new_lambda": new_lambda,
                "service_type": "sqs",
                "resource_name": event_source_arn.split(":")[-1],
                "resource_arn": event_source_arn,
                "source_uuid": mapping.get("UUID", ""),
                "state": mapping.get("State", ""),
                "enabled": (
                    mapping.get("State")
                    not in ("Disabled", "Disabling")
                ),
                "configuration": json_dump(
                    configuration
                ),
            })

    return rows


# ---------------------------------------------------------------------------
# EventBridge Rules
# ---------------------------------------------------------------------------

def get_eventbridge_targets(
    eventbridge_client,
    rule_name,
):
    targets = []

    paginator = eventbridge_client.get_paginator(
        "list_targets_by_rule"
    )

    for page in paginator.paginate(
        Rule=rule_name
    ):
        targets.extend(
            page.get("Targets", [])
        )

    return targets


def fetch_eventbridge_rules(
    eventbridge_client,
    old_lambda_arn,
    old_lambda,
    new_lambda,
):
    rows = []

    paginator = eventbridge_client.get_paginator(
        "list_rules"
    )

    for page in paginator.paginate():

        for rule in page.get("Rules", []):

            rule_name = rule["Name"]

            try:
                targets = get_eventbridge_targets(
                    eventbridge_client,
                    rule_name,
                )

            except ClientError as exc:
                print(
                    f"WARNING: Could not inspect "
                    f"EventBridge rule {rule_name}: {exc}",
                    file=sys.stderr,
                )
                continue

            for target in targets:

                if target.get("Arn") != old_lambda_arn:
                    continue

                configuration = {
                    "Description": rule.get(
                        "Description"
                    ),
                    "EventPattern": rule.get(
                        "EventPattern"
                    ),
                    "ScheduleExpression": rule.get(
                        "ScheduleExpression"
                    ),
                    "State": rule.get(
                        "State"
                    ),
                    "RoleArn": rule.get(
                        "RoleArn"
                    ),
                    "Target": target,
                }

                rows.append({
                    "environment": "uat",
                    "service": old_lambda,
                    "old_lambda": old_lambda,
                    "new_lambda": new_lambda,
                    "service_type": "eventbridge_rule",
                    "resource_name": rule_name,
                    "resource_arn": rule.get(
                        "Arn",
                        "",
                    ),
                    "source_uuid": "",
                    "state": rule.get(
                        "State",
                        "",
                    ),
                    "enabled": (
                        rule.get("State")
                        == "ENABLED"
                    ),
                    "configuration": json_dump(
                        configuration
                    ),
                })

    return rows


# ---------------------------------------------------------------------------
# EventBridge Scheduler
# ---------------------------------------------------------------------------

def fetch_schedules(
    scheduler_client,
    old_lambda_arn,
    old_lambda,
    new_lambda,
):
    rows = []

    paginator = scheduler_client.get_paginator(
        "list_schedules"
    )

    for page in paginator.paginate():

        for schedule_summary in page.get(
            "Schedules",
            [],
        ):

            schedule_name = schedule_summary[
                "Name"
            ]

            try:
                schedule = scheduler_client.get_schedule(
                    Name=schedule_name,
                    GroupName=schedule_summary.get(
                        "GroupName",
                        "default",
                    ),
                )

            except ClientError as exc:
                print(
                    f"WARNING: Could not inspect "
                    f"Scheduler schedule "
                    f"{schedule_name}: {exc}",
                    file=sys.stderr,
                )
                continue

            target = schedule.get(
                "Target",
                {},
            )

            if target.get("Arn") != old_lambda_arn:
                continue

            configuration = {
                "ScheduleExpression": schedule.get(
                    "ScheduleExpression"
                ),
                "ScheduleExpressionTimezone": schedule.get(
                    "ScheduleExpressionTimezone"
                ),
                "FlexibleTimeWindow": schedule.get(
                    "FlexibleTimeWindow"
                ),
                "StartDate": (
                    schedule["StartDate"].isoformat()
                    if schedule.get("StartDate")
                    else None
                ),
                "EndDate": (
                    schedule["EndDate"].isoformat()
                    if schedule.get("EndDate")
                    else None
                ),
                "State": schedule.get(
                    "State"
                ),
                "Description": schedule.get(
                    "Description"
                ),
                "GroupName": schedule.get(
                    "GroupName"
                ),
                "KmsKeyArn": schedule.get(
                    "KmsKeyArn"
                ),
                "Target": target,
            }

            rows.append({
                "environment": "uat",
                "service": old_lambda,
                "old_lambda": old_lambda,
                "new_lambda": new_lambda,
                "service_type": "scheduler",
                "resource_name": schedule_name,
                "resource_arn": "",
                "source_uuid": "",
                "state": schedule.get(
                    "State",
                    "",
                ),
                "enabled": (
                    schedule.get("State")
                    == "ENABLED"
                ),
                "configuration": json_dump(
                    configuration
                ),
            })

    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():

    parser = argparse.ArgumentParser(
        description=(
            "Export UAT Lambda triggers/services "
            "to CSV."
        )
    )

    parser.add_argument(
        "--region",
        default=DEFAULT_REGION,
    )

    parser.add_argument(
        "--output",
        default="uat_lambda_service_inventory.csv",
    )

    args = parser.parse_args()

    session = boto3.Session(
        region_name=args.region
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

    rows = []

    for service, mapping in LAMBDA_MAP.items():

        old_lambda = mapping["old_lambda"]
        new_lambda = mapping["new_lambda"]

        print()
        print("=" * 80)
        print(f"Service:     {service}")
        print(f"Old Lambda:  {old_lambda}")
        print(f"New Lambda:  {new_lambda}")
        print("=" * 80)

        # Verify old Lambda exists.
        try:
            old_lambda_arn = get_lambda_arn(
                lambda_client,
                old_lambda,
            )

            print(
                f"Lambda ARN: {old_lambda_arn}"
            )

        except ClientError as exc:
            print(
                f"ERROR: Lambda does not exist: "
                f"{old_lambda}",
                file=sys.stderr,
            )
            print(
                f"       {exc}",
                file=sys.stderr,
            )
            continue

        # SQS
        sqs_rows = fetch_sqs_mappings(
            lambda_client,
            old_lambda,
            new_lambda,
        )

        print(
            f"SQS mappings:        {len(sqs_rows)}"
        )

        rows.extend(sqs_rows)

        # EventBridge
        eventbridge_rows = fetch_eventbridge_rules(
            eventbridge_client,
            old_lambda_arn,
            old_lambda,
            new_lambda,
        )

        print(
            f"EventBridge rules:   "
            f"{len(eventbridge_rows)}"
        )

        rows.extend(eventbridge_rows)

        # Scheduler
        scheduler_rows = fetch_schedules(
            scheduler_client,
            old_lambda_arn,
            old_lambda,
            new_lambda,
        )

        print(
            f"Scheduler schedules: "
            f"{len(scheduler_rows)}"
        )

        rows.extend(scheduler_rows)

    fieldnames = [
        "environment",
        "service",
        "old_lambda",
        "new_lambda",
        "service_type",
        "resource_name",
        "resource_arn",
        "source_uuid",
        "state",
        "enabled",
        "configuration",
    ]

    with open(
        args.output,
        "w",
        newline="",
        encoding="utf-8",
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
        )

        writer.writeheader()
        writer.writerows(rows)

    print()
    print("=" * 80)
    print(
        f"Exported {len(rows)} service attachments"
    )
    print(
        f"CSV: {args.output}"
    )
    print("=" * 80)


if __name__ == "__main__":
    main()
