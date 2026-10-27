#!/usr/bin/env python3
"""
Operational CLI script for Phase 3 Old Resource Deletion & Decommissioning.
Can be run directly from terminal or CI/CD to inspect, dry-run, and execute
the removal of deprecated triggers and legacy Lambda functions.
"""

import argparse
import os
import sys
import boto3
from botocore.exceptions import ClientError

DEFAULT_REGION = "ap-south-1"

SERVICES = [
    "rm-additive-process",
    "rm-additive-prefetch",
    "rm-allocation-fetch-inputs",
    "rm-allocation-prefetch-inputs",
    "rm-allocation-process-inputs",
    "rm-allocation-scheduled-jobs",
    "rm-allocation-update-rbd",
    "rm-allocation-update-rbd-ftp",
    "rm-pricing-triggers",
]


def get_old_lambda_name(env, service):
    if env == "prod":
        return f"rm-allocation-serverless-funcs_{service}"
    if service in ("rm-additive-process", "rm-additive-prefetch"):
        return f"uat-{service}"
    return f"uat-rm-allocation-serverless-funcs-{service}"


def get_new_lambda_name(env, service):
    return f"{env}-rm-lambda-{service}"


def main():
    parser = argparse.ArgumentParser(description="Decommission legacy RM Lambda triggers and functions.")
    parser.add_argument("--env", choices=["uat", "prod"], default="uat", help="Target environment (uat or prod)")
    parser.add_argument("--region", default=DEFAULT_REGION, help="AWS Region (default: ap-south-1)")
    parser.add_argument("--execute", action="store_true", help="Actually execute deletions (default: DryRun)")
    parser.add_argument("--scheduler-action", choices=["disable", "delete"], default="disable", help="Action for old EventBridge schedules")
    parser.add_argument("--delete-lambdas", action="store_true", help="Delete legacy Lambda functions")
    args = parser.parse_args()

    execute = args.execute
    dry_run = not execute
    env = args.env

    print("=" * 80)
    print(f"RM LAMBDA CUTOVER & DECOMMISSIONING TOOL")
    print(f"Environment:       {env.upper()}")
    print(f"Execution Mode:    {'LIVE EXECUTION' if execute else 'DRY RUN (Safety Mode)'}")
    print(f"Scheduler Action:  {args.scheduler_action.upper()}")
    print(f"Delete Lambdas:    {args.delete_lambdas}")
    print("=" * 80)

    session = boto3.Session(region_name=args.region)
    events_client = session.client("events")
    scheduler_client = session.client("scheduler")
    lambda_client = session.client("lambda")

    old_to_new = {get_old_lambda_name(env, s): get_new_lambda_name(env, s) for s in SERVICES}

    # Step 1: Clean EventBridge Rules
    print("\n[STEP 1] Inspecting EventBridge Rules...")
    rules_paginator = events_client.get_paginator("list_rules")
    total_rule_targets_removed = 0

    for page in rules_paginator.paginate():
        for rule in page.get("Rules", []):
            rule_name = rule["Name"]
            try:
                targets = events_client.list_targets_by_rule(Rule=rule_name).get("Targets", [])
            except ClientError as ce:
                continue

            new_targets = [t for t in targets if t.get("Id", "").endswith("-new-lambda")]
            old_targets_to_remove = []

            for t in targets:
                tarn = t.get("Arn", "")
                tid = t.get("Id", "")
                if any(old_name in tarn for old_name in old_to_new):
                    if new_targets:
                        old_targets_to_remove.append(tid)
                    else:
                        print(f"  [GUARD] Rule {rule_name}: Skipped {tid} because new-lambda target is missing!")

            if old_targets_to_remove:
                print(f"  Rule: {rule_name} -> Removing old target(s): {old_targets_to_remove}")
                if execute:
                    events_client.remove_targets(Rule=rule_name, Ids=old_targets_to_remove)
                total_rule_targets_removed += len(old_targets_to_remove)

    print(f"Rule targets to remove: {total_rule_targets_removed}")

    # Step 2: EventBridge Scheduler
    print("\n[STEP 2] Inspecting EventBridge Scheduler Schedules...")
    total_schedules_retired = 0
    try:
        sched_paginator = scheduler_client.get_paginator("list_schedules")
        for page in sched_paginator.paginate():
            for s in page.get("Schedules", []):
                sname = s["Name"]
                group = s.get("GroupName", "default")
                if not sname.endswith("-new-lambda"):
                    new_sname = f"{sname}-new-lambda"
                    try:
                        scheduler_client.get_schedule(Name=new_sname, GroupName=group)
                        new_exists = True
                    except ClientError:
                        new_exists = False

                    if new_exists:
                        print(f"  Schedule: {sname} -> {args.scheduler_action} (new schedule: {new_sname})")
                        if execute:
                            if args.scheduler_action == "delete":
                                scheduler_client.delete_schedule(Name=sname, GroupName=group)
                            else:
                                cur = scheduler_client.get_schedule(Name=sname, GroupName=group)
                                if cur.get("State") != "DISABLED":
                                    scheduler_client.update_schedule(
                                        Name=sname,
                                        GroupName=group,
                                        ScheduleExpression=cur["ScheduleExpression"],
                                        FlexibleTimeWindow=cur["FlexibleTimeWindow"],
                                        Target=cur["Target"],
                                        State="DISABLED"
                                    )
                        total_schedules_retired += 1
    except ClientError as ce:
        print(f"  Scheduler check note: {ce}")

    print(f"Schedules to retire: {total_schedules_retired}")

    # Step 3: SQS Mappings on Old Lambdas
    print("\n[STEP 3] Inspecting SQS EventSourceMappings on Old Lambdas...")
    total_sqs_deleted = 0
    for old_fn in old_to_new:
        try:
            mappings = lambda_client.list_event_source_mappings(FunctionName=old_fn).get("EventSourceMappings", [])
            for m in mappings:
                mid = m["UUID"]
                q_arn = m.get("EventSourceArn", "")
                print(f"  Old Lambda: {old_fn} -> Queue: {q_arn.split(':')[-1]} (Mapping UUID: {mid})")
                if execute:
                    lambda_client.delete_event_source_mapping(UUID=mid)
                total_sqs_deleted += 1
        except ClientError as ce:
            if ce.response["Error"]["Code"] != "ResourceNotFoundException":
                print(f"  Error checking mappings for {old_fn}: {ce}")

    print(f"SQS mappings to delete: {total_sqs_deleted}")

    # Step 4: Delete Legacy Lambdas
    if args.delete_lambdas:
        print("\n[STEP 4] Deleting Legacy Lambda Functions...")
        total_lambdas_deleted = 0
        for old_fn in old_to_new:
            try:
                # Check if old exists
                lambda_client.get_function(FunctionName=old_fn)
                print(f"  Deleting: {old_fn}")
                if execute:
                    lambda_client.delete_function(FunctionName=old_fn)
                total_lambdas_deleted += 1
            except ClientError as ce:
                if ce.response["Error"]["Code"] == "ResourceNotFoundException":
                    print(f"  Already absent: {old_fn}")
                else:
                    print(f"  Error deleting {old_fn}: {ce}")
        print(f"Legacy Lambdas to delete: {total_lambdas_deleted}")
    else:
        print("\n[STEP 4] Skipping Legacy Lambda deletion (use --delete-lambdas to enable).")

    print("\n" + "=" * 80)
    if dry_run:
        print("DRY RUN COMPLETE: No AWS resources were altered.")
        print("To execute these changes, re-run with --execute.")
    else:
        print("EXECUTION COMPLETE: Deprecated triggers retired successfully.")
    print("=" * 80)


if __name__ == "__main__":
    main()
