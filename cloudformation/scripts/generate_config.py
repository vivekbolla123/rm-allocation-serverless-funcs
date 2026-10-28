#!/usr/bin/env python3
"""
Generates the standard, human-readable master environment configuration (env_config.yaml)
and CloudFormation parameter files from existing inventory CSVs and migration scripts.
"""

import csv
import json
import os
import yaml

REPO_ROOT = "/Users/vivekbolla/Desktop/RM/rm-allocation-serverless-funcs"
MIGRATION_DIR = os.path.join(REPO_ROOT, "QPDynamicAllocation/migrationScripts")
CFN_DIR = os.path.join(REPO_ROOT, "cloudformation")

SERVICES_METADATA = {
    "rm-additive-process": {
        "memory": 4096,
        "timeout": 900,
        "ephemeral_storage": 2048,
        "dd_service": "rm-additive-process",
        "description": "Additive process handler for RM additive model",
    },
    "rm-additive-prefetch": {
        "memory": 2048,
        "timeout": 900,
        "ephemeral_storage": 2048,
        "dd_service": "rm-additive-process",
        "description": "Additive prefetch handler for RM additive model",
    },
    "rm-allocation-fetch-inputs": {
        "memory": 8192,
        "timeout": 900,
        "ephemeral_storage": 1600,
        "dd_service": "rm-fetch-inputs",
        "description": "Fetch inputs service for RM dynamic allocation",
    },
    "rm-allocation-prefetch-inputs": {
        "memory": 2048,
        "timeout": 900,
        "ephemeral_storage": 4096,
        "dd_service": "rm-prefetch-inputs",
        "description": "Prefetch inputs service for RM dynamic allocation",
    },
    "rm-allocation-process-inputs": {
        "memory": 8192,
        "timeout": 900,
        "ephemeral_storage": 1600,
        "dd_service": "rm-process-inputs",
        "description": "Process inputs service for RM dynamic allocation",
    },
    "rm-allocation-scheduled-jobs": {
        "memory": 4096,
        "timeout": 900,
        "ephemeral_storage": 4096,
        "dd_service": "rm-scheduled-jobs",
        "description": "Scheduled jobs orchestrator for RM dynamic allocation",
    },
    "rm-allocation-update-rbd": {
        "memory": 2048,
        "timeout": 900,
        "ephemeral_storage": 4096,
        "dd_service": "rm-update-rbd",
        "description": "RBD update processor for RM dynamic allocation",
    },
    "rm-allocation-update-rbd-ftp": {
        "memory": 2048,
        "timeout": 900,
        "ephemeral_storage": 4096,
        "dd_service": "rm-update-rbd-ftp",
        "description": "RBD FTP update processor for RM dynamic allocation",
    },
    "rm-pricing-triggers": {
        "memory": 4096,
        "timeout": 900,
        "ephemeral_storage": 4096,
        "dd_service": "rm-pricing-tiggers",
        "description": "Sector monitor and pricing triggers handler",
    },
}

ENVIRONMENTS_BASE = {
    "uat": {
        "account_id": "891377165721",
        "vpc": {
            "vpc_id": "vpc-01452dce7b5b446e1",
            "subnets": [
                "subnet-053e2b8d03017c388",
                "subnet-0a05a901a38cd5672",
            ],
            "security_groups": [
                "sg-087f8184f725ec3fe",
            ],
        },
        "secrets": {
            "dd_api_key_ssm": "/copilot/qp/uat/secrets/DD_API_KEY",
        },
        "execution_role_arn": "arn:aws:iam::891377165721:role/lambda-execution-role",
    },
}


def parse_payload(inp_str):
    if not inp_str:
        return None
    try:
        val = json.loads(inp_str)
        if isinstance(val, dict) and "body" in val and isinstance(val["body"], str):
            try:
                val["body"] = json.loads(val["body"])
            except Exception:
                pass
        return val
    except Exception:
        return inp_str


def get_canonical_service(name):
    s = name
    for prefix in [
        "uat-rm-lambda-",
        "uat-rm-allocation-serverless-funcs-",
        "uat-"
    ]:
        if s.startswith(prefix):
            s = s[len(prefix):]
            break
    return s


def build_config():
    config = {
        "version": "1.0",
        "region": "ap-south-1",
        "services": SERVICES_METADATA,
        "environments": {},
    }

    for env, base in ENVIRONMENTS_BASE.items():
        env_dict = {
            "account_id": base["account_id"],
            "vpc": base["vpc"],
            "secrets": base["secrets"],
            "execution_role_arn": base["execution_role_arn"],
            "services": {s: {} for s in SERVICES_METADATA},
        }

        csv_file = os.path.join(MIGRATION_DIR, f"{env}_lambda_service_inventory.csv")
        rows = []
        if os.path.exists(csv_file):
            with open(csv_file, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

        for r in rows:
            svc = get_canonical_service(r["service"])
            stype = r["service_type"]
            cfg = json.loads(r["configuration"]) if r.get("configuration") else {}
            svc_dict = env_dict["services"].get(svc)
            if svc_dict is None:
                continue

            if stype == "sqs":
                sc = cfg.get("ScalingConfig") or {}
                sqs_info = {
                    "queue_name": r["resource_name"],
                    "batch_size": cfg.get("BatchSize", 10),
                    "enabled": r["enabled"].lower() == "true",
                }
                if sc.get("MaximumConcurrency"):
                    sqs_info["max_concurrency"] = sc["MaximumConcurrency"]
                if cfg.get("MaximumBatchingWindowInSeconds"):
                    sqs_info["batching_window_seconds"] = cfg["MaximumBatchingWindowInSeconds"]
                svc_dict["sqs"] = sqs_info

            elif stype == "eventbridge_rule":
                target = cfg.get("Target") or {}
                rule_info = {
                    "name": r["resource_name"],
                    "schedule": cfg.get("ScheduleExpression"),
                    "enabled": r["enabled"].lower() == "true",
                }
                if cfg.get("Description"):
                    desc = cfg["Description"].strip()
                    if desc:
                        rule_info["description"] = desc
                inp = parse_payload(target.get("Input"))
                if inp is not None:
                    rule_info["input"] = inp
                svc_dict.setdefault("rules", []).append(rule_info)

            elif stype == "scheduler":
                target = cfg.get("Target") or {}
                sched_info = {
                    "name": r["resource_name"],
                    "schedule": cfg.get("ScheduleExpression"),
                    "timezone": cfg.get("ScheduleExpressionTimezone", "Asia/Calcutta"),
                    "enabled": r["enabled"].lower() == "true",
                }
                if cfg.get("Description"):
                    desc = cfg["Description"].strip()
                    if desc:
                        sched_info["description"] = desc
                inp = parse_payload(target.get("Input"))
                if inp is not None:
                    sched_info["input"] = inp
                svc_dict.setdefault("schedules", []).append(sched_info)

        # Clean empty service maps if no triggers
        cleaned_services = {}
        for s, val in env_dict["services"].items():
            if val:
                cleaned_services[s] = val
            else:
                cleaned_services[s] = {"status": "no_triggers"}
        env_dict["services"] = cleaned_services

        config["environments"][env] = env_dict

    return config


def main():
    config = build_config()

    yaml_path = os.path.join(CFN_DIR, "environments/env_config.yaml")
    with open(yaml_path, "w", encoding="utf-8") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False, width=120)
    print(f"Wrote standard environment config to: {yaml_path}")

    # Generate standard CloudFormation parameter JSON files for UAT
    for env in ["uat"]:
        env_data = config["environments"][env]
        params = [
            {"ParameterKey": "Environment", "ParameterValue": env},
            {"ParameterKey": "VpcId", "ParameterValue": env_data["vpc"]["vpc_id"]},
            {"ParameterKey": "SubnetIds", "ParameterValue": ",".join(env_data["vpc"]["subnets"])},
            {"ParameterKey": "SecurityGroupIds", "ParameterValue": ",".join(env_data["vpc"]["security_groups"])},
            {"ParameterKey": "DatadogApiKeySsmPath", "ParameterValue": env_data["secrets"]["dd_api_key_ssm"]},
            {"ParameterKey": "ExistingExecutionRoleArn", "ParameterValue": env_data["execution_role_arn"]},
            {"ParameterKey": "ImageTag", "ParameterValue": "latest"},
        ]
        param_file = os.path.join(CFN_DIR, f"environments/parameters-{env}.json")
        with open(param_file, "w", encoding="utf-8") as f:
            json.dump(params, f, indent=2)
        print(f"Wrote CloudFormation parameters to: {param_file}")


if __name__ == "__main__":
    main()
