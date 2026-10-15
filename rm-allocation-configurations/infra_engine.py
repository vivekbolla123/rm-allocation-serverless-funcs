#!/usr/bin/env python3
"""
Infrastructure-as-Code Engine for RM Serverless Functions.
Provides declarative infrastructure lifecycle management (plan, apply, status, export-cfn, destroy)
per Lambda service using AWS Boto3 and CloudFormation synthesis.
"""

import argparse
import json
import os
import sys
import time
import boto3
import yaml
from botocore.exceptions import ClientError


# Terminal color formatting helpers
class Style:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    RED = "\033[31m"
    CYAN = "\033[36m"
    MAGENTA = "\033[35m"
    GRAY = "\033[90m"


def print_header(title):
    print(f"\n{Style.BOLD}{Style.CYAN}{'=' * 80}{Style.RESET}")
    print(f"{Style.BOLD}{Style.CYAN}{title.center(80)}{Style.RESET}")
    print(f"{Style.BOLD}{Style.CYAN}{'=' * 80}{Style.RESET}\n")


def load_config(service_dir):
    config_path = os.path.join(service_dir, "config.yaml")
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Infrastructure config file not found: {config_path}")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_base_env_vars(service_name, env, lambda_cfg):
    """
    Constructs the standard Datadog and environment variables for the function.
    """
    env_vars = {
        "env": env,
        "DD_LAMBDA_HANDLER": lambda_cfg.get("dd_handler", "lambda_function.lambda_handler"),
        "DD_SITE": "datadoghq.com",
        "DD_ENV": env,
        "DD_SERVICE": lambda_cfg.get("dd_service", service_name),
        "DD_VERSION": lambda_cfg.get("dd_version", "latest"),
        "DD_TRACE_ENABLED": "true",
        "DD_DATA_STREAMS_ENABLED": "true",
        "DD_SERVERLESS_LOGS_ENABLED": "true",
        "DD_LOGS_INJECTION": "true",
        "DD_COLD_START_TRACING": "false",
        "DD_TAGS": f"env:{env},service:{service_name},Department:RM,team:RM",
    }
    if "env_vars" in lambda_cfg and isinstance(lambda_cfg["env_vars"], dict):
        for k, v in lambda_cfg["env_vars"].items():
            env_vars[k] = str(v)
    return env_vars


def synthesize_cfn_template(service_name, env, env_config, global_config):
    """
    Synthesizes a standalone, production-ready CloudFormation template
    for this specific service and environment from config.yaml.
    """
    lambda_cfg = env_config.get("lambda", {})
    vpc_cfg = env_config.get("vpc", {})
    triggers_cfg = env_config.get("triggers", {})

    func_name = lambda_cfg.get("name", f"{env}-rm-lambda-{service_name}")
    defaults = global_config.get("defaults", {})
    mem_size = lambda_cfg.get("memory", defaults.get("memory", 2048))
    timeout = lambda_cfg.get("timeout", defaults.get("timeout", 900))
    ephemeral = lambda_cfg.get("ephemeral_storage", defaults.get("ephemeral_storage", 2048))
    image_uri = lambda_cfg.get("image_uri", "")
    existing_role = lambda_cfg.get("role_arn") or env_config.get("execution_role_arn", "")

    env_vars = build_base_env_vars(service_name, env, lambda_cfg)

    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": f"Dedicated infrastructure stack for {func_name}",
        "Parameters": {},
        "Resources": {},
        "Outputs": {
            "FunctionName": {
                "Description": "Lambda Function Name",
                "Value": func_name,
            },
            "FunctionArn": {
                "Description": "Lambda Function ARN",
                "Value": {"Fn::GetAtt": ["LambdaFunction", "Arn"]},
            },
        },
    }

    # 1. IAM Role (if not supplied)
    if not existing_role:
        template["Resources"]["LambdaExecutionRole"] = {
            "Type": "AWS::IAM::Role",
            "Properties": {
                "RoleName": f"{func_name}-exec-role",
                "AssumeRolePolicyDocument": {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": ["lambda.amazonaws.com"]},
                            "Action": ["sts:AssumeRole"],
                        }
                    ],
                },
                "ManagedPolicyArns": [
                    "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
                    "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole",
                ],
            },
        }
        role_ref = {"Fn::GetAtt": ["LambdaExecutionRole", "Arn"]}
    else:
        role_ref = existing_role

    # 2. CloudWatch Log Group
    template["Resources"]["LogGroup"] = {
        "Type": "AWS::Logs::LogGroup",
        "Properties": {
            "LogGroupName": f"/aws/lambda/{func_name}",
            "RetentionInDays": lambda_cfg.get("log_retention_days", 30),
        },
    }

    # 3. Lambda Function Resource
    lambda_properties = {
        "FunctionName": func_name,
        "Description": global_config.get("description", f"RM Service {service_name}"),
        "PackageType": "Image",
        "Code": {"ImageUri": image_uri},
        "Role": role_ref,
        "Timeout": timeout,
        "MemorySize": mem_size,
        "EphemeralStorage": {"Size": ephemeral},
        "Architectures": ["x86_64"],
        "Environment": {"Variables": env_vars},
        "Tags": [
            {"Key": "Environment", "Value": env},
            {"Key": "Service", "Value": service_name},
            {"Key": "ManagedBy", "Value": "InfraAsCode"},
            {"Key": "Department", "Value": "RM"},
        ],
    }

    if vpc_cfg.get("subnets") and vpc_cfg.get("security_groups"):
        lambda_properties["VpcConfig"] = {
            "SubnetIds": vpc_cfg["subnets"],
            "SecurityGroupIds": vpc_cfg["security_groups"],
        }

    template["Resources"]["LambdaFunction"] = {
        "Type": "AWS::Lambda::Function",
        "DependsOn": ["LogGroup"],
        "Properties": lambda_properties,
    }

    # 4. SQS Event Source Mapping
    if "sqs" in triggers_cfg and triggers_cfg["sqs"].get("enabled", True):
        sqs_cfg = triggers_cfg["sqs"]
        q_arn = sqs_cfg.get("queue_arn")
        if not q_arn and sqs_cfg.get("queue_name"):
            account_id = env_config.get("account_id", "")
            region = env_config.get("region", "ap-south-1")
            q_arn = f"arn:aws:sqs:{region}:{account_id}:{sqs_cfg['queue_name']}"

        esm_props = {
            "FunctionName": {"Fn::GetAtt": ["LambdaFunction", "Arn"]},
            "EventSourceArn": q_arn,
            "BatchSize": sqs_cfg.get("batch_size", 10),
            "Enabled": sqs_cfg.get("enabled", True),
        }
        if sqs_cfg.get("batching_window_seconds"):
            esm_props["MaximumBatchingWindowInSeconds"] = sqs_cfg["batching_window_seconds"]
        if sqs_cfg.get("max_concurrency"):
            esm_props["ScalingConfig"] = {
                "MaximumConcurrency": sqs_cfg["max_concurrency"]
            }

        template["Resources"]["SqsEventSourceMapping"] = {
            "Type": "AWS::Lambda::EventSourceMapping",
            "Properties": esm_props,
        }

    # 5. EventBridge Rules
    rules_list = triggers_cfg.get("rules", [])
    for idx, rule in enumerate(rules_list):
        r_name = rule["name"]
        res_id = f"EventBridgeRule{idx + 1}"
        perm_id = f"LambdaPermissionRule{idx + 1}"

        target = {
            "Arn": {"Fn::GetAtt": ["LambdaFunction", "Arn"]},
            "Id": f"{r_name}-target",
        }
        if "input" in rule:
            inp = rule["input"]
            target["Input"] = json.dumps(inp) if isinstance(inp, dict) else str(inp)

        rule_props = {
            "Name": r_name,
            "State": "ENABLED" if rule.get("enabled", True) else "DISABLED",
            "Targets": [target],
        }
        if rule.get("schedule"):
            rule_props["ScheduleExpression"] = rule["schedule"]
        if rule.get("description"):
            rule_props["Description"] = rule["description"]

        template["Resources"][res_id] = {
            "Type": "AWS::Events::Rule",
            "Properties": rule_props,
        }

        template["Resources"][perm_id] = {
            "Type": "AWS::Lambda::Permission",
            "Properties": {
                "FunctionName": {"Fn::GetAtt": ["LambdaFunction", "Arn"]},
                "Action": "lambda:InvokeFunction",
                "Principal": "events.amazonaws.com",
                "SourceArn": {"Fn::GetAtt": [res_id, "Arn"]},
            },
        }

    # 6. EventBridge Scheduler Schedules
    schedules_list = triggers_cfg.get("schedules", [])
    if schedules_list:
        template["Resources"]["SchedulerRole"] = {
            "Type": "AWS::IAM::Role",
            "Properties": {
                "RoleName": f"{func_name}-scheduler-role",
                "AssumeRolePolicyDocument": {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": ["scheduler.amazonaws.com"]},
                            "Action": ["sts:AssumeRole"],
                        }
                    ],
                },
                "Policies": [
                    {
                        "PolicyName": "InvokeLambda",
                        "PolicyDocument": {
                            "Version": "2012-10-17",
                            "Statement": [
                                {
                                    "Effect": "Allow",
                                    "Action": ["lambda:InvokeFunction"],
                                    "Resource": {"Fn::GetAtt": ["LambdaFunction", "Arn"]},
                                }
                            ],
                        },
                    }
                ],
            },
        }

        for idx, sched in enumerate(schedules_list):
            s_name = sched["name"]
            res_id = f"SchedulerSchedule{idx + 1}"
            target_obj = {
                "Arn": {"Fn::GetAtt": ["LambdaFunction", "Arn"]},
                "RoleArn": {"Fn::GetAtt": ["SchedulerRole", "Arn"]},
            }
            if "input" in sched:
                inp = sched["input"]
                target_obj["Input"] = json.dumps(inp) if isinstance(inp, dict) else str(inp)

            sched_props = {
                "Name": s_name,
                "GroupName": sched.get("group_name", "default"),
                "State": "ENABLED" if sched.get("enabled", True) else "DISABLED",
                "ScheduleExpression": sched["schedule"],
                "ScheduleExpressionTimezone": sched.get("timezone", "Asia/Calcutta"),
                "FlexibleTimeWindow": {"Mode": "OFF"},
                "Target": target_obj,
            }
            if sched.get("description"):
                sched_props["Description"] = sched["description"]

            template["Resources"][res_id] = {
                "Type": "AWS::Scheduler::Schedule",
                "Properties": sched_props,
            }

    return template


class InfraManager:
    def __init__(self, service_dir, env, region=None):
        self.service_dir = service_dir
        self.env = env.lower()
        self.config = load_config(service_dir)
        self.service_name = self.config["service_name"]

        if self.env not in self.config.get("environments", {}):
            raise ValueError(f"Environment '{self.env}' not defined in config.yaml")

        self.env_config = self.config["environments"][self.env]
        self.region = region or self.env_config.get("region", "ap-south-1")
        self.stack_name = f"{self.env}-rm-infra-{self.service_name}"
        self.func_name = self.env_config.get("lambda", {}).get(
            "name", f"{self.env}-rm-lambda-{self.service_name}"
        )
        self.account_id = self.env_config.get("account_id", "")

        self.session = boto3.Session(region_name=self.region)
        self.cfn = self.session.client("cloudformation")
        self.lam = self.session.client("lambda")
        self.events = self.session.client("events")
        self.scheduler = self.session.client("scheduler")
        self.sqs = self.session.client("sqs")
        self.ecr = self.session.client("ecr")

    def _check_ecr_image_exists(self, image_uri):
        if not image_uri:
            return False
        try:
            parts = image_uri.split("/")
            if len(parts) < 2:
                return True
            repo_and_tag = parts[1]
            if ":" in repo_and_tag:
                repo_name, tag = repo_and_tag.split(":", 1)
            else:
                repo_name = repo_and_tag
                tag = "latest"

            res = self.ecr.describe_images(
                repositoryName=repo_name,
                imageIds=[{"imageTag": tag}],
            )
            return len(res.get("imageDetails", [])) > 0
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ["RepositoryNotFoundException", "ImageNotFoundException"]:
                return False
            return True
        except Exception:
            return True

    def get_live_function_config(self):
        try:
            return self.lam.get_function_configuration(FunctionName=self.func_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                return None
            raise

    def export_cfn(self):
        template = synthesize_cfn_template(
            self.service_name, self.env, self.env_config, self.config
        )
        out_path = os.path.join(self.service_dir, f"template_{self.env}.yaml")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(f"# Synthesized CloudFormation template for {self.func_name}\n")
            yaml.dump(template, f, default_flow_style=False, sort_keys=False)
        print(f"{Style.GREEN}>> Exported CloudFormation template to: {out_path}{Style.RESET}")

    def plan(self):
        print_header(f"INFRASTRUCTURE PLAN: {self.service_name.upper()} ({self.env.upper()})")

        target_lambda = self.env_config.get("lambda", {})
        defaults = self.config.get("defaults", {})
        live_fn = self.get_live_function_config()
        has_pending_changes = False

        print(f"{Style.BOLD}Target Function:{Style.RESET} {self.func_name}")
        print(f"{Style.BOLD}Target Stack:{Style.RESET}    {self.stack_name}\n")

        print(f"{Style.BOLD}--- Compute Sizing Comparison ---{Style.RESET}")
        tgt_mem = target_lambda.get("memory", defaults.get("memory", 2048))
        tgt_to = target_lambda.get("timeout", defaults.get("timeout", 900))
        tgt_eph = target_lambda.get("ephemeral_storage", defaults.get("ephemeral_storage", 2048))

        if live_fn is None:
            has_pending_changes = True
            print(f"  {Style.GREEN}+ Lambda function does NOT exist yet. Will be created from scratch.{Style.RESET}")
            print(f"    - MemorySize:       {tgt_mem} MB")
            print(f"    - Timeout:          {tgt_to}s")
            print(f"    - EphemeralStorage: {tgt_eph} MB")
            print(f"    - ImageUri:         {target_lambda.get('image_uri', '')}")
        else:
            cur_mem = live_fn.get("MemorySize")
            if cur_mem != tgt_mem:
                has_pending_changes = True
            mem_chg = f"{cur_mem} MB -> {tgt_mem} MB" if cur_mem != tgt_mem else f"{cur_mem} MB (unchanged)"
            mem_color = Style.YELLOW if cur_mem != tgt_mem else Style.GRAY

            cur_to = live_fn.get("Timeout")
            if cur_to != tgt_to:
                has_pending_changes = True
            to_chg = f"{cur_to}s -> {tgt_to}s" if cur_to != tgt_to else f"{cur_to}s (unchanged)"
            to_color = Style.YELLOW if cur_to != tgt_to else Style.GRAY

            cur_eph = live_fn.get("EphemeralStorage", {}).get("Size", 512)
            if cur_eph != tgt_eph:
                has_pending_changes = True
            eph_chg = f"{cur_eph} MB -> {tgt_eph} MB" if cur_eph != tgt_eph else f"{cur_eph} MB (unchanged)"
            eph_color = Style.YELLOW if cur_eph != tgt_eph else Style.GRAY

            print(f"    Memory:    {mem_color}{mem_chg}{Style.RESET}")
            print(f"    Timeout:   {to_color}{to_chg}{Style.RESET}")
            print(f"    Ephemeral: {eph_color}{eph_chg}{Style.RESET}")

        # Environment variables comparison
        tgt_env_vars = build_base_env_vars(self.service_name, self.env, target_lambda)
        if live_fn:
            live_env_vars = live_fn.get("Environment", {}).get("Variables", {})
            env_added = {k: v for k, v in tgt_env_vars.items() if k not in live_env_vars}
            env_modified = {k: (live_env_vars[k], tgt_env_vars[k]) for k in tgt_env_vars if k in live_env_vars and live_env_vars[k] != tgt_env_vars[k]}
            print(f"\n{Style.BOLD}--- Environment Variables Diff ---{Style.RESET}")
            if not env_added and not env_modified:
                print(f"    {Style.GRAY}(All {len(tgt_env_vars)} variables match live configuration){Style.RESET}")
            else:
                has_pending_changes = True
                for k, v in env_added.items():
                    print(f"    {Style.GREEN}+ [NEW] {k} = {v}{Style.RESET}")
                for k, (old_v, new_v) in env_modified.items():
                    print(f"    {Style.YELLOW}~ [MODIFIED] {k}: {old_v} -> {new_v}{Style.RESET}")

        # Triggers comparison against live AWS resources
        triggers = self.env_config.get("triggers", {})
        print(f"\n{Style.BOLD}--- Triggers Comparison (Live AWS vs config.yaml) ---{Style.RESET}")

        # 1. SQS comparison
        if "sqs" in triggers:
            sqs = triggers["sqs"]
            q_name = sqs.get("queue_name")
            q_arn = sqs.get("queue_arn") or f"arn:aws:sqs:{self.region}:{self.account_id}:{q_name}"
            tgt_state = "ENABLED" if sqs.get("enabled", True) else "DISABLED"
            tgt_batch = sqs.get("batch_size", 10)

            esms = self.lam.list_event_source_mappings(FunctionName=self.func_name).get("EventSourceMappings", []) if live_fn else []
            matched_esm = next((esm for esm in esms if esm.get("EventSourceArn") == q_arn), None)

            if matched_esm is None:
                if sqs.get("enabled", True):
                    has_pending_changes = True
                    print(f"  {Style.CYAN}[SQS Queue]{Style.RESET} {Style.GREEN}+ [NEW] {q_name} (Batch: {tgt_batch}, State: {tgt_state}){Style.RESET}")
                else:
                    print(f"  {Style.CYAN}[SQS Queue]{Style.RESET} {Style.GRAY}✓ {q_name} (Not attached in AWS & disabled in config){Style.RESET}")
            else:
                live_state = "ENABLED" if "Enabled" in matched_esm.get("State", "") else "DISABLED"
                live_batch = matched_esm.get("BatchSize")
                if live_state == tgt_state and live_batch == tgt_batch:
                    print(f"  {Style.CYAN}[SQS Queue]{Style.RESET} {Style.GRAY}✓ {q_name} (Batch: {tgt_batch}, State: {tgt_state}) [in sync]{Style.RESET}")
                else:
                    has_pending_changes = True
                    print(f"  {Style.CYAN}[SQS Queue]{Style.RESET} {Style.YELLOW}~ [MODIFIED] {q_name}: Batch {live_batch}->{tgt_batch}, State {live_state}->{tgt_state}{Style.RESET}")

        # 2. EventBridge Rules comparison
        rules = triggers.get("rules", [])
        if rules:
            in_sync_count = 0
            rule_diffs = []
            for r in rules:
                r_name = r["name"]
                tgt_sched = r.get("schedule", "")
                tgt_st = "ENABLED" if r.get("enabled", True) else "DISABLED"
                try:
                    live_r = self.events.describe_rule(Name=r_name)
                    live_st = live_r.get("State")
                    live_sched = live_r.get("ScheduleExpression")
                    if live_st == tgt_st and live_sched == tgt_sched:
                        in_sync_count += 1
                    else:
                        rule_diffs.append((r_name, f"~ [MODIFIED] state {live_st}->{tgt_st}, sched {live_sched}->{tgt_sched}"))
                except ClientError as e:
                    if e.response["Error"]["Code"] == "ResourceNotFoundException":
                        rule_diffs.append((r_name, f"+ [NEW] schedule: {tgt_sched} [{tgt_st}]"))
                    else:
                        rule_diffs.append((r_name, f"? [ERROR] {e}"))

            if not rule_diffs:
                print(f"  {Style.CYAN}[EventBridge Rules ({len(rules)})]{Style.RESET} {Style.GRAY}✓ All {len(rules)} rules are in sync with AWS{Style.RESET}")
            else:
                has_pending_changes = True
                print(f"  {Style.CYAN}[EventBridge Rules ({len(rules)})]{Style.RESET} ({in_sync_count} in sync, {len(rule_diffs)} pending changes):")
                for r_name, diff_desc in rule_diffs:
                    color = Style.GREEN if "+ [NEW]" in diff_desc else Style.YELLOW
                    print(f"    - {r_name}: {color}{diff_desc}{Style.RESET}")

        # 3. EventBridge Scheduler Schedules comparison
        schedules = triggers.get("schedules", [])
        if schedules:
            in_sync_scheds = 0
            sched_diffs = []
            for s in schedules:
                s_name = s["name"]
                tgt_sched = s.get("schedule", "")
                tgt_tz = s.get("timezone", "Asia/Calcutta")
                tgt_st = "ENABLED" if s.get("enabled", True) else "DISABLED"
                try:
                    live_s = self.scheduler.get_schedule(Name=s_name)
                    live_st = live_s.get("State")
                    live_sched = live_s.get("ScheduleExpression")
                    live_tz = live_s.get("ScheduleExpressionTimezone", "Asia/Calcutta")
                    if live_st == tgt_st and live_sched == tgt_sched and live_tz == tgt_tz:
                        in_sync_scheds += 1
                    else:
                        sched_diffs.append((s_name, f"~ [MODIFIED] state {live_st}->{tgt_st}, sched {live_sched}->{tgt_sched}"))
                except ClientError as e:
                    if e.response["Error"]["Code"] == "ResourceNotFoundException":
                        sched_diffs.append((s_name, f"+ [NEW] schedule: {tgt_sched} [{tgt_st}]"))
                    else:
                        sched_diffs.append((s_name, f"? [ERROR] {e}"))

            if not sched_diffs:
                print(f"  {Style.CYAN}[Scheduler Jobs ({len(schedules)})]{Style.RESET} {Style.GRAY}✓ All {len(schedules)} schedules are in sync with AWS{Style.RESET}")
            else:
                has_pending_changes = True
                print(f"  {Style.CYAN}[Scheduler Jobs ({len(schedules)})]{Style.RESET} ({in_sync_scheds} in sync, {len(sched_diffs)} pending changes):")
                for s_name, diff_desc in sched_diffs:
                    color = Style.GREEN if "+ [NEW]" in diff_desc else Style.YELLOW
                    print(f"    - {s_name}: {color}{diff_desc}{Style.RESET}")

        if not triggers.get("sqs") and not rules and not schedules:
            print(f"  {Style.GRAY}(No automated triggers declared for this service in {self.env.upper()}){Style.RESET}")

        # Final Summary Banner
        if has_pending_changes:
            print(f"\n{Style.BOLD}{Style.YELLOW}{'=' * 80}{Style.RESET}")
            print(f"{Style.BOLD}{Style.YELLOW}⚠  PENDING CHANGES DETECTED. Run with --apply to deploy updates to AWS.{Style.RESET}")
            print(f"{Style.BOLD}{Style.YELLOW}{'=' * 80}{Style.RESET}")
        else:
            print(f"\n{Style.BOLD}{Style.GREEN}{'=' * 80}{Style.RESET}")
            print(f"{Style.BOLD}{Style.GREEN}✓  INFRASTRUCTURE IS UP-TO-DATE! Zero changes needed.{Style.RESET}")
            print(f"{Style.BOLD}{Style.GREEN}{'=' * 80}{Style.RESET}")

    def apply(self, mode="direct"):
        """
        Applies changes to AWS.
        mode="direct": Directly updates AWS resources via Boto3 (fast, idempotent, safe).
        mode="cfn": Creates or updates via CloudFormation stack.
        """
        print_header(f"APPLYING INFRASTRUCTURE: {self.service_name.upper()} ({self.env.upper()})")

        # Always export the up-to-date CFN template
        self.export_cfn()

        if mode == "cfn":
            self._apply_cfn()
        else:
            self._apply_direct()

    def _apply_direct(self):
        """Direct reconciliation via AWS Boto3 API."""
        start_time = time.time()
        target_lambda = self.env_config.get("lambda", {})
        defaults = self.config.get("defaults", {})
        tgt_mem = target_lambda.get("memory", defaults.get("memory", 2048))
        tgt_to = target_lambda.get("timeout", defaults.get("timeout", 900))
        tgt_eph = target_lambda.get("ephemeral_storage", defaults.get("ephemeral_storage", 2048))
        tgt_env_vars = build_base_env_vars(self.service_name, self.env, target_lambda)

        live_fn = self.get_live_function_config()

        if live_fn is None:
            image_uri = target_lambda.get("image_uri", "")
            if not self._check_ecr_image_exists(image_uri):
                print(f"{Style.YELLOW}>> Skipping creation of '{self.func_name}': Docker image '{image_uri}' is not yet pushed to ECR.{Style.RESET}")
                print(f"{Style.CYAN}   To create this Lambda in AWS, build and push its container image first:{Style.RESET}")
                print(f"   cd {self.service_name} && ./deploy_{self.env}.sh\n")
                return

            print(f"{Style.YELLOW}>> Function '{self.func_name}' does not exist in AWS. Creating...{Style.RESET}")
            role_arn = target_lambda.get("role_arn") or self.env_config.get("execution_role_arn")
            if not role_arn:
                raise ValueError("execution_role_arn or lambda.role_arn is required to create a new Lambda function.")

            create_kwargs = {
                "FunctionName": self.func_name,
                "PackageType": "Image",
                "Code": {"ImageUri": image_uri},
                "Role": role_arn,
                "Timeout": tgt_to,
                "MemorySize": tgt_mem,
                "EphemeralStorage": {"Size": tgt_eph},
                "Environment": {"Variables": tgt_env_vars},
                "Description": self.config.get("description", f"RM Service {self.service_name}"),
            }
            vpc_cfg = self.env_config.get("vpc", {})
            if vpc_cfg.get("subnets") and vpc_cfg.get("security_groups"):
                create_kwargs["VpcConfig"] = {
                    "SubnetIds": vpc_cfg["subnets"],
                    "SecurityGroupIds": vpc_cfg["security_groups"],
                }

            try:
                self.lam.create_function(**create_kwargs)
                self._wait_for_lambda_active()
                print(f"{Style.GREEN}>> Lambda function created successfully.{Style.RESET}")
            except ClientError as e:
                print(f"{Style.RED}>> Failed to create function '{self.func_name}': {e}{Style.RESET}\n")
                return
        else:
            # Check diff and update
            cur_mem = live_fn.get("MemorySize")
            cur_to = live_fn.get("Timeout")
            cur_eph = live_fn.get("EphemeralStorage", {}).get("Size", 512)
            cur_env_vars = live_fn.get("Environment", {}).get("Variables", {})

            needs_config_update = (
                cur_mem != tgt_mem
                or cur_to != tgt_to
                or cur_eph != tgt_eph
                or cur_env_vars != tgt_env_vars
            )

            if needs_config_update:
                print(f"{Style.YELLOW}>> Updating Lambda configuration (Memory: {tgt_mem}MB, Timeout: {tgt_to}s, Ephemeral: {tgt_eph}MB)...{Style.RESET}")
                update_kwargs = {
                    "FunctionName": self.func_name,
                    "MemorySize": tgt_mem,
                    "Timeout": tgt_to,
                    "EphemeralStorage": {"Size": tgt_eph},
                    "Environment": {"Variables": tgt_env_vars},
                    "Description": self.config.get("description", f"RM Service {self.service_name}"),
                }
                vpc_cfg = self.env_config.get("vpc", {})
                if vpc_cfg.get("subnets") and vpc_cfg.get("security_groups"):
                    update_kwargs["VpcConfig"] = {
                        "SubnetIds": vpc_cfg["subnets"],
                        "SecurityGroupIds": vpc_cfg["security_groups"],
                    }

                self.lam.update_function_configuration(**update_kwargs)
                self._wait_for_lambda_active()
                print(f"{Style.GREEN}>> Lambda configuration updated successfully.{Style.RESET}")
            else:
                print(f"{Style.GRAY}>> Lambda sizing and environment variables are already up-to-date.{Style.RESET}")

        # Synchronize triggers
        live_fn = self.get_live_function_config()
        fn_arn = live_fn["FunctionArn"]
        self._sync_triggers(fn_arn)

        duration = round(time.time() - start_time, 1)
        print(f"\n{Style.BOLD}{Style.GREEN}SUCCESS:{Style.RESET} All infrastructure reconciled in {duration}s!")

    def _sync_triggers(self, fn_arn):
        triggers = self.env_config.get("triggers", {})

        # 1. SQS Event Source Mapping
        if "sqs" in triggers:
            sqs_cfg = triggers["sqs"]
            q_name = sqs_cfg.get("queue_name")
            q_arn = sqs_cfg.get("queue_arn") or f"arn:aws:sqs:{self.region}:{self.account_id}:{q_name}"
            enabled = sqs_cfg.get("enabled", True)
            batch_size = sqs_cfg.get("batch_size", 10)

            print(f">> Reconciling SQS trigger for queue: {q_name}...")
            esms = self.lam.list_event_source_mappings(FunctionName=self.func_name).get("EventSourceMappings", [])
            matched_esm = None
            for esm in esms:
                if esm.get("EventSourceArn") == q_arn:
                    matched_esm = esm
                    break

            if matched_esm:
                uuid = matched_esm["UUID"]
                cur_state = matched_esm.get("State")
                cur_batch = matched_esm.get("BatchSize")
                needs_esm_update = (cur_batch != batch_size) or (enabled and "Enabled" not in cur_state)

                if needs_esm_update:
                    update_esm_kwargs = {
                        "UUID": uuid,
                        "BatchSize": batch_size,
                        "Enabled": enabled,
                    }
                    if sqs_cfg.get("batching_window_seconds"):
                        update_esm_kwargs["MaximumBatchingWindowInSeconds"] = sqs_cfg["batching_window_seconds"]
                    if sqs_cfg.get("max_concurrency"):
                        update_esm_kwargs["ScalingConfig"] = {"MaximumConcurrency": sqs_cfg["max_concurrency"]}
                    self.lam.update_event_source_mapping(**update_esm_kwargs)
                    print(f"{Style.GREEN}   SQS trigger updated (UUID: {uuid}).{Style.RESET}")
                else:
                    print(f"{Style.GRAY}   SQS trigger already up-to-date.{Style.RESET}")
            else:
                if enabled:
                    create_esm_kwargs = {
                        "EventSourceArn": q_arn,
                        "FunctionName": self.func_name,
                        "BatchSize": batch_size,
                        "Enabled": True,
                    }
                    if sqs_cfg.get("batching_window_seconds"):
                        create_esm_kwargs["MaximumBatchingWindowInSeconds"] = sqs_cfg["batching_window_seconds"]
                    if sqs_cfg.get("max_concurrency"):
                        create_esm_kwargs["ScalingConfig"] = {"MaximumConcurrency": sqs_cfg["max_concurrency"]}
                    try:
                        self.lam.create_event_source_mapping(**create_esm_kwargs)
                        print(f"{Style.GREEN}   SQS event source mapping created successfully.{Style.RESET}")
                    except ClientError as e:
                        print(f"{Style.YELLOW}   Could not create SQS mapping: {e}{Style.RESET}")

        # 2. EventBridge Rules
        rules = triggers.get("rules", [])
        if rules:
            print(f">> Reconciling {len(rules)} EventBridge rules...")
            for r in rules:
                r_name = r["name"]
                st = "ENABLED" if r.get("enabled", True) else "DISABLED"
                self.events.put_rule(
                    Name=r_name,
                    ScheduleExpression=r["schedule"],
                    State=st,
                    Description=r.get("description", f"Rule for {self.func_name}"),
                )
                target = {
                    "Id": f"{r_name}-target",
                    "Arn": fn_arn,
                }
                if "input" in r:
                    inp = r["input"]
                    target["Input"] = json.dumps(inp) if isinstance(inp, dict) else str(inp)
                self.events.put_targets(Rule=r_name, Targets=[target])

                # Grant permission
                try:
                    self.lam.add_permission(
                        FunctionName=self.func_name,
                        StatementId=f"{r_name}-invoke",
                        Action="lambda:InvokeFunction",
                        Principal="events.amazonaws.com",
                        SourceArn=f"arn:aws:events:{self.region}:{self.account_id}:rule/{r_name}",
                    )
                except ClientError as e:
                    if "ResourceConflictException" not in str(e):
                        pass

            print(f"{Style.GREEN}   EventBridge rules synchronized.{Style.RESET}")

        # 3. EventBridge Scheduler Schedules
        schedules = triggers.get("schedules", [])
        if schedules:
            print(f">> Reconciling {len(schedules)} Scheduler jobs...")
            for s in schedules:
                s_name = s["name"]
                st = "ENABLED" if s.get("enabled", True) else "DISABLED"
                sched_kwargs = {
                    "Name": s_name,
                    "GroupName": s.get("group_name", "default"),
                    "ScheduleExpression": s["schedule"],
                    "ScheduleExpressionTimezone": s.get("timezone", "Asia/Calcutta"),
                    "State": st,
                    "FlexibleTimeWindow": {"Mode": "OFF"},
                    "Target": {
                        "Arn": fn_arn,
                        "RoleArn": f"arn:aws:iam::{self.account_id}:role/{self.func_name}-scheduler-role",
                    },
                }
                if "input" in s:
                    inp = s["input"]
                    sched_kwargs["Target"]["Input"] = json.dumps(inp) if isinstance(inp, dict) else str(inp)

                try:
                    self.scheduler.update_schedule(**sched_kwargs)
                    print(f"{Style.GREEN}   Updated schedule: {s_name}{Style.RESET}")
                except ClientError as e:
                    if e.response["Error"]["Code"] == "ResourceNotFoundException":
                        try:
                            self.scheduler.create_schedule(**sched_kwargs)
                            print(f"{Style.GREEN}   Created schedule: {s_name}{Style.RESET}")
                        except Exception as ce:
                            print(f"{Style.YELLOW}   Could not create schedule {s_name}: {ce}{Style.RESET}")

    def _wait_for_lambda_active(self):
        for _ in range(60):
            res = self.lam.get_function_configuration(FunctionName=self.func_name)
            state = res.get("State", "Active")
            update_status = res.get("LastUpdateStatus", "Successful")
            if state in ["Active", "Failed"] and update_status in ["Successful", "Failed"]:
                return
            time.sleep(2)

    def _apply_cfn(self):
        """CloudFormation stack update."""
        template = synthesize_cfn_template(
            self.service_name, self.env, self.env_config, self.config
        )
        template_body = yaml.dump(template, default_flow_style=False)

        stack_exists = False
        try:
            self.cfn.describe_stacks(StackName=self.stack_name)
            stack_exists = True
        except ClientError as e:
            if "does not exist" not in str(e):
                raise

        start_time = time.time()
        if not stack_exists:
            print(f"{Style.YELLOW}>> Creating new stack '{self.stack_name}'...{Style.RESET}")
            self.cfn.create_stack(
                StackName=self.stack_name,
                TemplateBody=template_body,
                Capabilities=["CAPABILITY_NAMED_IAM"],
            )
            self._wait_for_stack("CREATE_COMPLETE")
        else:
            print(f"{Style.YELLOW}>> Updating stack '{self.stack_name}' via ChangeSet...{Style.RESET}")
            cs_name = f"cs-{int(time.time())}"
            try:
                self.cfn.create_change_set(
                    StackName=self.stack_name,
                    TemplateBody=template_body,
                    ChangeSetName=cs_name,
                    Capabilities=["CAPABILITY_NAMED_IAM"],
                )
                self._wait_for_changeset(cs_name)
                self.cfn.execute_change_set(
                    StackName=self.stack_name,
                    ChangeSetName=cs_name,
                )
                self._wait_for_stack("UPDATE_COMPLETE")
            except ClientError as e:
                if "didn't contain changes" in str(e) or "No updates are to be performed" in str(e):
                    print(f"{Style.GREEN}>> Up-to-date! No CloudFormation changes detected.{Style.RESET}")
                    return
                raise

        duration = round(time.time() - start_time, 1)
        print(f"\n{Style.BOLD}{Style.GREEN}SUCCESS:{Style.RESET} Stack updated in {duration}s!")

    def _wait_for_changeset(self, cs_name):
        while True:
            res = self.cfn.describe_change_set(
                StackName=self.stack_name, ChangeSetName=cs_name
            )
            status = res["Status"]
            if status == "CREATE_COMPLETE":
                return
            if status == "FAILED":
                reason = res.get("StatusReason", "")
                if "didn't contain changes" in reason:
                    raise ClientError(
                        {"Error": {"Message": "No updates are to be performed"}},
                        "CreateChangeSet",
                    )
                raise RuntimeError(f"ChangeSet failed: {reason}")
            time.sleep(2)

    def _wait_for_stack(self, target_status):
        print(f"Waiting for stack to reach {target_status}...")
        while True:
            try:
                res = self.cfn.describe_stacks(StackName=self.stack_name)
                cur = res["Stacks"][0]["StackStatus"]
                print(f"  Current Status: {cur}")
                if cur == target_status:
                    return
                if "FAILED" in cur or "ROLLBACK" in cur:
                    raise RuntimeError(f"Stack operation ended in error state: {cur}")
            except ClientError as e:
                if target_status == "DELETE_COMPLETE" and "does not exist" in str(e):
                    return
                raise
            time.sleep(4)

    def status(self):
        print_header(f"INFRASTRUCTURE STATUS: {self.service_name.upper()} ({self.env.upper()})")
        live_fn = self.get_live_function_config()

        print(f"Function Name: {self.func_name}")
        if not live_fn:
            print(f"Status:        {Style.RED}NOT FOUND in AWS{Style.RESET}")
            return

        print(f"Status:        {Style.GREEN}{live_fn.get('State', 'Active')}{Style.RESET}")
        print(f"Last Modified: {live_fn.get('LastModified')}")
        print(f"Memory:        {live_fn.get('MemorySize')} MB")
        print(f"Timeout:       {live_fn.get('Timeout')}s")
        print(f"Storage:       {live_fn.get('EphemeralStorage', {}).get('Size', 512)} MB")
        print(f"Package Type:  {live_fn.get('PackageType')}")
        print(f"Role:          {live_fn.get('Role')}")

        # List live triggers
        try:
            esms = self.lam.list_event_source_mappings(FunctionName=self.func_name).get("EventSourceMappings", [])
            if esms:
                print(f"\n{Style.BOLD}Attached Event Source Mappings (SQS):{Style.RESET}")
                for esm in esms:
                    q_arn = esm.get("EventSourceArn", "").split(":")[-1]
                    print(f"  - Queue: {q_arn} | State: {esm.get('State')} | Batch: {esm.get('BatchSize')}")
        except Exception:
            pass

    def destroy(self):
        print_header(f"DESTROY INFRASTRUCTURE: {self.service_name.upper()} ({self.env.upper()})")
        confirm = input(
            f"Are you sure you want to DESTROY all infrastructure for '{self.func_name}'? (yes/no): "
        )
        if confirm.lower() != "yes":
            print("Aborted.")
            return

        print(f">> Deleting Lambda function {self.func_name}...")
        try:
            self.lam.delete_function(FunctionName=self.func_name)
            print(f"{Style.GREEN}>> Lambda function deleted.{Style.RESET}")
        except ClientError as e:
            print(f"Error deleting function: {e}")


def main(service_dir=None):
    if service_dir is None:
        service_dir = os.getcwd()

    parser = argparse.ArgumentParser(
        description="Modular Infrastructure-as-Code manager for RM Serverless Functions"
    )
    parser.add_argument("--env", choices=["uat", "prod"], default="uat", help="Target environment (default: uat)")
    parser.add_argument("--plan", action="store_true", help="Inspect and preview infrastructure changes (Diff)")
    parser.add_argument("--apply", action="store_true", help="Apply declarative infrastructure updates to AWS")
    parser.add_argument("--status", action="store_true", help="Show live status of AWS resources")
    parser.add_argument("--export-cfn", action="store_true", help="Export standalone CloudFormation template")
    parser.add_argument("--destroy", action="store_true", help="Tear down infrastructure")
    parser.add_argument("--mode", choices=["direct", "cfn"], default="direct", help="Apply mode (direct Boto3 or CloudFormation stack)")
    args = parser.parse_args()

    manager = InfraManager(service_dir=service_dir, env=args.env)

    if args.apply:
        manager.apply(mode=args.mode)
    elif args.status:
        manager.status()
    elif args.export_cfn:
        manager.export_cfn()
    elif args.destroy:
        manager.destroy()
    else:
        # Default action is plan
        manager.plan()


if __name__ == "__main__":
    main()
