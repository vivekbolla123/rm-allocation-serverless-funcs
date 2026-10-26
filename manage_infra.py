#!/usr/bin/env python3
"""
Global Infrastructure-as-Code Manager for RM Serverless Functions.
Allows planning, applying, inspecting, and exporting infrastructure across all 9 Lambdas,
or targeting individual services directly.
"""

import argparse
import os
import sys

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
CONFIGS_DIR = os.path.join(REPO_ROOT, "rm-allocation-configurations")
if CONFIGS_DIR not in sys.path:
    sys.path.insert(0, CONFIGS_DIR)

from infra_engine import InfraManager, Style, print_header

# Ordered list of all 9 RM serverless functions
ALL_SERVICES = [
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


def main():
    parser = argparse.ArgumentParser(
        description="Global RM Serverless Functions Infrastructure Manager"
    )
    parser.add_argument(
        "--env",
        choices=["uat", "prod"],
        default="uat",
        help="Target environment (uat or prod, default: uat)",
    )
    parser.add_argument(
        "--service",
        choices=ALL_SERVICES,
        help="Target a specific service (default: all services)",
    )
    parser.add_argument(
        "--plan",
        action="store_true",
        help="Preview infrastructure diffs and dry-run",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply infrastructure updates to AWS",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show live AWS runtime status",
    )
    parser.add_argument(
        "--export-cfn",
        action="store_true",
        help="Synthesize and export CloudFormation templates",
    )
    parser.add_argument(
        "--mode",
        choices=["direct", "cfn"],
        default="direct",
        help="Apply mode: direct Boto3 reconciliation or CloudFormation stack",
    )
    args = parser.parse_args()

    targets = [args.service] if args.service else ALL_SERVICES

    print_header(f"RM SERVERLESS FUNCTIONS INFRASTRUCTURE MANAGER ({args.env.upper()})")
    print(f"Target Services ({len(targets)}):")
    for s in targets:
        print(f"  • {s}")
    print()

    for idx, svc in enumerate(targets, 1):
        infra_dir = os.path.join(REPO_ROOT, svc, "infra")
        if not os.path.exists(infra_dir):
            print(f"{Style.RED}[{idx}/{len(targets)}] Skipping {svc}: infra/ directory not found!{Style.RESET}")
            continue

        print(f"\n{Style.BOLD}{Style.MAGENTA}[{idx}/{len(targets)}] Processing: {svc}{Style.RESET}")
        manager = InfraManager(service_dir=infra_dir, env=args.env)

        if args.apply:
            manager.apply(mode=args.mode)
        elif args.status:
            manager.status()
        elif args.export_cfn:
            manager.export_cfn()
        else:
            manager.plan()

    print(f"\n{Style.BOLD}{Style.GREEN}All requested operations completed successfully.{Style.RESET}\n")


if __name__ == "__main__":
    main()
