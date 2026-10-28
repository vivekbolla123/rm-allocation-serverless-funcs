#!/usr/bin/env bash
# ==============================================================================
# RM Serverless Functions - CloudFormation Migration Orchestrator
# Manages the 3-phase rollout and cutover for RM Lambda functions across UAT & PROD.
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGION="ap-south-1"

usage() {
  cat <<EOF
Usage: $0 <command> [options]

Commands:
  validate              Validate all CloudFormation templates with AWS API
  generate-config       Generate/refresh env_config.yaml and parameter JSON files
  deploy-phase-1        Deploy Phase 1: Lambda Level Creation Stack
  deploy-phase-2        Deploy Phase 2: Activation Stack (Triggers & Schedulers)
  deploy-phase-3        Deploy Phase 3: Old Resource Deletion Stack
  status                Show status of all RM migration stacks
  cleanup-cli           Run the Phase 3 Python CLI tool (dry-run or live)

Options:
  --env <uat|prod>      Target environment (default: uat)
  --dry-run             Run in dry-run mode (Phase 3 only, default: true)
  --execute             Actually execute deletion/cutover (Phase 3 only)
  -h, --help            Show this help message

Examples:
  $0 validate
  $0 deploy-phase-1 --env uat
  $0 deploy-phase-2 --env uat
  $0 deploy-phase-3 --env uat --dry-run
  $0 deploy-phase-3 --env uat --execute
EOF
  exit 1
}

COMMAND="${1:-}"
shift || true

ENV="uat"
DRY_RUN="true"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env)
      ENV="$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN="true"
      shift
      ;;
    --execute)
      DRY_RUN="false"
      shift
      ;;
    -h|--help)
      usage
      ;;
    *)
      echo "Unknown option: $1"
      usage
      ;;
  esac
done

if [[ -z "$COMMAND" ]]; then
  usage
fi

STACK_PHASE_1="${ENV}-rm-lambda-creation"
STACK_PHASE_2="${ENV}-rm-lambda-activation"
STACK_PHASE_3="${ENV}-rm-lambda-deletion"

PARAM_FILE="${SCRIPT_DIR}/environments/parameters-${ENV}.json"

ENV_UPPER=$(echo "$ENV" | tr '[:lower:]' '[:upper:]')
echo "========================================================================"
echo " RM Migration Orchestrator | Command: ${COMMAND} | Environment: ${ENV_UPPER}"
echo "========================================================================"

case "$COMMAND" in
  validate)
    echo ">> Validating 01-lambda-creation.yaml..."
    aws cloudformation validate-template \
      --template-body "file://${SCRIPT_DIR}/01-lambda-creation.yaml" \
      --region "$REGION" > /dev/null
    echo "   [OK] 01-lambda-creation.yaml is valid."

    echo ">> Validating 02-activation.yaml..."
    aws cloudformation validate-template \
      --template-body "file://${SCRIPT_DIR}/02-activation.yaml" \
      --region "$REGION" > /dev/null
    echo "   [OK] 02-activation.yaml is valid."

    echo ">> Validating 03-old-resource-deletion.yaml..."
    aws cloudformation validate-template \
      --template-body "file://${SCRIPT_DIR}/03-old-resource-deletion.yaml" \
      --region "$REGION" > /dev/null
    echo "   [OK] 03-old-resource-deletion.yaml is valid."
    echo ""
    echo "All CloudFormation templates passed validation!"
    ;;

  generate-config)
    echo ">> Generating master env_config.yaml and parameter files..."
    python3 "${SCRIPT_DIR}/scripts/generate_config.py"
    ;;

  deploy-phase-1)
    echo ">> Deploying Phase 1: Lambda Level Creation Stack [${STACK_PHASE_1}]..."
    aws cloudformation deploy \
      --template-file "${SCRIPT_DIR}/01-lambda-creation.yaml" \
      --stack-name "$STACK_PHASE_1" \
      --parameter-overrides "file://${PARAM_FILE}" \
      --capabilities CAPABILITY_NAMED_IAM \
      --region "$REGION"
    echo ">> Phase 1 deployment complete!"
    ;;

  deploy-phase-2)
    echo ">> Deploying Phase 2: Activation Stack [${STACK_PHASE_2}]..."
    aws cloudformation deploy \
      --template-file "${SCRIPT_DIR}/02-activation.yaml" \
      --stack-name "$STACK_PHASE_2" \
      --parameter-overrides Environment="$ENV" \
      --capabilities CAPABILITY_NAMED_IAM \
      --region "$REGION"
    echo ">> Phase 2 deployment complete!"
    ;;

  deploy-phase-3)
    echo ">> Deploying Phase 3: Old Resource Deletion Stack [${STACK_PHASE_3}] (DryRun=${DRY_RUN})..."
    aws cloudformation deploy \
      --template-file "${SCRIPT_DIR}/03-old-resource-deletion.yaml" \
      --stack-name "$STACK_PHASE_3" \
      --parameter-overrides Environment="$ENV" DryRun="$DRY_RUN" SchedulerAction="disable" \
      --capabilities CAPABILITY_NAMED_IAM \
      --region "$REGION"
    echo ">> Phase 3 stack execution complete!"
    ;;

  status)
    echo "Checking stack statuses for ${ENV_UPPER}..."
    for stack in "$STACK_PHASE_1" "$STACK_PHASE_2" "$STACK_PHASE_3"; do
      echo -n "  ${stack}: "
      aws cloudformation describe-stacks \
        --stack-name "$stack" \
        --region "$REGION" \
        --query "Stacks[0].StackStatus" \
        --output text 2>/dev/null || echo "NOT DEPLOYED"
    done
    ;;

  cleanup-cli)
    CMD="python3 ${SCRIPT_DIR}/scripts/cleanup_old_resources.py --env ${ENV} --region ${REGION}"
    if [[ "$DRY_RUN" == "false" ]]; then
      CMD="${CMD} --execute"
    fi
    echo ">> Running CLI cleanup tool: ${CMD}"
    eval "$CMD"
    ;;

  *)
    echo "Unknown command: ${COMMAND}"
    usage
    ;;
esac
