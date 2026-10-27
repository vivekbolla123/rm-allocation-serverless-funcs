# RM Lambda Migration - AWS CloudFormation Architecture

This directory provides the complete, production-ready AWS CloudFormation infrastructure-as-code (IaC) suite for migrating the **9 RM serverless functions** from their legacy naming conventions to the unified, standardized naming architecture:

- **New Naming Pattern**: `{env}-rm-lambda-<service>`
- **Old UAT Names**: `uat-rm-allocation-serverless-funcs-<service>` (and `uat-rm-additive-*`)
- **Old PROD Names**: `rm-allocation-serverless-funcs_<service>` (underscore separated)

All configurations, memory allocations, timeouts, ephemeral storage limits, VPC settings, SQS triggers, EventBridge rules, and EventBridge Scheduler schedules have been extracted from existing Python scripts (`env_config.py`, `fetch_services.py`, `update_services.py`, `remove_old_targets.py`) and live inventory CSVs (`uat_lambda_service_inventory.csv`, `prod_lambda_service_inventory.csv`).

---

## 📁 Directory Structure

```
cloudformation/
├── environments/
│   ├── env_config.yaml              # Single master file defining all environments, accounts, and triggers
│   ├── parameters-uat.json          # Standard CloudFormation CLI parameters for UAT
│   └── parameters-prod.json         # Standard CloudFormation CLI parameters for PROD
├── 01-lambda-creation.yaml          # Phase 1: Provisions 9 new Lambdas, IAM role, logs, VPC, Datadog
├── 02-activation.yaml               # Phase 2: Attaches SQS mappings, Scheduler schedules, and EB targets
├── 03-old-resource-deletion.yaml    # Phase 3: Safe cutover & decommissioning with DryRun guardrails
├── deploy.sh                        # Unified bash orchestrator for validating & deploying all phases
├── scripts/
│   ├── generate_config.py           # Synchronizes env_config.yaml and parameter JSON files from CSVs
│   └── cleanup_old_resources.py     # Standalone Python CLI tool for decommission analysis and cutover
└── README.md                        # Complete operational documentation
```

---

## 🧩 1. Master Environment Definition (`environments/env_config.yaml`)

All environments, AWS accounts, VPC networks, secrets paths, and trigger inventories are centralized in `environments/env_config.yaml`:

| Environment | AWS Account ID | Region | VPC ID | Subnets | Security Groups | Datadog Secret SSM Path |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **UAT** | `891377165721` | `ap-south-1` | `vpc-01452dce7b5b446e1` | `subnet-053e2b8d03017c388`, `subnet-0a05a901a38cd5672` | `sg-087f8184f725ec3fe` | `/copilot/qp/uat/secrets/DD_API_KEY` |
| **PROD** | `471112573018` | `ap-south-1` | `vpc-prod-rm` (parameterized) | Private App Subnets | Lambda SG | `/copilot/qp/prod/secrets/DD_API_KEY` |

### Clean, Service-Oriented Schema Example:
```yaml
environments:
  uat:
    services:
      rm-allocation-scheduled-jobs:
        sqs:
          queue_name: test
          batch_size: 10
          enabled: false
          max_concurrency: 500
        rules:
          - name: QCProcess
            schedule: "cron(30 17 ? * sat *)"
            enabled: false
            description: "Create min max table for historic and profile fares"
            input:
              body:
                modules:
                  gen_qc_fares_table: 1
```

To refresh or update parameter JSON files from `env_config.yaml`:
```bash
./deploy.sh generate-config
```

---

## ⚙️ 2. The 9 Migrated Services

| Canonical Service Name | Memory | Timeout | Ephemeral Storage | Datadog Service Name | Old UAT Name | Old PROD Name | New Unified Name (`{env}-rm-lambda-*`) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **rm-additive-process** | 4096 MB | 900s | 2048 MB | `rm-additive-process` | `uat-rm-additive-process` | `rm-allocation-serverless-funcs_rm-additive-process` | `{env}-rm-lambda-rm-additive-process` |
| **rm-additive-prefetch** | 2048 MB | 900s | 2048 MB | `rm-additive-process` | `uat-rm-additive-prefetch` | `rm-allocation-serverless-funcs_rm-additive-prefetch` | `{env}-rm-lambda-rm-additive-prefetch` |
| **rm-allocation-fetch-inputs** | 8192 MB | 900s | 1600 MB | `rm-fetch-inputs` | `uat-rm-allocation-serverless-funcs-rm-allocation-fetch-inputs` | `rm-allocation-serverless-funcs_rm-allocation-fetch-inputs` | `{env}-rm-lambda-rm-allocation-fetch-inputs` |
| **rm-allocation-prefetch-inputs** | 2048 MB | 900s | 4096 MB | `rm-prefetch-inputs` | `uat-rm-allocation-serverless-funcs-rm-allocation-prefetch-inputs` | `rm-allocation-serverless-funcs_rm-allocation-prefetch-inputs` | `{env}-rm-lambda-rm-allocation-prefetch-inputs` |
| **rm-allocation-process-inputs** | 8192 MB | 900s | 1600 MB | `rm-process-inputs` | `uat-rm-allocation-serverless-funcs-rm-allocation-process-inputs` | `rm-allocation-serverless-funcs_rm-allocation-process-inputs` | `{env}-rm-lambda-rm-allocation-process-inputs` |
| **rm-allocation-scheduled-jobs** | 4096 MB | 900s | 4096 MB | `rm-scheduled-jobs` | `uat-rm-allocation-serverless-funcs-rm-allocation-scheduled-jobs` | `rm-allocation-serverless-funcs_rm-allocation-scheduled-jobs` | `{env}-rm-lambda-rm-allocation-scheduled-jobs` |
| **rm-allocation-update-rbd** | 2048 MB | 900s | 4096 MB | `rm-update-rbd` | `uat-rm-allocation-serverless-funcs-rm-allocation-update-rbd` | `rm-allocation-serverless-funcs_rm-allocation-update-rbd` | `{env}-rm-lambda-rm-allocation-update-rbd` |
| **rm-allocation-update-rbd-ftp** | 2048 MB | 900s | 4096 MB | `rm-update-rbd-ftp` | `uat-rm-allocation-serverless-funcs-rm-allocation-update-rbd-ftp` | `rm-allocation-serverless-funcs_rm-allocation-update-rbd-ftp` | `{env}-rm-lambda-rm-allocation-update-rbd-ftp` |
| **rm-pricing-triggers** | 4096 MB | 900s | 4096 MB | `rm-pricing-tiggers` | `uat-rm-allocation-serverless-funcs-rm-pricing-triggers` | `rm-allocation-serverless-funcs_rm-pricing-triggers` | `{env}-rm-lambda-rm-pricing-triggers` |

---

## 🚀 3. Three-Phase Migration Workflow

### Step 0: Pre-Flight Validation
Ensure all CloudFormation templates pass AWS validation:
```bash
./deploy.sh validate
```

---

### Phase 1: Lambda Level Creation (`01-lambda-creation.yaml`)
Deploys the 9 new Lambda functions into the target environment without altering live traffic.
- **Resources Provisioned**:
  - 9 `AWS::Lambda::Function` resources (`PackageType: Image`, referencing `{env}-rm-ecr-*`).
  - 9 `AWS::Logs::LogGroup` resources with 30-day retention.
  - Dedicated IAM Execution Role (if no existing role ARN is supplied).
  - Private VPC attachments (Subnets & Security Groups).
  - Datadog Serverless Unified Service Tagging (`DD_SERVICE`, `DD_ENV`, `DD_VERSION`).
  - Exported Stack Outputs for downstream cross-stack referencing.

```bash
# Deploy to UAT
./deploy.sh deploy-phase-1 --env uat

# Deploy to PROD
./deploy.sh deploy-phase-1 --env prod
```

---

### Phase 2: Activation (`02-activation.yaml`)
Wired triggers so the new Lambda functions begin processing traffic in parallel with legacy Lambdas (zero-downtime, safe dual-routing):
- **SQS EventSourceMappings**:
  - `rm-additive-process`: `rm-additive-model` (Batch: 1, MaxConcurrency: 60)
  - `rm-allocation-fetch-inputs`: `rm-allocation-fetch-inputs` (Batch: 1, MaxConcurrency: 10)
  - `rm-allocation-process-inputs`: `rm-allocation-market-list` (Batch: 1/10, MaxConcurrency: 300/600)
  - `rm-allocation-scheduled-jobs`: `rm-allocation-scheduled-jobs` (Batch: 1, MaxConcurrency: 5)
  - `rm-allocation-update-rbd`: `rm-allocation-rbds` (Batch: 5, MaxConcurrency: 400/1000)
  - `rm-allocation-update-rbd-ftp`: `rm-allocation-rbds-ftp` (Batch: 2/10, MaxConcurrency: 10)
- **EventBridge Scheduler (PROD)**:
  - 9 new schedules named `<name>-new-lambda` created with exact cron/rate expressions and `Asia/Calcutta` timezone.
- **EventBridge Rules**:
  - Attaches `<target-id>-new-lambda` to all 54 PROD / 25 UAT rules with identical JSON payloads and grants `lambda:InvokeFunction` permissions.

```bash
# Deploy to UAT
./deploy.sh deploy-phase-2 --env uat

# Deploy to PROD
./deploy.sh deploy-phase-2 --env prod
```

---

### Phase 3: Old Resource Deletion & Decommissioning (`03-old-resource-deletion.yaml`)
Retires legacy triggers and deletes deprecated resources with full guardrail protections:
- **Guardrail Rule**: Old triggers are NEVER retired unless the corresponding new Lambda and new target/schedule are confirmed active.
- **Operations**:
  1. Detaches old Lambda target IDs from EventBridge rules.
  2. Disables (or deletes) legacy EventBridge Scheduler schedules.
  3. Deletes deprecated SQS EventSourceMappings from legacy functions.
  4. (Optional) Deletes legacy Lambda functions once verified.

#### Step 3A: Dry-Run Inspection (Default)
```bash
./deploy.sh deploy-phase-3 --env uat --dry-run
# Or use the Python CLI:
python3 scripts/cleanup_old_resources.py --env uat
```

#### Step 3B: Live Cutover Execution
```bash
./deploy.sh deploy-phase-3 --env uat --execute
# Or with legacy lambda deletion enabled:
python3 scripts/cleanup_old_resources.py --env uat --execute --delete-lambdas
```

---

## 🔍 Stack Status Check
Check current CloudFormation stack states at any time:
```bash
./deploy.sh status --env uat
./deploy.sh status --env prod
```

---

## 🛡️ Rollback & Safety Plan

| Scenario | Rollback Action |
| :--- | :--- |
| **Phase 1 Failure** | Delete stack `${env}-rm-lambda-creation`. Live traffic remains 100% on legacy Lambdas. |
| **Phase 2 Incomplete** | Delete stack `${env}-rm-lambda-activation`. Removes new SQS mappings and `-new-lambda` targets. Legacy traffic continues uninterrupted. |
| **Phase 3 Reversal** | By default, EventBridge schedules are set to `DISABLED` (not deleted). Re-enable with `aws scheduler update-schedule --state ENABLED`. Re-add old targets using `update_services.py`. |
