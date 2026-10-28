# Infrastructure-as-Code (IaC) Guidelines: rm-pricing-triggers

## 1. Overview & Principles
This directory (`infra/`) contains the complete, declarative infrastructure specification and deployment tooling for **`rm-pricing-triggers`**.

### Core Principles
1. **Declarative Configuration (`config.yaml`)**:
   - The infrastructure state is declared in `config.yaml`.
   - Never make manual out-of-band changes in the AWS Console. Any change made in `config.yaml` is reproducible and version-controlled.
2. **Plan Before Apply (`--plan`)**:
   - Always run `python infra.py --env <uat|prod> --plan` first.
   - The engine compares your local `config.yaml` with the live AWS resources and prints a side-by-side diff.
3. **Idempotent Reconciliation (`--apply`)**:
   - Running `--apply` reconciles AWS resources safely to match `config.yaml`.
   - If nothing changed, zero AWS API calls are made.
4. **Environment Isolation**:
   - Sizing, VPC, roles, and triggers are configured independently under `uat` and `prod`.

---

## 2. Directory Layout
```text
rm-pricing-triggers/
├── infra/
│   ├── config.yaml       # Declarative infrastructure definition (Compute, VPC, Triggers)
│   ├── infra.py          # Service-level CLI entrypoint (plan, apply, status, export-cfn)
│   └── GUIDELINES.md     # This documentation
├── Dockerfile            # Container image definition
├── lambda_function.py    # Application handler entrypoint
└── deploy_uat.sh / deploy_prod.sh # Code build & container push scripts
```

---

## 3. Quickstart Commands

Make sure your virtual environment has `boto3` and `pyyaml` installed.

### Inspect Changes (Diff / Dry-Run)
```bash
# Preview UAT changes
python infra.py --env uat --plan

# Preview PROD changes
python infra.py --env prod --plan
```

### Apply Changes to AWS
```bash
# Deploy changes to UAT
python infra.py --env uat --apply

# Deploy changes to PROD
python infra.py --env prod --apply
```

### Check Live Status
```bash
# View live memory, timeout, ephemeral storage, and state
python infra.py --env uat --status
```

### Export Standalone CloudFormation Template
```bash
# Generates template.yaml from config.yaml
python infra.py --env uat --export-cfn
```

---

## 4. How-To Recipes (Common Changes)

### Recipe A: Increase Memory or Timeout
Edit `rm-pricing-triggers/infra/config.yaml`:
```yaml
environments:
  uat:
    lambda:
      memory: 4096      # Was 2048 (in MB)
      timeout: 900      # 15 minutes (in seconds)
      ephemeral_storage: 4096 # Temp disk /tmp in MB
```
Run: `python infra.py --env uat --plan` then `python infra.py --env uat --apply`.

### Recipe B: Add or Update an Environment Variable
Edit `rm-pricing-triggers/infra/config.yaml`:
```yaml
environments:
  uat:
    lambda:
      env_vars:
        FEATURE_FLAG_ENABLED: "true"
        BATCH_CHUNK_SIZE: "50"
```
Run: `python infra.py --env uat --plan` then `python infra.py --env uat --apply`.

### Recipe C: Add or Modify SQS Trigger
Edit `rm-pricing-triggers/infra/config.yaml`:
```yaml
environments:
  uat:
    triggers:
      sqs:
        queue_name: "test-rm-allocation-queues-rm-pricing-triggers"
        batch_size: 5            # Number of messages per invocation (1-10000)
        max_concurrency: 50      # Maximum concurrent lambda instances
        batching_window_seconds: 1 # Batching wait window
        enabled: true
```
To temporarily disable processing: set `enabled: false`.

### Recipe D: Add an EventBridge Scheduled Rule (Cron)
Edit `rm-pricing-triggers/infra/config.yaml`:
```yaml
environments:
  uat:
    triggers:
      rules:
        - name: "uat-rm-pricing-triggers-daily-midnight"
          schedule: "cron(0 18 * * ? *)" # 23:30 IST / 18:00 UTC
          enabled: true
          description: "Nightly trigger for rm-pricing-triggers"
          input:
            run_type: "Daily"
            batch_id: "auto"
```

### Recipe E: Add an EventBridge Scheduler Schedule (Timezone-Aware)
Edit `rm-pricing-triggers/infra/config.yaml`:
```yaml
environments:
  prod:
    triggers:
      schedules:
        - name: "prod-rm-pricing-triggers-morning"
          schedule: "cron(0 6 * * ? *)"
          timezone: "Asia/Calcutta"
          enabled: true
          description: "Morning schedule at 06:00 IST"
          input:
            source: "scheduler"
```

### Recipe F: Remove a Trigger
Simply delete the trigger item from `config.yaml` or set `enabled: false`.
Run `python infra.py --env uat --plan` to verify the deletion before running `--apply`.
