#!/usr/bin/env python3
"""
Shared environment configuration for the lambda-rename migration scripts
(fetch_services.py, update_services.py, remove_old_targets.py).

A single ENV toggle in each script ("uat") drives all names here, so the
old/new lambda mapping is defined in exactly one place.

Naming facts captured from the deploy scripts:

  New names:  uat-rm-lambda-<service>

  Old UAT names:
    * additive-process / additive-prefetch -> uat-rm-additive-<svc tail>
    * everything else -> uat-rm-allocation-serverless-funcs-<service>
                          (hyphen-separated)
"""

# The 9 services being migrated (canonical service names).
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

# AWS account id per environment (for reference / validation).
ACCOUNT_ID = {
    "uat": "891377165721",
}


def _normalize_env(env):
    env = (env or "").strip().lower()
    if env != "uat":
        raise ValueError(
            f"Unsupported ENV '{env}'. Only 'uat' is supported."
        )
    return env


def new_lambda_name(env, service):
    """New lambda name: {env}-rm-lambda-<service>."""
    env = _normalize_env(env)
    return f"{env}-rm-lambda-{service}"


def old_lambda_name(env, service):
    """Old lambda name, reproducing the pre-rename UAT naming."""
    env = _normalize_env(env)

    if service in ("rm-additive-process", "rm-additive-prefetch"):
        # Short form, e.g. uat-rm-additive-process.
        return f"uat-{service}"

    # Hyphenated serverless-funcs form.
    return f"uat-rm-allocation-serverless-funcs-{service}"


def build_lambda_map(env):
    """service -> {'old_lambda', 'new_lambda'} for the given environment."""
    env = _normalize_env(env)
    return {
        service: {
            "old_lambda": old_lambda_name(env, service),
            "new_lambda": new_lambda_name(env, service),
        }
        for service in SERVICES
    }


def old_lambdas_set(env):
    """Set of all old lambda names for the environment."""
    return {old_lambda_name(env, s) for s in SERVICES}


def output_csv_name(env):
    """Per-environment inventory CSV filename."""
    env = _normalize_env(env)
    return f"{env}_lambda_service_inventory.csv"
