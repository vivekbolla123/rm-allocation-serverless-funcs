"""
localtest/bootstrap.py
=============================================================================
Makes the production additive-model package importable on a laptop.

Two things have to happen BEFORE `additive_model` (or any of its helpers) is
imported:

  1. `rm-additive-process` and `rm-allocation-configurations` must be on
     sys.path, because the Lambda image flattens both into one directory.

  2. A stand-in for `configsettings` must already be in sys.modules. The real
     `rm-allocation-configurations/configsettings.py` calls AWS SSM at import
     time, which cannot work locally (and must not, since it would pull prod
     credentials). We register a plain module with the same attribute names,
     populated from LocalConfig.

`Constants.py` is imported as-is — it has no AWS dependency.

Usage (always call this first):

    from localtest.bootstrap import bootstrap
    cfg = bootstrap()
    from additive_model import AdditivePickupModelProcessor   # now safe
"""

import sys
import types

from localtest.config import LocalConfig, CONFIGURATIONS_DIR, PROCESS_DIR, LOCALTEST_DIR

_BOOTSTRAPPED = False


def build_configsettings_module(cfg):
    """A drop-in `configsettings` carrying only what the additive model reads."""
    mod = types.ModuleType('configsettings')
    mod.__doc__ = 'Local stand-in for configsettings (no SSM, no AWS).'

    mod.env = 'local'

    # --- ESB / MySQL -----------------------------------------------------
    # Both names are used: common_utils.DatabaseConfig reads ESB_NAV_SCHEMA_NAME,
    # additive_model.DatabaseConfig reads ESB_READ_SCHEMA. Keep them identical so
    # the two sets of table names resolve to the same seeded schema.
    mod.ESB_NAV_SCHEMA_NAME = cfg.mysql_schema
    mod.ESB_READ_SCHEMA = cfg.mysql_schema
    mod.ESB_DB_USERNAME = cfg.mysql_user
    mod.ESB_DB_PASSWORD = cfg.mysql_password
    mod.ESB_DB_WR_APPUSER_URL = f'{cfg.mysql_host}:{cfg.mysql_port}'
    mod.ESB_HOST = cfg.mysql_host
    mod.ESB_PORT = str(cfg.mysql_port)
    mod.ESB_USER = cfg.mysql_user
    mod.ESB_PASSWORD = cfg.mysql_password

    # --- MongoDB ---------------------------------------------------------
    mod.MONGO_URL = cfg.mongo_host
    mod.MONGO_PORT = cfg.mongo_port
    mod.MONGO_USERNAME = cfg.mongo_user
    mod.MONGO_PASSWORD = cfg.mongo_password
    mod.MONGO_AUTH_SOURCE = cfg.mongo_auth_source
    mod.MONGO_DB = cfg.mongo_db
    mod.MONGO_MASTER_DB = cfg.mongo_master_db

    # Demand-multiplier / WTP collections live in the master DB locally, which is
    # AdditiveMongoHelper._config_db()'s default, so ADDITIVE_CONFIG_DB stays unset.
    mod.ADDITIVE_DEMAND_MULTIPLIER_COLL = 'config_sector_demand_multiplier'
    mod.ADDITIVE_WTP_COLL = 'config_quarter_wtp'

    # --- S3 (never called locally; LocalAdditiveCache replaces every S3 call) ---
    mod.ADDITIVE_MODEL_BUCKET = 'local-no-s3'
    mod.ADDITIVE_MODEL_FILE_PATH = 'local/additive_model/'

    # --- misc names other modules in the package may touch ---------------
    mod.RM_DB_SCHEMA_NAME = 'QP_DW_RMALLOC'
    mod.RM_REPORTS_SCHEMA_NAME = 'QP_RM_REPORTS'
    mod.DW_DB_READ_SCHEMA = 'QP_DW_NAVFACT'
    mod.DW_DB_WRITE_SCHEMA = 'RM_DW_DATA'

    return mod


def bootstrap(cfg=None):
    """Wire sys.path + sys.modules. Idempotent. Returns the LocalConfig used."""
    global _BOOTSTRAPPED
    cfg = cfg or LocalConfig()

    if not _BOOTSTRAPPED:
        for path in (CONFIGURATIONS_DIR, PROCESS_DIR):
            if path not in sys.path:
                sys.path.insert(0, path)

        existing = sys.modules.get('configsettings')
        if existing is not None and getattr(existing, 'env', None) != 'local':
            raise RuntimeError(
                'The real `configsettings` is already imported. bootstrap() must run '
                'before anything imports the additive model.')
        sys.modules['configsettings'] = build_configsettings_module(cfg)

        _BOOTSTRAPPED = True

    cfg.ensure_dirs()
    return cfg


def ensure_localtest_importable():
    """Allow `python localtest/run_local.py` as well as `python -m localtest.run_local`."""
    repo_side = PROCESS_DIR
    if repo_side not in sys.path:
        sys.path.insert(0, repo_side)
    if LOCALTEST_DIR in sys.path:
        # Running the script directly puts localtest/ itself on sys.path, which would
        # shadow top-level modules. Drop it; PROCESS_DIR is what we want.
        sys.path.remove(LOCALTEST_DIR)
