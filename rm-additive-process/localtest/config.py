"""
localtest/config.py
=============================================================================
Connection + path configuration for the LOCAL additive-model test flow.

Nothing here touches AWS. Every value has a local default and can be
overridden with an environment variable so a developer can point the flow at
their own MySQL / MongoDB without editing code.

Env vars (all optional):
    ADDITIVE_LOCAL_MYSQL_HOST        default 127.0.0.1
    ADDITIVE_LOCAL_MYSQL_PORT        default 3306
    ADDITIVE_LOCAL_MYSQL_USER        default root
    ADDITIVE_LOCAL_MYSQL_PASSWORD    default '' (empty)
    ADDITIVE_LOCAL_MYSQL_SCHEMA      default qp_dl_navdata_local
    ADDITIVE_LOCAL_MONGO_HOST        default 127.0.0.1
    ADDITIVE_LOCAL_MONGO_PORT        default 27017
    ADDITIVE_LOCAL_MONGO_USER        default '' (no auth)
    ADDITIVE_LOCAL_MONGO_PASSWORD    default '' (no auth)
    ADDITIVE_LOCAL_MONGO_AUTH_SOURCE default admin
    ADDITIVE_LOCAL_MONGO_DB          default local_qp_dw_rmalloc
    ADDITIVE_LOCAL_MONGO_MASTER_DB   default local_qp_rm_master

The Mongo DB names are deliberately prefixed with `local_` so seeding can drop
and rebuild collections without any chance of clobbering a local dump of the
real qp_dw_rmalloc / qp_rm_master databases. Override them if you want the
exact production names.
"""

import os
from dataclasses import dataclass, field
from urllib.parse import quote_plus

# --------------------------------------------------------------------------
# Directory layout
# --------------------------------------------------------------------------
LOCALTEST_DIR = os.path.dirname(os.path.abspath(__file__))
PROCESS_DIR = os.path.dirname(LOCALTEST_DIR)                    # rm-additive-process
REPO_ROOT = os.path.dirname(PROCESS_DIR)                        # rm-allocation-serverless-funcs
CONFIGURATIONS_DIR = os.path.join(REPO_ROOT, 'rm-allocation-configurations')

INPUTS_DIR = os.path.join(LOCALTEST_DIR, 'inputs')
OUTPUTS_DIR = os.path.join(LOCALTEST_DIR, 'outputs')


def _env(name, default):
    val = os.environ.get(f'ADDITIVE_LOCAL_{name}')
    return default if val is None or val == '' else val


@dataclass
class LocalConfig:
    """Everything the local flow needs to talk to local MySQL / MongoDB."""

    # --- MySQL (stands in for the ESB / QP_DL_NAVDATA schema) ---
    mysql_host: str = field(default_factory=lambda: _env('MYSQL_HOST', '127.0.0.1'))
    mysql_port: int = field(default_factory=lambda: int(_env('MYSQL_PORT', '3306')))
    mysql_user: str = field(default_factory=lambda: _env('MYSQL_USER', 'root'))
    mysql_password: str = field(default_factory=lambda: os.environ.get(
        'ADDITIVE_LOCAL_MYSQL_PASSWORD', ''))
    mysql_schema: str = field(default_factory=lambda: _env('MYSQL_SCHEMA', 'qp_dl_navdata_local'))

    # --- MongoDB (stands in for qp_dw_rmalloc + qp_rm_master) ---
    mongo_host: str = field(default_factory=lambda: _env('MONGO_HOST', '127.0.0.1'))
    mongo_port: int = field(default_factory=lambda: int(_env('MONGO_PORT', '27017')))
    mongo_user: str = field(default_factory=lambda: os.environ.get('ADDITIVE_LOCAL_MONGO_USER', ''))
    mongo_password: str = field(default_factory=lambda: os.environ.get(
        'ADDITIVE_LOCAL_MONGO_PASSWORD', ''))
    mongo_auth_source: str = field(default_factory=lambda: _env('MONGO_AUTH_SOURCE', 'admin'))
    mongo_db: str = field(default_factory=lambda: _env('MONGO_DB', 'local_qp_dw_rmalloc'))
    mongo_master_db: str = field(default_factory=lambda: _env('MONGO_MASTER_DB',
                                                              'local_qp_rm_master'))

    # --- paths (inputs and outputs are the only artefacts this flow writes) ---
    inputs_dir: str = INPUTS_DIR
    outputs_dir: str = OUTPUTS_DIR

    # =====================================================================
    # Derived
    # =====================================================================
    @property
    def mysql_url(self):
        """SQLAlchemy URL for the seeded local schema."""
        pw = quote_plus(self.mysql_password)
        return (f"mysql+pymysql://{self.mysql_user}:{pw}"
                f"@{self.mysql_host}:{self.mysql_port}/{self.mysql_schema}")

    @property
    def mysql_server_url(self):
        """SQLAlchemy URL without a database — used to CREATE the schema."""
        pw = quote_plus(self.mysql_password)
        return (f"mysql+pymysql://{self.mysql_user}:{pw}"
                f"@{self.mysql_host}:{self.mysql_port}")

    def mongo_kwargs(self):
        """MongoClient kwargs; credentials are only sent when configured."""
        kw = {'host': self.mongo_host, 'port': self.mongo_port,
              'serverSelectionTimeoutMS': 5000}
        if self.mongo_user:
            kw.update(username=self.mongo_user,
                      password=self.mongo_password,
                      authSource=self.mongo_auth_source)
        return kw

    def mongo_client(self):
        from pymongo import MongoClient
        return MongoClient(**self.mongo_kwargs())

    def mysql_engine(self, with_schema=True):
        from sqlalchemy import create_engine
        url = self.mysql_url if with_schema else self.mysql_server_url
        return create_engine(url, pool_pre_ping=True)

    def mysql_reachable(self):
        """True when a local MySQL accepts a connection (schema need not exist)."""
        try:
            engine = self.mysql_engine(with_schema=False)
            with engine.connect():
                pass
            engine.dispose()
            return True
        except Exception:
            return False

    def mongo_reachable(self):
        try:
            client = self.mongo_client()
            client.admin.command('ping')
            client.close()
            return True
        except Exception:
            return False

    def ensure_dirs(self):
        for d in (self.inputs_dir, self.outputs_dir):
            os.makedirs(d, exist_ok=True)

    def describe(self):
        return (f"MySQL  : {self.mysql_user}@{self.mysql_host}:{self.mysql_port}"
                f"/{self.mysql_schema}\n"
                f"Mongo  : {self.mongo_host}:{self.mongo_port} "
                f"[{self.mongo_db} | {self.mongo_master_db}]"
                f"{' (auth)' if self.mongo_user else ' (no auth)'}\n"
                f"Inputs : {self.inputs_dir}\n"
                f"Outputs: {self.outputs_dir}")
