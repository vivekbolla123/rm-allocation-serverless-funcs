"""Local, CSV-driven test flow for the additive pickup model.

Import order matters: `localtest.bootstrap.bootstrap()` must run before anything
imports `additive_model` or its helpers, because it installs the local stand-in
for `configsettings` (the real one calls AWS SSM at import time).
"""
