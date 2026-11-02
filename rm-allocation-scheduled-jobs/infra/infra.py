#!/usr/bin/env python3
"""
Infrastructure-as-Code CLI for rm-allocation-scheduled-jobs.
Allows developers to plan, apply, inspect, and export infrastructure.
"""
import os
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SHARED_DIR = os.path.abspath(os.path.join(CURRENT_DIR, "..", "..", "rm-allocation-configurations"))

if SHARED_DIR not in sys.path:
    sys.path.insert(0, SHARED_DIR)

try:
    from infra_engine import main
except ImportError:
    REPO_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
    sys.path.insert(0, os.path.join(REPO_ROOT, "rm-allocation-configurations"))
    from infra_engine import main

if __name__ == "__main__":
    main(service_dir=CURRENT_DIR)
