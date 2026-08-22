#!/usr/bin/env python3
"""
run.py
=============================================================================
Quick runner for local additive model testing.

Usage:
    python run.py                                # default: --generate --seed --run --anchor 2026-08-21
    python run.py --run                          # run without regenerating inputs
    python run.py --run --sectors BOMDEL         # single sector
    python run.py --help                         # show all options
"""

import os
import sys

# Ensure this directory is in sys.path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from localtest.run_local import main

if __name__ == '__main__':
    # Default to fixed anchor date and full pipeline if no arguments are provided
    if len(sys.argv) == 1:
        default_args = ['--generate', '--seed', '--run', '--anchor', '2026-08-21']
        main(default_args)
    else:
        main(sys.argv[1:])
