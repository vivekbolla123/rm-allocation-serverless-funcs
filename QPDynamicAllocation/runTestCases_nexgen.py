import os
import sys

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if _BASE_DIR not in sys.path:
    sys.path.insert(0, _BASE_DIR)

from runTestCases_nextgen import run_nextgen_test

if __name__ == "__main__":
    run_nextgen_test()
