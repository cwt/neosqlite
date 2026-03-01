#!/usr/bin/env python3
"""
Comprehensive API Comparison Script between NeoSQLite and PyMongo

This script compares ALL NeoSQLite supported APIs and operators with MongoDB
and reports compatibility statistics.

This is the main entry point that uses the refactored api_comparison package.
"""

import sys

# Add parent directory to path for imports
sys.path.insert(0, "..")

from api_comparison import run_all_comparisons


if __name__ == "__main__":
    exit_code = run_all_comparisons()
    sys.exit(exit_code)
