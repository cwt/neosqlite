#!/usr/bin/env python3
"""
Comprehensive API Comparison Script between NeoSQLite and PyMongo

This script compares ALL NeoSQLite supported APIs and operators with MongoDB
and reports compatibility statistics.

This is the main entry point that uses the refactored api_comparison package.

Usage:
    python3 api_comparison_main.py              # Run compatibility tests
    python3 api_comparison_main.py --benchmark  # Run with default 10 iterations
    python3 api_comparison_main.py -b 100       # Run with 100 iterations
"""

import argparse
import sys

# Add parent directory to path for imports
sys.path.insert(0, "..")

from api_comparison import run_all_comparisons, run_benchmark


def main():
    parser = argparse.ArgumentParser(
        description="NeoSQLite vs PyMongo API Comparison"
    )
    parser.add_argument(
        "-b",
        "--benchmark",
        nargs="?",
        const="10",
        default=None,
        help="Run in benchmark mode with specified iterations (default: 10)",
    )
    parser.add_argument(
        "-s",
        "--silent",
        action="store_true",
        help="Run in silent mode (suppress output, useful for benchmarking)",
    )
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 1.0.0",
    )

    args = parser.parse_args()

    if args.benchmark is not None:
        try:
            iterations = int(args.benchmark)
        except ValueError:
            print(f"Error: Invalid iteration count: {args.benchmark}")
            sys.exit(1)

        if iterations < 1:
            print("Error: Iteration count must be at least 1")
            sys.exit(1)

        run_benchmark(iterations=iterations, silent=args.silent)
    else:
        exit_code = run_all_comparisons()
        sys.exit(exit_code)


if __name__ == "__main__":
    main()
