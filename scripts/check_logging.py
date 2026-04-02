#!/usr/bin/env python3
import ast
import os
import sys
from typing import Dict, List, Tuple

# Exceptions that are typically part of normal control flow and don't always need logging
SAFE_EXCEPTIONS = {"StopIteration", "ImportError", "GeneratorExit"}


def check_file_for_logging(filepath: str) -> Dict[str, List[Tuple[int, str]]]:
    """
    Parses a Python file and categorizes exception handlers.
    """
    results: Dict[str, List[Tuple[int, str]]] = {
        "critical": [],  # Catch-all Exception without logging
        "warning": [],  # Specific exceptions (ValueError, etc) without logging
        "info": [],  # Safe/Control flow exceptions
    }

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()
            tree = ast.parse(content)
            lines = content.splitlines()
    except Exception:
        return results

    for node in ast.walk(tree):
        if isinstance(node, ast.ExceptHandler):
            # Check for logging
            has_logging = False
            for stmt in ast.walk(node):
                if isinstance(stmt, ast.Call):
                    if (
                        isinstance(stmt.func, ast.Attribute)
                        and isinstance(stmt.func.value, ast.Name)
                        and stmt.func.value.id == "logger"
                    ):
                        has_logging = True
                        break

            if not has_logging:
                line_no = node.lineno
                line_text = lines[line_no - 1].strip()

                # Determine category
                exc_type = ""
                if isinstance(node.type, ast.Name):
                    exc_type = node.type.id
                elif isinstance(node.type, ast.Tuple):
                    exc_type = "Multiple"
                elif node.type is None:
                    exc_type = "Bare"

                if exc_type in SAFE_EXCEPTIONS:
                    results["info"].append((line_no, line_text))
                elif exc_type in ("Exception", "Bare"):
                    results["critical"].append((line_no, line_text))
                else:
                    results["warning"].append((line_no, line_text))

    return results


def main():
    search_dir = "neosqlite"
    if len(sys.argv) > 1:
        search_dir = sys.argv[1]

    all_results = []

    for root, _, files in os.walk(search_dir):
        for file in files:
            if file.endswith(".py"):
                path = os.path.join(root, file)
                res = check_file_for_logging(path)
                if any(res.values()):
                    all_results.append((path, res))

    # Print Summary Report
    print("\n" + "=" * 80)
    print(f"{'OBSERVABILITY HEALTH REPORT':^80}")
    print("=" * 80)

    critical_count = 0
    warning_count = 0

    # 1. Critical Issues First
    print("\n[!] CRITICAL: Silent Catch-Alls (Should be fixed immediately)")
    print("-" * 80)
    for path, res in all_results:
        for line, text in res["critical"]:
            print(f"  {path}:{line} -> {text}")
            critical_count += 1
    if not critical_count:
        print("  None found. Great job!")

    # 2. Warnings
    print(
        "\n[?] WARNING: Unlogged Specific Exceptions (Consider adding debug logs)"
    )
    print("-" * 80)
    for path, res in all_results:
        for line, text in res["warning"]:
            print(f"  {path}:{line} -> {text}")
            warning_count += 1
    if not warning_count:
        print("  None found.")

    # 3. Info (Optional)
    print("\n[i] INFO: Normal Control Flow (Safe to ignore)")
    print("-" * 80)
    info_count = sum(len(r["info"]) for _, r in all_results)
    print(
        f"  Identified {info_count} instances of safe control-flow (StopIteration, etc.)"
    )

    print("\n" + "=" * 80)
    print(f"SUMMARY: {critical_count} Critical, {warning_count} Warnings")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
