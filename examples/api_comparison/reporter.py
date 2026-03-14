"""
Compatibility Reporter - Tracks and reports API compatibility between NeoSQLite and PyMongo
"""

import csv
import json
import os
from datetime import datetime
from typing import Any, Optional
from .utils import compare_results


def _sort_for_display(value: Any) -> Any:
    """
    Sort lists and dicts for consistent display.

    Sorts lists of dicts by meaningful fields (excluding _id) to ensure
    consistent ordering even when auto-generated _id values differ between databases.
    Also sorts dict keys for consistent key ordering.

    Args:
        value: Value to sort (list, dict, or other)

    Returns:
        Sorted value (lists sorted, dicts with sorted keys)
    """
    if isinstance(value, list):
        if value and isinstance(value[0], dict):
            # Sort list of dicts by meaningful fields (excluding _id)
            def sort_key(doc):
                # Build a composite key from common identifying fields
                # Note: We intentionally exclude _id since it differs between databases
                key_parts = []
                # Try common identifying fields in priority order
                for field in [
                    "name",
                    "category",
                    "dept",
                    "title",
                    "value",
                    "_id",
                ]:
                    if field in doc:
                        key_parts.append(str(doc[field]))
                # Fall back to full dict string if no common fields
                if not key_parts:
                    key_parts.append(str(sorted(doc.items())))
                return "|".join(key_parts)

            # Sort the list AND recursively sort each dict inside
            return [
                _sort_for_display(item) for item in sorted(value, key=sort_key)
            ]
        # Otherwise sort by string representation and recurse
        return [
            _sort_for_display(item)
            for item in sorted(value, key=lambda x: str(x))
        ]
    elif isinstance(value, dict):
        # Return dict with sorted keys for consistent display, and recurse on values
        return {k: _sort_for_display(v) for k, v in sorted(value.items())}
    return value


def _truncate_json(value: Any, max_length: int = 100) -> str:
    """
    Convert value to JSON string and truncate if needed.

    Args:
        value: Value to convert to JSON
        max_length: Maximum length before truncation

    Returns:
        JSON string, truncated with '...' if exceeds max_length
    """
    try:
        # Sort for consistent display
        sorted_value = _sort_for_display(value)
        json_str = json.dumps(sorted_value, default=str, separators=(",", ":"))
        if len(json_str) > max_length:
            return json_str[:max_length] + "..."
        return json_str
    except (TypeError, ValueError):
        return str(value)[:max_length]


class CompatibilityReporter:
    """Tracks and reports API compatibility between NeoSQLite and PyMongo"""

    def __init__(self):
        self.total_tests = 0
        self.passed_tests = 0
        self.failed_tests = []
        self.skipped_tests = []
        self.passed_with_output = []  # Track passed tests with result output

    def record_result(
        self,
        category: str,
        api_name: str,
        passed: bool,
        neo_result: Any = None,
        mongo_result: Any = None,
        error: Optional[str] = None,
        skip_reason: Optional[str] = None,
        show_results: bool = False,
    ):
        """
        Record a test result.

        Args:
            category: Test category (e.g., "CRUD", "Aggregation Stages")
            api_name: API/test name
            passed: Whether test passed
            neo_result: Result from NeoSQLite
            mongo_result: Result from MongoDB
            error: Error message if test failed
            skip_reason: Reason for skipping test
            show_results: If True and test passed, show results in output
        """
        self.total_tests += 1
        if skip_reason:
            self.skipped_tests.append(
                {
                    "category": category,
                    "api": api_name,
                    "reason": skip_reason,
                }
            )
            return

        if passed:
            self.passed_tests += 1
            if show_results:
                self.passed_with_output.append(
                    {
                        "category": category,
                        "api": api_name,
                        "neo_result": neo_result,
                        "mongo_result": mongo_result,
                    }
                )
        else:
            self.failed_tests.append(
                {
                    "category": category,
                    "api": api_name,
                    "neo_result": str(neo_result)[:200] if neo_result else None,
                    "mongo_result": (
                        str(mongo_result)[:200] if mongo_result else None
                    ),
                    "error": error,
                }
            )

    def record_comparison(
        self,
        category: str,
        api_name: str,
        neo_results: Any,
        mongo_results: Any,
        tolerance: float = 1e-9,
        ignore_order: bool = True,
        skip_reason: Optional[str] = None,
    ):
        """
        Record a test result by comparing actual values between NeoSQLite and MongoDB.

        Args:
            category: Test category
            api_name: API/test name
            neo_results: Results from NeoSQLite
            mongo_results: Results from MongoDB
            tolerance: Tolerance for floating point comparisons
            ignore_order: If True, sort results before comparison
            skip_reason: Reason for skipping test (if MongoDB not available)
        """
        if mongo_results is None and skip_reason:
            self.total_tests += 1
            self.skipped_tests.append(
                {
                    "category": category,
                    "api": api_name,
                    "reason": skip_reason,
                }
            )
            return

        if isinstance(neo_results, list) and isinstance(mongo_results, list):
            passed, error = compare_results(
                neo_results, mongo_results, tolerance, ignore_order
            )
        else:
            passed = neo_results == mongo_results
            error = (
                f"Value mismatch: {neo_results} vs {mongo_results}"
                if not passed
                else None
            )

        self.record_result(
            category=category,
            api_name=api_name,
            passed=passed,
            neo_result=neo_results,
            mongo_result=mongo_results,
            error=error,
            skip_reason=skip_reason if mongo_results is None else None,
            show_results=not passed,
        )

    def get_compatibility_percentage(self) -> float:
        """Get the compatibility percentage"""
        if self.total_tests == 0:
            return 0.0
        effective_total = self.total_tests - len(self.skipped_tests)
        if effective_total == 0:
            return 100.0
        return (self.passed_tests / effective_total) * 100

    def print_report(self, show_passed_results: bool = False):
        """
        Print the compatibility report.

        Args:
            show_passed_results: If True, show JSON results for passed tests
        """
        print("\n" + "=" * 80)
        print("COMPATIBILITY REPORT")
        print("=" * 80)
        print(f"Total Tests: {self.total_tests}")
        print(f"Passed: {self.passed_tests}")
        print(f"Skipped: {len(self.skipped_tests)}")
        print(f"Failed: {len(self.failed_tests)}")
        print(f"Compatibility: {self.get_compatibility_percentage():.1f}%")
        print("=" * 80)

        # Show passed test results if requested
        if show_passed_results and self.passed_with_output:
            print("\nPASSED TESTS (Sample Results):")
            print("-" * 80)
            for passed_test in self.passed_with_output:
                print(f"\n[{passed_test['category']}] {passed_test['api']}")
                print(
                    f"  NeoSQLite: {_truncate_json(passed_test['neo_result'])}"
                )
                print(
                    f"  MongoDB:   {_truncate_json(passed_test['mongo_result'])}"
                )

        if self.failed_tests:
            print("\nINCOMPATIBLE APIs:")
            print("-" * 80)
            for failure in self.failed_tests:
                print(f"\n[{failure['category']}] {failure['api']}")
                if failure["error"]:
                    print(f"  Error: {failure['error']}")
                if failure["neo_result"]:
                    print(f"  NeoSQLite: {failure['neo_result']}")
                if failure["mongo_result"]:
                    print(f"  MongoDB: {failure['mongo_result']}")

        if self.skipped_tests:
            print("\n\nSKIPPED TESTS (Known Limitations):")
            print("-" * 80)
            for skip in self.skipped_tests:
                print(f"\n[{skip['category']}] {skip['api']}")
                print(f"  Reason: {skip['reason']}")

        print("\n" + "=" * 80)


# Global reporter instance - shared across all modules
reporter = CompatibilityReporter()


def get_neosqlite_version():
    """Get NeoSQLite version from pyproject.toml or package"""
    try:
        # Try to read from pyproject.toml first
        import os

        pyproject_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "pyproject.toml",
        )
        if os.path.exists(pyproject_path):
            with open(pyproject_path) as f:
                for line in f:
                    if line.startswith("version = "):
                        return line.split("=")[1].strip().strip('"')
    except Exception:
        pass

    # Fall back to package version
    try:
        import neosqlite

        return getattr(neosqlite, "__version__", "unknown")
    except ImportError:
        return "unknown"


class BenchmarkResult:
    """Stores timing results for a single category"""

    def __init__(self, category: str):
        self.category = category
        self.neo_timings: list[float] = []
        self.mongo_timings: list[float] = []
        self.neo_skipped = False
        self.mongo_skipped = False
        self.skip_reason: Optional[str] = None

    def add_neo_timing(self, timing_ms: float):
        self.neo_timings.append(timing_ms)

    def add_mongo_timing(self, timing_ms: float):
        self.mongo_timings.append(timing_ms)

    def mark_neo_skipped(self, reason: str):
        """Mark NeoSQLite side as skipped"""
        self.neo_skipped = True
        self.skip_reason = reason

    def mark_mongo_skipped(self, reason: str):
        """Mark MongoDB side as skipped"""
        self.mongo_skipped = True
        self.skip_reason = reason

    def is_partial(self) -> bool:
        """Check if this is a partial benchmark (one side skipped)"""
        return self.neo_skipped or self.mongo_skipped

    def is_fully_skipped(self) -> bool:
        """Check if both sides are skipped"""
        return self.neo_skipped and self.mongo_skipped

    def get_neo_stats(self) -> dict[str, float]:
        if not self.neo_timings:
            return {"min": 0, "max": 0, "avg": 0, "stddev": 0}
        return self._calc_stats(self.neo_timings)

    def get_mongo_stats(self) -> dict[str, float]:
        if not self.mongo_timings:
            return {"min": 0, "max": 0, "avg": 0, "stddev": 0}
        return self._calc_stats(self.mongo_timings)

    def _calc_stats(self, timings: list[float]) -> dict[str, float]:
        import statistics

        return {
            "min": min(timings),
            "max": max(timings),
            "avg": statistics.mean(timings),
            "stddev": statistics.stdev(timings) if len(timings) > 1 else 0,
        }

    def get_ratio(self):
        """Get Neo/Mongo ratio. Returns None if either side is skipped."""
        if self.neo_skipped or self.mongo_skipped:
            return None
        neo_avg = self.get_neo_stats()["avg"]
        mongo_avg = self.get_mongo_stats()["avg"]
        if mongo_avg == 0:
            return 0
        return neo_avg / mongo_avg

    def get_speedup(self):
        """Get speedup factor (1/ratio). Returns None if either side is skipped."""
        ratio = self.get_ratio()
        if ratio is None or ratio == 0:
            return None
        return 1 / ratio


class BenchmarkReporter:
    """Tracks and reports benchmark timing between NeoSQLite and PyMongo"""

    def __init__(self, iterations: int = 10):
        self.iterations = iterations
        self.results: dict[str, BenchmarkResult] = {}
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None

    def start_category(self, category: str):
        if category not in self.results:
            self.results[category] = BenchmarkResult(category)

    def record_neo_timing(self, category: str, timing_ms: float):
        if category not in self.results:
            self.results[category] = BenchmarkResult(category)
        self.results[category].add_neo_timing(timing_ms)

    def record_mongo_timing(self, category: str, timing_ms: float):
        if category not in self.results:
            self.results[category] = BenchmarkResult(category)
        self.results[category].add_mongo_timing(timing_ms)

    def mark_neo_skipped(self, category: str, reason: str):
        """Mark a category as skipped for NeoSQLite"""
        if category not in self.results:
            self.results[category] = BenchmarkResult(category)
        self.results[category].mark_neo_skipped(reason)

    def mark_mongo_skipped(self, category: str, reason: str):
        """Mark a category as skipped for MongoDB"""
        if category not in self.results:
            self.results[category] = BenchmarkResult(category)
        self.results[category].mark_mongo_skipped(reason)

    def get_total_neo_time(self):
        total = 0
        for result in self.results.values():
            total += sum(result.neo_timings)
        return total

    def get_total_mongo_time(self):
        total = 0
        for result in self.results.values():
            total += sum(result.mongo_timings)
        return total

    def export_markdown(self, filepath: Optional[str] = None) -> str:
        """Export benchmark results to markdown file"""
        if filepath is None:
            version = get_neosqlite_version()
            timestamp = datetime.now().strftime("%Y%m%d%H%M")
            filename = f"Benchmark-NeoSQLite-{version}-{timestamp}.md"
            # Resolve path relative to project root (parent of examples directory)
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
            filepath = os.path.join(project_root, "documents", "benchmarks", filename)

        lines = []
        lines.append("# NeoSQLite Benchmark Results")
        lines.append("")
        lines.append(f"**Date:** {datetime.now().strftime('%Y%m%d%H%M')}")
        lines.append(f"**Iterations:** {self.iterations}")
        lines.append(f"**Version:** {get_neosqlite_version()}")
        lines.append("")
        lines.append("## Summary")
        lines.append("")
        lines.append("| Metric | Value |")
        lines.append("|--------|-------|")

        # Calculate totals excluding partial/skipped categories
        total_neo: float = 0
        total_mongo: float = 0
        valid_categories = 0
        partial_categories = 0
        skipped_categories: list[tuple[str, Optional[str]]] = []

        for result in self.results.values():
            if result.is_fully_skipped():
                skipped_categories.append((result.category, result.skip_reason))
            elif result.is_partial():
                partial_categories += 1
            else:
                total_neo += sum(result.neo_timings)
                total_mongo += sum(result.mongo_timings)
                valid_categories += 1

        speedup = total_mongo / total_neo if total_neo > 0 else 0

        lines.append(f"| Total Categories | {len(self.results)} |")
        lines.append(f"| Valid Comparisons | {valid_categories} |")
        if partial_categories > 0:
            lines.append(
                f"| Partial (one side skipped) | {partial_categories} |"
            )
        if skipped_categories:
            lines.append(f"| Fully Skipped | {len(skipped_categories)} |")
        lines.append(
            f"| Total NeoSQLite Time (valid only) | {total_neo:.0f}ms |"
        )
        lines.append(
            f"| Total MongoDB Time (valid only) | {total_mongo:.0f}ms |"
        )
        lines.append(f"| NeoSQLite Speedup (valid only) | {speedup:.2f}x |")
        lines.append("")
        lines.append("---")
        lines.append("")
        lines.append("## Category Results")
        lines.append("")
        lines.append(
            "| Category | NeoSQLite (ms) | MongoDB (ms) | Ratio (Neo/Mongo) | Speedup |"
        )
        lines.append(
            "|----------|----------------|--------------|-------------------|---------|"
        )

        for category, result in self.results.items():
            neo = result.get_neo_stats()
            mongo = result.get_mongo_stats()
            ratio = result.get_ratio()
            speedup_val = result.get_speedup()

            # Handle partial/skipped results
            if result.is_fully_skipped():
                lines.append(
                    f"| {category} | ⚠️ SKIPPED | ⚠️ SKIPPED | N/A | N/A |"
                )
            elif result.neo_skipped:
                lines.append(
                    f"| {category} | ⚠️ SKIPPED | {mongo['avg']:.2f} | N/A | N/A |"
                )
            elif result.mongo_skipped:
                # When mongo is skipped, show N/A even if some timing was recorded
                # (e.g., session_methods where only start_session() ran)
                lines.append(
                    f"| {category} | {neo['avg']:.2f} | ⚠️ SKIPPED | N/A | N/A |"
                )
            else:
                ratio_str = f"{ratio:.2f}" if ratio is not None else "N/A"
                speedup_str = (
                    f"{speedup_val:.2f}x" if speedup_val is not None else "N/A"
                )
                lines.append(
                    f"| {category} | {neo['avg']:.2f} | {mongo['avg']:.2f} | {ratio_str} | {speedup_str} |"
                )

        lines.append("")

        # Add notes about partial/skipped categories
        if partial_categories > 0 or skipped_categories:
            lines.append("### Notes")
            lines.append("")
            lines.append(
                "⚠️ **SKIPPED** indicates the benchmark was not run for that side."
            )
            lines.append("")
            if partial_categories > 0:
                lines.append("**Partial Comparisons (one side skipped):**")
                lines.append("")
                for category, result in self.results.items():
                    if result.is_partial():
                        skip_side = (
                            "MongoDB" if result.mongo_skipped else "NeoSQLite"
                        )
                        lines.append(
                            f"- **{category}**: {skip_side} skipped - {result.skip_reason}"
                        )
                lines.append("")

        lines.append("---")
        lines.append("")
        lines.append("## Detailed Timing (Iteration-level)")
        lines.append("")

        for category, result in self.results.items():
            if result.is_fully_skipped():
                continue

            neo = result.get_neo_stats()
            mongo = result.get_mongo_stats()
            lines.append(f"### {category}")
            lines.append("")
            lines.append(f"- **Iterations:** {self.iterations}")

            if not result.neo_skipped:
                lines.append(
                    f"- **NeoSQLite:** min={neo['min']:.1f}ms, max={neo['max']:.1f}ms, avg={neo['avg']:.2f}ms, stddev={neo['stddev']:.2f}ms"
                )
            else:
                lines.append(
                    f"- **NeoSQLite:** ⚠️ SKIPPED - {result.skip_reason}"
                )

            if not result.mongo_skipped:
                lines.append(
                    f"- **MongoDB:** min={mongo['min']:.1f}ms, max={mongo['max']:.1f}ms, avg={mongo['avg']:.2f}ms, stddev={mongo['stddev']:.2f}ms"
                )
            else:
                # Show skip reason for MongoDB even if some timing was recorded
                lines.append(f"- **MongoDB:** ⚠️ SKIPPED - {result.skip_reason}")

            lines.append("")

        content = "\n".join(lines)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w") as f:
            f.write(content)

        return filepath

    def export_csv(self, filepath: Optional[str] = None) -> str:
        """Export benchmark results to CSV file"""
        if filepath is None:
            version = get_neosqlite_version()
            timestamp = datetime.now().strftime("%Y%m%d%H%M")
            filename = f"Benchmark-NeoSQLite-{version}-{timestamp}.csv"
            # Resolve path relative to project root (parent of examples directory)
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
            filepath = os.path.join(project_root, "documents", "benchmarks", filename)

        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "Category",
                    "Status",
                    "SkipReason",
                    "NeoSQLite_Avg_ms",
                    "NeoSQLite_Min_ms",
                    "NeoSQLite_Max_ms",
                    "NeoSQLite_StdDev",
                    "MongoDB_Avg_ms",
                    "MongoDB_Min_ms",
                    "MongoDB_Max_ms",
                    "MongoDB_StdDev",
                    "Ratio",
                    "Speedup",
                ]
            )

            for category, result in self.results.items():
                neo = result.get_neo_stats()
                mongo = result.get_mongo_stats()
                ratio = result.get_ratio()
                speedup_val = result.get_speedup()

                # Determine status
                if result.is_fully_skipped():
                    status = "SKIPPED"
                elif result.is_partial():
                    status = "PARTIAL"
                else:
                    status = "VALID"

                skip_reason = result.skip_reason or ""

                # Format values - when skipped, show N/A even if timing was recorded
                neo_avg = (
                    f"{neo['avg']:.2f}" if not result.neo_skipped else "N/A"
                )
                neo_min = (
                    f"{neo['min']:.2f}" if not result.neo_skipped else "N/A"
                )
                neo_max = (
                    f"{neo['max']:.2f}" if not result.neo_skipped else "N/A"
                )
                neo_std = (
                    f"{neo['stddev']:.2f}" if not result.neo_skipped else "N/A"
                )

                mongo_avg = (
                    f"{mongo['avg']:.2f}" if not result.mongo_skipped else "N/A"
                )
                mongo_min = (
                    f"{mongo['min']:.2f}" if not result.mongo_skipped else "N/A"
                )
                mongo_max = (
                    f"{mongo['max']:.2f}" if not result.mongo_skipped else "N/A"
                )
                mongo_std = (
                    f"{mongo['stddev']:.2f}"
                    if not result.mongo_skipped
                    else "N/A"
                )

                ratio_str = f"{ratio:.2f}" if ratio is not None else "N/A"
                speedup_str = (
                    f"{speedup_val:.2f}" if speedup_val is not None else "N/A"
                )

                writer.writerow(
                    [
                        category,
                        status,
                        skip_reason,
                        neo_avg,
                        neo_min,
                        neo_max,
                        neo_std,
                        mongo_avg,
                        mongo_min,
                        mongo_max,
                        mongo_std,
                        ratio_str,
                        speedup_str,
                    ]
                )

        return filepath


# Global benchmark reporter instance (initialized when first used)
benchmark_reporter: BenchmarkReporter | None = None
