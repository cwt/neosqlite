"""
Compatibility Reporter - Tracks and reports API compatibility between NeoSQLite and PyMongo
"""

import json
from typing import Any, Optional


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
