"""
Compatibility Reporter - Tracks and reports API compatibility between NeoSQLite and PyMongo
"""

from typing import Any, Optional


class CompatibilityReporter:
    """Tracks and reports API compatibility between NeoSQLite and PyMongo"""

    def __init__(self):
        self.total_tests = 0
        self.passed_tests = 0
        self.failed_tests = []
        self.skipped_tests = []

    def record_result(
        self,
        category: str,
        api_name: str,
        passed: bool,
        neo_result: Any = None,
        mongo_result: Any = None,
        error: Optional[str] = None,
        skip_reason: Optional[str] = None,
    ):
        """Record a test result"""
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

    def print_report(self):
        """Print the compatibility report"""
        print("\n" + "=" * 80)
        print("COMPATIBILITY REPORT")
        print("=" * 80)
        print(f"Total Tests: {self.total_tests}")
        print(f"Passed: {self.passed_tests}")
        print(f"Skipped: {len(self.skipped_tests)}")
        print(f"Failed: {len(self.failed_tests)}")
        print(f"Compatibility: {self.get_compatibility_percentage():.1f}%")
        print("=" * 80)

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
