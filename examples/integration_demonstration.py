"""
Integration example showing how temporary table aggregation can be integrated
into the existing NeoSQLite QueryEngine.
"""

import sys
import os

# Add the neosqlite package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import neosqlite
from neosqlite.temporary_table_aggregation import integrate_with_neosqlite


def demonstrate_integration():
    """Demonstrate how temporary table aggregation can be integrated."""
    print("=== Integration Demonstration ===\n")

    # Create an in-memory database
    with neosqlite.Connection(":memory:") as conn:
        # Get collections
        products = conn.products
        categories = conn.categories

        # Insert sample data
        print("1. Inserting sample data...")
        product_docs = [
            {
                "name": "Laptop",
                "category": "Electronics",
                "tags": ["computer", "portable"],
                "price": 1200,
            },
            {
                "name": "Mouse",
                "category": "Electronics",
                "tags": ["computer", "accessory"],
                "price": 25,
            },
            {
                "name": "Keyboard",
                "category": "Electronics",
                "tags": ["computer", "accessory"],
                "price": 75,
            },
            {
                "name": "Book",
                "category": "Education",
                "tags": ["learning", "paper"],
                "price": 30,
            },
            {
                "name": "Desk",
                "category": "Furniture",
                "tags": ["office", "wood"],
                "price": 300,
            },
        ]
        products.insert_many(product_docs)

        category_docs = [
            {"name": "Electronics", "description": "Electronic devices"},
            {"name": "Education", "description": "Educational materials"},
            {"name": "Furniture", "description": "Home furniture"},
        ]
        categories.insert_many(category_docs)

        print(
            f"   Inserted {len(product_docs)} products and {len(category_docs)} categories\n"
        )

        # Test case 1: Simple pipeline (should use existing optimization)
        print(
            "2. Testing simple pipeline (should use existing SQL optimization)..."
        )
        simple_pipeline = [
            {"$match": {"category": "Electronics"}},
            {"$sort": {"price": 1}},
        ]

        # Use the integrated approach
        try:
            results = integrate_with_neosqlite(
                products.query_engine, simple_pipeline
            )
            print(f"   Integrated approach results count: {len(results)}")
            for doc in results:
                print(f"     {doc['name']}: ${doc['price']}")
        except Exception as e:
            print(f"   Error in integrated approach: {e}")
            import traceback

            traceback.print_exc()

        print()

        # Test case 2: Complex pipeline (should use temporary tables)
        print("3. Testing complex pipeline (should use temporary tables)...")
        complex_pipeline = [
            {"$match": {"category": "Electronics"}},
            {"$unwind": "$tags"},
            {
                "$lookup": {
                    "from": "categories",
                    "localField": "category",
                    "foreignField": "name",
                    "as": "categoryInfo",
                }
            },
            {"$sort": {"tags": 1}},
            {"$limit": 5},
        ]

        # Use the integrated approach
        try:
            results = integrate_with_neosqlite(
                products.query_engine, complex_pipeline
            )
            print(f"   Integrated approach results count: {len(results)}")
            for doc in results:
                print(f"     {doc['name']}: {doc['tags']}")
                if doc.get("categoryInfo"):
                    for cat_info in doc["categoryInfo"]:
                        print(
                            f"       Category: {cat_info.get('description', 'N/A')}"
                        )
        except Exception as e:
            print(f"   Error in integrated approach: {e}")
            import traceback

            traceback.print_exc()

        print()

        # Test case 3: Pipeline that would fall back to Python
        print(
            "4. Testing pipeline with unsupported stages (should fall back to Python)..."
        )
        unsupported_pipeline = [{"$project": {"name": 1, "price": 1, "_id": 0}}]

        # Use the integrated approach
        try:
            results = integrate_with_neosqlite(
                products.query_engine, unsupported_pipeline
            )
            print(f"   Integrated approach results count: {len(results)}")
            for doc in results[:3]:  # Show first 3 results
                print(f"     {doc}")
        except Exception as e:
            print(f"   Error in integrated approach: {e}")
            import traceback

            traceback.print_exc()

        print()

        print("5. Integration benefits:")
        print("   • Uses existing SQL optimization for simple pipelines")
        print(
            "   • Uses temporary tables for complex pipelines that current implementation can't optimize"
        )
        print("   • Falls back to Python for unsupported operations")
        print("   • Maintains full backward compatibility")
        print("   • Provides better performance for a wider range of pipelines")
        print("   • Automatic resource management with guaranteed cleanup")

        print("\n6. How to integrate into NeoSQLite:")
        print(
            "   • Replace the fallback Python processing in QueryEngine.aggregate_with_constraints"
        )
        print("   • Add the temporary table approach as an intermediate step")
        print("   • Maintain the existing SQL optimization as the first choice")
        print("   • Keep the Python fallback as the last resort")


if __name__ == "__main__":
    demonstrate_integration()
    print("\n=== Demonstration Complete ===")
