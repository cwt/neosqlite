import time
import neosqlite

# --- Configuration ---
NUM_DOCUMENTS = 100000
NUM_GROUPS = 100
# -------------------


def run_benchmark():
    """
    Compares the performance of SQL-optimized vs. Python-based $group aggregation.
    """
    print("--- $group Performance Benchmark ---")
    print(f"Preparing {NUM_DOCUMENTS} documents for {NUM_GROUPS} groups...")

    with neosqlite.Connection(":memory:") as conn:
        collection = conn["performance_test"]

        # 1. Insert test data
        docs = [
            {"store_id": i % NUM_GROUPS, "price": float(i)}
            for i in range(NUM_DOCUMENTS)
        ]
        collection.insert_many(docs)
        print("Data preparation complete.\n")

        # 2. Define the pipelines
        # This pipeline is simple and will trigger the SQL GROUP BY optimization.
        pipeline_optimized = [
            {"$group": {"_id": "$store_id", "total": {"$sum": "$price"}}}
        ]

        # By adding a no-op stage before $group, we force the implementation
        # to fall back to the slower, Python-based processing.
        pipeline_fallback = [
            {"$skip": 0},  # This forces the fallback
            {"$group": {"_id": "$store_id", "total": {"$sum": "$price"}}},
        ]

        # 3. Benchmark the optimized path
        start_time_optimized = time.perf_counter()
        result_optimized = collection.aggregate(pipeline_optimized)
        end_time_optimized = time.perf_counter()
        duration_optimized = end_time_optimized - start_time_optimized
        print(
            f"Optimized (SQL GROUP BY) time: {duration_optimized:.4f} seconds"
        )
        assert len(result_optimized) == NUM_GROUPS

        # 4. Benchmark the fallback path
        start_time_fallback = time.perf_counter()
        result_fallback = collection.aggregate(pipeline_fallback)
        end_time_fallback = time.perf_counter()
        duration_fallback = end_time_fallback - start_time_fallback
        print(f"Fallback (Python) time:       {duration_fallback:.4f} seconds")
        assert len(result_fallback) == NUM_GROUPS

        # 5. Show the results
        print("-" * 35)
        if duration_optimized > 0:
            improvement = duration_fallback / duration_optimized
            print(f"Performance Improvement: {improvement:.2f}x faster")
        else:
            print("Optimized version was too fast to measure improvement.")
        print("-" * 35)


if __name__ == "__main__":
    run_benchmark()
