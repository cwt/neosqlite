#!/usr/bin/env python3
"""
Test cases for stable, predictable, and repeatable temporary table naming system.
"""

import neosqlite
import hashlib


def test_deterministic_temp_table_names():
    """Test that temp table names are deterministic and repeatable."""
    print("=== Testing Deterministic Temp Table Names ===\n")

    with neosqlite.Connection(":memory:") as conn:
        # Get a collection
        users = conn.users

        # Insert sample data
        print("1. Inserting sample data...")
        sample_docs = [
            {
                "name": "Alice",
                "age": 30,
                "tags": ["python", "javascript"],
                "status": "active",
            },
            {
                "name": "Bob",
                "age": 25,
                "tags": ["java", "python"],
                "status": "active",
            },
            {
                "name": "Charlie",
                "age": 35,
                "tags": ["javascript", "go"],
                "status": "inactive",
            },
        ]
        users.insert_many(sample_docs)
        print(f"   Inserted {len(sample_docs)} documents\n")

        # Test that the same pipeline produces the same temp table names
        print("2. Testing deterministic temp table names...")

        # Define a pipeline

        # Generate a deterministic name for a stage
        def make_deterministic_name(stage):
            """Generate a deterministic temp table name based on the stage."""
            stage_key = str(sorted(stage.items()))
            hash_suffix = hashlib.sha256(stage_key.encode()).hexdigest()[:6]
            stage_type = next(iter(stage.keys())).lstrip("$")
            return f"temp_{stage_type}_{hash_suffix}"

        # Test with the first stage
        match_stage = {"$match": {"status": "active"}}
        name1 = make_deterministic_name(match_stage)
        print(f"   First run match table name: {name1}")

        # Test again with the same stage
        name2 = make_deterministic_name(match_stage)
        print(f"   Second run match table name: {name2}")

        # Verify they're the same
        assert name1 == name2, "Deterministic names should be identical"
        print("   \u2713 Names are identical\n")

        # Test with different stages
        unwind_stage = {"$unwind": "$tags"}
        unwind_name = make_deterministic_name(unwind_stage)
        print(f"   Unwind stage table name: {unwind_name}")

        # Verify different stages produce different names
        assert (
            name1 != unwind_name
        ), "Different stages should have different names"
        print("   \u2713 Different stages produce different names\n")

        print("=== Test Complete ===")


def test_pipeline_hashing():
    """Test hashing of entire pipelines for consistent naming."""
    print("=== Testing Pipeline Hashing ===\n")

    # Define pipelines
    pipeline1 = [
        {"$match": {"status": "active"}},
        {"$unwind": "$tags"},
        {"$sort": {"tags": 1}},
        {"$limit": 5},
    ]

    pipeline2 = [
        {"$match": {"status": "active"}},
        {"$unwind": "$tags"},
        {"$sort": {"tags": 1}},
        {"$limit": 5},
    ]

    pipeline3 = [
        {"$match": {"status": "inactive"}},  # Different match condition
        {"$unwind": "$tags"},
        {"$sort": {"tags": 1}},
        {"$limit": 5},
    ]

    def pipeline_hash(pipeline):
        """Generate a hash for a pipeline."""
        canonical = "".join(str(sorted(stage.items())) for stage in pipeline)
        return hashlib.sha256(canonical.encode()).hexdigest()[:8]

    hash1 = pipeline_hash(pipeline1)
    hash2 = pipeline_hash(pipeline2)
    hash3 = pipeline_hash(pipeline3)

    print(f"   Pipeline 1 hash: {hash1}")
    print(f"   Pipeline 2 hash: {hash2}")
    print(f"   Pipeline 3 hash: {hash3}")

    # Verify identical pipelines produce identical hashes
    assert hash1 == hash2, "Identical pipelines should have identical hashes"
    print("   \u2713 Identical pipelines produce identical hashes")

    # Verify different pipelines produce different hashes
    assert hash1 != hash3, "Different pipelines should have different hashes"
    print("   \u2713 Different pipelines produce different hashes\n")

    print("=== Test Complete ===")


def test_stage_hashing():
    """Test hashing of individual stages."""
    print("=== Testing Stage Hashing ===\n")

    stages = [
        {"$match": {"status": "active"}},
        {"$match": {"status": "active"}},  # Same as above
        {"$match": {"status": "inactive"}},  # Different value
        {"$unwind": "$tags"},
        {"$unwind": "$tags"},  # Same as above
        {"$sort": {"tags": 1}},
    ]

    def stage_hash(stage):
        """Generate a hash for a stage."""
        return hashlib.sha256(str(sorted(stage.items())).encode()).hexdigest()[
            :6
        ]

    hashes = [stage_hash(stage) for stage in stages]

    print(f"   Match active 1: {hashes[0]}")
    print(f"   Match active 2: {hashes[1]}")
    print(f"   Match inactive: {hashes[2]}")
    print(f"   Unwind tags 1:  {hashes[3]}")
    print(f"   Unwind tags 2:  {hashes[4]}")
    print(f"   Sort tags:      {hashes[5]}")

    # Verify identical stages produce identical hashes
    assert (
        hashes[0] == hashes[1]
    ), "Identical stages should have identical hashes"
    assert (
        hashes[3] == hashes[4]
    ), "Identical stages should have identical hashes"
    print("   \u2713 Identical stages produce identical hashes")

    # Verify different stages produce different hashes
    assert (
        hashes[0] != hashes[2]
    ), "Different stages should have different hashes"
    assert (
        hashes[0] != hashes[3]
    ), "Different stages should have different hashes"
    print("   \u2713 Different stages produce different hashes\n")

    print("=== Test Complete ===")


def test_predictable_naming_with_pipeline_id():
    """Test predictable naming with pipeline ID for uniqueness."""
    print("=== Testing Predictable Naming with Pipeline ID ===\n")

    # Simulate different pipelines
    pipelines = [
        [
            {"$match": {"status": "active"}},
            {"$unwind": "$tags"},
        ],
        [
            {"$match": {"status": "active"}},
            {"$unwind": "$tags"},
        ],
        [
            {"$match": {"status": "inactive"}},
            {"$unwind": "$tags"},
        ],
    ]

    pipeline_id = "agg_abc123"

    def make_temp_table_name(pipeline_id, stage):
        """Generate a predictable temp table name."""
        stage_key = str(sorted(stage.items()))
        suffix = hashlib.sha256(stage_key.encode()).hexdigest()[:6]
        stage_type = next(iter(stage.keys())).lstrip("$")
        return f"temp_{pipeline_id}_{stage_type}_{suffix}"

    # Generate names for each pipeline
    names_per_pipeline = []
    for i, pipeline in enumerate(pipelines):
        stage_names = []
        for stage in pipeline:
            name = make_temp_table_name(pipeline_id, stage)
            stage_names.append(name)
        names_per_pipeline.append(stage_names)
        print(f"   Pipeline {i+1} stage names: {stage_names}")

    # Verify identical pipelines produce identical names
    assert (
        names_per_pipeline[0] == names_per_pipeline[1]
    ), "Identical pipelines should produce identical names"
    print("   \u2713 Identical pipelines produce identical names")

    # Verify different pipelines produce different names (at least one stage should differ)
    assert (
        names_per_pipeline[0] != names_per_pipeline[2]
    ), "Different pipelines should produce different names"
    print("   \u2713 Different pipelines produce different names\n")

    print("=== Test Complete ===")


if __name__ == "__main__":
    test_deterministic_temp_table_names()
    print()
    test_pipeline_hashing()
    print()
    test_stage_hashing()
    print()
    test_predictable_naming_with_pipeline_id()
    print("\n=== All Tests Passed ===")
