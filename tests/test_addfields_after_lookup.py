"""Test for $addFields after $lookup bug fix (issue from NEOSQLITE_BUG_REPORT.md)."""

import pytest

import neosqlite


class TestAddFieldsAfterLookup:
    """Test that $addFields works correctly after $lookup stage."""

    def test_addfields_after_lookup_basic(self):
        """Test that basic $addFields works after $lookup without SQL errors."""
        db = neosqlite.Connection(":memory:")
        db.posts.insert_one({"_id": 1, "title": "Post 1"})
        db.comments.insert_one({"_id": 1, "text": "Great post!", "post_id": 1})
        db.comments.insert_one({"_id": 2, "text": "Thanks", "post_id": 1})

        # Simple $addFields that copies a field - should not error
        pipeline = [
            {"$match": {"_id": 1}},
            {
                "$lookup": {
                    "from": "comments",
                    "localField": "_id",
                    "foreignField": "post_id",
                    "as": "comments",
                }
            },
            {"$addFields": {"post_title": "$title"}},
        ]

        results = list(db.posts.aggregate(pipeline))
        assert len(results) == 1
        post = results[0]

        # Should have added the post_title field
        assert "post_title" in post
        assert post["post_title"] == "Post 1"
        assert len(post["comments"]) == 2

    def test_addfields_after_lookup_with_filter(self):
        """Test that $addFields with $filter works after $lookup."""
        db = neosqlite.Connection(":memory:")
        db.blog_posts.insert_one(
            {"_id": 1, "title": "Test", "datetime": "2024-01-01"}
        )
        db.blog_comments.insert_one(
            {
                "_id": 1,
                "comment_author": "alice",
                "parent_post": 1,
                "datetime": "2024-01-01T10:00:00",
            }
        )
        db.blog_comments.insert_one(
            {
                "_id": 2,
                "comment_author": "bob",
                "parent_post": 1,
                "datetime": "2024-01-01T09:00:00",
            }
        )
        db.blog_comments.insert_one(
            {
                "_id": 3,
                "comment_author": "charlie",
                "parent_post": 1,
                "datetime": "2024-01-01T08:00:00",
            }
        )

        # Pipeline that was failing: $lookup + $addFields with $filter
        pipeline = [
            {"$match": {"_id": 1}},
            {
                "$lookup": {
                    "from": "blog_comments",
                    "localField": "_id",
                    "foreignField": "parent_post",
                    "as": "comments",
                }
            },
            {
                "$addFields": {
                    "comments": {
                        "$filter": {
                            "input": "$comments",
                            "as": "comment",
                            "cond": {
                                "$in": [
                                    "$$comment.comment_author",
                                    ["alice", "bob"],
                                ]
                            },
                        }
                    }
                }
            },
        ]

        results = list(db.blog_posts.aggregate(pipeline))
        assert len(results) == 1
        post = results[0]

        # Should have filtered to only alice and bob (not charlie)
        assert len(post["comments"]) == 2
        authors = {c["comment_author"] for c in post["comments"]}
        assert authors == {"alice", "bob"}

    def test_addfields_after_lookup_with_filter_single_value(self):
        """Test $filter with $in using a single value in the list."""
        db = neosqlite.Connection(":memory:")
        db.posts.insert_one({"_id": 1, "title": "Test"})
        db.comments.insert_many(
            [
                {"_id": i, "author": f"user{i}", "post_id": 1}
                for i in range(1, 6)
            ]
        )

        pipeline = [
            {"$match": {"_id": 1}},
            {
                "$lookup": {
                    "from": "comments",
                    "localField": "_id",
                    "foreignField": "post_id",
                    "as": "comments",
                }
            },
            {
                "$addFields": {
                    "comments": {
                        "$filter": {
                            "input": "$comments",
                            "as": "c",
                            "cond": {"$in": ["$$c.author", ["user1", "user3"]]},
                        }
                    }
                }
            },
        ]

        results = list(db.posts.aggregate(pipeline))
        assert len(results) == 1
        assert len(results[0]["comments"]) == 2
        authors = {c["author"] for c in results[0]["comments"]}
        assert authors == {"user1", "user3"}

    def test_addfields_after_lookup_no_sql_error(self):
        """Test that no SQL error is raised during $addFields after $lookup."""
        db = neosqlite.Connection(":memory:")
        db.collection1.insert_one({"_id": 1, "field1": "value1"})
        db.collection2.insert_one({"_id": 1, "ref_id": 1, "field2": "value2"})

        # This should not raise sqlite3.OperationalError: no such column: _id
        pipeline = [
            {"$match": {"_id": 1}},
            {
                "$lookup": {
                    "from": "collection2",
                    "localField": "_id",
                    "foreignField": "ref_id",
                    "as": "joined",
                }
            },
            {"$addFields": {"new_field": "$field1"}},
        ]

        # Should not raise an error
        results = list(db.collection1.aggregate(pipeline))
        assert len(results) == 1
        assert "new_field" in results[0]
        assert results[0]["new_field"] == "value1"

    def test_addfields_after_lookup_stays_in_tier2(self):
        """Test that $addFields with complex expressions stays in Tier 2 (temp tables).

        This verifies the hybrid Python approach works correctly without
        falling back to Tier 3 (full Python fallback).
        """
        db = neosqlite.Connection(":memory:")
        db.posts.insert_one({"_id": 1, "title": "Test"})
        db.comments.insert_one({"_id": 1, "author": "alice", "post_id": 1})
        db.comments.insert_one({"_id": 2, "author": "bob", "post_id": 1})

        # Track which tier is used
        tier_used = []

        def tier_callback(old_tier, new_tier, pipeline):
            tier_used.append(new_tier)

        # Register the callback
        query_engine = db.posts.query_engine
        query_engine.add_tier_change_callback(tier_callback)

        pipeline = [
            {"$match": {"_id": 1}},
            {
                "$lookup": {
                    "from": "comments",
                    "localField": "_id",
                    "foreignField": "post_id",
                    "as": "comments",
                }
            },
            {
                "$addFields": {
                    "comments": {
                        "$filter": {
                            "input": "$comments",
                            "as": "c",
                            "cond": {"$in": ["$$c.author", ["alice"]]},
                        }
                    }
                }
            },
        ]

        results = list(db.posts.aggregate(pipeline))

        # Verify we got correct results
        assert len(results) == 1
        assert len(results[0]["comments"]) == 1
        assert results[0]["comments"][0]["author"] == "alice"

        # Verify we stayed in Tier 2 (hybrid approach)
        # If we fell back to Tier 3, we would see 'tier3' in the list
        assert (
            "tier2" in tier_used
        ), f"Expected Tier 2 but got tiers: {tier_used}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
