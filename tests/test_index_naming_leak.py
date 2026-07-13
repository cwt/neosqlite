import neosqlite


def test_index_and_search_index_naming_isolation():
    # Setup two collections whose names overlap, e.g., 'foo' and 'foo_bar'
    with neosqlite.Connection(":memory:") as conn:
        foo = conn.foo
        foo_bar = conn.foo_bar

        # Create indexes on foo_bar
        foo_bar.create_index("age")
        foo_bar.create_index("bio", fts=True)

        # foo has no indexes (other than internal _id index which is ignored/filtered)
        assert len(foo.list_indexes()) == 0
        assert len(foo.list_search_indexes()) == 0

        # Verify that listing indexes on foo does not leak/return foo_bar's indexes
        foo_indexes = foo.list_indexes()
        assert len(foo_indexes) == 0

        foo_search_indexes = foo.list_search_indexes()
        assert len(foo_search_indexes) == 0
