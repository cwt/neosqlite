import neosqlite
import sqlite3

# Test the new native JSON indexing
with neosqlite.Connection(':memory:') as conn:
    # Get a collection
    users = conn.users
    
    # Insert some documents
    users.insert_one({'name': 'Alice', 'age': 30, 'profile': {'followers': 100}})
    users.insert_one({'name': 'Bob', 'age': 25, 'profile': {'followers': 200}})
    users.insert_one({'name': 'Charlie', 'age': 35, 'profile': {'followers': 150}})
    
    # Create indexes
    users.create_index('name')
    users.create_index('age')
    users.create_index('profile.followers')
    users.create_index(['name', 'age'])  # Compound index
    
    # Check that indexes are created
    indexes = users.list_indexes()
    print("Created indexes:", indexes)
    
    # Test that we can query using the indexes
    result = users.find_one({'name': 'Alice'})
    print("Found Alice:", result)
    
    result = users.find_one({'age': 25})
    print("Found person with age 25:", result)
    
    result = users.find_one({'profile.followers': 150})
    print("Found person with 150 followers:", result)
    
    # Test unique constraint
    try:
        users.create_index('unique_field', unique=True)
        users.insert_one({'unique_field': 'value1'})
        users.insert_one({'unique_field': 'value1'})  # Should fail
    except sqlite3.IntegrityError as e:
        print("Unique constraint working correctly:", str(e))
    
    # Test dropping indexes
    print("Before dropping:", users.list_indexes())
    users.drop_index('name')
    print("After dropping 'name' index:", users.list_indexes())
    
    users.drop_indexes()
    print("After dropping all indexes:", users.list_indexes())