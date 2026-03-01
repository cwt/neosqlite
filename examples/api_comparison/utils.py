"""
Utility functions for API comparison tests
"""

from typing import Optional
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure


def test_pymongo_connection() -> Optional[MongoClient]:
    """Test connection to MongoDB"""
    try:
        client: MongoClient = MongoClient(
            "mongodb://localhost:27017/", serverSelectionTimeoutMS=5000
        )
        client.admin.command("ping")
        print("MongoDB connection successful")
        return client
    except ConnectionFailure:
        print("Failed to connect to MongoDB")
        return None
