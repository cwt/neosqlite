#!/usr/bin/env python3
"""
Example script demonstrating nested array unwinding in neosqlite.
"""

import neosqlite


def main():
    # Create an in-memory database
    with neosqlite.Connection(":memory:") as conn:
        # Get a collection
        sales = conn.sales

        # Insert sample data with nested arrays
        sales.insert_many(
            [
                {
                    "_id": 1,
                    "region": "East",
                    "orders": [
                        {
                            "orderId": "E001",
                            "items": [
                                {"product": "Book", "quantity": 2, "price": 10},
                                {"product": "Pen", "quantity": 5, "price": 1},
                            ],
                        }
                    ],
                },
                {
                    "_id": 2,
                    "region": "West",
                    "orders": [
                        {
                            "orderId": "W001",
                            "items": [
                                {
                                    "product": "Desk",
                                    "quantity": 1,
                                    "price": 100,
                                },
                                {
                                    "product": "Chair",
                                    "quantity": 2,
                                    "price": 50,
                                },
                            ],
                        }
                    ],
                },
            ]
        )

        print("All sales documents:")
        for sale in sales.find():
            print(f"  {sale}")

        print("\nUnwinding orders and items:")
        pipeline = [
            {"$unwind": "$orders"},
            {"$unwind": "$orders.items"},
        ]
        for doc in sales.aggregate(pipeline):
            print(f"  {doc}")

        print("\nUnwinding orders and items, sorted by price:")
        pipeline = [
            {"$unwind": "$orders"},
            {"$unwind": "$orders.items"},
            {"$sort": {"orders.items.price": -1}},
        ]
        for doc in sales.aggregate(pipeline):
            print(f"  {doc}")


if __name__ == "__main__":
    main()
