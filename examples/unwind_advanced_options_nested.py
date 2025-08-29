#!/usr/bin/env python3
"""
Example demonstrating advanced $unwind options with nested arrays in NeoSQLite.

This example shows how the advanced options work with deeply nested array structures.
"""

import neosqlite


def main():
    print("=== NeoSQLite Advanced $unwind Options - Nested Arrays ===\n")

    # Create an in-memory database
    with neosqlite.Connection(":memory:") as conn:
        # Get a collection
        orders = conn.orders

        # Insert sample data with deeply nested arrays
        sample_docs = [
            {
                "_id": 1,
                "customer": "Alice",
                "orderGroups": [
                    {
                        "groupName": "Electronics",
                        "shipments": [
                            {
                                "shipmentId": "S001",
                                "items": [
                                    {"name": "Laptop", "quantity": 1},
                                    {"name": "Mouse", "quantity": 2},
                                ],
                            },
                            {
                                "shipmentId": "S002",
                                "items": [{"name": "Phone", "quantity": 1}],
                            },
                        ],
                    },
                    {
                        "groupName": "Books",
                        "shipments": [
                            {
                                "shipmentId": "S003",
                                "items": [
                                    {"name": "Novel", "quantity": 3},
                                    {"name": "Magazine", "quantity": 3},
                                ],
                            }
                        ],
                    },
                ],
            },
            {
                "_id": 2,
                "customer": "Bob",
                "orderGroups": [
                    {
                        "groupName": "Clothing",
                        "shipments": [],  # Empty shipments array
                    }
                ],
            },
            {
                "_id": 3,
                "customer": "Charlie",
                "orderGroups": [],  # Empty orderGroups array
            },
        ]

        orders.insert_many(sample_docs)
        print("1. Inserted sample order data:")
        for doc in orders.find():
            print(f"   {doc}")
        print()

        # Example 1: Deep nested unwind with includeArrayIndex at all levels
        print(
            "2. Example: Deep nested unwind with includeArrayIndex at all levels"
        )
        print(
            "   Pipeline: Unwind orderGroups, shipments, and items with indices"
        )
        pipeline1 = [
            {
                "$unwind": {
                    "path": "$orderGroups",
                    "includeArrayIndex": "groupIndex",
                }
            },
            {
                "$unwind": {
                    "path": "$orderGroups.shipments",
                    "includeArrayIndex": "shipmentIndex",
                }
            },
            {
                "$unwind": {
                    "path": "$orderGroups.shipments.items",
                    "includeArrayIndex": "itemIndex",
                }
            },
        ]

        result1 = orders.aggregate(pipeline1)
        print("   Results (all levels unwound with indices):")
        for doc in result1:
            customer = doc["customer"]
            group = doc["orderGroups"]["groupName"]
            shipment = doc["orderGroups"]["shipments"]["shipmentId"]
            item = doc["orderGroups"]["shipments"]["items"]["name"]
            quantity = doc["orderGroups"]["shipments"]["items"]["quantity"]
            group_idx = doc["groupIndex"]
            shipment_idx = doc["shipmentIndex"]
            item_idx = doc["itemIndex"]
            print(
                f"     {customer}: Group '{group}'[{group_idx}], Shipment '{shipment}'[{shipment_idx}], Item '{item}'[{item_idx}] (qty: {quantity})"
            )
        print()

        # Example 2: Deep nested unwind with preserveNullAndEmptyArrays
        print("3. Example: Deep nested unwind with preserveNullAndEmptyArrays")
        print("   Pipeline: Unwind all levels, preserve empty arrays")
        pipeline2 = [
            {
                "$unwind": {
                    "path": "$orderGroups",
                    "preserveNullAndEmptyArrays": True,
                }
            },
            {
                "$unwind": {
                    "path": "$orderGroups.shipments",
                    "preserveNullAndEmptyArrays": True,
                }
            },
            {
                "$unwind": {
                    "path": "$orderGroups.shipments.items",
                    "preserveNullAndEmptyArrays": True,
                }
            },
        ]

        result2 = orders.aggregate(pipeline2)
        print("   Results (preserving documents with empty arrays):")
        for doc in result2:
            customer = doc["customer"]
            if "orderGroups" in doc and doc["orderGroups"] is not None:
                group = doc["orderGroups"]["groupName"]
                if (
                    "shipments" in doc["orderGroups"]
                    and doc["orderGroups"]["shipments"] is not None
                ):
                    shipment = doc["orderGroups"]["shipments"]["shipmentId"]
                    if (
                        "items" in doc["orderGroups"]["shipments"]
                        and doc["orderGroups"]["shipments"]["items"] is not None
                    ):
                        item = doc["orderGroups"]["shipments"]["items"]["name"]
                        quantity = doc["orderGroups"]["shipments"]["items"][
                            "quantity"
                        ]
                        print(
                            f"     {customer}: Group '{group}', Shipment '{shipment}', Item '{item}' (qty: {quantity})"
                        )
                    else:
                        print(
                            f"     {customer}: Group '{group}', Shipment '{shipment}', Item: null (empty items)"
                        )
                else:
                    print(
                        f"     {customer}: Group '{group}', Shipment: null (empty shipments)"
                    )
            else:
                print(f"     {customer}: Group: null (empty orderGroups)")
        print()

        # Example 3: Mixed options in nested unwind
        print("4. Example: Mixed options in nested unwind")
        print(
            "   Pipeline: Include indices at top level, preserve arrays at lower levels"
        )
        pipeline3 = [
            {
                "$unwind": {
                    "path": "$orderGroups",
                    "includeArrayIndex": "groupIndex",
                }
            },
            {
                "$unwind": {
                    "path": "$orderGroups.shipments",
                    "preserveNullAndEmptyArrays": True,
                }
            },
            {
                "$unwind": {
                    "path": "$orderGroups.shipments.items",
                    "includeArrayIndex": "itemIndex",
                }
            },
        ]

        result3 = orders.aggregate(pipeline3)
        print("   Results (mixed options - indices and preservation):")
        for doc in result3:
            customer = doc["customer"]
            group = doc["orderGroups"]["groupName"]
            group_idx = doc["groupIndex"]
            if (
                "shipments" in doc["orderGroups"]
                and doc["orderGroups"]["shipments"] is not None
            ):
                shipment = doc["orderGroups"]["shipments"]["shipmentId"]
                if (
                    "items" in doc["orderGroups"]["shipments"]
                    and doc["orderGroups"]["shipments"]["items"] is not None
                ):
                    item = doc["orderGroups"]["shipments"]["items"]["name"]
                    quantity = doc["orderGroups"]["shipments"]["items"][
                        "quantity"
                    ]
                    item_idx = doc["itemIndex"]
                    print(
                        f"     {customer}: Group '{group}'[{group_idx}], Shipment '{shipment}', Item '{item}'[{item_idx}] (qty: {quantity})"
                    )
                else:
                    print(
                        f"     {customer}: Group '{group}'[{group_idx}], Shipment '{shipment}', Item: null"
                    )
            else:
                print(
                    f"     {customer}: Group '{group}'[{group_idx}], Shipment: null"
                )
        print()

        print("=== Key Observations ===")
        print("• Advanced options work at any level of nested array unwinding")
        print("• includeArrayIndex provides positional context at each level")
        print(
            "• preserveNullAndEmptyArrays prevents data loss in complex structures"
        )
        print(
            "• Options can be mixed and matched at different unwinding levels"
        )
        print("• Deep nesting increases result set size multiplicatively")


if __name__ == "__main__":
    main()
    print("\n=== Example Complete ===")
