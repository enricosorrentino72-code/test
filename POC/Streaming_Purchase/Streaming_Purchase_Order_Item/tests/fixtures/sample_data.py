"""
Sample data fixtures for testing.

Provides reusable test data for:
- Purchase order items
- EventHub messages
- DQX validation scenarios
- Performance testing datasets
"""

import json
from datetime import datetime, timedelta
from typing import List, Dict, Any
import random
import uuid


class SampleDataGenerator:
    """Generator for various types of sample test data."""

    @staticmethod
    def generate_purchase_order_items(count: int = 100) -> List[Dict[str, Any]]:
        """Generate sample purchase order items."""
        items = []
        statuses = ["NEW", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]
        item_categories = ["ELECTRONICS", "CLOTHING", "BOOKS", "HOME", "SPORTS"]

        for i in range(count):
            item = {
                "order_id": f"ORD{i:08d}",
                "line_item_id": f"LINE{i:06d}",
                "item_id": f"ITEM{(i % 1000):05d}",
                "item_name": f"Test Item {i}",
                "category": random.choice(item_categories),
                "quantity": random.randint(1, 100),
                "unit_price": round(random.uniform(10.0, 1000.0), 2),
                "total_amount": 0,  # Will be calculated
                "store_id": f"STORE{(i % 50):03d}",
                "customer_id": f"CUST{(i % 10000):06d}",
                "order_date": (datetime.now() - timedelta(days=random.randint(0, 365))).isoformat(),
                "status": random.choice(statuses),
                "created_timestamp": datetime.now().isoformat(),
                "updated_timestamp": datetime.now().isoformat()
            }

            # Calculate total amount
            item["total_amount"] = round(item["quantity"] * item["unit_price"], 2)
            items.append(item)

        return items

    @staticmethod
    def generate_eventhub_messages(count: int = 50) -> List[Dict[str, Any]]:
        """Generate sample EventHub messages."""
        messages = []

        for i in range(count):
            message = {
                "message_id": str(uuid.uuid4()),
                "event_type": "purchase_order_created",
                "timestamp": datetime.now().isoformat(),
                "partition_key": f"store_{i % 10}",
                "data": {
                    "order_id": f"ORD{i:08d}",
                    "store_id": f"STORE{(i % 10):03d}",
                    "items": SampleDataGenerator.generate_purchase_order_items(random.randint(1, 5))
                },
                "metadata": {
                    "source": "test_system",
                    "version": "1.0",
                    "correlation_id": str(uuid.uuid4())
                }
            }
            messages.append(message)

        return messages

    @staticmethod
    def generate_dqx_test_data() -> Dict[str, List[Dict[str, Any]]]:
        """Generate data for DQX validation testing."""

        # Valid records
        valid_records = []
        for i in range(50):
            record = {
                "order_id": f"ORD{i:08d}",
                "item_id": f"ITEM{i:05d}",
                "quantity": random.randint(1, 100),
                "amount": round(random.uniform(10.0, 1000.0), 2),
                "email": f"customer{i}@example.com",
                "phone": f"{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
                "timestamp": datetime.now().isoformat(),
                "status": random.choice(["NEW", "PROCESSING", "SHIPPED"])
            }
            valid_records.append(record)

        # Invalid records for testing DQX rules
        invalid_records = [
            # Null values
            {"order_id": None, "item_id": "ITEM001", "quantity": 10, "amount": 100.0, "email": "test@example.com", "phone": "123-456-7890", "timestamp": datetime.now().isoformat(), "status": "NEW"},
            {"order_id": "ORD001", "item_id": None, "quantity": 10, "amount": 100.0, "email": "test@example.com", "phone": "123-456-7890", "timestamp": datetime.now().isoformat(), "status": "NEW"},

            # Invalid quantity
            {"order_id": "ORD002", "item_id": "ITEM002", "quantity": -5, "amount": 100.0, "email": "test@example.com", "phone": "123-456-7890", "timestamp": datetime.now().isoformat(), "status": "NEW"},
            {"order_id": "ORD003", "item_id": "ITEM003", "quantity": 0, "amount": 100.0, "email": "test@example.com", "phone": "123-456-7890", "timestamp": datetime.now().isoformat(), "status": "NEW"},

            # Invalid email
            {"order_id": "ORD004", "item_id": "ITEM004", "quantity": 10, "amount": 100.0, "email": "invalid-email", "phone": "123-456-7890", "timestamp": datetime.now().isoformat(), "status": "NEW"},
            {"order_id": "ORD005", "item_id": "ITEM005", "quantity": 10, "amount": 100.0, "email": "test@", "phone": "123-456-7890", "timestamp": datetime.now().isoformat(), "status": "NEW"},

            # Invalid phone
            {"order_id": "ORD006", "item_id": "ITEM006", "quantity": 10, "amount": 100.0, "email": "test@example.com", "phone": "invalid-phone", "timestamp": datetime.now().isoformat(), "status": "NEW"},
            {"order_id": "ORD007", "item_id": "ITEM007", "quantity": 10, "amount": 100.0, "email": "test@example.com", "phone": "123-45-6789", "timestamp": datetime.now().isoformat(), "status": "NEW"},

            # Invalid status
            {"order_id": "ORD008", "item_id": "ITEM008", "quantity": 10, "amount": 100.0, "email": "test@example.com", "phone": "123-456-7890", "timestamp": datetime.now().isoformat(), "status": "INVALID_STATUS"},

            # Invalid amount
            {"order_id": "ORD009", "item_id": "ITEM009", "quantity": 10, "amount": -50.0, "email": "test@example.com", "phone": "123-456-7890", "timestamp": datetime.now().isoformat(), "status": "NEW"},
        ]

        return {
            "valid_records": valid_records,
            "invalid_records": invalid_records,
            "all_records": valid_records + invalid_records
        }

    @staticmethod
    def generate_performance_test_data(size: str = "small") -> List[Dict[str, Any]]:
        """Generate data for performance testing."""
        sizes = {
            "small": 1000,
            "medium": 10000,
            "large": 100000,
            "xlarge": 1000000
        }

        count = sizes.get(size, 1000)
        return SampleDataGenerator.generate_purchase_order_items(count)

    @staticmethod
    def generate_bronze_layer_data(count: int = 1000) -> List[Dict[str, Any]]:
        """Generate raw data for bronze layer testing."""
        raw_data = []

        for i in range(count):
            # Simulate raw, potentially messy data
            record = {
                "raw_order_id": f"ORD{i:08d}",
                "raw_item_data": json.dumps({
                    "item_id": f"ITEM{i:05d}",
                    "name": f"Item {i}",
                    "price": random.uniform(10.0, 1000.0)
                }),
                "raw_quantity": str(random.randint(1, 100)),  # String instead of int
                "raw_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "raw_metadata": {
                    "source_system": "legacy_system",
                    "batch_id": f"BATCH{(i // 100):04d}",
                    "ingestion_time": datetime.now().isoformat()
                },
                "_corrupt_record": None if i % 100 != 0 else "corrupted_data_here"  # Simulate corruption
            }
            raw_data.append(record)

        return raw_data

    @staticmethod
    def generate_silver_layer_data(count: int = 1000) -> List[Dict[str, Any]]:
        """Generate cleaned data for silver layer testing."""
        silver_data = []

        for i in range(count):
            record = {
                "order_id": f"ORD{i:08d}",
                "line_item_id": f"LINE{i:06d}",
                "item_id": f"ITEM{i:05d}",
                "item_name": f"Cleaned Item {i}",
                "quantity": random.randint(1, 100),
                "unit_price": round(random.uniform(10.0, 1000.0), 2),
                "total_amount": 0,  # Will be calculated
                "store_id": f"STORE{(i % 50):03d}",
                "order_date": datetime.now().date().isoformat(),
                "processed_timestamp": datetime.now().isoformat(),
                "data_quality_score": round(random.uniform(0.8, 1.0), 2),
                "dqx_validation_flags": {
                    "completeness": True,
                    "validity": True,
                    "consistency": True,
                    "uniqueness": True
                }
            }

            record["total_amount"] = round(record["quantity"] * record["unit_price"], 2)
            silver_data.append(record)

        return silver_data

    @staticmethod
    def generate_error_scenarios() -> Dict[str, List[Dict[str, Any]]]:
        """Generate data for error handling testing."""
        return {
            "connection_errors": [
                {"error_type": "network_timeout", "retry_count": 3, "max_retries": 5},
                {"error_type": "connection_refused", "retry_count": 1, "max_retries": 3},
                {"error_type": "dns_resolution_failed", "retry_count": 0, "max_retries": 1}
            ],
            "data_errors": [
                {"error_type": "schema_mismatch", "expected_schema": "v2", "actual_schema": "v1"},
                {"error_type": "data_corruption", "corrupted_fields": ["quantity", "amount"]},
                {"error_type": "missing_required_field", "missing_fields": ["order_id"]}
            ],
            "resource_errors": [
                {"error_type": "memory_exhausted", "available_memory": "512MB", "required_memory": "1GB"},
                {"error_type": "disk_full", "available_space": "0GB", "required_space": "10GB"},
                {"error_type": "cpu_overload", "cpu_usage": "95%", "threshold": "80%"}
            ]
        }

    @staticmethod
    def save_sample_data_to_files(output_dir: str = "./fixtures/data/"):
        """Save all sample data to JSON files for testing."""
        import os

        os.makedirs(output_dir, exist_ok=True)

        # Generate and save different types of sample data
        datasets = {
            "purchase_order_items.json": SampleDataGenerator.generate_purchase_order_items(100),
            "eventhub_messages.json": SampleDataGenerator.generate_eventhub_messages(50),
            "dqx_test_data.json": SampleDataGenerator.generate_dqx_test_data(),
            "performance_small.json": SampleDataGenerator.generate_performance_test_data("small"),
            "bronze_layer_data.json": SampleDataGenerator.generate_bronze_layer_data(1000),
            "silver_layer_data.json": SampleDataGenerator.generate_silver_layer_data(1000),
            "error_scenarios.json": SampleDataGenerator.generate_error_scenarios()
        }

        for filename, data in datasets.items():
            filepath = os.path.join(output_dir, filename)
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2, default=str)

        print(f"Sample data files created in {output_dir}")


# Convenience functions for common test scenarios
def get_valid_purchase_order():
    """Get a single valid purchase order for testing."""
    return SampleDataGenerator.generate_purchase_order_items(1)[0]


def get_invalid_purchase_order():
    """Get a single invalid purchase order for testing."""
    return SampleDataGenerator.generate_dqx_test_data()["invalid_records"][0]


def get_sample_eventhub_message():
    """Get a single sample EventHub message for testing."""
    return SampleDataGenerator.generate_eventhub_messages(1)[0]


def get_dqx_test_dataset():
    """Get the complete DQX test dataset."""
    return SampleDataGenerator.generate_dqx_test_data()


# Schema definitions for validation
PURCHASE_ORDER_SCHEMA = {
    "type": "object",
    "properties": {
        "order_id": {"type": "string", "pattern": "^ORD[0-9]{8}$"},
        "line_item_id": {"type": "string", "pattern": "^LINE[0-9]{6}$"},
        "item_id": {"type": "string", "pattern": "^ITEM[0-9]{5}$"},
        "quantity": {"type": "integer", "minimum": 1},
        "unit_price": {"type": "number", "minimum": 0},
        "total_amount": {"type": "number", "minimum": 0},
        "store_id": {"type": "string", "pattern": "^STORE[0-9]{3}$"},
        "status": {"type": "string", "enum": ["NEW", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]}
    },
    "required": ["order_id", "item_id", "quantity", "unit_price"]
}

EVENTHUB_MESSAGE_SCHEMA = {
    "type": "object",
    "properties": {
        "message_id": {"type": "string"},
        "event_type": {"type": "string"},
        "timestamp": {"type": "string"},
        "data": {"type": "object"},
        "metadata": {"type": "object"}
    },
    "required": ["message_id", "event_type", "timestamp", "data"]
}