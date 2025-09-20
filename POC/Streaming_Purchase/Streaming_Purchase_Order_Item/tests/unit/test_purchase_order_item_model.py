"""
Unit tests for PurchaseOrderItem data model.

This module contains comprehensive unit tests for the PurchaseOrderItem class,
testing data validation, financial calculations, and business logic.
"""

import pytest
import json
from datetime import datetime, timedelta
from decimal import Decimal

# Import the class under test
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'class'))

from purchase_order_item_model import PurchaseOrderItem


class TestPurchaseOrderItemModel:
    """Comprehensive unit tests for PurchaseOrderItem data model."""

    def test_purchase_order_creation_valid_data(self):
        """Test creating a PurchaseOrderItem with valid data."""
        # Arrange
        order_data = {
            "order_id": "ORD-123456",
            "product_id": "PROD-001",
            "product_name": "Test Product",
            "quantity": 5,
            "unit_price": 10.99,
            "total_amount": 54.95,
            "customer_id": "CUST-001",
            "vendor_id": "VEND-001",
            "warehouse_location": "NYC-01"
        }

        # Act
        order = PurchaseOrderItem(**order_data)

        # Assert
        assert order.order_id == "ORD-123456"
        assert order.product_id == "PROD-001"
        assert order.quantity == 5
        assert order.unit_price == 10.99
        assert order.total_amount == 54.95
        assert order.is_valid is True
        assert order.validation_errors is None
        assert order.data_quality_score == 1.0

    def test_financial_validation_calculations(self):
        """Test financial validation and calculation accuracy."""
        # Test Case 1: Valid calculation
        order = PurchaseOrderItem(
            order_id="ORD-001",
            product_id="PROD-001",
            product_name="Test Product",
            quantity=3,
            unit_price=15.50,
            total_amount=46.50,  # 3 * 15.50 = 46.50
            customer_id="CUST-001",
            vendor_id="VEND-001",
            warehouse_location="NYC-01"
        )
        assert order.is_valid is True
        assert order.validation_errors is None

        # Test Case 2: Invalid calculation
        order_invalid = PurchaseOrderItem(
            order_id="ORD-002",
            product_id="PROD-002",
            product_name="Test Product 2",
            quantity=2,
            unit_price=20.00,
            total_amount=50.00,  # Should be 40.00
            customer_id="CUST-001",
            vendor_id="VEND-001",
            warehouse_location="NYC-01"
        )
        assert order_invalid.is_valid is False
        assert "Total amount mismatch" in order_invalid.validation_errors

        # Test Case 3: With discounts and taxes
        order_complex = PurchaseOrderItem(
            order_id="ORD-003",
            product_id="PROD-003",
            product_name="Test Product 3",
            quantity=2,
            unit_price=25.00,
            total_amount=48.00,  # 2*25 - 5 + 3 = 48
            discount_amount=5.00,
            tax_amount=3.00,
            customer_id="CUST-001",
            vendor_id="VEND-001",
            warehouse_location="NYC-01"
        )
        assert order_complex.is_valid is True

    def test_status_combination_validation(self):
        """Test validation of status combinations."""
        # Test Case 1: Invalid - Cancelled order with PAID status
        order_invalid_status = PurchaseOrderItem(
            order_id="ORD-004",
            product_id="PROD-004",
            product_name="Test Product 4",
            quantity=1,
            unit_price=100.00,
            total_amount=100.00,
            customer_id="CUST-001",
            vendor_id="VEND-001",
            warehouse_location="NYC-01",
            order_status="CANCELLED",
            payment_status="PAID"
        )
        assert order_invalid_status.is_valid is False
        assert "Cancelled orders should not be in PAID status" in order_invalid_status.validation_errors

        # Test Case 2: Invalid - Delivered order with PENDING payment
        order_delivered_pending = PurchaseOrderItem(
            order_id="ORD-005",
            product_id="PROD-005",
            product_name="Test Product 5",
            quantity=1,
            unit_price=75.00,
            total_amount=75.00,
            customer_id="CUST-001",
            vendor_id="VEND-001",
            warehouse_location="NYC-01",
            order_status="DELIVERED",
            payment_status="PENDING"
        )
        assert order_delivered_pending.is_valid is False
        assert "Delivered orders should not have PENDING payment" in order_delivered_pending.validation_errors

        # Test Case 3: Valid status combination
        order_valid_status = PurchaseOrderItem(
            order_id="ORD-006",
            product_id="PROD-006",
            product_name="Test Product 6",
            quantity=1,
            unit_price=50.00,
            total_amount=50.00,
            customer_id="CUST-001",
            vendor_id="VEND-001",
            warehouse_location="NYC-01",
            order_status="DELIVERED",
            payment_status="PAID"
        )
        assert order_valid_status.is_valid is True

    def test_to_dict_serialization(self):
        """Test conversion to dictionary for serialization."""
        # Arrange
        order = PurchaseOrderItem(
            order_id="ORD-007",
            product_id="PROD-007",
            product_name="Serialization Test",
            quantity=2,
            unit_price=30.00,
            total_amount=60.00,
            customer_id="CUST-001",
            vendor_id="VEND-001",
            warehouse_location="NYC-01",
            shipping_method="EXPRESS",
            priority_level="HIGH"
        )

        # Act
        order_dict = order.to_dict()

        # Assert
        assert isinstance(order_dict, dict)
        assert order_dict["order_id"] == "ORD-007"
        assert order_dict["product_name"] == "Serialization Test"
        assert order_dict["quantity"] == 2
        assert order_dict["unit_price"] == 30.00
        assert order_dict["shipping_method"] == "EXPRESS"
        assert order_dict["priority_level"] == "HIGH"
        assert "timestamp" in order_dict
        assert isinstance(order_dict["timestamp"], str)  # Should be ISO format

    def test_from_dict_deserialization(self):
        """Test creation from dictionary data."""
        # Arrange
        order_data = {
            "order_id": "ORD-008",
            "product_id": "PROD-008",
            "product_name": "Deserialization Test",
            "quantity": 3,
            "unit_price": 25.00,
            "total_amount": 75.00,
            "customer_id": "CUST-002",
            "vendor_id": "VEND-002",
            "warehouse_location": "LA-01",
            "order_status": "PROCESSING",
            "payment_status": "PAID",
            "currency": "USD",
            "shipping_method": "STANDARD"
        }

        # Act
        order = PurchaseOrderItem.from_dict(order_data)

        # Assert
        assert order.order_id == "ORD-008"
        assert order.product_name == "Deserialization Test"
        assert order.quantity == 3
        assert order.customer_id == "CUST-002"
        assert order.order_status == "PROCESSING"
        assert order.payment_status == "PAID"
        assert order.shipping_method == "STANDARD"

    def test_json_conversion_roundtrip(self):
        """Test JSON serialization and deserialization roundtrip."""
        # Arrange
        original_order = PurchaseOrderItem(
            order_id="ORD-009",
            product_id="PROD-009",
            product_name="JSON Test Product",
            quantity=4,
            unit_price=12.50,
            total_amount=50.00,
            customer_id="CUST-003",
            vendor_id="VEND-003",
            warehouse_location="CHI-01"
        )

        # Act
        json_string = original_order.to_json()
        order_dict = json.loads(json_string)
        recreated_order = PurchaseOrderItem.from_dict(order_dict)

        # Assert
        assert isinstance(json_string, str)
        assert recreated_order.order_id == original_order.order_id
        assert recreated_order.product_name == original_order.product_name
        assert recreated_order.quantity == original_order.quantity
        assert recreated_order.unit_price == original_order.unit_price
        assert recreated_order.total_amount == original_order.total_amount

    def test_fiscal_quarter_calculation(self):
        """Test automatic fiscal quarter calculation."""
        # Test different quarters
        test_cases = [
            (datetime(2024, 1, 15), "Q1-2024"),   # January -> Q1
            (datetime(2024, 4, 10), "Q2-2024"),   # April -> Q2
            (datetime(2024, 7, 20), "Q3-2024"),   # July -> Q3
            (datetime(2024, 10, 5), "Q4-2024"),   # October -> Q4
        ]

        for test_date, expected_quarter in test_cases:
            order = PurchaseOrderItem(
                order_id=f"ORD-{test_date.month:02d}",
                product_id="PROD-QUARTER",
                product_name="Quarter Test",
                quantity=1,
                unit_price=100.00,
                total_amount=100.00,
                customer_id="CUST-001",
                vendor_id="VEND-001",
                warehouse_location="NYC-01",
                created_at=test_date
            )
            assert order.fiscal_quarter == expected_quarter

    def test_validation_error_handling(self):
        """Test comprehensive validation error handling."""
        # Test Case 1: Multiple validation errors
        order_multiple_errors = PurchaseOrderItem(
            order_id="ORD-ERROR",
            product_id="PROD-ERROR",
            product_name="Error Test",
            quantity=-1,  # Invalid: negative quantity
            unit_price=-5.00,  # Invalid: negative price
            total_amount=100.00,  # Invalid: doesn't match calculation
            customer_id="CUST-001",
            vendor_id="VEND-001",
            warehouse_location="NYC-01",
            order_status="CANCELLED",  # Invalid combination
            payment_status="PAID"      # with this status
        )

        assert order_multiple_errors.is_valid is False
        assert "Quantity must be positive" in order_multiple_errors.validation_errors
        assert "Unit price cannot be negative" in order_multiple_errors.validation_errors
        assert "Cancelled orders should not be in PAID status" in order_multiple_errors.validation_errors
        assert order_multiple_errors.data_quality_score < 1.0

        # Test Case 2: Edge case with zero values
        order_zero = PurchaseOrderItem(
            order_id="ORD-ZERO",
            product_id="PROD-ZERO",
            product_name="Zero Test",
            quantity=0,  # Invalid: zero quantity
            unit_price=0.00,  # Valid: zero price
            total_amount=0.00,
            customer_id="CUST-001",
            vendor_id="VEND-001",
            warehouse_location="NYC-01"
        )

        assert order_zero.is_valid is False
        assert "Quantity must be positive" in order_zero.validation_errors

    def test_financial_summary_calculation(self):
        """Test financial summary calculation method."""
        # Arrange
        order = PurchaseOrderItem(
            order_id="ORD-SUMMARY",
            product_id="PROD-SUMMARY",
            product_name="Summary Test",
            quantity=5,
            unit_price=20.00,
            total_amount=108.00,
            discount_amount=10.00,
            tax_amount=8.00,
            shipping_cost=10.00,
            customer_id="CUST-001",
            vendor_id="VEND-001",
            warehouse_location="NYC-01"
        )

        # Act
        summary = order.get_financial_summary()

        # Assert
        assert summary["subtotal"] == 100.00  # 5 * 20.00
        assert summary["discount"] == 10.00
        assert summary["tax"] == 8.00
        assert summary["shipping"] == 10.00
        assert summary["net_amount"] == 98.00  # 108 - 10 = 98
        assert summary["total"] == 108.00

    def test_string_representations(self):
        """Test string representation methods."""
        # Arrange
        order = PurchaseOrderItem(
            order_id="ORD-STR",
            product_id="PROD-STR",
            product_name="String Test Product",
            quantity=2,
            unit_price=15.00,
            total_amount=30.00,
            customer_id="CUST-001",
            vendor_id="VEND-001",
            warehouse_location="NYC-01"
        )

        # Act & Assert
        str_repr = str(order)
        assert "ORD-STR" in str_repr
        assert "String Test Product" in str_repr
        assert "30.00" in str_repr

        repr_str = repr(order)
        assert "PurchaseOrderItem" in repr_str
        assert "ORD-STR" in repr_str


class TestPurchaseOrderItemEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_large_quantities_and_prices(self):
        """Test handling of large quantities and prices."""
        order = PurchaseOrderItem(
            order_id="ORD-LARGE",
            product_id="PROD-LARGE",
            product_name="Large Order Test",
            quantity=1000000,
            unit_price=9999.99,
            total_amount=9999990000.00,
            customer_id="CUST-001",
            vendor_id="VEND-001",
            warehouse_location="NYC-01"
        )

        # Should handle large numbers correctly
        assert order.is_valid is True
        assert order.quantity == 1000000
        assert order.unit_price == 9999.99

    def test_precision_handling(self):
        """Test floating point precision handling."""
        order = PurchaseOrderItem(
            order_id="ORD-PRECISION",
            product_id="PROD-PRECISION",
            product_name="Precision Test",
            quantity=3,
            unit_price=33.33,
            total_amount=99.99,  # 3 * 33.33 = 99.99
            customer_id="CUST-001",
            vendor_id="VEND-001",
            warehouse_location="NYC-01"
        )

        # Should handle floating point precision correctly
        assert order.is_valid is True

    def test_unicode_and_special_characters(self):
        """Test handling of unicode and special characters."""
        order = PurchaseOrderItem(
            order_id="ORD-UNICODE",
            product_id="PROD-UNICODE",
            product_name="Test Product with Special Chars: äöü@#$%",
            quantity=1,
            unit_price=25.00,
            total_amount=25.00,
            customer_id="CUST-UNICODE-äöü",
            vendor_id="VEND-001",
            warehouse_location="NYC-01"
        )

        assert order.is_valid is True
        assert "äöü@#$%" in order.product_name
        assert "äöü" in order.customer_id


# Pytest fixtures for common test data
@pytest.fixture
def valid_order_data():
    """Fixture providing valid order data."""
    return {
        "order_id": "ORD-FIXTURE",
        "product_id": "PROD-FIXTURE",
        "product_name": "Fixture Test Product",
        "quantity": 3,
        "unit_price": 29.99,
        "total_amount": 89.97,
        "customer_id": "CUST-FIXTURE",
        "vendor_id": "VEND-FIXTURE",
        "warehouse_location": "TEST-01"
    }


@pytest.fixture
def sample_orders():
    """Fixture providing multiple sample orders."""
    return [
        PurchaseOrderItem(
            order_id=f"ORD-SAMPLE-{i}",
            product_id=f"PROD-{i}",
            product_name=f"Sample Product {i}",
            quantity=i + 1,
            unit_price=10.0 * (i + 1),
            total_amount=10.0 * (i + 1) * (i + 1),
            customer_id=f"CUST-{i}",
            vendor_id=f"VEND-{i}",
            warehouse_location="SAMPLE-01"
        )
        for i in range(5)
    ]


class TestPurchaseOrderItemWithFixtures:
    """Tests using pytest fixtures."""

    def test_with_valid_order_fixture(self, valid_order_data):
        """Test using valid order data fixture."""
        order = PurchaseOrderItem(**valid_order_data)
        assert order.is_valid is True
        assert order.order_id == "ORD-FIXTURE"

    def test_batch_operations(self, sample_orders):
        """Test operations on multiple orders."""
        assert len(sample_orders) == 5

        # All orders should be valid
        for order in sample_orders:
            assert order.is_valid is True

        # Test aggregations
        total_amount = sum(order.total_amount for order in sample_orders)
        assert total_amount > 0

        # Test filtering
        high_value_orders = [order for order in sample_orders if order.total_amount > 100]
        assert len(high_value_orders) > 0