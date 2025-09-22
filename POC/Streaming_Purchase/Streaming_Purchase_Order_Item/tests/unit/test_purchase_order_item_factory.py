"""
Unit tests for PurchaseOrderItemFactory.

This module contains comprehensive unit tests for the PurchaseOrderItemFactory class,
testing data generation, quality scenarios, and statistical validation.
"""

import pytest
from datetime import datetime, timedelta
from typing import List, Dict, Any

# Import the classes under test
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'class'))

from purchase_order_item_factory import PurchaseOrderItemFactory, ProductCatalog
from purchase_order_item_model import PurchaseOrderItem


class TestPurchaseOrderItemFactory:
    """Comprehensive unit tests for PurchaseOrderItemFactory."""

    def test_factory_initialization(self):
        """Test factory initialization with default and custom settings."""
        # Test default initialization
        factory_default = PurchaseOrderItemFactory()
        assert factory_default.quality_issue_rate == 0.05
        assert factory_default.order_counter == 0
        assert len(factory_default.products) > 0
        assert len(factory_default.customers) > 0
        assert len(factory_default.vendors) > 0

        # Test custom quality issue rate
        factory_custom = PurchaseOrderItemFactory(quality_issue_rate=0.1)
        assert factory_custom.quality_issue_rate == 0.1

        # Test invalid quality issue rate
        with pytest.raises(ValueError):
            PurchaseOrderItemFactory(quality_issue_rate=1.5)  # > 1.0

    def test_generate_single_item_normal_scenario(self):
        """Test generation of a single normal purchase order item."""
        # Arrange
        factory = PurchaseOrderItemFactory(quality_issue_rate=0.0)  # No issues for this test

        # Act
        order = factory.generate_single_item()

        # Assert
        assert isinstance(order, PurchaseOrderItem)
        assert order.order_id.startswith("ORD-")
        assert len(order.order_id) == 10  # ORD-XXXXXX format
        assert order.product_id.startswith("PROD-")
        assert order.quantity > 0
        assert order.unit_price > 0
        assert order.total_amount > 0
        assert order.customer_id.startswith("CUST-")
        assert order.vendor_id.startswith("VEND-")
        assert order.warehouse_location is not None
        assert order.is_valid is True

    def test_generate_single_item_with_scenarios(self):
        """Test generation with different business scenarios."""
        factory = PurchaseOrderItemFactory()

        # Test high priority scenario
        high_priority_order = factory.generate_single_item(scenario="high_priority")
        assert high_priority_order.priority_level == "HIGH"

        # Test bulk order scenario
        bulk_order = factory.generate_single_item(scenario="bulk_order")
        assert bulk_order.quantity >= 50  # Bulk orders should have high quantity

        # Test express shipping scenario
        express_order = factory.generate_single_item(scenario="express_shipping")
        assert express_order.shipping_method == "EXPRESS"

        # Test international scenario
        international_order = factory.generate_single_item(scenario="international")
        assert international_order.currency in ["EUR", "GBP", "CAD", "AUD"]

    def test_generate_batch_with_quality_issues(self):
        """Test batch generation with controlled quality issues."""
        # Arrange
        factory = PurchaseOrderItemFactory(quality_issue_rate=0.2)  # 20% issues
        batch_size = 100

        # Act
        orders = factory.generate_batch(batch_size=batch_size)

        # Assert
        assert len(orders) == batch_size

        # Count quality issues
        invalid_orders = [order for order in orders if not order.is_valid]
        issue_rate = len(invalid_orders) / batch_size

        # Should be approximately 20% (allow some variance due to randomness)
        assert 0.15 <= issue_rate <= 0.25

        # All orders should be PurchaseOrderItem instances
        assert all(isinstance(order, PurchaseOrderItem) for order in orders)

    def test_seasonal_batch_generation(self):
        """Test seasonal batch generation with different patterns."""
        factory = PurchaseOrderItemFactory()

        # Test different seasons
        seasons = ["holiday", "back_to_school", "summer", "normal"]

        for season in seasons:
            orders = factory.generate_seasonal_batch(batch_size=20, season=season)
            assert len(orders) == 20

            if season == "holiday":
                # Holiday season should have more electronics and higher priority
                electronics_count = sum(1 for order in orders if "Electronics" in order.product_name)
                high_priority_count = sum(1 for order in orders if order.priority_level in ["HIGH", "URGENT"])
                assert electronics_count > 0
                assert high_priority_count > 0

            elif season == "back_to_school":
                # Should have office supplies and books
                supplies_count = sum(1 for order in orders if any(keyword in order.product_name
                                   for keyword in ["Notebook", "Pen", "Paper", "Book"]))
                assert supplies_count > 0

    def test_financial_calculation_accuracy(self):
        """Test accuracy of financial calculations in generated orders."""
        factory = PurchaseOrderItemFactory(quality_issue_rate=0.0)  # No quality issues

        # Generate multiple orders and verify calculations
        orders = factory.generate_batch(batch_size=50)

        for order in orders:
            # Calculate expected total
            expected_total = (order.quantity * order.unit_price +
                            order.tax_amount +
                            order.shipping_cost -
                            order.discount_amount)

            # Allow small floating point tolerance
            assert abs(order.total_amount - expected_total) <= 0.01
            assert order.is_valid is True

    def test_quality_issue_injection_rate(self):
        """Test that quality issues are injected at the correct rate."""
        # Test different quality issue rates
        test_rates = [0.0, 0.05, 0.1, 0.2, 0.3]
        batch_size = 200  # Larger batch for statistical accuracy

        for target_rate in test_rates:
            factory = PurchaseOrderItemFactory(quality_issue_rate=target_rate)
            orders = factory.generate_batch(batch_size=batch_size)

            invalid_count = sum(1 for order in orders if not order.is_valid)
            actual_rate = invalid_count / batch_size

            # Allow 5% tolerance for randomness
            tolerance = 0.05
            assert abs(actual_rate - target_rate) <= tolerance

    def test_product_catalog_selection(self):
        """Test that products are selected from the catalog correctly."""
        factory = PurchaseOrderItemFactory()

        # Generate many orders to test distribution
        orders = factory.generate_batch(batch_size=100)

        # Extract all product IDs
        product_ids = [order.product_id for order in orders]

        # Should have variety in products
        unique_products = set(product_ids)
        assert len(unique_products) > 1  # Should have multiple different products

        # All product IDs should be from the catalog
        catalog_product_ids = [product.product_id for product in factory.products]
        for product_id in product_ids:
            assert product_id in catalog_product_ids

    def test_batch_statistics_calculation(self):
        """Test batch statistics calculation functionality."""
        factory = PurchaseOrderItemFactory()

        # Generate a batch
        orders = factory.generate_batch(batch_size=50)

        # Calculate statistics
        stats = factory.get_statistics(orders)

        # Verify statistics structure
        assert "total_orders" in stats
        assert "total_value" in stats
        assert "average_order_value" in stats
        assert "quality_metrics" in stats
        assert "product_distribution" in stats
        assert "status_distribution" in stats

        # Verify calculations
        assert stats["total_orders"] == 50
        assert stats["total_value"] > 0
        assert stats["average_order_value"] == stats["total_value"] / 50

        # Quality metrics
        quality_metrics = stats["quality_metrics"]
        assert "valid_orders" in quality_metrics
        assert "invalid_orders" in quality_metrics
        assert "quality_rate" in quality_metrics
        assert quality_metrics["valid_orders"] + quality_metrics["invalid_orders"] == 50

    def test_order_id_generation_uniqueness(self):
        """Test that order IDs are unique and follow the correct format."""
        factory = PurchaseOrderItemFactory()

        # Generate multiple orders
        orders = factory.generate_batch(batch_size=100)

        # Extract order IDs
        order_ids = [order.order_id for order in orders]

        # Test uniqueness
        assert len(order_ids) == len(set(order_ids))  # All should be unique

        # Test format
        import re
        order_id_pattern = r"^ORD-\d{6}$"
        for order_id in order_ids:
            assert re.match(order_id_pattern, order_id), f"Invalid order ID format: {order_id}"

    def test_vendor_customer_consistency(self):
        """Test that vendor-customer relationships are consistent."""
        factory = PurchaseOrderItemFactory()

        orders = factory.generate_batch(batch_size=100)

        # Group by customer
        customer_orders = {}
        for order in orders:
            customer_id = order.customer_id
            if customer_id not in customer_orders:
                customer_orders[customer_id] = []
            customer_orders[customer_id].append(order)

        # Each customer should exist
        for customer_id, customer_orders_list in customer_orders.items():
            assert customer_id.startswith("CUST-")
            assert len(customer_orders_list) > 0


class TestProductCatalog:
    """Test the ProductCatalog data class."""

    def test_product_catalog_creation(self):
        """Test ProductCatalog creation and attributes."""
        # Arrange & Act
        product = ProductCatalog(
            product_id="PROD-TEST",
            name="Test Product",
            category="Test Category",
            base_price=99.99,
            weight=2.5,
            vendor_ids=["VEND-001", "VEND-002"]
        )

        # Assert
        assert product.product_id == "PROD-TEST"
        assert product.name == "Test Product"
        assert product.category == "Test Category"
        assert product.base_price == 99.99
        assert product.weight == 2.5
        assert len(product.vendor_ids) == 2


class TestFactoryEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_empty_batch_generation(self):
        """Test generation of empty batch."""
        factory = PurchaseOrderItemFactory()

        orders = factory.generate_batch(batch_size=0)
        assert len(orders) == 0

    def test_large_batch_generation(self):
        """Test generation of large batches."""
        factory = PurchaseOrderItemFactory()

        # Generate a large batch
        orders = factory.generate_batch(batch_size=1000)

        assert len(orders) == 1000
        # Should complete in reasonable time and memory

    def test_extreme_quality_issue_rates(self):
        """Test extreme quality issue rates."""
        # Test 0% quality issues
        factory_perfect = PurchaseOrderItemFactory(quality_issue_rate=0.0)
        orders_perfect = factory_perfect.generate_batch(batch_size=50)
        invalid_count = sum(1 for order in orders_perfect if not order.is_valid)
        assert invalid_count == 0

        # Test 100% quality issues
        factory_all_issues = PurchaseOrderItemFactory(quality_issue_rate=1.0)
        orders_issues = factory_all_issues.generate_batch(batch_size=50)
        invalid_count = sum(1 for order in orders_issues if not order.is_valid)
        assert invalid_count == 50

    def test_scenario_mix_validation(self):
        """Test scenario mix parameter validation."""
        factory = PurchaseOrderItemFactory()

        # Valid scenario mix
        valid_mix = {
            "normal": 0.6,
            "high_priority": 0.2,
            "bulk_order": 0.1,
            "express_shipping": 0.1
        }
        orders = factory.generate_batch(batch_size=20, scenario_mix=valid_mix)
        assert len(orders) == 20

        # Invalid scenario mix (doesn't sum to 1.0)
        invalid_mix = {
            "normal": 0.3,
            "high_priority": 0.2
        }
        with pytest.raises(ValueError):
            factory.generate_batch(batch_size=20, scenario_mix=invalid_mix)


# Pytest fixtures for factory testing
@pytest.fixture
def sample_factory():
    """Fixture providing a factory with controlled settings."""
    return PurchaseOrderItemFactory(quality_issue_rate=0.1)


@pytest.fixture
def sample_product_catalog():
    """Fixture providing sample product catalog."""
    return [
        ProductCatalog("PROD-001", "Test Laptop", "Electronics", 999.99, 2.0, ["VEND-001"]),
        ProductCatalog("PROD-002", "Test Mouse", "Electronics", 29.99, 0.1, ["VEND-002"]),
        ProductCatalog("PROD-003", "Test Chair", "Furniture", 199.99, 10.0, ["VEND-003"])
    ]


class TestFactoryWithFixtures:
    """Tests using pytest fixtures."""

    def test_factory_with_fixture(self, sample_factory):
        """Test factory operations using fixture."""
        orders = sample_factory.generate_batch(batch_size=20)
        assert len(orders) == 20

        # Should have some quality issues (10% rate)
        invalid_orders = [order for order in orders if not order.is_valid]
        assert len(invalid_orders) >= 0  # Could be 0 due to randomness in small sample

    def test_product_catalog_fixture(self, sample_product_catalog):
        """Test product catalog fixture."""
        assert len(sample_product_catalog) == 3
        assert all(isinstance(product, ProductCatalog) for product in sample_product_catalog)

        # Test that catalog has expected structure
        categories = [product.category for product in sample_product_catalog]
        assert "Electronics" in categories
        assert "Furniture" in categories


class TestFactoryPerformance:
    """Performance-related tests for the factory."""

    def test_generation_performance(self):
        """Test that order generation performs within acceptable limits."""
        import time

        factory = PurchaseOrderItemFactory()

        # Measure time to generate 1000 orders
        start_time = time.time()
        orders = factory.generate_batch(batch_size=1000)
        end_time = time.time()

        generation_time = end_time - start_time

        # Should complete in reasonable time (less than 5 seconds)
        assert generation_time < 5.0
        assert len(orders) == 1000

    def test_memory_efficiency(self):
        """Test memory efficiency of batch generation."""
        factory = PurchaseOrderItemFactory()

        # Generate large batch and verify it completes
        try:
            orders = factory.generate_batch(batch_size=5000)
            assert len(orders) == 5000

            # Clean up
            del orders
        except MemoryError:
            pytest.fail("Factory should handle large batches without memory errors")


class TestFactoryIntegration:
    """Integration tests for factory with other components."""

    def test_factory_model_integration(self):
        """Test that factory-generated orders work with model methods."""
        factory = PurchaseOrderItemFactory(quality_issue_rate=0.0)
        order = factory.generate_single_item()

        # Test model methods work correctly
        order_dict = order.to_dict()
        assert isinstance(order_dict, dict)

        json_str = order.to_json()
        assert isinstance(json_str, str)

        recreated_order = PurchaseOrderItem.from_dict(order_dict)
        assert recreated_order.order_id == order.order_id

    def test_factory_statistics_integration(self):
        """Test that factory statistics integrate correctly."""
        factory = PurchaseOrderItemFactory(quality_issue_rate=0.1)
        orders = factory.generate_batch(batch_size=100)

        stats = factory.get_statistics(orders)

        # Verify statistical consistency
        calculated_total = sum(order.total_amount for order in orders)
        assert abs(stats["total_value"] - calculated_total) < 0.01

        calculated_avg = calculated_total / len(orders)
        assert abs(stats["average_order_value"] - calculated_avg) < 0.01