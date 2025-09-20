# Databricks notebook source
# MAGIC %md
# MAGIC # Purchase Order Item Factory
# MAGIC
# MAGIC Factory class for generating realistic purchase order data with configurable quality scenarios.
# MAGIC
# MAGIC ## Features:
# MAGIC - Realistic product catalog with categories
# MAGIC - Customer and vendor generation
# MAGIC - Configurable data quality issues (5% by default)
# MAGIC - Business scenario simulation
# MAGIC - Financial calculations with proper rounding

# COMMAND ----------

import random
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

# COMMAND ----------

# Import the data model
from purchase_order_item_model import PurchaseOrderItem

# COMMAND ----------

@dataclass
class ProductCatalog:
    """Product information for realistic data generation."""
    product_id: str
    name: str
    category: str
    base_price: float
    weight: float
    vendor_ids: List[str]

# COMMAND ----------

class PurchaseOrderItemFactory:
    """
    Factory class for generating realistic purchase order items with various scenarios.

    This factory simulates real-world purchase order patterns including:
    - Normal orders (95%)
    - Data quality issues (5%) for DQX testing
    - Various business scenarios
    - Seasonal patterns
    """

    def __init__(self, quality_issue_rate: float = 0.05):
        """
        Initialize the factory with product catalog and configuration.

        Args:
            quality_issue_rate: Percentage of orders with quality issues (default 5%)
        """
        self.quality_issue_rate = quality_issue_rate
        self.order_counter = 0
        self._initialize_catalogs()

    def _initialize_catalogs(self):
        """Initialize product, customer, and vendor catalogs."""
        # Product catalog
        self.products = [
            ProductCatalog("PROD-1001", "Laptop Pro 15", "Electronics", 1299.99, 2.5, ["VEND-101", "VEND-102"]),
            ProductCatalog("PROD-1002", "Wireless Mouse", "Electronics", 29.99, 0.1, ["VEND-103", "VEND-104"]),
            ProductCatalog("PROD-1003", "USB-C Hub", "Electronics", 49.99, 0.2, ["VEND-103", "VEND-105"]),
            ProductCatalog("PROD-1004", "Monitor 27-inch", "Electronics", 399.99, 5.0, ["VEND-102", "VEND-106"]),
            ProductCatalog("PROD-1005", "Mechanical Keyboard", "Electronics", 89.99, 1.0, ["VEND-103", "VEND-107"]),
            ProductCatalog("PROD-2001", "Office Chair", "Furniture", 249.99, 15.0, ["VEND-201", "VEND-202"]),
            ProductCatalog("PROD-2002", "Standing Desk", "Furniture", 599.99, 30.0, ["VEND-201", "VEND-203"]),
            ProductCatalog("PROD-2003", "Bookshelf", "Furniture", 179.99, 20.0, ["VEND-202", "VEND-204"]),
            ProductCatalog("PROD-3001", "Notebook Set", "Office Supplies", 12.99, 0.5, ["VEND-301", "VEND-302"]),
            ProductCatalog("PROD-3002", "Pen Pack", "Office Supplies", 8.99, 0.1, ["VEND-301", "VEND-303"]),
            ProductCatalog("PROD-3003", "Paper Ream", "Office Supplies", 24.99, 2.5, ["VEND-302", "VEND-304"]),
            ProductCatalog("PROD-3004", "Stapler", "Office Supplies", 15.99, 0.3, ["VEND-303", "VEND-305"]),
            ProductCatalog("PROD-4001", "Coffee Maker", "Appliances", 79.99, 3.0, ["VEND-401", "VEND-402"]),
            ProductCatalog("PROD-4002", "Microwave", "Appliances", 129.99, 12.0, ["VEND-401", "VEND-403"]),
            ProductCatalog("PROD-4003", "Water Cooler", "Appliances", 199.99, 15.0, ["VEND-402", "VEND-404"]),
        ]

        # Customer IDs
        self.customer_ids = [f"CUST-{i:05d}" for i in range(1000, 2000)]

        # Warehouse locations
        self.warehouses = [
            "WAREHOUSE-EAST", "WAREHOUSE-WEST", "WAREHOUSE-CENTRAL",
            "WAREHOUSE-NORTH", "WAREHOUSE-SOUTH", "WAREHOUSE-INTL"
        ]

        # Shipping methods
        self.shipping_methods = ["STANDARD", "EXPRESS", "OVERNIGHT", "ECONOMY", "PRIORITY"]

        # Order status patterns
        self.order_patterns = [
            {"order_status": "NEW", "payment_status": "PENDING", "weight": 0.3},
            {"order_status": "PROCESSING", "payment_status": "PAID", "weight": 0.25},
            {"order_status": "SHIPPED", "payment_status": "PAID", "weight": 0.25},
            {"order_status": "DELIVERED", "payment_status": "PAID", "weight": 0.15},
            {"order_status": "CANCELLED", "payment_status": "REFUNDED", "weight": 0.05},
        ]

    def generate_order_id(self) -> str:
        """Generate a unique order ID."""
        self.order_counter += 1
        timestamp = datetime.utcnow().strftime("%Y%m%d")
        return f"PO-{timestamp}-{self.order_counter:06d}"

    def _select_order_pattern(self) -> Dict[str, str]:
        """Select an order pattern based on weighted distribution."""
        weights = [p["weight"] for p in self.order_patterns]
        pattern = random.choices(self.order_patterns, weights=weights)[0]
        return {"order_status": pattern["order_status"], "payment_status": pattern["payment_status"]}

    def _calculate_financials(self, quantity: int, unit_price: float) -> Dict[str, float]:
        """
        Calculate financial components for an order.

        Args:
            quantity: Number of items
            unit_price: Price per unit

        Returns:
            Dictionary with financial calculations
        """
        base_amount = quantity * unit_price

        # Discount (0-15% for bulk orders)
        discount_rate = 0
        if quantity > 50:
            discount_rate = random.uniform(0.1, 0.15)
        elif quantity > 20:
            discount_rate = random.uniform(0.05, 0.1)
        elif quantity > 10:
            discount_rate = random.uniform(0.02, 0.05)

        discount_amount = round(base_amount * discount_rate, 2)

        # Tax (5-10% based on location)
        tax_rate = random.uniform(0.05, 0.10)
        tax_amount = round((base_amount - discount_amount) * tax_rate, 2)

        # Shipping (based on order value and method)
        if base_amount > 500:
            shipping_cost = 0  # Free shipping for large orders
        else:
            shipping_cost = round(random.uniform(5.99, 29.99), 2)

        # Total amount
        total_amount = round(base_amount - discount_amount + tax_amount + shipping_cost, 2)

        return {
            "discount_amount": discount_amount,
            "tax_amount": tax_amount,
            "shipping_cost": shipping_cost,
            "total_amount": total_amount
        }

    def _inject_quality_issue(self, order: PurchaseOrderItem) -> PurchaseOrderItem:
        """
        Inject a data quality issue into an order for DQX testing.

        Args:
            order: The order to modify

        Returns:
            Modified order with quality issue
        """
        issue_type = random.choice([
            "negative_quantity",
            "invalid_total",
            "status_mismatch",
            "missing_customer",
            "invalid_date",
            "extreme_values"
        ])

        if issue_type == "negative_quantity":
            order.quantity = -random.randint(1, 10)
        elif issue_type == "invalid_total":
            order.total_amount = order.total_amount * -1
        elif issue_type == "status_mismatch":
            order.order_status = "DELIVERED"
            order.payment_status = "PENDING"
        elif issue_type == "missing_customer":
            order.customer_id = ""
        elif issue_type == "invalid_date":
            order.created_at = datetime.utcnow() + timedelta(days=30)  # Future date
        elif issue_type == "extreme_values":
            order.quantity = random.randint(10000, 99999)
            order.unit_price = random.uniform(0.001, 0.01)

        # Mark as invalid
        order.is_valid = False
        order.validation_errors = f"Quality issue injected: {issue_type}"
        order.data_quality_score = random.uniform(0.1, 0.5)

        return order

    def generate_single_item(self,
                            inject_quality_issue: Optional[bool] = None,
                            specific_scenario: Optional[str] = None) -> PurchaseOrderItem:
        """
        Generate a single purchase order item.

        Args:
            inject_quality_issue: Force quality issue injection
            specific_scenario: Generate specific scenario (e.g., "high_value", "bulk_order")

        Returns:
            PurchaseOrderItem instance
        """
        # Select product
        product = random.choice(self.products)

        # Determine quantity based on scenario
        if specific_scenario == "bulk_order":
            quantity = random.randint(100, 500)
        elif specific_scenario == "high_value":
            quantity = random.randint(10, 50)
            product = random.choice([p for p in self.products if p.base_price > 500])
        else:
            # Normal distribution with occasional bulk orders
            quantity = int(abs(random.gauss(10, 5))) + 1
            if random.random() < 0.1:  # 10% chance of bulk order
                quantity *= 10

        # Calculate unit price with small variations
        unit_price = round(product.base_price * random.uniform(0.95, 1.05), 2)

        # Calculate financials
        financials = self._calculate_financials(quantity, unit_price)

        # Select order pattern
        pattern = self._select_order_pattern()

        # Select priority based on order value
        if financials["total_amount"] > 5000:
            priority = random.choice(["HIGH", "URGENT"])
        elif financials["total_amount"] > 1000:
            priority = random.choice(["NORMAL", "HIGH"])
        else:
            priority = random.choice(["LOW", "NORMAL", "NORMAL"])  # Weight towards NORMAL

        # Create order
        order = PurchaseOrderItem(
            order_id=self.generate_order_id(),
            product_id=product.product_id,
            product_name=product.name,
            quantity=quantity,
            unit_price=unit_price,
            total_amount=financials["total_amount"],
            customer_id=random.choice(self.customer_ids),
            vendor_id=random.choice(product.vendor_ids),
            warehouse_location=random.choice(self.warehouses),
            currency="USD",
            order_status=pattern["order_status"],
            payment_status=pattern["payment_status"],
            shipping_method=random.choice(self.shipping_methods),
            priority_level=priority,
            discount_amount=financials["discount_amount"],
            tax_amount=financials["tax_amount"],
            shipping_cost=financials["shipping_cost"],
            created_at=datetime.utcnow() - timedelta(hours=random.randint(0, 72))
        )

        # Add Databricks metadata
        order.cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "local")
        order.notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

        # Inject quality issue if specified or randomly based on rate
        should_inject = inject_quality_issue if inject_quality_issue is not None else random.random() < self.quality_issue_rate
        if should_inject:
            order = self._inject_quality_issue(order)

        return order

    def generate_batch(self, batch_size: int = 50, scenario_mix: Optional[Dict[str, float]] = None) -> List[PurchaseOrderItem]:
        """
        Generate a batch of purchase order items.

        Args:
            batch_size: Number of items to generate
            scenario_mix: Dictionary of scenario weights (e.g., {"normal": 0.8, "bulk_order": 0.1, "high_value": 0.1})

        Returns:
            List of PurchaseOrderItem instances
        """
        if scenario_mix is None:
            scenario_mix = {"normal": 0.85, "bulk_order": 0.1, "high_value": 0.05}

        orders = []
        scenarios = list(scenario_mix.keys())
        weights = list(scenario_mix.values())

        for _ in range(batch_size):
            scenario = random.choices(scenarios, weights=weights)[0]
            scenario_param = None if scenario == "normal" else scenario
            order = self.generate_single_item(specific_scenario=scenario_param)
            orders.append(order)

        return orders

    def generate_seasonal_batch(self, batch_size: int = 50, season: str = "normal") -> List[PurchaseOrderItem]:
        """
        Generate orders with seasonal patterns.

        Args:
            batch_size: Number of items to generate
            season: Season type ("holiday", "back_to_school", "black_friday", "normal")

        Returns:
            List of PurchaseOrderItem instances with seasonal characteristics
        """
        if season == "holiday":
            # More electronics and appliances
            preferred_categories = ["Electronics", "Appliances"]
            scenario_mix = {"normal": 0.5, "bulk_order": 0.2, "high_value": 0.3}
        elif season == "back_to_school":
            # More office supplies
            preferred_categories = ["Office Supplies", "Electronics"]
            scenario_mix = {"normal": 0.6, "bulk_order": 0.3, "high_value": 0.1}
        elif season == "black_friday":
            # High volume, all categories
            scenario_mix = {"normal": 0.3, "bulk_order": 0.4, "high_value": 0.3}
            preferred_categories = None
        else:
            preferred_categories = None
            scenario_mix = None

        # Generate batch with seasonal preferences
        orders = self.generate_batch(batch_size, scenario_mix)

        # Adjust for preferred categories if specified
        if preferred_categories:
            for order in orders:
                if random.random() < 0.7:  # 70% chance to adjust
                    preferred_products = [p for p in self.products if p.category in preferred_categories]
                    if preferred_products:
                        product = random.choice(preferred_products)
                        order.product_id = product.product_id
                        order.product_name = product.name

        return orders

    def get_statistics(self, orders: List[PurchaseOrderItem]) -> Dict[str, Any]:
        """
        Calculate statistics for a batch of orders.

        Args:
            orders: List of purchase orders

        Returns:
            Dictionary with statistical summaries
        """
        if not orders:
            return {}

        total_revenue = sum(o.total_amount for o in orders)
        valid_orders = [o for o in orders if o.is_valid]
        invalid_orders = [o for o in orders if not o.is_valid]

        status_distribution = {}
        for order in orders:
            status = order.order_status
            status_distribution[status] = status_distribution.get(status, 0) + 1

        return {
            "total_orders": len(orders),
            "valid_orders": len(valid_orders),
            "invalid_orders": len(invalid_orders),
            "quality_rate": len(valid_orders) / len(orders) if orders else 0,
            "total_revenue": round(total_revenue, 2),
            "average_order_value": round(total_revenue / len(orders), 2) if orders else 0,
            "status_distribution": status_distribution,
            "min_order_value": round(min(o.total_amount for o in orders), 2) if orders else 0,
            "max_order_value": round(max(o.total_amount for o in orders), 2) if orders else 0
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example Usage

# COMMAND ----------

# Initialize factory
factory = PurchaseOrderItemFactory(quality_issue_rate=0.05)

# Generate a single order
single_order = factory.generate_single_item()
print("Single Order:", single_order)
print()

# Generate a batch
batch = factory.generate_batch(batch_size=10)
print(f"Generated {len(batch)} orders")
print("Statistics:", factory.get_statistics(batch))

# COMMAND ----------

# Export for use in other notebooks
__all__ = ['PurchaseOrderItemFactory', 'ProductCatalog']