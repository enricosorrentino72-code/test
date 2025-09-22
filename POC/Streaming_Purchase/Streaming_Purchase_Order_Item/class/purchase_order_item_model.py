# Databricks notebook source
# MAGIC %md
# MAGIC # Purchase Order Item Data Model
# MAGIC
# MAGIC This notebook defines the PurchaseOrderItem dataclass used across the streaming pipeline.
# MAGIC
# MAGIC ## Features:
# MAGIC - Core purchase order fields with financial calculations
# MAGIC - Business context (customer, vendor, warehouse)
# MAGIC - Status tracking for orders and payments
# MAGIC - Comprehensive metadata for audit trail

# COMMAND ----------

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any
import json

# COMMAND ----------

@dataclass
class PurchaseOrderItem:
    """
    Data model for Purchase Order Items in the streaming pipeline.

    This class represents a single purchase order item with complete business context,
    financial calculations, and metadata for tracking through the pipeline.
    """

    # Core purchase order fields
    order_id: str
    product_id: str
    product_name: str
    quantity: int
    unit_price: float
    total_amount: float

    # Business context
    customer_id: str
    vendor_id: str
    warehouse_location: str
    currency: str = "USD"

    # Status tracking
    order_status: str = "NEW"  # NEW, PROCESSING, SHIPPED, DELIVERED, CANCELLED
    payment_status: str = "PENDING"  # PENDING, PAID, REFUNDED, FAILED

    # Timestamps and dates
    timestamp: datetime = field(default_factory=datetime.utcnow)
    created_at: datetime = field(default_factory=datetime.utcnow)
    order_date: str = field(default="")
    fiscal_quarter: str = field(default="")

    # Additional business fields
    shipping_method: Optional[str] = None
    priority_level: str = "NORMAL"  # LOW, NORMAL, HIGH, URGENT
    discount_amount: float = 0.0
    tax_amount: float = 0.0
    shipping_cost: float = 0.0

    # Quality and validation flags
    is_valid: bool = True
    validation_errors: Optional[str] = None
    data_quality_score: float = 1.0

    # Technical metadata
    source_system: str = "DATABRICKS_STREAMING"
    processing_timestamp: Optional[datetime] = None
    cluster_id: Optional[str] = None
    notebook_path: Optional[str] = None

    def __post_init__(self):
        """Perform validation and calculations after initialization."""
        # Set order_date if not provided
        if not self.order_date:
            self.order_date = self.created_at.strftime("%Y-%m-%d")

        # Calculate fiscal quarter if not provided
        if not self.fiscal_quarter:
            month = self.created_at.month
            quarter = (month - 1) // 3 + 1
            self.fiscal_quarter = f"Q{quarter}-{self.created_at.year}"

        # Validate financial calculations
        self.validate_financial_data()

    def validate_financial_data(self) -> bool:
        """
        Validate financial calculations and business rules.

        Returns:
            bool: True if all validations pass, False otherwise
        """
        errors = []

        # Check quantity is positive
        if self.quantity <= 0:
            errors.append("Quantity must be positive")

        # Check unit price is non-negative
        if self.unit_price < 0:
            errors.append("Unit price cannot be negative")

        # Validate total amount calculation
        expected_total = self.quantity * self.unit_price - self.discount_amount + self.tax_amount + self.shipping_cost
        if abs(self.total_amount - expected_total) > 0.01:  # Allow for rounding
            errors.append(f"Total amount mismatch: expected {expected_total:.2f}, got {self.total_amount:.2f}")

        # Validate status combinations
        if self.order_status == "CANCELLED" and self.payment_status == "PAID":
            errors.append("Cancelled orders should not be in PAID status")

        if self.order_status == "DELIVERED" and self.payment_status == "PENDING":
            errors.append("Delivered orders should not have PENDING payment")

        # Update validation status
        if errors:
            self.is_valid = False
            self.validation_errors = "; ".join(errors)
            self.data_quality_score = max(0, 1 - (len(errors) * 0.2))
        else:
            self.is_valid = True
            self.validation_errors = None
            self.data_quality_score = 1.0

        return self.is_valid

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the purchase order item to a dictionary for serialization.

        Returns:
            Dict[str, Any]: Dictionary representation of the purchase order
        """
        return {
            # Core fields
            "order_id": self.order_id,
            "product_id": self.product_id,
            "product_name": self.product_name,
            "quantity": self.quantity,
            "unit_price": self.unit_price,
            "total_amount": self.total_amount,

            # Business context
            "customer_id": self.customer_id,
            "vendor_id": self.vendor_id,
            "warehouse_location": self.warehouse_location,
            "currency": self.currency,

            # Status
            "order_status": self.order_status,
            "payment_status": self.payment_status,

            # Timestamps
            "timestamp": self.timestamp.isoformat(),
            "created_at": self.created_at.isoformat(),
            "order_date": self.order_date,
            "fiscal_quarter": self.fiscal_quarter,

            # Additional fields
            "shipping_method": self.shipping_method,
            "priority_level": self.priority_level,
            "discount_amount": self.discount_amount,
            "tax_amount": self.tax_amount,
            "shipping_cost": self.shipping_cost,

            # Quality
            "is_valid": self.is_valid,
            "validation_errors": self.validation_errors,
            "data_quality_score": self.data_quality_score,

            # Metadata
            "source_system": self.source_system,
            "processing_timestamp": self.processing_timestamp.isoformat() if self.processing_timestamp else None,
            "cluster_id": self.cluster_id,
            "notebook_path": self.notebook_path
        }

    def to_json(self) -> str:
        """
        Convert the purchase order item to JSON string.

        Returns:
            str: JSON representation of the purchase order
        """
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PurchaseOrderItem':
        """
        Create a PurchaseOrderItem from a dictionary.

        Args:
            data: Dictionary containing purchase order data

        Returns:
            PurchaseOrderItem: New instance created from dictionary
        """
        # Convert timestamp strings back to datetime objects
        if isinstance(data.get('timestamp'), str):
            data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        if isinstance(data.get('created_at'), str):
            data['created_at'] = datetime.fromisoformat(data['created_at'])
        if isinstance(data.get('processing_timestamp'), str):
            data['processing_timestamp'] = datetime.fromisoformat(data['processing_timestamp'])

        return cls(**data)

    def calculate_net_amount(self) -> float:
        """
        Calculate the net amount after all adjustments.

        Returns:
            float: Net amount including all costs and discounts
        """
        base_amount = self.quantity * self.unit_price
        net_amount = base_amount - self.discount_amount + self.tax_amount + self.shipping_cost
        return round(net_amount, 2)

    def get_financial_summary(self) -> Dict[str, float]:
        """
        Get a summary of all financial components.

        Returns:
            Dict[str, float]: Financial breakdown
        """
        base_amount = self.quantity * self.unit_price
        return {
            "base_amount": base_amount,
            "discount_amount": self.discount_amount,
            "tax_amount": self.tax_amount,
            "shipping_cost": self.shipping_cost,
            "net_amount": self.calculate_net_amount(),
            "total_amount": self.total_amount
        }

    def __str__(self) -> str:
        """String representation of the purchase order."""
        return (f"PurchaseOrder(id={self.order_id}, "
                f"product={self.product_name}, "
                f"qty={self.quantity}, "
                f"total=${self.total_amount:.2f}, "
                f"status={self.order_status})")

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return (f"PurchaseOrderItem(order_id='{self.order_id}', "
                f"product_id='{self.product_id}', "
                f"total_amount={self.total_amount}, "
                f"order_status='{self.order_status}', "
                f"payment_status='{self.payment_status}')")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example Usage

# COMMAND ----------

# Example: Create a purchase order item
example_order = PurchaseOrderItem(
    order_id="PO-2024-001234",
    product_id="PROD-5678",
    product_name="Wireless Mouse",
    quantity=10,
    unit_price=29.99,
    total_amount=317.89,  # Including tax and shipping
    customer_id="CUST-9876",
    vendor_id="VEND-5432",
    warehouse_location="WAREHOUSE-EAST",
    order_status="PROCESSING",
    payment_status="PAID",
    shipping_method="STANDARD",
    priority_level="NORMAL",
    discount_amount=15.00,
    tax_amount=22.90,
    shipping_cost=10.00
)

print("Purchase Order Created:")
print(example_order)
print("\nFinancial Summary:")
print(example_order.get_financial_summary())
print("\nValidation Status:", example_order.is_valid)

# COMMAND ----------

# Export the class for use in other notebooks
__all__ = ['PurchaseOrderItem']