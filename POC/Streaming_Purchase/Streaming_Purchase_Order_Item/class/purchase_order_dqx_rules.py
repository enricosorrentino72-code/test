"""Purchase Order DQX Quality Rules Module

This module defines comprehensive data quality rules for purchase order items
using the Databricks DQX framework. It implements validation for financial accuracy,
business logic consistency, and data integrity.

Author: DataOps Team
Date: 2024
"""

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

# Configure logging
logger = logging.getLogger(__name__)

# DQX Framework imports with fallback
DQX_AVAILABLE = False
try:
    from databricks.labs.dqx import check_funcs
    from databricks.labs.dqx.engine import DQEngine
    from databricks.labs.dqx.rule import DQDatasetRule, DQForEachColRule, DQRowRule
    from databricks.sdk import WorkspaceClient
    DQX_AVAILABLE = True
    logger.info("✅ Databricks DQX Framework imported successfully")
except ImportError as e:
    logger.warning(f"⚠️ DQX Framework not available: {e}")
    DQEngine = None
    DQRowRule = None
    DQDatasetRule = None
    check_funcs = None


@dataclass
class DQXRuleConfig:
    """Configuration for DQX rules"""
    criticality: str = "error"
    quality_threshold: float = 0.95
    enable_financial_rules: bool = True
    enable_format_rules: bool = True
    enable_range_rules: bool = True
    enable_consistency_rules: bool = True


class PurchaseOrderDQXRules:
    """Databricks DQX quality rules for purchase order item validation.

    This class defines comprehensive quality rules specific to purchase order data,
    including financial calculations, business logic validation, and data integrity checks.
    """

    def __init__(self, config: Optional[DQXRuleConfig] = None):
        """Initialize DQX quality rules for purchase orders.

        Args:
            config: Optional configuration for rule behavior
        """
        self.config = config or DQXRuleConfig()
        self.dqx_rules = []
        self.rule_categories = {
            'critical': [],
            'high': [],
            'medium': [],
            'low': []
        }

        # Initialize DQX engine if available
        self.dqx_engine = None
        if DQX_AVAILABLE:
            try:
                ws = WorkspaceClient()
                self.dqx_engine = DQEngine(ws) if ws else DQEngine()
            except Exception as e:
                logger.warning(f"DQX Engine initialization failed: {e}")
                self.dqx_engine = DQEngine() if DQEngine else None

        # Define all quality rules
        self._define_purchase_order_quality_rules()

    def _define_purchase_order_quality_rules(self):
        """Define comprehensive DQX quality rules for purchase order data"""
        if not DQX_AVAILABLE:
            logger.warning("⚠️ DQX Framework not available - rules will be empty")
            return

        logger.info("📋 Defining DQX quality rules for purchase order validation...")

        # Define rules by category
        self._define_critical_rules()
        self._define_high_priority_rules()
        self._define_medium_priority_rules()
        self._define_low_priority_rules()

        # Combine all rules
        self._combine_all_rules()

        logger.info(f"✅ Defined {len(self.dqx_rules)} DQX quality rules for purchase orders")

    def _define_critical_rules(self):
        """Define critical validation rules (error level)"""
        if not DQX_AVAILABLE or not self.config.enable_financial_rules:
            return

        try:
            # 1. Financial Accuracy - Total amount calculation
            self.rule_categories['critical'].append(
                DQRowRule(
                    name="total_amount_calculation_valid",
                    criticality="error",
                    check_func=check_funcs.sql_expression,
                    column="total_amount",
                    check_func_kwargs={
                        "expression": "abs(total_amount - (quantity * unit_price)) <= 0.01",
                        "msg": "Total amount must equal quantity * unit_price (±0.01 tolerance)"
                    }
                )
            )

            # 2. Positive Values Validation
            positive_value_fields = ['quantity', 'unit_price', 'total_amount']
            for field in positive_value_fields:
                self.rule_categories['critical'].append(
                    DQRowRule(
                        name=f"{field}_positive",
                        criticality="error",
                        check_func=check_funcs.sql_expression,
                        column=field,
                        check_func_kwargs={
                            "expression": f"{field} > 0",
                            "msg": f"{field} must be positive"
                        }
                    )
                )

            # 3. Required Fields Not Null
            required_fields = ['order_id', 'product_id', 'customer_id', 'order_date']
            for field in required_fields:
                self.rule_categories['critical'].append(
                    DQRowRule(
                        name=f"{field}_not_null",
                        criticality="error",
                        check_func=check_funcs.is_not_null_and_not_empty,
                        column=field
                    )
                )

            # 4. Tax Calculation Validation (if applicable)
            self.rule_categories['critical'].append(
                DQRowRule(
                    name="tax_amount_valid",
                    criticality="error",
                    check_func=check_funcs.sql_expression,
                    column="tax_amount",
                    check_func_kwargs={
                        "expression": "tax_amount >= 0 AND tax_amount <= total_amount * 0.3",
                        "msg": "Tax amount must be between 0 and 30% of total amount"
                    }
                )
            )

            logger.info(f"✅ Defined {len(self.rule_categories['critical'])} critical rules")

        except Exception as e:
            logger.error(f"❌ Error defining critical rules: {e}")

    def _define_high_priority_rules(self):
        """Define high priority validation rules (warn level)"""
        if not DQX_AVAILABLE:
            return

        try:
            # 1. Status Validation
            self.rule_categories['high'].append(
                DQRowRule(
                    name="order_status_valid",
                    criticality="warn",
                    check_func=check_funcs.sql_expression,
                    column="order_status",
                    check_func_kwargs={
                        "expression": "order_status IN ('NEW', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED', 'RETURNED')",
                        "msg": "Order status must be a valid value"
                    }
                )
            )

            self.rule_categories['high'].append(
                DQRowRule(
                    name="payment_status_valid",
                    criticality="warn",
                    check_func=check_funcs.sql_expression,
                    column="payment_status",
                    check_func_kwargs={
                        "expression": "payment_status IN ('PENDING', 'PAID', 'REFUNDED', 'FAILED', 'PARTIAL')",
                        "msg": "Payment status must be a valid value"
                    }
                )
            )

            # 2. Business Logic Consistency
            self.rule_categories['high'].append(
                DQRowRule(
                    name="payment_order_status_consistency",
                    criticality="warn",
                    check_func=check_funcs.sql_expression,
                    column="order_status",
                    check_func_kwargs={
                        "expression": """
                            (order_status = 'DELIVERED' AND payment_status = 'PAID') OR
                            (order_status = 'CANCELLED' AND payment_status IN ('REFUNDED', 'FAILED', 'PENDING')) OR
                            (order_status IN ('NEW', 'PROCESSING', 'SHIPPED') AND payment_status IN ('PENDING', 'PAID')) OR
                            (order_status = 'RETURNED' AND payment_status = 'REFUNDED')
                        """,
                        "msg": "Payment status should align with order status"
                    }
                )
            )

            # 3. Shipping Address Validation
            self.rule_categories['high'].append(
                DQRowRule(
                    name="shipping_address_complete",
                    criticality="warn",
                    check_func=check_funcs.sql_expression,
                    column="shipping_address",
                    check_func_kwargs={
                        "expression": "shipping_address IS NOT NULL AND LENGTH(shipping_address) >= 10",
                        "msg": "Shipping address should be complete (at least 10 characters)"
                    }
                )
            )

            logger.info(f"✅ Defined {len(self.rule_categories['high'])} high priority rules")

        except Exception as e:
            logger.error(f"❌ Error defining high priority rules: {e}")

    def _define_medium_priority_rules(self):
        """Define medium priority validation rules (warn level)"""
        if not DQX_AVAILABLE or not self.config.enable_format_rules:
            return

        try:
            # 1. Format Validation - Order ID
            self.rule_categories['medium'].append(
                DQRowRule(
                    name="order_id_format",
                    criticality="warn",
                    check_func=check_funcs.sql_expression,
                    column="order_id",
                    check_func_kwargs={
                        "expression": "order_id RLIKE '^ORD-[0-9]{6}$'",
                        "msg": "Order ID should follow format: ORD-XXXXXX"
                    }
                )
            )

            # 2. Format Validation - Product ID
            self.rule_categories['medium'].append(
                DQRowRule(
                    name="product_id_format",
                    criticality="warn",
                    check_func=check_funcs.sql_expression,
                    column="product_id",
                    check_func_kwargs={
                        "expression": "product_id RLIKE '^PRD[0-9]{3}$'",
                        "msg": "Product ID should follow format: PRD###"
                    }
                )
            )

            # 3. Email Format Validation
            self.rule_categories['medium'].append(
                DQRowRule(
                    name="customer_email_format",
                    criticality="warn",
                    check_func=check_funcs.sql_expression,
                    column="customer_email",
                    check_func_kwargs={
                        "expression": "customer_email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'",
                        "msg": "Customer email should be in valid format"
                    }
                )
            )

            logger.info(f"✅ Defined {len(self.rule_categories['medium'])} medium priority rules")

        except Exception as e:
            logger.error(f"❌ Error defining medium priority rules: {e}")

    def _define_low_priority_rules(self):
        """Define low priority validation rules (warn level)"""
        if not DQX_AVAILABLE or not self.config.enable_range_rules:
            return

        try:
            # 1. Range Validation - Quantity
            self.rule_categories['low'].append(
                DQRowRule(
                    name="quantity_reasonable_range",
                    criticality="warn",
                    check_func=check_funcs.sql_expression,
                    column="quantity",
                    check_func_kwargs={
                        "expression": "quantity <= 1000",
                        "msg": "Quantity should not exceed 1000 units"
                    }
                )
            )

            # 2. Range Validation - Unit Price
            self.rule_categories['low'].append(
                DQRowRule(
                    name="unit_price_reasonable_range",
                    criticality="warn",
                    check_func=check_funcs.sql_expression,
                    column="unit_price",
                    check_func_kwargs={
                        "expression": "unit_price <= 50000",
                        "msg": "Unit price should not exceed 50,000"
                    }
                )
            )

            # 3. Timestamp Validation
            self.rule_categories['low'].append(
                DQRowRule(
                    name="order_date_not_future",
                    criticality="warn",
                    check_func=check_funcs.sql_expression,
                    column="order_date",
                    check_func_kwargs={
                        "expression": "order_date <= CURRENT_TIMESTAMP()",
                        "msg": "Order date cannot be in the future"
                    }
                )
            )

            # 4. Discount Validation
            self.rule_categories['low'].append(
                DQRowRule(
                    name="discount_percentage_valid",
                    criticality="warn",
                    check_func=check_funcs.sql_expression,
                    column="discount_percentage",
                    check_func_kwargs={
                        "expression": "discount_percentage >= 0 AND discount_percentage <= 100",
                        "msg": "Discount percentage should be between 0 and 100"
                    }
                )
            )

            logger.info(f"✅ Defined {len(self.rule_categories['low'])} low priority rules")

        except Exception as e:
            logger.error(f"❌ Error defining low priority rules: {e}")

    def _combine_all_rules(self):
        """Combine all rules from different categories"""
        self.dqx_rules = []
        for category, rules in self.rule_categories.items():
            self.dqx_rules.extend(rules)

        logger.info("📊 Total DQX rules by category:")
        for category, rules in self.rule_categories.items():
            logger.info(f"   {category}: {len(rules)} rules")

    def get_rules(self) -> List:
        """Get all defined DQX rules.

        Returns:
            List of all DQX quality rules
        """
        return self.dqx_rules

    def get_rules_by_category(self, category: str) -> List:
        """Get rules by specific category.

        Args:
            category: Rule category (critical, high, medium, low)

        Returns:
            List of rules for the specified category
        """
        return self.rule_categories.get(category, [])

    def get_critical_rules(self) -> List:
        """Get only critical validation rules"""
        return self.rule_categories.get('critical', [])

    def get_warning_rules(self) -> List:
        """Get all warning level rules (high, medium, low)"""
        warning_rules = []
        for category in ['high', 'medium', 'low']:
            warning_rules.extend(self.rule_categories.get(category, []))
        return warning_rules

    def add_custom_rule(self, rule, category: str = 'medium'):
        """Add a custom DQX rule.

        Args:
            rule: DQX rule object
            category: Rule category for organization
        """
        if DQX_AVAILABLE and rule:
            self.dqx_rules.append(rule)
            if category in self.rule_categories:
                self.rule_categories[category].append(rule)
            logger.info(f"✅ Added custom DQX rule to {category} category")

    def get_rule_summary(self) -> Dict[str, Any]:
        """Get a summary of all defined rules.

        Returns:
            Dictionary containing rule statistics and configuration
        """
        return {
            'total_rules': len(self.dqx_rules),
            'rules_by_category': {cat: len(rules) for cat, rules in self.rule_categories.items()},
            'dqx_available': DQX_AVAILABLE,
            'config': {
                'criticality': self.config.criticality,
                'quality_threshold': self.config.quality_threshold,
                'enabled_features': {
                    'financial_rules': self.config.enable_financial_rules,
                    'format_rules': self.config.enable_format_rules,
                    'range_rules': self.config.enable_range_rules,
                    'consistency_rules': self.config.enable_consistency_rules
                }
            }
        }

    def validate_rule_configuration(self) -> bool:
        """Validate that rules are properly configured.

        Returns:
            True if configuration is valid, False otherwise
        """
        if not DQX_AVAILABLE:
            logger.warning("DQX Framework not available")
            return False

        if len(self.dqx_rules) == 0:
            logger.warning("No DQX rules defined")
            return False

        logger.info(f"✅ Rule configuration valid: {len(self.dqx_rules)} rules defined")
        return True


# Fallback implementation for when DQX is not available
class BasicPurchaseOrderValidation:
    """Fallback validation when DQX framework is not available.

    This provides basic validation logic that can be applied using standard PySpark.
    """

    @staticmethod
    def get_basic_validation_conditions():
        """Get basic validation conditions as PySpark expressions.

        Returns:
            Dictionary of validation conditions
        """
        return {
            'total_amount_valid': "abs(total_amount - (quantity * unit_price)) <= 0.01",
            'quantity_positive': "quantity > 0",
            'unit_price_positive': "unit_price > 0",
            'order_id_not_null': "order_id IS NOT NULL AND order_id != ''",
            'product_id_not_null': "product_id IS NOT NULL AND product_id != ''",
            'customer_id_not_null': "customer_id IS NOT NULL AND customer_id != ''",
            'order_status_valid': "order_status IN ('NEW', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED', 'RETURNED')",
            'payment_status_valid': "payment_status IN ('PENDING', 'PAID', 'REFUNDED', 'FAILED', 'PARTIAL')"
        }
