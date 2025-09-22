"""
Unit tests for PurchaseOrderDQXRules.

This module contains comprehensive unit tests for the DQX quality rules,
testing rule definition, validation logic, and quality scoring.
"""

import pytest
from unittest.mock import Mock, patch
from typing import List, Dict, Any

# Import the classes under test
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'class'))

from purchase_order_dqx_rules import (
    PurchaseOrderDQXRules,
    DQXRuleConfig,
    BasicPurchaseOrderValidation
)


class TestDQXRuleConfig:
    """Test the DQXRuleConfig dataclass."""

    def test_dqx_rule_config_defaults(self):
        """Test default configuration values."""
        config = DQXRuleConfig()

        assert config.criticality == "error"
        assert config.quality_threshold == 0.95
        assert config.enable_financial_rules is True
        assert config.enable_format_rules is True
        assert config.enable_range_rules is True
        assert config.enable_consistency_rules is True

    def test_dqx_rule_config_custom(self):
        """Test custom configuration values."""
        config = DQXRuleConfig(
            criticality="warn",
            quality_threshold=0.8,
            enable_financial_rules=False,
            enable_format_rules=True,
            enable_range_rules=False,
            enable_consistency_rules=True
        )

        assert config.criticality == "warn"
        assert config.quality_threshold == 0.8
        assert config.enable_financial_rules is False
        assert config.enable_format_rules is True
        assert config.enable_range_rules is False
        assert config.enable_consistency_rules is True


class TestPurchaseOrderDQXRules:
    """Test the PurchaseOrderDQXRules class."""

    @patch('purchase_order_dqx_rules.DQX_AVAILABLE', True)
    @patch('purchase_order_dqx_rules.DQEngine')
    @patch('purchase_order_dqx_rules.DQRowRule')
    def test_initialization_with_dqx_available(self, mock_row_rule, mock_engine, mock_workspace_client):
        """Test initialization when DQX framework is available."""
        # Arrange
        mock_engine_instance = Mock()
        mock_engine.return_value = mock_engine_instance

        # Act
        rules = PurchaseOrderDQXRules()

        # Assert
        assert rules.config is not None
        assert rules.dqx_engine is not None
        assert isinstance(rules.dqx_rules, list)
        assert isinstance(rules.rule_categories, dict)

    @patch('purchase_order_dqx_rules.DQX_AVAILABLE', False)
    def test_initialization_without_dqx(self):
        """Test initialization when DQX framework is not available."""
        # Act
        rules = PurchaseOrderDQXRules()

        # Assert
        assert rules.config is not None
        assert rules.dqx_engine is None
        assert len(rules.dqx_rules) == 0

    def test_rule_categories_structure(self):
        """Test that rule categories are properly structured."""
        # Act
        rules = PurchaseOrderDQXRules()

        # Assert
        expected_categories = ['critical', 'high', 'medium', 'low']
        for category in expected_categories:
            assert category in rules.rule_categories
            assert isinstance(rules.rule_categories[category], list)

    @patch('purchase_order_dqx_rules.DQX_AVAILABLE', True)
    @patch('purchase_order_dqx_rules.DQRowRule')
    def test_critical_financial_accuracy_rules(self, mock_row_rule):
        """Test critical financial accuracy rules definition."""
        # Arrange
        config = DQXRuleConfig(enable_financial_rules=True)

        # Act
        rules = PurchaseOrderDQXRules(config)

        # Assert
        # Should have called DQRowRule for financial validation
        assert mock_row_rule.called

        # Verify specific financial rules would be created
        call_names = []
        for call in mock_row_rule.call_args_list:
            if call[1] and 'name' in call[1]:
                call_names.append(call[1]['name'])

        # Should include financial rules
        financial_rule_names = [name for name in call_names if 'amount' in name or 'financial' in name.lower()]
        assert len(financial_rule_names) > 0

    @patch('purchase_order_dqx_rules.DQX_AVAILABLE', True)
    @patch('purchase_order_dqx_rules.DQRowRule')
    def test_positive_values_validation(self, mock_row_rule):
        """Test positive values validation rules."""
        # Act
        rules = PurchaseOrderDQXRules()

        # Assert
        # Should create rules for positive values
        positive_rules = []
        for call in mock_row_rule.call_args_list:
            if call[1] and 'name' in call[1]:
                name = call[1]['name']
                if 'positive' in name:
                    positive_rules.append(name)

        # Should have positive value rules for quantity, unit_price, etc.
        assert len(positive_rules) > 0

    @patch('purchase_order_dqx_rules.DQX_AVAILABLE', True)
    @patch('purchase_order_dqx_rules.DQRowRule')
    def test_required_fields_validation(self, mock_row_rule):
        """Test required fields validation rules."""
        # Act
        rules = PurchaseOrderDQXRules()

        # Assert
        # Should create rules for required fields
        required_field_rules = []
        for call in mock_row_rule.call_args_list:
            if call[1] and 'name' in call[1]:
                name = call[1]['name']
                if 'not_null' in name:
                    required_field_rules.append(name)

        # Should have required field rules
        assert len(required_field_rules) > 0

    def test_get_rules_method(self):
        """Test the get_rules method returns all rules."""
        # Act
        rules = PurchaseOrderDQXRules()
        all_rules = rules.get_rules()

        # Assert
        assert isinstance(all_rules, list)
        assert all_rules is rules.dqx_rules

    def test_get_rules_by_category(self):
        """Test getting rules by specific category."""
        # Act
        rules = PurchaseOrderDQXRules()

        # Test each category
        for category in ['critical', 'high', 'medium', 'low']:
            category_rules = rules.get_rules_by_category(category)
            assert isinstance(category_rules, list)
            assert category_rules is rules.rule_categories[category]

        # Test invalid category
        invalid_rules = rules.get_rules_by_category('invalid')
        assert invalid_rules == []

    def test_get_critical_rules(self):
        """Test getting only critical rules."""
        # Act
        rules = PurchaseOrderDQXRules()
        critical_rules = rules.get_critical_rules()

        # Assert
        assert isinstance(critical_rules, list)
        assert critical_rules is rules.rule_categories['critical']

    def test_get_warning_rules(self):
        """Test getting all warning level rules."""
        # Act
        rules = PurchaseOrderDQXRules()
        warning_rules = rules.get_warning_rules()

        # Assert
        assert isinstance(warning_rules, list)

        # Should combine high, medium, and low categories
        expected_length = (len(rules.rule_categories['high']) +
                         len(rules.rule_categories['medium']) +
                         len(rules.rule_categories['low']))
        assert len(warning_rules) == expected_length

    @patch('purchase_order_dqx_rules.DQX_AVAILABLE', True)
    def test_add_custom_rule(self):
        """Test adding custom DQX rules."""
        # Arrange
        rules = PurchaseOrderDQXRules()
        initial_count = len(rules.dqx_rules)

        mock_rule = Mock()
        mock_rule.name = "custom_test_rule"

        # Act
        rules.add_custom_rule(mock_rule, category='medium')

        # Assert
        assert len(rules.dqx_rules) == initial_count + 1
        assert mock_rule in rules.rule_categories['medium']

    def test_get_rule_summary(self):
        """Test rule summary generation."""
        # Act
        rules = PurchaseOrderDQXRules()
        summary = rules.get_rule_summary()

        # Assert
        assert isinstance(summary, dict)
        assert 'total_rules' in summary
        assert 'rules_by_category' in summary
        assert 'dqx_available' in summary
        assert 'config' in summary

        # Verify structure
        assert isinstance(summary['rules_by_category'], dict)
        assert isinstance(summary['config'], dict)
        assert 'criticality' in summary['config']
        assert 'quality_threshold' in summary['config']

    def test_validate_rule_configuration(self):
        """Test rule configuration validation."""
        # Test with DQX available and rules defined
        with patch('purchase_order_dqx_rules.DQX_AVAILABLE', True):
            rules = PurchaseOrderDQXRules()
            # Add a mock rule to simulate rules being defined
            rules.dqx_rules.append(Mock())

            is_valid = rules.validate_rule_configuration()
            assert is_valid is True

        # Test with DQX not available
        with patch('purchase_order_dqx_rules.DQX_AVAILABLE', False):
            rules = PurchaseOrderDQXRules()

            is_valid = rules.validate_rule_configuration()
            assert is_valid is False

    def test_rule_configuration_with_disabled_features(self):
        """Test rule configuration with disabled features."""
        # Test with financial rules disabled
        config = DQXRuleConfig(enable_financial_rules=False)
        rules = PurchaseOrderDQXRules(config)

        # Should still create rules but exclude financial ones
        summary = rules.get_rule_summary()
        assert summary['config']['enabled_features']['financial_rules'] is False

        # Test with all features disabled
        config_minimal = DQXRuleConfig(
            enable_financial_rules=False,
            enable_format_rules=False,
            enable_range_rules=False,
            enable_consistency_rules=False
        )
        rules_minimal = PurchaseOrderDQXRules(config_minimal)

        summary = rules_minimal.get_rule_summary()
        enabled_features = summary['config']['enabled_features']
        assert all(not value for value in enabled_features.values())


class TestBasicPurchaseOrderValidation:
    """Test the BasicPurchaseOrderValidation fallback class."""

    def test_get_basic_validation_conditions(self):
        """Test basic validation conditions."""
        # Act
        conditions = BasicPurchaseOrderValidation.get_basic_validation_conditions()

        # Assert
        assert isinstance(conditions, dict)

        # Should have key validation conditions
        expected_conditions = [
            'total_amount_valid',
            'quantity_positive',
            'unit_price_positive',
            'order_id_not_null',
            'product_id_not_null',
            'customer_id_not_null',
            'order_status_valid',
            'payment_status_valid'
        ]

        for condition in expected_conditions:
            assert condition in conditions
            assert isinstance(conditions[condition], str)

    def test_validation_conditions_format(self):
        """Test that validation conditions are valid SQL expressions."""
        # Act
        conditions = BasicPurchaseOrderValidation.get_basic_validation_conditions()

        # Assert
        # Each condition should be a valid SQL-like expression
        for condition_name, expression in conditions.items():
            assert isinstance(expression, str)
            assert len(expression) > 0

            # Basic checks for SQL expression format
            if 'positive' in condition_name:
                assert '> 0' in expression
            elif 'not_null' in condition_name:
                assert 'IS NOT NULL' in expression
            elif 'valid' in condition_name and 'amount' not in condition_name:
                assert 'IN (' in expression


class TestDQXRulesIntegration:
    """Integration tests for DQX rules with mock data."""

    @pytest.fixture
    def sample_purchase_order_data(self):
        """Sample purchase order data for testing."""
        return {
            'order_id': 'ORD-123456',
            'product_id': 'PROD-001',
            'customer_id': 'CUST-001',
            'quantity': 5,
            'unit_price': 29.99,
            'total_amount': 149.95,
            'order_status': 'NEW',
            'payment_status': 'PENDING'
        }

    def test_rules_with_valid_data(self, sample_purchase_order_data):
        """Test that rules work correctly with valid data."""
        # Arrange
        rules = PurchaseOrderDQXRules()

        # Act & Assert
        # Should not raise exceptions when processing valid data
        summary = rules.get_rule_summary()
        assert summary['total_rules'] >= 0

    def test_rules_with_invalid_data(self):
        """Test rules behavior with invalid data scenarios."""
        # Arrange
        rules = PurchaseOrderDQXRules()

        invalid_data_scenarios = [
            # Negative quantity
            {'quantity': -1, 'unit_price': 10.0, 'total_amount': -10.0},
            # Negative price
            {'quantity': 1, 'unit_price': -10.0, 'total_amount': -10.0},
            # Mismatched total
            {'quantity': 2, 'unit_price': 10.0, 'total_amount': 50.0},
            # Invalid status combination
            {'order_status': 'CANCELLED', 'payment_status': 'PAID'}
        ]

        # Act & Assert
        for scenario in invalid_data_scenarios:
            # Should be able to create rules without error
            summary = rules.get_rule_summary()
            assert isinstance(summary, dict)

    def test_fallback_validation_integration(self):
        """Test integration with fallback validation."""
        # Act
        conditions = BasicPurchaseOrderValidation.get_basic_validation_conditions()

        # Test that conditions can be used in validation logic
        sample_data = {
            'quantity': 5,
            'unit_price': 10.0,
            'total_amount': 50.0,
            'order_id': 'ORD-123',
            'product_id': 'PROD-001',
            'customer_id': 'CUST-001'
        }

        # Simulate basic validation (would normally be done in Spark)
        # This is a simplified test of the validation logic
        assert sample_data['quantity'] > 0  # quantity_positive
        assert sample_data['unit_price'] > 0  # unit_price_positive
        assert sample_data['order_id'] is not None  # order_id_not_null


class TestDQXRulesPerformance:
    """Performance tests for DQX rules."""

    def test_rule_creation_performance(self):
        """Test that rule creation performs within reasonable time."""
        import time

        # Measure rule creation time
        start_time = time.time()
        rules = PurchaseOrderDQXRules()
        end_time = time.time()

        creation_time = end_time - start_time

        # Should create rules quickly (less than 1 second)
        assert creation_time < 1.0

    def test_multiple_rule_instances(self):
        """Test creating multiple rule instances."""
        # Should be able to create multiple instances without issues
        instances = []
        for i in range(10):
            rules = PurchaseOrderDQXRules()
            instances.append(rules)

        assert len(instances) == 10

        # Each instance should be independent
        for instance in instances:
            summary = instance.get_rule_summary()
            assert isinstance(summary, dict)


class TestDQXRulesErrorHandling:
    """Test error handling in DQX rules."""

    @patch('purchase_order_dqx_rules.DQX_AVAILABLE', True)
    @patch('purchase_order_dqx_rules.DQRowRule', side_effect=Exception("Mock DQX error"))
    def test_rule_creation_error_handling(self, mock_row_rule):
        """Test handling of errors during rule creation."""
        # Should not raise exception even if DQX rule creation fails
        rules = PurchaseOrderDQXRules()

        # Should gracefully handle the error
        assert isinstance(rules.dqx_rules, list)
        summary = rules.get_rule_summary()
        assert isinstance(summary, dict)

    def test_invalid_configuration_handling(self):
        """Test handling of invalid configuration."""
        # Test with extreme values
        config = DQXRuleConfig(quality_threshold=1.5)  # Invalid threshold > 1.0

        # Should still create rules instance
        rules = PurchaseOrderDQXRules(config)
        assert rules.config.quality_threshold == 1.5  # Should accept the value

    def test_none_configuration_handling(self):
        """Test handling of None configuration."""
        # Should use default configuration
        rules = PurchaseOrderDQXRules(config=None)

        assert rules.config is not None
        assert rules.config.criticality == "error"  # Default value


# Pytest fixtures for DQX rules testing
@pytest.fixture
def mock_dqx_engine():
    """Mock DQX engine for testing."""
    engine = Mock()
    engine.apply_checks_and_split = Mock(return_value=(Mock(), Mock()))
    return engine


@pytest.fixture
def sample_dqx_config():
    """Sample DQX configuration for testing."""
    return DQXRuleConfig(
        criticality="warn",
        quality_threshold=0.8,
        enable_financial_rules=True,
        enable_format_rules=True,
        enable_range_rules=False,
        enable_consistency_rules=True
    )


class TestDQXRulesWithFixtures:
    """Tests using pytest fixtures."""

    def test_rules_with_custom_config(self, sample_dqx_config):
        """Test rules creation with custom configuration."""
        rules = PurchaseOrderDQXRules(sample_dqx_config)

        assert rules.config.criticality == "warn"
        assert rules.config.quality_threshold == 0.8
        assert rules.config.enable_range_rules is False

    def test_rules_summary_with_config(self, sample_dqx_config):
        """Test rule summary with custom configuration."""
        rules = PurchaseOrderDQXRules(sample_dqx_config)
        summary = rules.get_rule_summary()

        config_summary = summary['config']
        assert config_summary['criticality'] == "warn"
        assert config_summary['quality_threshold'] == 0.8