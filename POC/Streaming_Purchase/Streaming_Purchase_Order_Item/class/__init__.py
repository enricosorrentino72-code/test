# Purchase Order Item Classes Package
"""
This package contains all the class definitions for the Purchase Order Item streaming pipeline.

Classes:
- PurchaseOrderItem: Data model with financial validation
- PurchaseOrderItemFactory: Factory for generating realistic purchase order data
- PurchaseOrderItemProducer: EventHub producer for streaming purchase order data
- PurchaseOrderItemListener: EventHub listener for Bronze layer processing
- PurchaseOrderDQXRules: DQX quality rules for purchase order validation
- PurchaseOrderDQXPipeline: Bronze to Silver transformation with DQX quality validation
- PurchaseOrderSilverManager: Silver layer table management with DQX metadata
- PurchaseOrderDQXMonitor: Monitoring and dashboard for DQX pipeline
"""

__version__ = "1.1.0"
__author__ = "Purchase Order Pipeline Team"

# Import main classes for easy access
from .purchase_order_item_model import PurchaseOrderItem
from .purchase_order_item_factory import PurchaseOrderItemFactory
from .purchase_order_item_producer import PurchaseOrderItemProducer
from .purchase_order_item_listener import PurchaseOrderItemListener

# Import DQX classes
from .purchase_order_dqx_rules import PurchaseOrderDQXRules, DQXRuleConfig
from .purchase_order_dqx_pipeline import PurchaseOrderDQXPipeline, PipelineConfig
from .purchase_order_silver_manager import PurchaseOrderSilverManager, SilverTableConfig
from .purchase_order_dqx_monitor import PurchaseOrderDQXMonitor, MonitorConfig

__all__ = [
    # Core classes
    "PurchaseOrderItem",
    "PurchaseOrderItemFactory",
    "PurchaseOrderItemProducer",
    "PurchaseOrderItemListener",

    # DQX classes
    "PurchaseOrderDQXRules",
    "PurchaseOrderDQXPipeline",
    "PurchaseOrderSilverManager",
    "PurchaseOrderDQXMonitor",

    # Configuration classes
    "DQXRuleConfig",
    "PipelineConfig",
    "SilverTableConfig",
    "MonitorConfig"
]