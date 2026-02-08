"""
Financial Positions Snapshot Library

A robust, high-performance library for capturing snapshots of live financial 
positions from Kafka topics and persisting them to SQL Server databases.
"""

__version__ = "1.0.0"
__author__ = "Financial Engineering Team"

from financial_snapshot.service import FinancialSnapshotService
from financial_snapshot.config import Config, load_config
from financial_snapshot.models import FinancialPosition

__all__ = [
    "FinancialSnapshotService",
    "Config",
    "load_config",
    "FinancialPosition",
]
