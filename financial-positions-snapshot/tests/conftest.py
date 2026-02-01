"""
Pytest configuration and fixtures.
"""

import sys
from pathlib import Path

# Add the parent directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest


@pytest.fixture
def sample_kafka_message():
    """Fixture for sample Kafka message data."""
    return {
        "position_id": "POS-12345",
        "account_id": "ACC-67890",
        "symbol": "AAPL",
        "quantity": "100.0",
        "price": "150.25",
        "market_value": "15025.0",
        "timestamp": "2026-02-01T12:00:00Z",
        "currency": "USD",
        "position_type": "LONG",
    }


@pytest.fixture
def sample_position_list():
    """Fixture for list of sample positions."""
    from datetime import datetime
    from decimal import Decimal
    from financial_snapshot.models import FinancialPosition
    
    return [
        FinancialPosition(
            position_id=f"POS-{i}",
            account_id=f"ACC-{i//10}",
            symbol=symbol,
            quantity=Decimal("100.0"),
            price=Decimal("150.25"),
            market_value=Decimal("15025.0"),
            timestamp=datetime(2026, 2, 1, 12, 0, 0),
            currency="USD",
        )
        for i, symbol in enumerate(["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"] * 20)
    ]
