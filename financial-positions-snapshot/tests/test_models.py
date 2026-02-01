"""Unit tests for data models."""

from datetime import datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from financial_snapshot.models import (
    FinancialPosition,
    PositionType,
    Currency,
    SnapshotMetadata,
    ConsumerOffset,
)


class TestPositionType:
    """Tests for PositionType enum."""
    
    def test_enum_values(self):
        """Test enum values."""
        assert PositionType.LONG.value == "LONG"
        assert PositionType.SHORT.value == "SHORT"
        assert PositionType.HEDGED.value == "HEDGED"


class TestFinancialPosition:
    """Tests for FinancialPosition model."""
    
    def test_valid_position(self):
        """Test creating a valid position."""
        position = FinancialPosition(
            position_id="POS-123",
            account_id="ACC-456",
            symbol="AAPL",
            quantity=Decimal("100.0"),
            price=Decimal("150.25"),
            market_value=Decimal("15025.0"),
            timestamp=datetime(2026, 2, 1, 12, 0, 0),
            currency="USD",
            position_type=PositionType.LONG,
        )
        
        assert position.position_id == "POS-123"
        assert position.account_id == "ACC-456"
        assert position.symbol == "AAPL"
        assert position.quantity == Decimal("100.0")
        assert position.price == Decimal("150.25")
        assert position.market_value == Decimal("15025.0")
        assert position.currency == "USD"
        assert position.position_type == PositionType.LONG
    
    def test_symbol_normalization(self):
        """Test symbol is normalized to uppercase."""
        position = FinancialPosition(
            position_id="POS-123",
            account_id="ACC-456",
            symbol="aapl",
            quantity=Decimal("100.0"),
            price=Decimal("150.25"),
            market_value=Decimal("15025.0"),
            timestamp=datetime(2026, 2, 1, 12, 0, 0),
        )
        
        assert position.symbol == "AAPL"
    
    def test_currency_normalization(self):
        """Test currency is normalized to uppercase."""
        position = FinancialPosition(
            position_id="POS-123",
            account_id="ACC-456",
            symbol="AAPL",
            quantity=Decimal("100.0"),
            price=Decimal("150.25"),
            market_value=Decimal("15025.0"),
            timestamp=datetime(2026, 2, 1, 12, 0, 0),
            currency="usd",
        )
        
        assert position.currency == "USD"
    
    def test_market_value_validation(self):
        """Test market value calculation validation."""
        # Valid market value
        position = FinancialPosition(
            position_id="POS-123",
            account_id="ACC-456",
            symbol="AAPL",
            quantity=Decimal("100.0"),
            price=Decimal("150.25"),
            market_value=Decimal("15025.0"),
            timestamp=datetime(2026, 2, 1, 12, 0, 0),
        )
        assert position.market_value == Decimal("15025.0")
        
        # Slightly off but within tolerance - auto-corrected
        position = FinancialPosition(
            position_id="POS-123",
            account_id="ACC-456",
            symbol="AAPL",
            quantity=Decimal("100.0"),
            price=Decimal("150.25"),
            market_value=Decimal("15025.005"),  # Slightly off
            timestamp=datetime(2026, 2, 1, 12, 0, 0),
        )
        assert position.market_value == Decimal("15025.0")
    
    def test_invalid_market_value(self):
        """Test invalid market value raises error."""
        with pytest.raises(ValidationError):
            FinancialPosition(
                position_id="POS-123",
                account_id="ACC-456",
                symbol="AAPL",
                quantity=Decimal("100.0"),
                price=Decimal("150.25"),
                market_value=Decimal("20000.0"),  # Way off
                timestamp=datetime(2026, 2, 1, 12, 0, 0),
            )
    
    def test_missing_required_fields(self):
        """Test missing required fields raises error."""
        with pytest.raises(ValidationError):
            FinancialPosition(
                position_id="POS-123",
                symbol="AAPL",
                # Missing other required fields
            )
    
    def test_price_must_be_positive(self):
        """Test price must be positive."""
        with pytest.raises(ValidationError):
            FinancialPosition(
                position_id="POS-123",
                account_id="ACC-456",
                symbol="AAPL",
                quantity=Decimal("100.0"),
                price=Decimal("-150.25"),  # Negative
                market_value=Decimal("15025.0"),
                timestamp=datetime(2026, 2, 1, 12, 0, 0),
            )
    
    def test_to_sql_tuple(self):
        """Test conversion to SQL tuple."""
        position = FinancialPosition(
            position_id="POS-123",
            account_id="ACC-456",
            symbol="AAPL",
            quantity=Decimal("100.0"),
            price=Decimal("150.25"),
            market_value=Decimal("15025.0"),
            timestamp=datetime(2026, 2, 1, 12, 0, 0),
            currency="USD",
            position_type=PositionType.LONG,
        )
        
        sql_tuple = position.to_sql_tuple()
        
        assert sql_tuple[0] == "POS-123"
        assert sql_tuple[1] == "ACC-456"
        assert sql_tuple[2] == "AAPL"
        assert sql_tuple[3] == 100.0
        assert sql_tuple[4] == 150.25
        assert sql_tuple[5] == 15025.0
        assert sql_tuple[6] == datetime(2026, 2, 1, 12, 0, 0)
        assert sql_tuple[7] == "USD"
        assert sql_tuple[8] == "LONG"
    
    def test_optional_fields(self):
        """Test optional fields."""
        position = FinancialPosition(
            position_id="POS-123",
            account_id="ACC-456",
            symbol="AAPL",
            quantity=Decimal("100.0"),
            price=Decimal("150.25"),
            market_value=Decimal("15025.0"),
            timestamp=datetime(2026, 2, 1, 12, 0, 0),
            cost_basis=Decimal("14000.0"),
            unrealized_pnl=Decimal("1025.0"),
        )
        
        assert position.cost_basis == Decimal("14000.0")
        assert position.unrealized_pnl == Decimal("1025.0")
    
    def test_short_position(self):
        """Test short position with negative quantity."""
        position = FinancialPosition(
            position_id="POS-123",
            account_id="ACC-456",
            symbol="AAPL",
            quantity=Decimal("-100.0"),
            price=Decimal("150.25"),
            market_value=Decimal("15025.0"),
            timestamp=datetime(2026, 2, 1, 12, 0, 0),
            position_type=PositionType.SHORT,
        )
        
        assert position.quantity == Decimal("-100.0")
        assert position.position_type == PositionType.SHORT


class TestSnapshotMetadata:
    """Tests for SnapshotMetadata model."""
    
    def test_valid_metadata(self):
        """Test creating valid metadata."""
        start_time = datetime(2026, 2, 1, 12, 0, 0)
        end_time = datetime(2026, 2, 1, 12, 5, 0)
        
        metadata = SnapshotMetadata(
            snapshot_id="snapshot_20260201_120000",
            start_time=start_time,
            end_time=end_time,
            record_count=1000,
            success=True,
        )
        
        assert metadata.snapshot_id == "snapshot_20260201_120000"
        assert metadata.start_time == start_time
        assert metadata.end_time == end_time
        assert metadata.record_count == 1000
        assert metadata.success is True
        assert metadata.error_message is None
    
    def test_failed_snapshot(self):
        """Test failed snapshot metadata."""
        metadata = SnapshotMetadata(
            snapshot_id="snapshot_20260201_120000",
            start_time=datetime(2026, 2, 1, 12, 0, 0),
            success=False,
            error_message="Connection timeout",
        )
        
        assert metadata.success is False
        assert metadata.error_message == "Connection timeout"
    
    def test_record_count_validation(self):
        """Test record count must be non-negative."""
        with pytest.raises(ValidationError):
            SnapshotMetadata(
                snapshot_id="snapshot_20260201_120000",
                start_time=datetime(2026, 2, 1, 12, 0, 0),
                record_count=-1,
            )


class TestConsumerOffset:
    """Tests for ConsumerOffset model."""
    
    def test_valid_offset(self):
        """Test creating valid offset."""
        offset = ConsumerOffset(
            topic="financial-positions",
            partition=0,
            offset=12345,
        )
        
        assert offset.topic == "financial-positions"
        assert offset.partition == 0
        assert offset.offset == 12345
        assert isinstance(offset.timestamp, datetime)
    
    def test_with_timestamp(self):
        """Test offset with custom timestamp."""
        timestamp = datetime(2026, 2, 1, 12, 0, 0)
        
        offset = ConsumerOffset(
            topic="financial-positions",
            partition=0,
            offset=12345,
            timestamp=timestamp,
        )
        
        assert offset.timestamp == timestamp
