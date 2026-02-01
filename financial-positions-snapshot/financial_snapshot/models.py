"""Data models for financial positions."""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class PositionType(str, Enum):
    """Position type enumeration."""
    LONG = "LONG"
    SHORT = "SHORT"
    HEDGED = "HEDGED"


class Currency(str, Enum):
    """Common currency codes."""
    USD = "USD"
    EUR = "EUR"
    GBP = "GBP"
    JPY = "JPY"
    CHF = "CHF"
    AUD = "AUD"
    CAD = "CAD"


class FinancialPosition(BaseModel):
    """
    Financial position data model.
    
    Represents a single financial position at a point in time.
    """
    
    position_id: str = Field(
        ...,
        description="Unique position identifier",
        min_length=1,
        max_length=100
    )
    account_id: str = Field(
        ...,
        description="Account identifier",
        min_length=1,
        max_length=100
    )
    symbol: str = Field(
        ...,
        description="Financial instrument symbol",
        min_length=1,
        max_length=50
    )
    quantity: Decimal = Field(
        ...,
        description="Position quantity",
        decimal_places=8
    )
    price: Decimal = Field(
        ...,
        description="Current price per unit",
        decimal_places=4,
        gt=0
    )
    market_value: Decimal = Field(
        ...,
        description="Total market value",
        decimal_places=4
    )
    timestamp: datetime = Field(
        ...,
        description="Position timestamp"
    )
    currency: str = Field(
        default="USD",
        description="Position currency",
        min_length=3,
        max_length=10
    )
    position_type: PositionType = Field(
        default=PositionType.LONG,
        description="Type of position"
    )
    
    # Optional fields for extended information
    cost_basis: Optional[Decimal] = Field(
        default=None,
        description="Cost basis of the position",
        decimal_places=4
    )
    unrealized_pnl: Optional[Decimal] = Field(
        default=None,
        description="Unrealized profit/loss",
        decimal_places=4
    )
    
    class Config:
        """Pydantic configuration."""
        json_encoders = {
            Decimal: lambda v: float(v),
            datetime: lambda v: v.isoformat(),
        }
        use_enum_values = True
    
    @field_validator('symbol')
    @classmethod
    def validate_symbol(cls, v: str) -> str:
        """Validate and normalize symbol."""
        return v.strip().upper()
    
    @field_validator('currency')
    @classmethod
    def validate_currency(cls, v: str) -> str:
        """Validate and normalize currency code."""
        return v.strip().upper()
    
    @model_validator(mode='after')
    def validate_market_value(self) -> 'FinancialPosition':
        """Validate market value calculation."""
        expected_value = abs(self.quantity * self.price)
        tolerance = Decimal('0.01')  # 1 cent tolerance
        
        if abs(self.market_value - expected_value) > tolerance:
            # Auto-correct if within reasonable bounds
            if abs(self.market_value - expected_value) < expected_value * Decimal('0.01'):
                self.market_value = expected_value
            else:
                raise ValueError(
                    f"Market value {self.market_value} does not match "
                    f"quantity * price = {expected_value}"
                )
        
        return self
    
    def to_sql_tuple(self) -> tuple:
        """
        Convert to tuple for SQL insertion.
        
        Returns:
            tuple: Values in order matching SQL schema
        """
        return (
            self.position_id,
            self.account_id,
            self.symbol,
            float(self.quantity),
            float(self.price),
            float(self.market_value),
            self.timestamp,
            self.currency,
            self.position_type.value,
        )


class SnapshotMetadata(BaseModel):
    """Metadata for a snapshot batch."""
    
    snapshot_id: str = Field(
        ...,
        description="Unique snapshot identifier"
    )
    start_time: datetime = Field(
        ...,
        description="Snapshot start time"
    )
    end_time: Optional[datetime] = Field(
        default=None,
        description="Snapshot end time"
    )
    record_count: int = Field(
        default=0,
        ge=0,
        description="Number of records in snapshot"
    )
    success: bool = Field(
        default=False,
        description="Whether snapshot completed successfully"
    )
    error_message: Optional[str] = Field(
        default=None,
        description="Error message if failed"
    )
    
    class Config:
        """Pydantic configuration."""
        json_encoders = {
            datetime: lambda v: v.isoformat(),
        }


class ConsumerOffset(BaseModel):
    """Kafka consumer offset information."""
    
    topic: str
    partition: int
    offset: int
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        """Pydantic configuration."""
        json_encoders = {
            datetime: lambda v: v.isoformat(),
        }
