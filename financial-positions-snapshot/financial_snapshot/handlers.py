"""Abstract base class and implementations for Kafka message handlers."""

import json
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Type
from decimal import Decimal

from pydantic import BaseModel, ValidationError

from financial_snapshot.models import FinancialPosition


logger = logging.getLogger(__name__)


class AbcMsgHandler(ABC):
    """
    Abstract base class for Kafka message handlers.
    
    All message handlers must implement parse_message to convert
    raw Kafka message bytes into validated Python objects.
    """
    
    @abstractmethod
    def parse_message(self, message_value: bytes, message_key: Optional[bytes] = None) -> BaseModel:
        """
        Parse and validate a Kafka message.
        
        Args:
            message_value: Raw message value bytes
            message_key: Optional message key bytes
            
        Returns:
            BaseModel: Validated Pydantic model instance
            
        Raises:
            ValueError: If message cannot be parsed or validated
        """
        pass
    
    @abstractmethod
    def get_table_name(self, message_key: Optional[bytes] = None) -> str:
        """
        Get target SQL table name for this message.
        
        Args:
            message_key: Optional message key bytes
            
        Returns:
            str: SQL table name (database.schema.table format)
        """
        pass
    
    @abstractmethod
    def get_model_class(self) -> Type[BaseModel]:
        """
        Get the Pydantic model class for this handler.
        
        Returns:
            Type[BaseModel]: Pydantic model class
        """
        pass


class BaseMsgHandler(AbcMsgHandler):
    """
    Generic base message handler for JSON messages.
    
    Handles JSON-encoded messages and validates them against
    a configured Pydantic model.
    """
    
    def __init__(
        self,
        model_class: Type[BaseModel],
        table_name: str,
        key_to_table_mapping: Optional[Dict[str, str]] = None
    ):
        """
        Initialize base message handler.
        
        Args:
            model_class: Pydantic model class for validation
            table_name: Default SQL table name
            key_to_table_mapping: Optional mapping of message keys to table names
        """
        self.model_class = model_class
        self.default_table_name = table_name
        self.key_to_table_mapping = key_to_table_mapping or {}
        
        logger.info(
            "BaseMsgHandler initialized",
            extra={
                "model": model_class.__name__,
                "table": table_name,
                "key_mappings": len(self.key_to_table_mapping)
            }
        )
    
    def parse_message(self, message_value: bytes, message_key: Optional[bytes] = None) -> BaseModel:
        """
        Parse JSON message and validate against model.
        
        Args:
            message_value: Raw message value bytes
            message_key: Optional message key bytes
            
        Returns:
            BaseModel: Validated model instance
            
        Raises:
            ValueError: If parsing or validation fails
        """
        try:
            # Decode JSON
            data = json.loads(message_value.decode('utf-8'))
            
            # Validate with Pydantic model
            model_instance = self.model_class(**data)
            
            return model_instance
            
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON: {e}") from e
        except ValidationError as e:
            raise ValueError(f"Validation failed: {e}") from e
        except Exception as e:
            raise ValueError(f"Unexpected error parsing message: {e}") from e
    
    def get_table_name(self, message_key: Optional[bytes] = None) -> str:
        """
        Get target table name based on message key.
        
        Args:
            message_key: Optional message key bytes
            
        Returns:
            str: SQL table name
        """
        if message_key and self.key_to_table_mapping:
            key_str = message_key.decode('utf-8') if isinstance(message_key, bytes) else str(message_key)
            return self.key_to_table_mapping.get(key_str, self.default_table_name)
        return self.default_table_name
    
    def get_model_class(self) -> Type[BaseModel]:
        """Get the Pydantic model class."""
        return self.model_class


class AvroMsgHandler(BaseMsgHandler):
    """
    Avro message handler for schema-based messages.
    
    Handles Avro-encoded messages with schema registry integration.
    """
    
    def __init__(
        self,
        model_class: Type[BaseModel],
        table_name: str,
        schema_registry_url: Optional[str] = None,
        key_to_table_mapping: Optional[Dict[str, str]] = None
    ):
        """
        Initialize Avro message handler.
        
        Args:
            model_class: Pydantic model class for validation
            table_name: Default SQL table name
            schema_registry_url: Optional Confluent Schema Registry URL
            key_to_table_mapping: Optional mapping of message keys to table names
        """
        super().__init__(model_class, table_name, key_to_table_mapping)
        self.schema_registry_url = schema_registry_url
        self._deserializer = None
        
        logger.info(
            "AvroMsgHandler initialized",
            extra={
                "schema_registry": schema_registry_url or "not configured"
            }
        )
    
    def _get_deserializer(self):
        """Lazy-load Avro deserializer."""
        if self._deserializer is None:
            try:
                from confluent_kafka.schema_registry import SchemaRegistryClient
                from confluent_kafka.schema_registry.avro import AvroDeserializer
                
                if self.schema_registry_url:
                    schema_registry_client = SchemaRegistryClient({
                        'url': self.schema_registry_url
                    })
                    self._deserializer = AvroDeserializer(schema_registry_client)
                else:
                    logger.warning("Schema registry URL not configured, falling back to JSON")
                    
            except ImportError:
                logger.warning("Avro libraries not available, falling back to JSON parsing")
                
        return self._deserializer
    
    def parse_message(self, message_value: bytes, message_key: Optional[bytes] = None) -> BaseModel:
        """
        Parse Avro message or fall back to JSON.
        
        Args:
            message_value: Raw message value bytes
            message_key: Optional message key bytes
            
        Returns:
            BaseModel: Validated model instance
            
        Raises:
            ValueError: If parsing or validation fails
        """
        deserializer = self._get_deserializer()
        
        if deserializer:
            try:
                # Deserialize Avro message
                data = deserializer(message_value, None)
                
                # Validate with Pydantic model
                model_instance = self.model_class(**data)
                
                return model_instance
                
            except Exception as e:
                logger.warning(
                    f"Avro deserialization failed, trying JSON: {e}"
                )
                # Fall back to JSON parsing
                return super().parse_message(message_value, message_key)
        else:
            # No Avro support, use JSON
            return super().parse_message(message_value, message_key)


class FinancialPositionHandler(BaseMsgHandler):
    """Handler specifically for FinancialPosition messages."""
    
    def __init__(
        self,
        table_name: str = "FinanceDB.dbo.FinancialPositions",
        key_to_table_mapping: Optional[Dict[str, str]] = None
    ):
        """
        Initialize FinancialPosition handler.
        
        Args:
            table_name: SQL table name (database.schema.table)
            key_to_table_mapping: Optional key-to-table mapping
        """
        super().__init__(
            model_class=FinancialPosition,
            table_name=table_name,
            key_to_table_mapping=key_to_table_mapping
        )


# Handler registry for dynamic handler selection
HANDLER_REGISTRY: Dict[str, Type[AbcMsgHandler]] = {
    "base": BaseMsgHandler,
    "avro": AvroMsgHandler,
    "financial_position": FinancialPositionHandler,
}


def get_handler(
    handler_type: str,
    model_class: Type[BaseModel],
    table_name: str,
    **kwargs
) -> AbcMsgHandler:
    """
    Factory function to create message handler instances.
    
    Args:
        handler_type: Type of handler ("base", "avro", "financial_position")
        model_class: Pydantic model class
        table_name: SQL table name
        **kwargs: Additional handler-specific arguments
        
    Returns:
        AbcMsgHandler: Handler instance
        
    Raises:
        ValueError: If handler type is unknown
    """
    handler_class = HANDLER_REGISTRY.get(handler_type.lower())
    
    if not handler_class:
        raise ValueError(
            f"Unknown handler type: {handler_type}. "
            f"Available: {list(HANDLER_REGISTRY.keys())}"
        )
    
    if handler_type.lower() == "financial_position":
        return handler_class(table_name=table_name, **kwargs)
    else:
        return handler_class(
            model_class=model_class,
            table_name=table_name,
            **kwargs
        )
