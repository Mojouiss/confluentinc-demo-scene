"""Kafka consumer module for financial positions."""

import json
import logging
from typing import List, Optional, Dict, Any, Callable
from datetime import datetime

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from pydantic import ValidationError

from financial_snapshot.config import KafkaConfig
from financial_snapshot.models import FinancialPosition, ConsumerOffset


logger = logging.getLogger(__name__)


class KafkaConsumerError(Exception):
    """Base exception for Kafka consumer errors."""
    pass


class MessageValidationError(KafkaConsumerError):
    """Exception for message validation errors."""
    pass


class FinancialPositionConsumer:
    """
    Kafka consumer for financial positions.
    
    Provides reliable consumption of financial position messages with
    batch processing, error handling, and offset management.
    """
    
    def __init__(self, config: KafkaConfig):
        """
        Initialize the Kafka consumer.
        
        Args:
            config: Kafka configuration
        """
        self.config = config
        self._consumer: Optional[Consumer] = None
        self._message_count = 0
        self._error_count = 0
        self._last_commit_offset: Dict[int, int] = {}
        
        logger.info(
            "Initializing Kafka consumer",
            extra={
                "bootstrap_servers": config.bootstrap_servers,
                "topic": config.topic,
                "group_id": config.group_id,
            }
        )
    
    def _create_consumer(self) -> Consumer:
        """
        Create and configure Kafka consumer.
        
        Returns:
            Consumer: Configured Kafka consumer
        """
        consumer_config = {
            'bootstrap.servers': self.config.bootstrap_servers,
            'group.id': self.config.group_id,
            'auto.offset.reset': self.config.auto_offset_reset,
            'enable.auto.commit': self.config.enable_auto_commit,
            'session.timeout.ms': self.config.session_timeout_ms,
            'max.poll.interval.ms': 300000,  # 5 minutes
            'api.version.request': True,
            'client.id': f'financial-snapshot-{self.config.group_id}',
        }
        
        return Consumer(consumer_config)
    
    def connect(self) -> None:
        """
        Connect to Kafka and subscribe to topic.
        
        Raises:
            KafkaConsumerError: If connection fails
        """
        try:
            self._consumer = self._create_consumer()
            self._consumer.subscribe([self.config.topic])
            
            logger.info(
                "Successfully connected to Kafka",
                extra={"topic": self.config.topic}
            )
        except KafkaException as e:
            logger.error(
                "Failed to connect to Kafka",
                extra={"error": str(e)},
                exc_info=True
            )
            raise KafkaConsumerError(f"Failed to connect to Kafka: {e}") from e
    
    def disconnect(self) -> None:
        """Close the Kafka consumer connection."""
        if self._consumer:
            try:
                self._consumer.close()
                logger.info("Kafka consumer closed successfully")
            except Exception as e:
                logger.error(
                    "Error closing Kafka consumer",
                    extra={"error": str(e)},
                    exc_info=True
                )
            finally:
                self._consumer = None
    
    def _parse_message(self, message_value: bytes) -> FinancialPosition:
        """
        Parse and validate a Kafka message.
        
        Args:
            message_value: Raw message bytes
            
        Returns:
            FinancialPosition: Validated position object
            
        Raises:
            MessageValidationError: If message is invalid
        """
        try:
            # Decode JSON
            data = json.loads(message_value.decode('utf-8'))
            
            # Validate with Pydantic
            position = FinancialPosition(**data)
            
            return position
            
        except json.JSONDecodeError as e:
            raise MessageValidationError(f"Invalid JSON: {e}") from e
        except ValidationError as e:
            raise MessageValidationError(f"Validation failed: {e}") from e
        except Exception as e:
            raise MessageValidationError(f"Unexpected error: {e}") from e
    
    def consume_batch(
        self,
        batch_size: int,
        timeout: float = 1.0,
        error_handler: Optional[Callable[[Exception, Any], None]] = None
    ) -> List[FinancialPosition]:
        """
        Consume a batch of messages.
        
        Args:
            batch_size: Maximum number of messages to consume
            timeout: Poll timeout in seconds
            error_handler: Optional callback for handling errors
            
        Returns:
            List of validated FinancialPosition objects
            
        Raises:
            KafkaConsumerError: If consumer is not connected
        """
        if not self._consumer:
            raise KafkaConsumerError("Consumer not connected. Call connect() first.")
        
        positions = []
        messages_polled = 0
        
        logger.debug(f"Starting batch consumption (size: {batch_size})")
        
        while len(positions) < batch_size:
            try:
                msg = self._consumer.poll(timeout=timeout)
                
                if msg is None:
                    # No more messages available
                    if messages_polled == 0:
                        logger.debug("No messages available in this poll cycle")
                    break
                
                messages_polled += 1
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition, continue
                        logger.debug(
                            f"Reached end of partition {msg.partition()}"
                        )
                        continue
                    else:
                        error_msg = f"Kafka error: {msg.error()}"
                        logger.error(error_msg)
                        self._error_count += 1
                        if error_handler:
                            error_handler(KafkaConsumerError(error_msg), msg)
                        continue
                
                # Parse and validate message
                try:
                    position = self._parse_message(msg.value())
                    positions.append(position)
                    self._message_count += 1
                    
                except MessageValidationError as e:
                    logger.warning(
                        "Message validation failed",
                        extra={
                            "error": str(e),
                            "partition": msg.partition(),
                            "offset": msg.offset(),
                        }
                    )
                    self._error_count += 1
                    if error_handler:
                        error_handler(e, msg)
                    continue
                    
            except Exception as e:
                logger.error(
                    "Unexpected error during message consumption",
                    extra={"error": str(e)},
                    exc_info=True
                )
                self._error_count += 1
                if error_handler:
                    error_handler(e, None)
        
        logger.info(
            "Batch consumption completed",
            extra={
                "positions_consumed": len(positions),
                "messages_polled": messages_polled,
                "total_messages": self._message_count,
                "total_errors": self._error_count,
            }
        )
        
        return positions
    
    def commit_offsets(self, asynchronous: bool = True) -> None:
        """
        Commit current offsets.
        
        Args:
            asynchronous: Whether to commit asynchronously
            
        Raises:
            KafkaConsumerError: If commit fails
        """
        if not self._consumer:
            raise KafkaConsumerError("Consumer not connected")
        
        try:
            if asynchronous:
                self._consumer.commit(asynchronous=True)
            else:
                offsets = self._consumer.commit(asynchronous=False)
                if offsets:
                    logger.debug(
                        "Committed offsets",
                        extra={"offsets": [
                            f"{tp.topic}:{tp.partition}@{tp.offset}"
                            for tp in offsets
                        ]}
                    )
        except KafkaException as e:
            logger.error(
                "Failed to commit offsets",
                extra={"error": str(e)},
                exc_info=True
            )
            raise KafkaConsumerError(f"Failed to commit offsets: {e}") from e
    
    def get_current_offsets(self) -> List[ConsumerOffset]:
        """
        Get current consumer offsets.
        
        Returns:
            List of ConsumerOffset objects
        """
        if not self._consumer:
            return []
        
        offsets = []
        
        try:
            # Get assigned partitions
            partitions = self._consumer.assignment()
            
            for partition in partitions:
                # Get committed offset
                committed = self._consumer.committed([partition])
                
                if committed and committed[0].offset >= 0:
                    offsets.append(
                        ConsumerOffset(
                            topic=partition.topic,
                            partition=partition.partition,
                            offset=committed[0].offset,
                        )
                    )
        except Exception as e:
            logger.error(
                "Failed to get current offsets",
                extra={"error": str(e)},
                exc_info=True
            )
        
        return offsets
    
    def seek_to_beginning(self) -> None:
        """Seek to the beginning of all assigned partitions."""
        if not self._consumer:
            raise KafkaConsumerError("Consumer not connected")
        
        partitions = self._consumer.assignment()
        for partition in partitions:
            partition.offset = 0
        
        self._consumer.seek(partition)
        logger.info("Seeked to beginning of all partitions")
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get consumer statistics.
        
        Returns:
            Dictionary with consumer statistics
        """
        return {
            "messages_consumed": self._message_count,
            "errors": self._error_count,
            "error_rate": (
                self._error_count / self._message_count 
                if self._message_count > 0 
                else 0
            ),
            "offsets": self.get_current_offsets(),
        }
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
