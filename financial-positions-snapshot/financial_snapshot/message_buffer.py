"""Message buffer for continuous Kafka consumption."""

import logging
import threading
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

from pydantic import BaseModel


logger = logging.getLogger(__name__)


@dataclass
class BufferedMessage:
    """Represents a buffered Kafka message."""
    
    topic: str
    partition: int
    offset: int
    key: Optional[bytes]
    value: BaseModel  # Validated Pydantic model
    table_name: str
    timestamp: datetime


class MessageBuffer:
    """
    Thread-safe buffer for Kafka messages.
    
    Supports continuous buffering by multiple consumers and
    periodic snapshot draining by the writer.
    """
    
    def __init__(self, max_size: int = 100000):
        """
        Initialize message buffer.
        
        Args:
            max_size: Maximum number of messages to buffer
        """
        self.max_size = max_size
        self._lock = threading.RLock()
        
        # Buffer structure: topic -> partition -> list of messages
        self._buffer: Dict[str, Dict[int, List[BufferedMessage]]] = defaultdict(
            lambda: defaultdict(list)
        )
        
        # Track offsets: topic -> partition -> max offset
        self._max_offsets: Dict[str, Dict[int, int]] = defaultdict(dict)
        
        self._total_messages = 0
        self._messages_added = 0
        self._messages_drained = 0
        
        logger.info(
            "MessageBuffer initialized",
            extra={"max_size": max_size}
        )
    
    def add_message(
        self,
        topic: str,
        partition: int,
        offset: int,
        key: Optional[bytes],
        value: BaseModel,
        table_name: str
    ) -> bool:
        """
        Add a message to the buffer.
        
        Args:
            topic: Kafka topic
            partition: Partition number
            offset: Message offset
            key: Message key
            value: Validated Pydantic model
            table_name: Target SQL table name
            
        Returns:
            bool: True if added, False if buffer is full
        """
        with self._lock:
            if self._total_messages >= self.max_size:
                logger.warning(
                    "Buffer is full, cannot add message",
                    extra={
                        "current_size": self._total_messages,
                        "max_size": self.max_size
                    }
                )
                return False
            
            message = BufferedMessage(
                topic=topic,
                partition=partition,
                offset=offset,
                key=key,
                value=value,
                table_name=table_name,
                timestamp=datetime.utcnow()
            )
            
            self._buffer[topic][partition].append(message)
            
            # Update max offset for this partition
            current_max = self._max_offsets[topic].get(partition, -1)
            self._max_offsets[topic][partition] = max(current_max, offset)
            
            self._total_messages += 1
            self._messages_added += 1
            
            if self._messages_added % 1000 == 0:
                logger.debug(
                    f"Buffer stats: {self._total_messages} messages buffered",
                    extra={
                        "total_added": self._messages_added,
                        "total_drained": self._messages_drained
                    }
                )
            
            return True
    
    def drain_snapshot(self) -> Tuple[List[BufferedMessage], Dict[str, Dict[int, int]]]:
        """
        Drain all buffered messages for snapshot.
        
        Returns a snapshot of all messages and their max offsets.
        The buffer is cleared after draining.
        
        Returns:
            Tuple containing:
                - List of all buffered messages
                - Dict mapping topic -> partition -> next offset to commit
        """
        with self._lock:
            if self._total_messages == 0:
                logger.debug("Buffer is empty, nothing to drain")
                return [], {}
            
            # Collect all messages
            messages = []
            for topic, partitions in self._buffer.items():
                for partition, partition_messages in partitions.items():
                    messages.extend(partition_messages)
            
            # Copy offset information (next offset = max_offset + 1)
            next_offsets = {}
            for topic, partitions in self._max_offsets.items():
                next_offsets[topic] = {}
                for partition, max_offset in partitions.items():
                    next_offsets[topic][partition] = max_offset + 1
            
            # Clear buffer
            drained_count = self._total_messages
            self._buffer.clear()
            self._buffer = defaultdict(lambda: defaultdict(list))
            self._max_offsets.clear()
            self._max_offsets = defaultdict(dict)
            self._total_messages = 0
            self._messages_drained += drained_count
            
            logger.info(
                f"Drained {drained_count} messages from buffer",
                extra={
                    "topics": len(next_offsets),
                    "total_drained": self._messages_drained
                }
            )
            
            return messages, next_offsets
    
    def get_size(self) -> int:
        """
        Get current buffer size.
        
        Returns:
            int: Number of messages in buffer
        """
        with self._lock:
            return self._total_messages
    
    def get_statistics(self) -> Dict:
        """
        Get buffer statistics.
        
        Returns:
            Dict: Buffer statistics
        """
        with self._lock:
            topic_stats = {}
            for topic, partitions in self._buffer.items():
                topic_stats[topic] = {
                    "partitions": len(partitions),
                    "messages": sum(len(msgs) for msgs in partitions.values())
                }
            
            return {
                "total_messages": self._total_messages,
                "messages_added": self._messages_added,
                "messages_drained": self._messages_drained,
                "topics": topic_stats,
                "utilization_percent": (self._total_messages / self.max_size * 100) if self.max_size > 0 else 0
            }
    
    def is_full(self) -> bool:
        """
        Check if buffer is full.
        
        Returns:
            bool: True if buffer is at capacity
        """
        with self._lock:
            return self._total_messages >= self.max_size
    
    def clear(self) -> None:
        """Clear the buffer completely."""
        with self._lock:
            self._buffer.clear()
            self._buffer = defaultdict(lambda: defaultdict(list))
            self._max_offsets.clear()
            self._max_offsets = defaultdict(dict)
            self._total_messages = 0
            logger.info("Buffer cleared")
