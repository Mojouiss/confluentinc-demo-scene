"""Multi-consumer Kafka consumer module with continuous buffering."""

import json
import logging
import threading
from typing import List, Optional, Callable, Dict
from datetime import datetime

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from financial_snapshot.config import KafkaConfig, HandlerConfig
from financial_snapshot.handlers import AbcMsgHandler, get_handler
from financial_snapshot.message_buffer import MessageBuffer
from financial_snapshot.models import FinancialPosition


logger = logging.getLogger(__name__)


class ContinuousConsumer:
    """
    Single continuous Kafka consumer thread.
    
    Consumes messages continuously and adds them to a shared buffer
    after validation.
    """
    
    def __init__(
        self,
        consumer_id: int,
        kafka_config: KafkaConfig,
        handler_config: HandlerConfig,
        message_buffer: MessageBuffer,
        handlers: Dict[str, AbcMsgHandler]
    ):
        """
        Initialize continuous consumer.
        
        Args:
            consumer_id: Unique consumer identifier
            kafka_config: Kafka configuration
            handler_config: Handler configuration
            message_buffer: Shared message buffer
            handlers: Dictionary mapping topics to message handlers
        """
        self.consumer_id = consumer_id
        self.kafka_config = kafka_config
        self.handler_config = handler_config
        self.message_buffer = message_buffer
        self.handlers = handlers
        
        self._consumer: Optional[Consumer] = None
        self._running = False
        self._thread: Optional[threading.Thread] = None
        
        self._messages_consumed = 0
        self._errors = 0
        
        logger.info(
            f"ContinuousConsumer {consumer_id} initialized",
            extra={"topics": kafka_config.topics}
        )
    
    def _create_consumer(self) -> Consumer:
        """Create Kafka consumer."""
        consumer_config = {
            'bootstrap.servers': self.kafka_config.bootstrap_servers,
            'group.id': self.kafka_config.group_id,
            'auto.offset.reset': self.kafka_config.auto_offset_reset,
            'enable.auto.commit': False,  # Always manual commit
            'session.timeout.ms': self.kafka_config.session_timeout_ms,
            'max.poll.interval.ms': 300000,
            'api.version.request': True,
            'client.id': f'financial-snapshot-{self.kafka_config.group_id}-{self.consumer_id}',
        }
        
        return Consumer(consumer_config)
    
    def start(self, initial_offsets: Optional[Dict[str, Dict[int, int]]] = None) -> None:
        """
        Start the consumer thread.
        
        Args:
            initial_offsets: Optional initial offsets to seek to (topic -> partition -> offset)
        """
        if self._running:
            logger.warning(f"Consumer {self.consumer_id} is already running")
            return
        
        self._running = True
        self._thread = threading.Thread(
            target=self._consume_loop,
            args=(initial_offsets,),
            name=f"Consumer-{self.consumer_id}",
            daemon=True
        )
        self._thread.start()
        
        logger.info(f"Consumer {self.consumer_id} started")
    
    def _consume_loop(self, initial_offsets: Optional[Dict[str, Dict[int, int]]] = None) -> None:
        """
        Main consumption loop.
        
        Args:
            initial_offsets: Optional initial offsets to seek to
        """
        try:
            self._consumer = self._create_consumer()
            self._consumer.subscribe(self.kafka_config.topics)
            
            # Wait for partition assignment
            assignment_timeout = 10.0
            assignment_start = datetime.utcnow()
            
            while self._running:
                msg = self._consumer.poll(timeout=0.1)
                
                if msg is None:
                    # Check if we have assignment yet
                    if (datetime.utcnow() - assignment_start).total_seconds() > assignment_timeout:
                        # We have assignment, break out to apply initial offsets if needed
                        break
                    continue
                
                # We got a message, we have assignment
                break
            
            # Apply initial offsets if provided
            if initial_offsets and self.kafka_config.load_offsets_from_sql:
                self._apply_initial_offsets(initial_offsets)
            
            # Main consumption loop
            while self._running:
                msg = self._consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Consumer {self.consumer_id} reached end of partition {msg.partition()}")
                        continue
                    else:
                        logger.error(
                            f"Consumer {self.consumer_id} Kafka error: {msg.error()}",
                            exc_info=True
                        )
                        self._errors += 1
                        continue
                
                # Process message
                try:
                    self._process_message(msg)
                except Exception as e:
                    logger.error(
                        f"Consumer {self.consumer_id} error processing message",
                        extra={
                            "topic": msg.topic(),
                            "partition": msg.partition(),
                            "offset": msg.offset(),
                            "error": str(e)
                        },
                        exc_info=True
                    )
                    self._errors += 1
                    
        except Exception as e:
            logger.critical(
                f"Consumer {self.consumer_id} fatal error",
                extra={"error": str(e)},
                exc_info=True
            )
        finally:
            if self._consumer:
                self._consumer.close()
                logger.info(f"Consumer {self.consumer_id} closed")
    
    def _apply_initial_offsets(self, initial_offsets: Dict[str, Dict[int, int]]) -> None:
        """
        Apply initial offsets from SQL Server.
        
        Args:
            initial_offsets: Dictionary mapping topic -> partition -> offset
        """
        try:
            assignment = self._consumer.assignment()
            
            for tp in assignment:
                topic = tp.topic
                partition = tp.partition
                
                if topic in initial_offsets and partition in initial_offsets[topic]:
                    offset = initial_offsets[topic][partition]
                    tp.offset = offset
                    self._consumer.seek(tp)
                    
                    logger.info(
                        f"Consumer {self.consumer_id} seeking to offset",
                        extra={
                            "topic": topic,
                            "partition": partition,
                            "offset": offset
                        }
                    )
        except Exception as e:
            logger.error(
                f"Consumer {self.consumer_id} failed to apply initial offsets: {e}",
                exc_info=True
            )
    
    def _process_message(self, msg) -> None:
        """
        Process a single Kafka message.
        
        Args:
            msg: Kafka message
        """
        topic = msg.topic()
        partition = msg.partition()
        offset = msg.offset()
        key = msg.key()
        value = msg.value()
        
        # Get handler for this topic
        handler = self.handlers.get(topic)
        if not handler:
            logger.warning(
                f"No handler configured for topic {topic}, skipping message",
                extra={"partition": partition, "offset": offset}
            )
            return
        
        # Parse and validate message
        try:
            validated_model = handler.parse_message(value, key)
            table_name = handler.get_table_name(key)
            
            # Add to buffer
            added = self.message_buffer.add_message(
                topic=topic,
                partition=partition,
                offset=offset,
                key=key,
                value=validated_model,
                table_name=table_name
            )
            
            if added:
                self._messages_consumed += 1
                
                if self._messages_consumed % 1000 == 0:
                    logger.debug(
                        f"Consumer {self.consumer_id} progress",
                        extra={
                            "messages_consumed": self._messages_consumed,
                            "errors": self._errors,
                            "buffer_size": self.message_buffer.get_size()
                        }
                    )
            else:
                logger.warning(
                    f"Consumer {self.consumer_id} buffer full, message not added",
                    extra={"topic": topic, "partition": partition, "offset": offset}
                )
                
        except ValueError as e:
            logger.warning(
                f"Consumer {self.consumer_id} validation error",
                extra={
                    "topic": topic,
                    "partition": partition,
                    "offset": offset,
                    "error": str(e)
                }
            )
            self._errors += 1
    
    def stop(self) -> None:
        """Stop the consumer."""
        if not self._running:
            return
        
        logger.info(f"Stopping consumer {self.consumer_id}")
        self._running = False
        
        if self._thread:
            self._thread.join(timeout=10)
    
    def get_statistics(self) -> Dict:
        """Get consumer statistics."""
        return {
            "consumer_id": self.consumer_id,
            "running": self._running,
            "messages_consumed": self._messages_consumed,
            "errors": self._errors
        }


class MultiConsumerManager:
    """
    Manages multiple continuous Kafka consumers.
    
    Coordinates multiple consumer threads that continuously consume
    and buffer messages from Kafka topics.
    """
    
    def __init__(
        self,
        kafka_config: KafkaConfig,
        handler_config: HandlerConfig,
        message_buffer: MessageBuffer
    ):
        """
        Initialize multi-consumer manager.
        
        Args:
            kafka_config: Kafka configuration
            handler_config: Handler configuration
            message_buffer: Shared message buffer
        """
        self.kafka_config = kafka_config
        self.handler_config = handler_config
        self.message_buffer = message_buffer
        
        self.consumers: List[ContinuousConsumer] = []
        self.handlers: Dict[str, AbcMsgHandler] = {}
        
        self._initialize_handlers()
        
        logger.info(
            "MultiConsumerManager initialized",
            extra={
                "num_consumers": kafka_config.num_consumers,
                "topics": kafka_config.topics
            }
        )
    
    def _initialize_handlers(self) -> None:
        """Initialize message handlers for each topic."""
        for topic in self.kafka_config.topics:
            # Get table name for this topic
            table_name = self.handler_config.topic_to_table.get(
                topic,
                "FinanceDB.dbo.FinancialPositions"
            )
            
            # Create handler
            handler = get_handler(
                handler_type="financial_position",
                model_class=FinancialPosition,
                table_name=table_name,
                key_to_table_mapping=self.handler_config.key_to_table,
                schema_registry_url=self.handler_config.schema_registry_url
            )
            
            self.handlers[topic] = handler
            
            logger.info(
                f"Handler initialized for topic {topic}",
                extra={"table": table_name}
            )
    
    def start(self, initial_offsets: Optional[Dict[str, Dict[int, int]]] = None) -> None:
        """
        Start all consumers.
        
        Args:
            initial_offsets: Optional initial offsets to seek to
        """
        for i in range(self.kafka_config.num_consumers):
            consumer = ContinuousConsumer(
                consumer_id=i,
                kafka_config=self.kafka_config,
                handler_config=self.handler_config,
                message_buffer=self.message_buffer,
                handlers=self.handlers
            )
            
            self.consumers.append(consumer)
            consumer.start(initial_offsets)
        
        logger.info(f"Started {len(self.consumers)} consumers")
    
    def stop(self) -> None:
        """Stop all consumers."""
        logger.info("Stopping all consumers")
        
        for consumer in self.consumers:
            consumer.stop()
        
        logger.info("All consumers stopped")
    
    def get_statistics(self) -> Dict:
        """Get statistics from all consumers."""
        consumer_stats = [c.get_statistics() for c in self.consumers]
        
        total_messages = sum(s["messages_consumed"] for s in consumer_stats)
        total_errors = sum(s["errors"] for s in consumer_stats)
        
        return {
            "num_consumers": len(self.consumers),
            "total_messages_consumed": total_messages,
            "total_errors": total_errors,
            "consumers": consumer_stats,
            "buffer_statistics": self.message_buffer.get_statistics()
        }
