"""Main service orchestrator for financial position snapshots with transactional processing."""

import logging
from datetime import datetime
from typing import Optional
from confluent_kafka import KafkaException

from financial_snapshot.config import Config
from financial_snapshot.multi_consumer import MultiConsumerManager
from financial_snapshot.message_buffer import MessageBuffer
from financial_snapshot.transactional_writer import TransactionalWriter
from financial_snapshot.scheduler import SnapshotScheduler


logger = logging.getLogger(__name__)


class FinancialSnapshotService:
    """
    Main service for financial position snapshots with transactional processing.
    
    Architecture:
    1. Multiple consumers continuously consume from Kafka and buffer messages
    2. Periodically (every N seconds), take a snapshot of buffered messages
    3. Write data + offsets to SQL Server in a single transaction
    4. Commit offsets to Kafka only after successful SQL transaction
    """
    
    def __init__(self, config: Config):
        """
        Initialize the snapshot service.
        
        Args:
            config: Service configuration
        """
        self.config = config
        self.message_buffer: Optional[MessageBuffer] = None
        self.consumer_manager: Optional[MultiConsumerManager] = None
        self.writer: Optional[TransactionalWriter] = None
        self.scheduler: Optional[SnapshotScheduler] = None
        
        self._total_snapshots = 0
        self._total_records_processed = 0
        self._total_errors = 0
        
        logger.info("Financial Snapshot Service initialized")
    
    def _initialize_components(self) -> None:
        """Initialize all service components."""
        logger.info("Initializing service components")
        
        # Initialize message buffer
        self.message_buffer = MessageBuffer(
            max_size=self.config.snapshot.buffer_size
        )
        
        # Initialize transactional writer
        self.writer = TransactionalWriter(
            sql_config=self.config.sql_server,
            consumer_group=self.config.kafka.group_id,
            offsets_table=self.config.kafka.offsets_table
        )
        
        # Initialize tables
        self.writer.initialize_tables()
        
        # Load initial offsets from SQL if configured
        initial_offsets = None
        if self.config.kafka.load_offsets_from_sql:
            logger.info("Loading initial offsets from SQL Server")
            initial_offsets = self.writer.load_initial_offsets()
            
            if initial_offsets:
                logger.info(
                    f"Loaded offsets for {len(initial_offsets)} topics",
                    extra={"topics": list(initial_offsets.keys())}
                )
            else:
                logger.info("No initial offsets found in SQL Server")
        
        # Initialize multi-consumer manager
        self.consumer_manager = MultiConsumerManager(
            kafka_config=self.config.kafka,
            handler_config=self.config.handler,
            message_buffer=self.message_buffer
        )
        
        # Start consumers
        self.consumer_manager.start(initial_offsets=initial_offsets)
        
        # Initialize scheduler
        self.scheduler = SnapshotScheduler(
            self.config.snapshot,
            self._execute_snapshot
        )
        
        logger.info("All components initialized successfully")
    
    def _execute_snapshot(self) -> None:
        """
        Execute a single snapshot cycle.
        
        This method:
        1. Drains messages from buffer
        2. Writes data + offsets to SQL in single transaction
        3. Commits offsets to Kafka on success
        """
        snapshot_id = f"snapshot_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        start_time = datetime.utcnow()
        
        logger.info(
            f"Executing snapshot: {snapshot_id}",
            extra={"start_time": start_time.isoformat()}
        )
        
        try:
            # Drain messages from buffer
            messages, next_offsets = self.message_buffer.drain_snapshot()
            
            if not messages:
                logger.info("No new positions to snapshot")
                return
            
            logger.info(
                f"Drained {len(messages)} messages from buffer",
                extra={
                    "topics": len(next_offsets),
                    "total_partitions": sum(len(p) for p in next_offsets.values())
                }
            )
            
            # Write to SQL Server (data + offsets in single transaction)
            records_written = self.writer.write_snapshot(
                messages=messages,
                next_offsets=next_offsets
            )
            
            # Commit offsets to Kafka (only after successful SQL write)
            if records_written > 0:
                self._commit_to_kafka(next_offsets)
            
            # Update statistics
            self._total_snapshots += 1
            self._total_records_processed += records_written
            
            duration = (datetime.utcnow() - start_time).total_seconds()
            
            logger.info(
                f"Snapshot {snapshot_id} completed successfully",
                extra={
                    "records_processed": records_written,
                    "duration_seconds": round(duration, 3),
                    "records_per_second": round(
                        records_written / duration, 2
                    ) if duration > 0 else 0,
                }
            )
            
        except Exception as e:
            self._total_errors += 1
            
            logger.error(
                f"Snapshot {snapshot_id} failed",
                extra={
                    "error": str(e),
                    "snapshot_id": snapshot_id,
                },
                exc_info=True
            )
            
            # Don't re-raise - let scheduler continue
    
    def _commit_to_kafka(self, next_offsets: dict) -> None:
        """
        Commit offsets to Kafka brokers.
        
        This is called only after successful SQL transaction.
        
        Args:
            next_offsets: Dictionary mapping topic -> partition -> next offset
        """
        try:
            # Get any consumer to commit (they all share the same consumer group)
            if self.consumer_manager and self.consumer_manager.consumers:
                consumer = self.consumer_manager.consumers[0]
                
                if consumer._consumer:
                    from confluent_kafka import TopicPartition
                    
                    # Build list of TopicPartition objects
                    partitions = []
                    for topic, partition_offsets in next_offsets.items():
                        for partition, offset in partition_offsets.items():
                            tp = TopicPartition(topic, partition, offset)
                            partitions.append(tp)
                    
                    # Commit offsets
                    consumer._consumer.commit(offsets=partitions, asynchronous=False)
                    
                    logger.info(
                        f"Committed offsets to Kafka",
                        extra={
                            "topics": len(next_offsets),
                            "partitions": len(partitions)
                        }
                    )
                    
        except Exception as e:
            logger.error(
                "Failed to commit offsets to Kafka",
                extra={"error": str(e)},
                exc_info=True
            )
            # Non-fatal - offsets are saved in SQL, we can continue
    
    def start(self) -> None:
        """
        Start the snapshot service.
        
        This method blocks until the service is stopped.
        """
        try:
            logger.info("Starting Financial Snapshot Service")
            
            # Initialize components
            self._initialize_components()
            
            # Start scheduler (blocks)
            self.scheduler.start()
            
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down")
        except Exception as e:
            logger.critical(
                "Fatal error in service",
                extra={"error": str(e)},
                exc_info=True
            )
            raise
        finally:
            self._shutdown()
    
    def _shutdown(self) -> None:
        """Gracefully shutdown the service."""
        logger.info("Shutting down Financial Snapshot Service")
        
        # Stop scheduler
        if self.scheduler:
            self.scheduler.stop()
        
        # Stop consumers
        if self.consumer_manager:
            try:
                self.consumer_manager.stop()
            except Exception as e:
                logger.error(
                    "Error stopping consumers",
                    extra={"error": str(e)}
                )
        
        # Final snapshot of remaining buffered messages
        if self.message_buffer and self.writer:
            try:
                logger.info("Processing final snapshot of remaining messages")
                messages, next_offsets = self.message_buffer.drain_snapshot()
                
                if messages:
                    self.writer.write_snapshot(messages, next_offsets)
                    self._commit_to_kafka(next_offsets)
                    logger.info(f"Final snapshot written: {len(messages)} messages")
            except Exception as e:
                logger.error(
                    "Error in final snapshot",
                    extra={"error": str(e)}
                )
        
        # Close writer
        if self.writer:
            try:
                self.writer.close()
            except Exception as e:
                logger.error(
                    "Error closing writer",
                    extra={"error": str(e)}
                )
        
        # Log final statistics
        logger.info(
            "Service shutdown complete",
            extra=self.get_statistics()
        )
    
    def get_statistics(self) -> dict:
        """
        Get comprehensive service statistics.
        
        Returns:
            dict: Service statistics
        """
        stats = {
            "service": {
                "total_snapshots": self._total_snapshots,
                "total_records_processed": self._total_records_processed,
                "total_errors": self._total_errors,
            }
        }
        
        if self.consumer_manager:
            stats["consumers"] = self.consumer_manager.get_statistics()
        
        if self.writer:
            stats["writer"] = self.writer.get_statistics()
        
        if self.scheduler:
            stats["scheduler"] = self.scheduler.get_statistics()
        
        if self.message_buffer:
            stats["buffer"] = self.message_buffer.get_statistics()
        
        return stats
