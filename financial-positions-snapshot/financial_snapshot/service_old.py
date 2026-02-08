"""Main service orchestrator for financial position snapshots."""

import logging
from datetime import datetime
from typing import Optional

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from financial_snapshot.config import Config
from financial_snapshot.kafka_consumer import (
    FinancialPositionConsumer,
    KafkaConsumerError,
)
from financial_snapshot.sql_writer import SQLServerWriter, SQLWriterError
from financial_snapshot.scheduler import SnapshotScheduler
from financial_snapshot.models import SnapshotMetadata


logger = logging.getLogger(__name__)


class FinancialSnapshotService:
    """
    Main service for financial position snapshots.
    
    Orchestrates the complete snapshot process including Kafka consumption,
    validation, and SQL persistence.
    """
    
    def __init__(self, config: Config):
        """
        Initialize the snapshot service.
        
        Args:
            config: Service configuration
        """
        self.config = config
        self.consumer: Optional[FinancialPositionConsumer] = None
        self.writer: Optional[SQLServerWriter] = None
        self.scheduler: Optional[SnapshotScheduler] = None
        
        self._total_snapshots = 0
        self._total_records_processed = 0
        self._total_errors = 0
        
        logger.info("Financial Snapshot Service initialized")
    
    def _initialize_components(self) -> None:
        """Initialize all service components."""
        logger.info("Initializing service components")
        
        # Initialize Kafka consumer
        self.consumer = FinancialPositionConsumer(self.config.kafka)
        self.consumer.connect()
        
        # Initialize SQL writer
        self.writer = SQLServerWriter(self.config.sql_server)
        
        # Verify/create table
        if not self.writer.verify_table_exists():
            logger.warning("Target table does not exist, attempting to create")
            if not self.writer.create_table_if_not_exists():
                raise RuntimeError("Failed to create target table")
        
        # Initialize scheduler
        self.scheduler = SnapshotScheduler(
            self.config.snapshot,
            self._execute_snapshot
        )
        
        logger.info("All components initialized successfully")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((KafkaConsumerError, SQLWriterError)),
    )
    def _execute_snapshot(self) -> None:
        """
        Execute a single snapshot cycle.
        
        This method is called by the scheduler at regular intervals.
        """
        snapshot_id = f"snapshot_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        start_time = datetime.utcnow()
        
        metadata = SnapshotMetadata(
            snapshot_id=snapshot_id,
            start_time=start_time,
        )
        
        logger.info(
            f"Executing snapshot: {snapshot_id}",
            extra={"start_time": start_time.isoformat()}
        )
        
        try:
            # Consume batch from Kafka
            positions = self.consumer.consume_batch(
                batch_size=self.config.snapshot.batch_size,
                timeout=1.0
            )
            
            if not positions:
                logger.info("No new positions to snapshot")
                metadata.success = True
                return
            
            # Write to SQL Server
            records_written = self.writer.write_batches(
                positions,
                batch_size=self.config.sql_server.batch_size
            )
            
            # Commit Kafka offsets
            if records_written > 0:
                self.consumer.commit_offsets(asynchronous=False)
            
            # Update metadata
            metadata.end_time = datetime.utcnow()
            metadata.record_count = records_written
            metadata.success = True
            
            # Update statistics
            self._total_snapshots += 1
            self._total_records_processed += records_written
            
            duration = (metadata.end_time - start_time).total_seconds()
            
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
            metadata.end_time = datetime.utcnow()
            metadata.success = False
            metadata.error_message = str(e)
            
            self._total_errors += 1
            
            logger.error(
                f"Snapshot {snapshot_id} failed",
                extra={
                    "error": str(e),
                    "snapshot_id": snapshot_id,
                },
                exc_info=True
            )
            
            # Re-raise to trigger retry mechanism
            raise
    
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
        
        # Close consumer
        if self.consumer:
            try:
                self.consumer.disconnect()
            except Exception as e:
                logger.error(
                    "Error disconnecting consumer",
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
        
        if self.consumer:
            stats["consumer"] = self.consumer.get_statistics()
        
        if self.writer:
            stats["writer"] = self.writer.get_statistics()
        
        if self.scheduler:
            stats["scheduler"] = self.scheduler.get_statistics()
        
        return stats
