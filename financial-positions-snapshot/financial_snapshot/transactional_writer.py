"""Transactional SQL Server writer with offset management."""

import logging
from datetime import datetime
from typing import List, Dict, Tuple, Optional
from collections import defaultdict

import pyodbc

from financial_snapshot.config import SQLServerConfig
from financial_snapshot.message_buffer import BufferedMessage
from financial_snapshot.offset_manager import KafkaOffsetManager
from financial_snapshot.sql_writer import ConnectionPool, SQLWriterError


logger = logging.getLogger(__name__)


class TransactionalWriter:
    """
    Transactional SQL Server writer.
    
    Writes data messages and Kafka offsets in a single transaction
    to ensure exactly-once semantics.
    """
    
    def __init__(
        self,
        sql_config: SQLServerConfig,
        consumer_group: str,
        offsets_table: str = "KafkaOffsets"
    ):
        """
        Initialize transactional writer.
        
        Args:
            sql_config: SQL Server configuration
            consumer_group: Kafka consumer group ID
            offsets_table: Name of the offsets table
        """
        self.sql_config = sql_config
        self.consumer_group = consumer_group
        self.connection_pool = ConnectionPool(sql_config)
        self.offset_manager = KafkaOffsetManager(sql_config, offsets_table)
        
        self._total_records_written = 0
        self._total_transactions = 0
        self._error_count = 0
        
        logger.info(
            "TransactionalWriter initialized",
            extra={
                "server": sql_config.server,
                "database": sql_config.database,
                "consumer_group": consumer_group
            }
        )
    
    def initialize_tables(self) -> None:
        """Initialize database tables if they don't exist."""
        try:
            with self.connection_pool.get_connection() as conn:
                # Create offsets table
                self.offset_manager.create_offsets_table_if_not_exists(conn)
                
                # Create data tables (will be created dynamically per table name)
                conn.commit()
                
            logger.info("Tables initialized successfully")
        except Exception as e:
            logger.error(
                "Failed to initialize tables",
                extra={"error": str(e)},
                exc_info=True
            )
            raise
    
    def write_snapshot(
        self,
        messages: List[BufferedMessage],
        next_offsets: Dict[str, Dict[int, int]]
    ) -> int:
        """
        Write messages and offsets in a single transaction.
        
        This ensures atomicity - either both data and offsets are committed,
        or neither are.
        
        Args:
            messages: List of buffered messages to write
            next_offsets: Dictionary mapping topic -> partition -> next offset
            
        Returns:
            int: Number of records written
            
        Raises:
            SQLWriterError: If write fails
        """
        if not messages:
            logger.debug("No messages to write")
            return 0
        
        transaction_start = datetime.utcnow()
        records_written = 0
        
        try:
            with self.connection_pool.get_connection() as conn:
                cursor = conn.cursor()
                
                try:
                    # Group messages by table name
                    messages_by_table = self._group_by_table(messages)
                    
                    # Write data for each table
                    for table_name, table_messages in messages_by_table.items():
                        self._write_table_batch(cursor, table_name, table_messages)
                        records_written += len(table_messages)
                    
                    # Write offsets for all topics/partitions
                    for topic, partitions in next_offsets.items():
                        self.offset_manager.save_offsets(
                            connection=conn,
                            topic=topic,
                            consumer_group=self.consumer_group,
                            offsets=partitions
                        )
                    
                    # Commit transaction
                    conn.commit()
                    
                    self._total_records_written += records_written
                    self._total_transactions += 1
                    
                    transaction_duration = (datetime.utcnow() - transaction_start).total_seconds()
                    
                    logger.info(
                        "Snapshot written successfully",
                        extra={
                            "records": records_written,
                            "tables": len(messages_by_table),
                            "topics": len(next_offsets),
                            "duration_seconds": round(transaction_duration, 3),
                            "records_per_second": round(
                                records_written / transaction_duration, 2
                            ) if transaction_duration > 0 else 0,
                        }
                    )
                    
                except Exception as e:
                    # Rollback transaction on any error
                    conn.rollback()
                    self._error_count += 1
                    
                    logger.error(
                        "Transaction failed, rolled back",
                        extra={
                            "error": str(e),
                            "records_attempted": len(messages),
                        },
                        exc_info=True
                    )
                    raise SQLWriterError(f"Failed to write snapshot: {e}") from e
                    
        except Exception as e:
            logger.error(
                "Connection error during write",
                extra={"error": str(e)},
                exc_info=True
            )
            raise SQLWriterError(f"Connection error: {e}") from e
        
        return records_written
    
    def _group_by_table(
        self,
        messages: List[BufferedMessage]
    ) -> Dict[str, List[BufferedMessage]]:
        """
        Group messages by target table name.
        
        Args:
            messages: List of buffered messages
            
        Returns:
            Dict mapping table name to list of messages
        """
        grouped = defaultdict(list)
        for msg in messages:
            grouped[msg.table_name].append(msg)
        return dict(grouped)
    
    def _write_table_batch(
        self,
        cursor: pyodbc.Cursor,
        table_name: str,
        messages: List[BufferedMessage]
    ) -> None:
        """
        Write a batch of messages to a specific table.
        
        Args:
            cursor: Database cursor (in transaction)
            table_name: Full table name (database.schema.table)
            messages: Messages for this table
        """
        if not messages:
            return
        
        # Ensure table exists
        self._ensure_table_exists(cursor, table_name)
        
        # Build INSERT query
        insert_query = f"""
            INSERT INTO {table_name} (
                position_id,
                account_id,
                symbol,
                quantity,
                price,
                market_value,
                timestamp,
                currency,
                position_type,
                snapshot_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
        """
        
        # Prepare batch data
        batch_data = []
        for msg in messages:
            # Convert Pydantic model to tuple
            if hasattr(msg.value, 'to_sql_tuple'):
                batch_data.append(msg.value.to_sql_tuple())
            else:
                # Generic fallback
                logger.warning(
                    f"Message value doesn't have to_sql_tuple method",
                    extra={"table": table_name}
                )
        
        if batch_data:
            # Execute batch insert
            cursor.fast_executemany = True
            cursor.executemany(insert_query, batch_data)
            
            logger.debug(
                f"Wrote {len(batch_data)} records to {table_name}"
            )
    
    def _ensure_table_exists(
        self,
        cursor: pyodbc.Cursor,
        table_name: str
    ) -> None:
        """
        Ensure target table exists, create if necessary.
        
        Args:
            cursor: Database cursor
            table_name: Full table name (database.schema.table)
        """
        # Parse table name
        parts = table_name.split('.')
        if len(parts) == 3:
            db, schema, table = parts
        else:
            db = self.sql_config.database
            schema = "dbo"
            table = parts[-1]
        
        # Check if table exists
        check_query = """
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_CATALOG = ? AND TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """
        
        cursor.execute(check_query, (db, schema, table))
        exists = cursor.fetchone()[0] > 0
        
        if not exists:
            logger.info(f"Creating table {table_name}")
            
            # Create table with standard financial positions schema
            create_query = f"""
                CREATE TABLE {table_name} (
                    snapshot_id BIGINT IDENTITY(1,1) PRIMARY KEY,
                    position_id VARCHAR(100) NOT NULL,
                    account_id VARCHAR(100) NOT NULL,
                    symbol VARCHAR(50) NOT NULL,
                    quantity DECIMAL(18,8) NOT NULL,
                    price DECIMAL(18,4) NOT NULL,
                    market_value DECIMAL(18,4) NOT NULL,
                    timestamp DATETIME2 NOT NULL,
                    currency VARCHAR(10) NOT NULL,
                    position_type VARCHAR(20) NOT NULL,
                    snapshot_time DATETIME2 NOT NULL DEFAULT GETDATE()
                );
                
                CREATE INDEX idx_{table}_snapshot_time ON {table_name} (snapshot_time);
                CREATE INDEX idx_{table}_position_id ON {table_name} (position_id);
                CREATE INDEX idx_{table}_account_id ON {table_name} (account_id);
            """
            
            cursor.execute(create_query)
            logger.info(f"Table {table_name} created successfully")
    
    def load_initial_offsets(self) -> Dict[str, Dict[int, int]]:
        """
        Load initial offsets from SQL Server.
        
        Returns:
            Dict mapping topic -> partition -> offset
        """
        try:
            with self.connection_pool.get_connection() as conn:
                # Get all topics
                all_offsets = {}
                
                # Query for all topics for this consumer group
                cursor = conn.cursor()
                query = f"""
                    SELECT DISTINCT topic
                    FROM {self.offset_manager.full_table_name}
                    WHERE consumer_group = ?
                """
                
                cursor.execute(query, (self.consumer_group,))
                
                for row in cursor.fetchall():
                    topic = row[0]
                    offsets = self.offset_manager.load_offsets(
                        connection=conn,
                        topic=topic,
                        consumer_group=self.consumer_group
                    )
                    if offsets:
                        all_offsets[topic] = offsets
                
                logger.info(
                    f"Loaded initial offsets for {len(all_offsets)} topics",
                    extra={"topics": list(all_offsets.keys())}
                )
                
                return all_offsets
                
        except Exception as e:
            logger.error(
                "Failed to load initial offsets",
                extra={"error": str(e)},
                exc_info=True
            )
            return {}
    
    def get_statistics(self) -> Dict:
        """
        Get writer statistics.
        
        Returns:
            dict: Writer statistics
        """
        return {
            "total_records_written": self._total_records_written,
            "total_transactions": self._total_transactions,
            "error_count": self._error_count,
            "avg_records_per_transaction": (
                self._total_records_written / self._total_transactions
                if self._total_transactions > 0
                else 0
            ),
        }
    
    def close(self) -> None:
        """Close all database connections."""
        self.connection_pool.close_all()
        logger.info("TransactionalWriter closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
