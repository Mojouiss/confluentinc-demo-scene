"""SQL Server writer module for financial positions."""

import logging
from contextlib import contextmanager
from datetime import datetime
from typing import List, Optional, Generator
from queue import Queue, Empty
import pyodbc

from financial_snapshot.config import SQLServerConfig
from financial_snapshot.models import FinancialPosition


logger = logging.getLogger(__name__)


class SQLWriterError(Exception):
    """Base exception for SQL writer errors."""
    pass


class ConnectionPoolError(SQLWriterError):
    """Exception for connection pool errors."""
    pass


class ConnectionPool:
    """
    Thread-safe connection pool for SQL Server.
    
    Manages a pool of database connections for improved performance
    and resource utilization.
    """
    
    def __init__(self, config: SQLServerConfig):
        """
        Initialize connection pool.
        
        Args:
            config: SQL Server configuration
        """
        self.config = config
        self.connection_string = config.get_connection_string()
        self._pool: Queue = Queue(maxsize=config.connection_pool_size)
        self._created_connections = 0
        
        # Pre-create connections
        for _ in range(config.connection_pool_size):
            try:
                conn = self._create_connection()
                self._pool.put(conn)
            except Exception as e:
                logger.error(
                    "Failed to create connection during pool initialization",
                    extra={"error": str(e)},
                    exc_info=True
                )
        
        logger.info(
            "Connection pool initialized",
            extra={"pool_size": config.connection_pool_size}
        )
    
    def _create_connection(self) -> pyodbc.Connection:
        """
        Create a new database connection.
        
        Returns:
            pyodbc.Connection: Database connection
            
        Raises:
            ConnectionPoolError: If connection creation fails
        """
        try:
            conn = pyodbc.connect(
                self.connection_string,
                timeout=self.config.connection_timeout,
                autocommit=False
            )
            self._created_connections += 1
            
            logger.debug(
                "Created new database connection",
                extra={"connection_id": self._created_connections}
            )
            
            return conn
            
        except pyodbc.Error as e:
            logger.error(
                "Failed to create database connection",
                extra={"error": str(e)},
                exc_info=True
            )
            raise ConnectionPoolError(
                f"Failed to create database connection: {e}"
            ) from e
    
    @contextmanager
    def get_connection(self) -> Generator[pyodbc.Connection, None, None]:
        """
        Get a connection from the pool (context manager).
        
        Yields:
            pyodbc.Connection: Database connection
            
        Raises:
            ConnectionPoolError: If no connection available
        """
        conn = None
        try:
            # Try to get connection from pool (non-blocking)
            try:
                conn = self._pool.get(block=False)
            except Empty:
                # Pool exhausted, create new connection if under limit
                if self._created_connections < self.config.connection_pool_size:
                    conn = self._create_connection()
                else:
                    # Wait for available connection
                    conn = self._pool.get(block=True, timeout=30)
            
            # Test connection
            try:
                conn.cursor().execute("SELECT 1")
            except pyodbc.Error:
                # Connection is dead, create new one
                logger.warning("Connection is dead, creating new one")
                conn.close()
                conn = self._create_connection()
            
            yield conn
            
        except Exception as e:
            logger.error(
                "Error in connection context",
                extra={"error": str(e)},
                exc_info=True
            )
            raise
        finally:
            # Return connection to pool
            if conn:
                try:
                    # Rollback any uncommitted transactions
                    conn.rollback()
                    self._pool.put(conn, block=False)
                except Exception as e:
                    logger.error(
                        "Failed to return connection to pool",
                        extra={"error": str(e)},
                        exc_info=True
                    )
                    try:
                        conn.close()
                    except:
                        pass
    
    def close_all(self) -> None:
        """Close all connections in the pool."""
        while not self._pool.empty():
            try:
                conn = self._pool.get(block=False)
                conn.close()
            except Empty:
                break
            except Exception as e:
                logger.error(
                    "Error closing connection",
                    extra={"error": str(e)},
                    exc_info=True
                )
        
        logger.info("All connections closed")


class SQLServerWriter:
    """
    SQL Server writer for financial positions.
    
    Provides efficient batch writing with connection pooling,
    transaction management, and error handling.
    """
    
    def __init__(self, config: SQLServerConfig):
        """
        Initialize SQL Server writer.
        
        Args:
            config: SQL Server configuration
        """
        self.config = config
        self.connection_pool = ConnectionPool(config)
        self._total_records_written = 0
        self._batch_count = 0
        self._error_count = 0
        
        logger.info(
            "SQL Server writer initialized",
            extra={
                "server": config.server,
                "database": config.database,
                "table": config.table_name,
            }
        )
    
    def _build_insert_query(self) -> str:
        """
        Build parameterized INSERT query.
        
        Returns:
            str: SQL INSERT statement
        """
        return f"""
            INSERT INTO {self.config.table_name} (
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
    
    def write_batch(
        self,
        positions: List[FinancialPosition],
        snapshot_time: Optional[datetime] = None
    ) -> int:
        """
        Write a batch of positions to the database.
        
        Args:
            positions: List of FinancialPosition objects
            snapshot_time: Optional snapshot timestamp
            
        Returns:
            int: Number of records written
            
        Raises:
            SQLWriterError: If write fails
        """
        if not positions:
            logger.debug("No positions to write")
            return 0
        
        batch_start = datetime.utcnow()
        records_written = 0
        
        try:
            with self.connection_pool.get_connection() as conn:
                cursor = conn.cursor()
                
                # Prepare batch data
                insert_query = self._build_insert_query()
                batch_data = [pos.to_sql_tuple() for pos in positions]
                
                # Execute batch insert
                try:
                    cursor.fast_executemany = True  # Enable fast batch mode
                    cursor.executemany(insert_query, batch_data)
                    conn.commit()
                    
                    records_written = len(positions)
                    self._total_records_written += records_written
                    self._batch_count += 1
                    
                    batch_duration = (datetime.utcnow() - batch_start).total_seconds()
                    
                    logger.info(
                        "Batch written successfully",
                        extra={
                            "records": records_written,
                            "batch_number": self._batch_count,
                            "duration_seconds": round(batch_duration, 3),
                            "records_per_second": round(
                                records_written / batch_duration, 2
                            ) if batch_duration > 0 else 0,
                        }
                    )
                    
                except pyodbc.Error as e:
                    conn.rollback()
                    self._error_count += 1
                    
                    logger.error(
                        "Failed to write batch",
                        extra={
                            "error": str(e),
                            "records": len(positions),
                        },
                        exc_info=True
                    )
                    raise SQLWriterError(f"Failed to write batch: {e}") from e
                
        except ConnectionPoolError as e:
            self._error_count += 1
            logger.error(
                "Connection pool error",
                extra={"error": str(e)},
                exc_info=True
            )
            raise SQLWriterError(f"Connection pool error: {e}") from e
        
        return records_written
    
    def write_batches(
        self,
        positions: List[FinancialPosition],
        batch_size: Optional[int] = None
    ) -> int:
        """
        Write positions in multiple batches.
        
        Args:
            positions: List of FinancialPosition objects
            batch_size: Optional batch size (uses config default if None)
            
        Returns:
            int: Total number of records written
        """
        if not positions:
            return 0
        
        batch_size = batch_size or self.config.batch_size
        total_written = 0
        
        # Split into batches
        for i in range(0, len(positions), batch_size):
            batch = positions[i:i + batch_size]
            written = self.write_batch(batch)
            total_written += written
        
        logger.info(
            "All batches written",
            extra={
                "total_records": total_written,
                "num_batches": (len(positions) + batch_size - 1) // batch_size,
            }
        )
        
        return total_written
    
    def verify_table_exists(self) -> bool:
        """
        Verify that the target table exists.
        
        Returns:
            bool: True if table exists, False otherwise
        """
        try:
            with self.connection_pool.get_connection() as conn:
                cursor = conn.cursor()
                
                query = """
                    SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_NAME = ?
                """
                
                cursor.execute(query, (self.config.table_name,))
                result = cursor.fetchone()
                
                exists = result[0] > 0
                
                logger.debug(
                    f"Table existence check: {exists}",
                    extra={"table": self.config.table_name}
                )
                
                return exists
                
        except Exception as e:
            logger.error(
                "Failed to verify table existence",
                extra={"error": str(e)},
                exc_info=True
            )
            return False
    
    def create_table_if_not_exists(self) -> bool:
        """
        Create the target table if it doesn't exist.
        
        Returns:
            bool: True if table was created or already exists
        """
        if self.verify_table_exists():
            logger.info(f"Table {self.config.table_name} already exists")
            return True
        
        try:
            with self.connection_pool.get_connection() as conn:
                cursor = conn.cursor()
                
                create_table_query = f"""
                    CREATE TABLE {self.config.table_name} (
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
                    
                    CREATE INDEX idx_snapshot_time 
                        ON {self.config.table_name} (snapshot_time);
                    CREATE INDEX idx_position_id 
                        ON {self.config.table_name} (position_id);
                    CREATE INDEX idx_account_id 
                        ON {self.config.table_name} (account_id);
                    CREATE INDEX idx_symbol 
                        ON {self.config.table_name} (symbol);
                """
                
                cursor.execute(create_table_query)
                conn.commit()
                
                logger.info(f"Table {self.config.table_name} created successfully")
                return True
                
        except Exception as e:
            logger.error(
                "Failed to create table",
                extra={"error": str(e)},
                exc_info=True
            )
            return False
    
    def get_statistics(self) -> dict:
        """
        Get writer statistics.
        
        Returns:
            dict: Writer statistics
        """
        return {
            "total_records_written": self._total_records_written,
            "batch_count": self._batch_count,
            "error_count": self._error_count,
            "avg_batch_size": (
                self._total_records_written / self._batch_count
                if self._batch_count > 0
                else 0
            ),
        }
    
    def close(self) -> None:
        """Close all database connections."""
        self.connection_pool.close_all()
        logger.info("SQL Server writer closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
