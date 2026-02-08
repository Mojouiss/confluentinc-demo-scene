"""Unit tests for SQL Server writer module."""

from datetime import datetime
from decimal import Decimal
from unittest.mock import Mock, MagicMock, patch, call
from queue import Queue

import pytest
import pyodbc

from financial_snapshot.config import SQLServerConfig
from financial_snapshot.sql_writer import (
    SQLServerWriter,
    ConnectionPool,
    SQLWriterError,
    ConnectionPoolError,
)
from financial_snapshot.models import FinancialPosition


@pytest.fixture
def sql_config():
    """Fixture for SQL Server configuration."""
    return SQLServerConfig(
        server="test-server",
        database="test-db",
        username="test-user",
        password="test-pass",
        connection_pool_size=2,
        batch_size=100,
    )


@pytest.fixture
def sample_position():
    """Fixture for sample financial position."""
    return FinancialPosition(
        position_id="POS-123",
        account_id="ACC-456",
        symbol="AAPL",
        quantity=Decimal("100.0"),
        price=Decimal("150.25"),
        market_value=Decimal("15025.0"),
        timestamp=datetime(2026, 2, 1, 12, 0, 0),
        currency="USD",
    )


class TestConnectionPool:
    """Tests for ConnectionPool."""
    
    @patch('financial_snapshot.sql_writer.pyodbc.connect')
    def test_initialization(self, mock_connect, sql_config):
        """Test connection pool initialization."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        pool = ConnectionPool(sql_config)
        
        # Should create pool_size connections
        assert mock_connect.call_count == sql_config.connection_pool_size
    
    @patch('financial_snapshot.sql_writer.pyodbc.connect')
    def test_get_connection(self, mock_connect, sql_config):
        """Test getting connection from pool."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        pool = ConnectionPool(sql_config)
        
        with pool.get_connection() as conn:
            assert conn is not None
            mock_cursor.execute.assert_called()
    
    @patch('financial_snapshot.sql_writer.pyodbc.connect')
    def test_connection_returned_to_pool(self, mock_connect, sql_config):
        """Test connection is returned to pool after use."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        pool = ConnectionPool(sql_config)
        
        initial_pool_size = pool._pool.qsize()
        
        with pool.get_connection() as conn:
            pass
        
        # Connection should be returned
        assert pool._pool.qsize() == initial_pool_size
    
    @patch('financial_snapshot.sql_writer.pyodbc.connect')
    def test_connection_rollback_on_exit(self, mock_connect, sql_config):
        """Test connection rollback on context exit."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        pool = ConnectionPool(sql_config)
        
        with pool.get_connection() as conn:
            pass
        
        mock_conn.rollback.assert_called_once()
    
    @patch('financial_snapshot.sql_writer.pyodbc.connect')
    def test_dead_connection_recreated(self, mock_connect, sql_config):
        """Test dead connection is recreated."""
        # First connection works, then fails, then new one works
        mock_conn1 = MagicMock()
        mock_conn1.cursor().execute.side_effect = pyodbc.Error("Dead connection")
        
        mock_conn2 = MagicMock()
        mock_cursor2 = MagicMock()
        mock_conn2.cursor.return_value = mock_cursor2
        
        mock_connect.side_effect = [mock_conn1, mock_conn1, mock_conn2]
        
        pool = ConnectionPool(sql_config)
        
        with pool.get_connection() as conn:
            assert conn == mock_conn2
    
    @patch('financial_snapshot.sql_writer.pyodbc.connect')
    def test_close_all(self, mock_connect, sql_config):
        """Test closing all connections."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        pool = ConnectionPool(sql_config)
        pool.close_all()
        
        assert pool._pool.empty()


class TestSQLServerWriter:
    """Tests for SQLServerWriter."""
    
    @patch('financial_snapshot.sql_writer.ConnectionPool')
    def test_initialization(self, mock_pool, sql_config):
        """Test writer initialization."""
        writer = SQLServerWriter(sql_config)
        
        assert writer.config == sql_config
        assert writer._total_records_written == 0
        assert writer._batch_count == 0
    
    def test_build_insert_query(self, sql_config):
        """Test building INSERT query."""
        with patch('financial_snapshot.sql_writer.ConnectionPool'):
            writer = SQLServerWriter(sql_config)
            query = writer._build_insert_query()
            
            assert "INSERT INTO" in query
            assert sql_config.table_name in query
            assert "position_id" in query
            assert "VALUES" in query
    
    @patch('financial_snapshot.sql_writer.ConnectionPool')
    def test_write_batch_empty(self, mock_pool, sql_config):
        """Test writing empty batch."""
        writer = SQLServerWriter(sql_config)
        
        records_written = writer.write_batch([])
        
        assert records_written == 0
    
    @patch('financial_snapshot.sql_writer.ConnectionPool')
    def test_write_batch_success(
        self,
        mock_pool_cls,
        sql_config,
        sample_position
    ):
        """Test successful batch write."""
        # Setup mock connection pool
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        mock_pool = MagicMock()
        mock_pool.get_connection.return_value.__enter__.return_value = mock_conn
        mock_pool_cls.return_value = mock_pool
        
        writer = SQLServerWriter(sql_config)
        
        positions = [sample_position]
        records_written = writer.write_batch(positions)
        
        assert records_written == 1
        assert writer._total_records_written == 1
        assert writer._batch_count == 1
        
        # Verify SQL execution
        mock_cursor.executemany.assert_called_once()
        mock_conn.commit.assert_called_once()
    
    @patch('financial_snapshot.sql_writer.ConnectionPool')
    def test_write_batch_failure(
        self,
        mock_pool_cls,
        sql_config,
        sample_position
    ):
        """Test batch write failure handling."""
        # Setup mock connection pool with error
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.executemany.side_effect = pyodbc.Error("SQL error")
        mock_conn.cursor.return_value = mock_cursor
        
        mock_pool = MagicMock()
        mock_pool.get_connection.return_value.__enter__.return_value = mock_conn
        mock_pool_cls.return_value = mock_pool
        
        writer = SQLServerWriter(sql_config)
        
        positions = [sample_position]
        
        with pytest.raises(SQLWriterError):
            writer.write_batch(positions)
        
        # Verify rollback
        mock_conn.rollback.assert_called_once()
        assert writer._error_count == 1
    
    @patch('financial_snapshot.sql_writer.ConnectionPool')
    def test_write_batches(
        self,
        mock_pool_cls,
        sql_config,
        sample_position
    ):
        """Test writing multiple batches."""
        # Setup mock connection pool
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        mock_pool = MagicMock()
        mock_pool.get_connection.return_value.__enter__.return_value = mock_conn
        mock_pool_cls.return_value = mock_pool
        
        writer = SQLServerWriter(sql_config)
        
        # Create 250 positions (should be split into 3 batches of 100, 100, 50)
        positions = [sample_position] * 250
        
        total_written = writer.write_batches(positions, batch_size=100)
        
        assert total_written == 250
        # Should call executemany 3 times
        assert mock_cursor.executemany.call_count == 3
    
    @patch('financial_snapshot.sql_writer.ConnectionPool')
    def test_verify_table_exists(self, mock_pool_cls, sql_config):
        """Test table existence verification."""
        # Setup mock connection pool
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [1]  # Table exists
        mock_conn.cursor.return_value = mock_cursor
        
        mock_pool = MagicMock()
        mock_pool.get_connection.return_value.__enter__.return_value = mock_conn
        mock_pool_cls.return_value = mock_pool
        
        writer = SQLServerWriter(sql_config)
        
        exists = writer.verify_table_exists()
        
        assert exists is True
        mock_cursor.execute.assert_called()
    
    @patch('financial_snapshot.sql_writer.ConnectionPool')
    def test_create_table_if_not_exists(self, mock_pool_cls, sql_config):
        """Test table creation."""
        # Setup mock connection pool
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [0]  # Table doesn't exist
        mock_conn.cursor.return_value = mock_cursor
        
        mock_pool = MagicMock()
        mock_pool.get_connection.return_value.__enter__.return_value = mock_conn
        mock_pool_cls.return_value = mock_pool
        
        writer = SQLServerWriter(sql_config)
        
        created = writer.create_table_if_not_exists()
        
        # Should attempt to create table
        assert mock_cursor.execute.call_count >= 2
        assert mock_conn.commit.called
    
    @patch('financial_snapshot.sql_writer.ConnectionPool')
    def test_get_statistics(self, mock_pool, sql_config):
        """Test getting writer statistics."""
        writer = SQLServerWriter(sql_config)
        writer._total_records_written = 1000
        writer._batch_count = 10
        writer._error_count = 2
        
        stats = writer.get_statistics()
        
        assert stats['total_records_written'] == 1000
        assert stats['batch_count'] == 10
        assert stats['error_count'] == 2
        assert stats['avg_batch_size'] == 100.0
    
    @patch('financial_snapshot.sql_writer.ConnectionPool')
    def test_context_manager(self, mock_pool_cls, sql_config):
        """Test using writer as context manager."""
        mock_pool = MagicMock()
        mock_pool_cls.return_value = mock_pool
        
        with SQLServerWriter(sql_config) as writer:
            assert writer is not None
        
        mock_pool.close_all.assert_called_once()
