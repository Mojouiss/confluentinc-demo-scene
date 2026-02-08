"""Unit tests for main service module."""

from unittest.mock import Mock, MagicMock, patch

import pytest

from financial_snapshot.config import Config
from financial_snapshot.service import FinancialSnapshotService


@pytest.fixture
def test_config():
    """Fixture for test configuration."""
    return Config()


class TestFinancialSnapshotService:
    """Tests for FinancialSnapshotService."""
    
    def test_initialization(self, test_config):
        """Test service initialization."""
        service = FinancialSnapshotService(test_config)
        
        assert service.config == test_config
        assert service.consumer is None
        assert service.writer is None
        assert service.scheduler is None
        assert service._total_snapshots == 0
    
    @patch('financial_snapshot.service.SnapshotScheduler')
    @patch('financial_snapshot.service.SQLServerWriter')
    @patch('financial_snapshot.service.FinancialPositionConsumer')
    def test_initialize_components(
        self,
        mock_consumer_cls,
        mock_writer_cls,
        mock_scheduler_cls,
        test_config
    ):
        """Test component initialization."""
        # Setup mocks
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer
        
        mock_writer = MagicMock()
        mock_writer.verify_table_exists.return_value = True
        mock_writer_cls.return_value = mock_writer
        
        mock_scheduler = MagicMock()
        mock_scheduler_cls.return_value = mock_scheduler
        
        service = FinancialSnapshotService(test_config)
        service._initialize_components()
        
        # Verify components were created
        assert service.consumer is not None
        assert service.writer is not None
        assert service.scheduler is not None
        
        mock_consumer.connect.assert_called_once()
        mock_writer.verify_table_exists.assert_called_once()
    
    @patch('financial_snapshot.service.SnapshotScheduler')
    @patch('financial_snapshot.service.SQLServerWriter')
    @patch('financial_snapshot.service.FinancialPositionConsumer')
    def test_initialize_components_creates_table(
        self,
        mock_consumer_cls,
        mock_writer_cls,
        mock_scheduler_cls,
        test_config
    ):
        """Test table creation during initialization."""
        # Setup mocks
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer
        
        mock_writer = MagicMock()
        mock_writer.verify_table_exists.return_value = False
        mock_writer.create_table_if_not_exists.return_value = True
        mock_writer_cls.return_value = mock_writer
        
        mock_scheduler = MagicMock()
        mock_scheduler_cls.return_value = mock_scheduler
        
        service = FinancialSnapshotService(test_config)
        service._initialize_components()
        
        mock_writer.create_table_if_not_exists.assert_called_once()
    
    @patch('financial_snapshot.service.FinancialPositionConsumer')
    @patch('financial_snapshot.service.SQLServerWriter')
    def test_execute_snapshot_success(
        self,
        mock_writer_cls,
        mock_consumer_cls,
        test_config
    ):
        """Test successful snapshot execution."""
        from datetime import datetime
        from decimal import Decimal
        from financial_snapshot.models import FinancialPosition
        
        # Create sample position
        position = FinancialPosition(
            position_id="POS-123",
            account_id="ACC-456",
            symbol="AAPL",
            quantity=Decimal("100.0"),
            price=Decimal("150.25"),
            market_value=Decimal("15025.0"),
            timestamp=datetime(2026, 2, 1, 12, 0, 0),
        )
        
        # Setup mocks
        mock_consumer = MagicMock()
        mock_consumer.consume_batch.return_value = [position]
        mock_consumer_cls.return_value = mock_consumer
        
        mock_writer = MagicMock()
        mock_writer.write_batches.return_value = 1
        mock_writer_cls.return_value = mock_writer
        
        service = FinancialSnapshotService(test_config)
        service.consumer = mock_consumer
        service.writer = mock_writer
        
        service._execute_snapshot()
        
        assert service._total_snapshots == 1
        assert service._total_records_processed == 1
        
        mock_consumer.consume_batch.assert_called_once()
        mock_writer.write_batches.assert_called_once()
        mock_consumer.commit_offsets.assert_called_once()
    
    @patch('financial_snapshot.service.FinancialPositionConsumer')
    @patch('financial_snapshot.service.SQLServerWriter')
    def test_execute_snapshot_no_data(
        self,
        mock_writer_cls,
        mock_consumer_cls,
        test_config
    ):
        """Test snapshot execution with no data."""
        # Setup mocks
        mock_consumer = MagicMock()
        mock_consumer.consume_batch.return_value = []
        mock_consumer_cls.return_value = mock_consumer
        
        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer
        
        service = FinancialSnapshotService(test_config)
        service.consumer = mock_consumer
        service.writer = mock_writer
        
        service._execute_snapshot()
        
        # Should not write or commit if no data
        mock_writer.write_batches.assert_not_called()
        mock_consumer.commit_offsets.assert_not_called()
    
    @patch('financial_snapshot.service.FinancialPositionConsumer')
    @patch('financial_snapshot.service.SQLServerWriter')
    def test_execute_snapshot_error(
        self,
        mock_writer_cls,
        mock_consumer_cls,
        test_config
    ):
        """Test snapshot execution error handling."""
        # Setup mocks
        mock_consumer = MagicMock()
        mock_consumer.consume_batch.side_effect = Exception("Test error")
        mock_consumer_cls.return_value = mock_consumer
        
        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer
        
        service = FinancialSnapshotService(test_config)
        service.consumer = mock_consumer
        service.writer = mock_writer
        
        with pytest.raises(Exception):
            service._execute_snapshot()
        
        assert service._total_errors == 1
    
    def test_get_statistics(self, test_config):
        """Test getting service statistics."""
        service = FinancialSnapshotService(test_config)
        service._total_snapshots = 10
        service._total_records_processed = 5000
        
        stats = service.get_statistics()
        
        assert stats['service']['total_snapshots'] == 10
        assert stats['service']['total_records_processed'] == 5000
    
    @patch('financial_snapshot.service.SnapshotScheduler')
    @patch('financial_snapshot.service.SQLServerWriter')
    @patch('financial_snapshot.service.FinancialPositionConsumer')
    def test_shutdown(
        self,
        mock_consumer_cls,
        mock_writer_cls,
        mock_scheduler_cls,
        test_config
    ):
        """Test graceful shutdown."""
        # Setup mocks
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer
        
        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer
        
        mock_scheduler = MagicMock()
        mock_scheduler_cls.return_value = mock_scheduler
        
        service = FinancialSnapshotService(test_config)
        service.consumer = mock_consumer
        service.writer = mock_writer
        service.scheduler = mock_scheduler
        
        service._shutdown()
        
        mock_scheduler.stop.assert_called_once()
        mock_consumer.disconnect.assert_called_once()
        mock_writer.close.assert_called_once()
