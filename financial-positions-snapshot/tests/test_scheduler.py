"""Unit tests for scheduler module."""

import time
from unittest.mock import Mock, patch, call

import pytest

from financial_snapshot.config import SnapshotConfig
from financial_snapshot.scheduler import SnapshotScheduler


@pytest.fixture
def snapshot_config():
    """Fixture for snapshot configuration."""
    return SnapshotConfig(
        interval_seconds=10,
        batch_size=1000,
    )


class TestSnapshotScheduler:
    """Tests for SnapshotScheduler."""
    
    def test_initialization(self, snapshot_config):
        """Test scheduler initialization."""
        callback = Mock()
        scheduler = SnapshotScheduler(snapshot_config, callback)
        
        assert scheduler.config == snapshot_config
        assert scheduler.snapshot_callback == callback
        assert scheduler._running is False
        assert scheduler._cycle_count == 0
    
    def test_start_stop(self, snapshot_config):
        """Test starting and stopping scheduler."""
        callback = Mock()
        scheduler = SnapshotScheduler(snapshot_config, callback)
        
        # Start in separate thread and stop immediately
        import threading
        
        def run_scheduler():
            scheduler.start()
        
        thread = threading.Thread(target=run_scheduler)
        thread.start()
        
        # Give it a moment to start
        time.sleep(0.1)
        
        # Stop scheduler
        scheduler.stop()
        
        # Wait for thread to finish
        thread.join(timeout=5)
        
        assert scheduler._running is False
    
    def test_snapshot_callback_executed(self, snapshot_config):
        """Test that snapshot callback is executed."""
        callback = Mock()
        scheduler = SnapshotScheduler(snapshot_config, callback)
        
        # Mock sleep to avoid waiting
        with patch.object(scheduler, '_interruptible_sleep'):
            # Start and stop after one cycle
            import threading
            
            def run_scheduler():
                scheduler.start()
            
            thread = threading.Thread(target=run_scheduler)
            thread.start()
            
            # Wait for at least one cycle
            time.sleep(0.5)
            scheduler.stop()
            thread.join(timeout=5)
        
        # Callback should have been called at least once
        assert callback.call_count >= 1
        assert scheduler._cycle_count >= 1
    
    def test_callback_error_handling(self, snapshot_config):
        """Test error handling in callback."""
        callback = Mock(side_effect=Exception("Test error"))
        scheduler = SnapshotScheduler(snapshot_config, callback)
        
        # Mock sleep to avoid waiting
        with patch.object(scheduler, '_interruptible_sleep'):
            import threading
            
            def run_scheduler():
                scheduler.start()
            
            thread = threading.Thread(target=run_scheduler)
            thread.start()
            
            time.sleep(0.5)
            scheduler.stop()
            thread.join(timeout=5)
        
        # Scheduler should continue running despite errors
        assert callback.called
    
    def test_interruptible_sleep(self, snapshot_config):
        """Test interruptible sleep."""
        callback = Mock()
        scheduler = SnapshotScheduler(snapshot_config, callback)
        scheduler._running = True
        
        start_time = time.time()
        
        # Sleep for 2 seconds
        import threading
        
        def sleep_task():
            scheduler._interruptible_sleep(2.0)
        
        thread = threading.Thread(target=sleep_task)
        thread.start()
        
        # Stop after 0.5 seconds
        time.sleep(0.5)
        scheduler.stop()
        
        thread.join(timeout=5)
        elapsed = time.time() - start_time
        
        # Should wake up before full 2 seconds
        assert elapsed < 2.0
    
    def test_get_statistics(self, snapshot_config):
        """Test getting scheduler statistics."""
        callback = Mock()
        scheduler = SnapshotScheduler(snapshot_config, callback)
        scheduler._cycle_count = 5
        
        stats = scheduler.get_statistics()
        
        assert stats['running'] is False
        assert stats['cycle_count'] == 5
    
    @patch('financial_snapshot.scheduler.signal.signal')
    def test_signal_handlers(self, mock_signal, snapshot_config):
        """Test signal handler setup."""
        callback = Mock()
        scheduler = SnapshotScheduler(snapshot_config, callback)
        
        # Verify signal handlers were registered
        assert mock_signal.call_count >= 2
