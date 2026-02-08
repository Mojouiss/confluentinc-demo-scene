"""Snapshot scheduler module."""

import logging
import time
from datetime import datetime, timedelta
from typing import Optional, Callable
import signal
import sys

from financial_snapshot.config import SnapshotConfig


logger = logging.getLogger(__name__)


class SnapshotScheduler:
    """
    Scheduler for periodic snapshot operations.
    
    Manages the timing and execution of snapshot cycles with
    graceful shutdown handling.
    """
    
    def __init__(
        self,
        config: SnapshotConfig,
        snapshot_callback: Callable[[], None]
    ):
        """
        Initialize scheduler.
        
        Args:
            config: Snapshot configuration
            snapshot_callback: Function to call for each snapshot
        """
        self.config = config
        self.snapshot_callback = snapshot_callback
        self._running = False
        self._cycle_count = 0
        self._last_snapshot_time: Optional[datetime] = None
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info(
            "Snapshot scheduler initialized",
            extra={
                "interval_seconds": config.interval_seconds,
                "batch_size": config.batch_size,
            }
        )
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(
            f"Received signal {signum}, initiating graceful shutdown"
        )
        self.stop()
    
    def start(self) -> None:
        """
        Start the scheduler.
        
        Runs indefinitely until stop() is called or a signal is received.
        """
        self._running = True
        logger.info("Snapshot scheduler started")
        
        while self._running:
            cycle_start = datetime.utcnow()
            
            try:
                logger.info(
                    f"Starting snapshot cycle {self._cycle_count + 1}",
                    extra={"timestamp": cycle_start.isoformat()}
                )
                
                # Execute snapshot callback
                self.snapshot_callback()
                
                self._cycle_count += 1
                self._last_snapshot_time = cycle_start
                
                cycle_duration = (datetime.utcnow() - cycle_start).total_seconds()
                
                logger.info(
                    f"Snapshot cycle {self._cycle_count} completed",
                    extra={
                        "duration_seconds": round(cycle_duration, 3),
                        "total_cycles": self._cycle_count,
                    }
                )
                
                # Calculate sleep time
                sleep_time = max(
                    0,
                    self.config.interval_seconds - cycle_duration
                )
                
                if sleep_time > 0:
                    logger.debug(
                        f"Sleeping for {sleep_time:.1f} seconds until next cycle"
                    )
                    self._interruptible_sleep(sleep_time)
                else:
                    logger.warning(
                        "Snapshot cycle took longer than interval",
                        extra={
                            "cycle_duration": cycle_duration,
                            "interval": self.config.interval_seconds,
                            "overrun": cycle_duration - self.config.interval_seconds,
                        }
                    )
                    
            except Exception as e:
                logger.error(
                    "Error in snapshot cycle",
                    extra={
                        "error": str(e),
                        "cycle": self._cycle_count + 1,
                    },
                    exc_info=True
                )
                
                # Sleep a bit before retry
                if self._running:
                    retry_delay = min(60, self.config.retry_backoff_seconds)
                    logger.info(f"Retrying in {retry_delay} seconds")
                    self._interruptible_sleep(retry_delay)
        
        logger.info(
            "Snapshot scheduler stopped",
            extra={"total_cycles": self._cycle_count}
        )
    
    def _interruptible_sleep(self, seconds: float) -> None:
        """
        Sleep that can be interrupted by stop().
        
        Args:
            seconds: Number of seconds to sleep
        """
        end_time = time.time() + seconds
        
        while self._running and time.time() < end_time:
            time.sleep(min(1.0, end_time - time.time()))
    
    def stop(self) -> None:
        """Stop the scheduler."""
        if self._running:
            logger.info("Stopping snapshot scheduler")
            self._running = False
    
    def get_statistics(self) -> dict:
        """
        Get scheduler statistics.
        
        Returns:
            dict: Scheduler statistics
        """
        return {
            "running": self._running,
            "cycle_count": self._cycle_count,
            "last_snapshot_time": (
                self._last_snapshot_time.isoformat()
                if self._last_snapshot_time
                else None
            ),
            "next_snapshot_time": (
                (self._last_snapshot_time + 
                 timedelta(seconds=self.config.interval_seconds)).isoformat()
                if self._last_snapshot_time
                else None
            ),
        }
