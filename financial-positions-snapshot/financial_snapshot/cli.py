"""Command-line interface for the Financial Snapshot Service."""

import argparse
import logging
import sys
from pathlib import Path

from financial_snapshot.config import load_config, get_default_config
from financial_snapshot.service import FinancialSnapshotService
from financial_snapshot.logging_config import setup_logging


logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Financial Positions Snapshot Service",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with configuration file
  %(prog)s --config config.yaml
  
  # Run with environment variables
  export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
  export SQL_SERVER="localhost"
  %(prog)s
  
  # Run with custom log file
  %(prog)s --config config.yaml --log-file /var/log/snapshot.log
        """
    )
    
    parser.add_argument(
        "-c", "--config",
        type=str,
        help="Path to YAML configuration file"
    )
    
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Override log level"
    )
    
    parser.add_argument(
        "--log-file",
        type=str,
        help="Path to log file"
    )
    
    parser.add_argument(
        "--log-format",
        type=str,
        choices=["json", "text"],
        help="Log format"
    )
    
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Verify configuration and connections, then exit"
    )
    
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 1.0.0"
    )
    
    return parser.parse_args()


def verify_configuration(config) -> bool:
    """
    Verify configuration and connections.
    
    Args:
        config: Service configuration
        
    Returns:
        bool: True if verification succeeds
    """
    from financial_snapshot.kafka_consumer import FinancialPositionConsumer
    from financial_snapshot.sql_writer import SQLServerWriter
    
    logger.info("Verifying configuration...")
    
    # Test Kafka connection
    try:
        logger.info("Testing Kafka connection...")
        consumer = FinancialPositionConsumer(config.kafka)
        consumer.connect()
        consumer.disconnect()
        logger.info("✓ Kafka connection successful")
    except Exception as e:
        logger.error(f"✗ Kafka connection failed: {e}")
        return False
    
    # Test SQL Server connection
    try:
        logger.info("Testing SQL Server connection...")
        writer = SQLServerWriter(config.sql_server)
        
        if not writer.verify_table_exists():
            logger.warning("Target table does not exist")
            response = input("Create table? (y/n): ")
            if response.lower() == 'y':
                if writer.create_table_if_not_exists():
                    logger.info("✓ Table created successfully")
                else:
                    logger.error("✗ Failed to create table")
                    return False
        else:
            logger.info("✓ SQL Server connection successful")
        
        writer.close()
    except Exception as e:
        logger.error(f"✗ SQL Server connection failed: {e}")
        return False
    
    logger.info("✓ Configuration verification complete")
    return True


def main() -> int:
    """
    Main entry point.
    
    Returns:
        int: Exit code
    """
    args = parse_args()
    
    try:
        # Load configuration
        if args.config:
            config_path = Path(args.config)
            if not config_path.exists():
                print(f"Error: Configuration file not found: {args.config}")
                return 1
            config = load_config(args.config)
        else:
            config = get_default_config()
        
        # Override with command-line arguments
        if args.log_level:
            config.logging.level = args.log_level
        if args.log_file:
            config.logging.log_file = args.log_file
        if args.log_format:
            config.logging.format = args.log_format
        
        # Setup logging
        setup_logging(config.logging)
        
        logger.info("=" * 60)
        logger.info("Financial Positions Snapshot Service v1.0.0")
        logger.info("=" * 60)
        
        # Verify configuration if requested
        if args.verify_only:
            if verify_configuration(config):
                logger.info("Verification successful!")
                return 0
            else:
                logger.error("Verification failed!")
                return 1
        
        # Create and start service
        service = FinancialSnapshotService(config)
        service.start()
        
        return 0
        
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 0
    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
