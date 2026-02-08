"""Example usage of the Financial Positions Snapshot library."""

import logging
from datetime import datetime
from decimal import Decimal

from financial_snapshot import (
    FinancialSnapshotService,
    Config,
    FinancialPosition,
)
from financial_snapshot.config import (
    KafkaConfig,
    SQLServerConfig,
    SnapshotConfig,
    LoggingConfig,
)
from financial_snapshot.logging_config import setup_logging


def example_basic_usage():
    """Example: Basic usage with default configuration."""
    # Create configuration
    config = Config(
        kafka=KafkaConfig(
            bootstrap_servers="localhost:9092",
            topic="financial-positions",
        ),
        sql_server=SQLServerConfig(
            server="localhost",
            database="FinanceDB",
            username="sa",
            password="YourPassword",
        ),
        snapshot=SnapshotConfig(
            interval_seconds=300,  # 5 minutes
        ),
        logging=LoggingConfig(
            level="INFO",
        ),
    )
    
    # Setup logging
    setup_logging(config.logging)
    
    # Create and start service
    service = FinancialSnapshotService(config)
    
    try:
        service.start()
    except KeyboardInterrupt:
        logging.info("Service interrupted by user")


def example_with_config_file():
    """Example: Load configuration from file."""
    from financial_snapshot.config import load_config
    from financial_snapshot.logging_config import setup_logging
    
    # Load configuration from YAML file
    config = load_config("config.yaml")
    
    # Setup logging
    setup_logging(config.logging)
    
    # Create and start service
    service = FinancialSnapshotService(config)
    service.start()


def example_manual_snapshot():
    """Example: Execute a single snapshot manually."""
    from financial_snapshot.kafka_consumer import FinancialPositionConsumer
    from financial_snapshot.sql_writer import SQLServerWriter
    
    # Create configuration
    kafka_config = KafkaConfig(
        bootstrap_servers="localhost:9092",
        topic="financial-positions",
    )
    
    sql_config = SQLServerConfig(
        server="localhost",
        database="FinanceDB",
        username="sa",
        password="YourPassword",
    )
    
    # Create consumer and writer
    with FinancialPositionConsumer(kafka_config) as consumer, \
         SQLServerWriter(sql_config) as writer:
        
        # Consume batch
        positions = consumer.consume_batch(batch_size=1000)
        
        print(f"Consumed {len(positions)} positions")
        
        # Write to database
        if positions:
            records_written = writer.write_batches(positions)
            consumer.commit_offsets()
            
            print(f"Written {records_written} records to database")


def example_creating_test_data():
    """Example: Creating test position data."""
    import json
    from confluent_kafka import Producer
    
    # Create sample position
    position = FinancialPosition(
        position_id="POS-12345",
        account_id="ACC-67890",
        symbol="AAPL",
        quantity=Decimal("100.0"),
        price=Decimal("150.25"),
        market_value=Decimal("15025.0"),
        timestamp=datetime.utcnow(),
        currency="USD",
    )
    
    # Convert to JSON
    position_json = position.model_dump_json()
    print(f"Position JSON: {position_json}")
    
    # Produce to Kafka (example)
    producer_config = {
        'bootstrap.servers': 'localhost:9092',
    }
    
    producer = Producer(producer_config)
    
    try:
        producer.produce(
            topic='financial-positions',
            value=position_json.encode('utf-8'),
            callback=lambda err, msg: print(
                f"Delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}"
                if not err else f"Error: {err}"
            )
        )
        producer.flush()
    finally:
        producer.flush()


def example_statistics():
    """Example: Getting service statistics."""
    config = Config()
    service = FinancialSnapshotService(config)
    
    # ... run service for a while ...
    
    # Get statistics
    stats = service.get_statistics()
    
    print(f"Total snapshots: {stats['service']['total_snapshots']}")
    print(f"Total records: {stats['service']['total_records_processed']}")
    
    if 'consumer' in stats:
        print(f"Consumer errors: {stats['consumer']['errors']}")
    
    if 'writer' in stats:
        print(f"Writer batches: {stats['writer']['batch_count']}")


if __name__ == "__main__":
    # Run basic example
    print("Financial Positions Snapshot - Example Usage")
    print("=" * 60)
    
    # Uncomment the example you want to run:
    # example_basic_usage()
    # example_with_config_file()
    # example_manual_snapshot()
    # example_creating_test_data()
    # example_statistics()
    
    print("\nSee source code for more examples")
