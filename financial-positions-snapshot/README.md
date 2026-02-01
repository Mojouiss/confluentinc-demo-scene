# Financial Positions Snapshot Library

A robust, high-performance Python library for capturing snapshots of live financial positions from Kafka topics and persisting them to SQL Server databases with zero data loss.

## Features

- 🔄 **Scheduled Snapshots**: Automated snapshots every 5 minutes
- 📊 **Batch Processing**: Efficient batching to minimize database pressure
- 🛡️ **Zero Data Loss**: Reliable offset management and error handling
- ⚡ **High Performance**: Optimized for throughput with connection pooling
- 🔍 **Type Safety**: Full Pydantic validation for all data models
- 📝 **Comprehensive Logging**: Detailed logging for monitoring and debugging
- 🧪 **Well Tested**: Extensive unit test coverage
- 🔧 **Configurable**: Flexible configuration via environment variables or config files

## Architecture

The library follows a modular, layered architecture:

```
┌─────────────────┐
│   Scheduler     │  - Orchestrates snapshot cycles
└────────┬────────┘
         │
┌────────▼────────┐
│ Kafka Consumer  │  - Consumes financial positions
└────────┬────────┘
         │
┌────────▼────────┐
│  Data Models    │  - Validates and transforms data
└────────┬────────┘
         │
┌────────▼────────┐
│  SQL Writer     │  - Batched writes to SQL Server
└─────────────────┘
```

## Installation

### Requirements

- Python 3.8+
- SQL Server database
- Kafka cluster

### Install Dependencies

```bash
pip install -r requirements.txt
```

## Configuration

Create a `config.yaml` file or set environment variables:

```yaml
kafka:
  bootstrap_servers: "localhost:9092"
  topic: "financial-positions"
  group_id: "financial-snapshot-consumer"
  auto_offset_reset: "earliest"
  enable_auto_commit: false
  
sql_server:
  server: "localhost"
  database: "FinanceDB"
  username: "sa"
  password: "YourPassword"
  driver: "ODBC Driver 17 for SQL Server"
  connection_pool_size: 5
  batch_size: 1000
  
snapshot:
  interval_seconds: 300  # 5 minutes
  batch_size: 5000
  commit_interval: 1000
  
logging:
  level: "INFO"
  format: "json"
```

## Usage

### Basic Usage

```python
from financial_snapshot import FinancialSnapshotService
from financial_snapshot.config import load_config

# Load configuration
config = load_config("config.yaml")

# Create and run service
service = FinancialSnapshotService(config)
service.start()
```

### Command Line

```bash
# Run with config file
python -m financial_snapshot --config config.yaml

# Run with environment variables
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export SQL_SERVER="localhost"
python -m financial_snapshot
```

## Data Model

Financial positions are validated using Pydantic models:

```python
{
  "position_id": "POS-12345",
  "account_id": "ACC-67890",
  "symbol": "AAPL",
  "quantity": 100.0,
  "price": 150.25,
  "market_value": 15025.0,
  "timestamp": "2026-02-01T03:24:00Z",
  "currency": "USD",
  "position_type": "LONG"
}
```

## Database Schema

The library expects a table with the following schema:

```sql
CREATE TABLE FinancialPositions (
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
    snapshot_time DATETIME2 NOT NULL DEFAULT GETDATE(),
    INDEX idx_snapshot_time (snapshot_time),
    INDEX idx_position_id (position_id),
    INDEX idx_account_id (account_id)
);
```

## Testing

Run the test suite:

```bash
# Run all tests
pytest tests/

# Run with coverage
pytest tests/ --cov=financial_snapshot --cov-report=html

# Run specific test module
pytest tests/test_kafka_consumer.py -v
```

## Performance Considerations

- **Batch Size**: Adjust `batch_size` based on message rate (default: 5000)
- **Connection Pool**: Configure pool size based on workload (default: 5)
- **Commit Interval**: Balance between performance and data loss risk (default: 1000)
- **Snapshot Interval**: Default 5 minutes, configurable based on requirements

## Error Handling

The library implements comprehensive error handling:

- **Connection Failures**: Automatic retry with exponential backoff
- **Kafka Errors**: Proper offset management and reprocessing
- **SQL Errors**: Transaction rollback and error logging
- **Data Validation**: Pydantic validation with detailed error messages

## Monitoring

Key metrics logged for monitoring:

- Messages consumed per snapshot cycle
- Batch write durations
- SQL connection pool utilization
- Consumer lag
- Error rates and types

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Run the test suite
5. Submit a pull request

## License

MIT License - See LICENSE file for details

## Support

For issues and questions, please open an issue on GitHub.
