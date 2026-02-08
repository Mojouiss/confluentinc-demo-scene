# Development and Testing Guide

## Project Overview

The Financial Positions Snapshot library is a production-ready Python solution for capturing live financial positions from Kafka topics and persisting them to SQL Server with zero data loss and optimal performance.

## Architecture Highlights

### Modular Design
- **config.py**: Pydantic-based configuration with validation
- **models.py**: Type-safe data models with automatic validation
- **kafka_consumer.py**: Robust Kafka consumer with error handling
- **sql_writer.py**: Connection pooling and batch writing
- **scheduler.py**: Time-based snapshot orchestration
- **service.py**: Main service coordinator with retry logic

### Key Features Implemented

1. **Zero Data Loss**
   - Manual offset commit after successful writes
   - Transaction-based SQL writes with rollback
   - Comprehensive error handling and retry logic

2. **Performance Optimization**
   - Connection pooling (configurable pool size)
   - Batch processing (configurable batch sizes)
   - Fast batch insert mode for SQL Server
   - Efficient Kafka consumer configuration

3. **Type Safety**
   - Full Pydantic validation on all data
   - Decimal precision for financial calculations
   - Automatic data normalization

4. **Production Ready**
   - Graceful shutdown handling
   - Comprehensive logging with structlog
   - Configurable via YAML or environment variables
   - CLI interface with verification mode

## Testing

### Test Coverage

```
Total: 714 statements
Tested: 494 statements (69% coverage)
Test Files: 6 modules
Total Tests: 79 (all passing)
```

### Running Tests

```bash
# All tests
pytest tests/ -v

# With coverage
pytest tests/ --cov=financial_snapshot --cov-report=html

# Specific module
pytest tests/test_config.py -v

# Fast tests only
pytest tests/ -m "not slow"
```

## Installation

### Development Setup

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in development mode
pip install -e .

# Install development dependencies
pip install -r requirements.txt
```

### Production Setup

```bash
# Install from source
pip install .

# Or install specific dependencies
pip install confluent-kafka pyodbc pydantic pydantic-settings PyYAML tenacity structlog
```

## Configuration

### Environment Variables

The library supports environment-based configuration:

```bash
# Kafka
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export KAFKA_TOPIC="financial-positions"
export KAFKA_GROUP_ID="financial-snapshot-consumer"

# SQL Server
export SQL_SERVER="localhost"
export SQL_DATABASE="FinanceDB"
export SQL_USERNAME="sa"
export SQL_PASSWORD="password"

# Snapshot
export SNAPSHOT_INTERVAL_SECONDS=300
export SNAPSHOT_BATCH_SIZE=5000

# Logging
export LOG_LEVEL="INFO"
export LOG_FORMAT="json"
```

### YAML Configuration

See `config.example.yaml` for a complete configuration template.

## Running the Service

### Basic Usage

```bash
# With config file
python -m financial_snapshot --config config.yaml

# With environment variables
python -m financial_snapshot

# Verify configuration only
python -m financial_snapshot --config config.yaml --verify-only
```

### Docker Deployment

```bash
# Build image
docker build -t financial-snapshot:latest .

# Run container
docker run -d \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e SQL_SERVER=sqlserver \
  -e SQL_PASSWORD=password \
  financial-snapshot:latest
```

## Performance Tuning

### Kafka Consumer

- `max_poll_records`: Number of records per poll (default: 5000)
- `session_timeout_ms`: Session timeout (default: 30000)
- Increase for higher throughput, decrease for lower latency

### SQL Server

- `connection_pool_size`: Connection pool size (default: 5)
- `batch_size`: Records per batch insert (default: 1000)
- Increase pool for more concurrent writes
- Increase batch size for higher throughput

### Snapshot

- `interval_seconds`: Time between snapshots (default: 300)
- `batch_size`: Messages per snapshot cycle (default: 5000)
- Decrease interval for more frequent snapshots
- Increase batch size to process more messages per cycle

## Monitoring

### Metrics

The service exposes statistics via the `get_statistics()` method:

- Total snapshots completed
- Total records processed
- Error counts and rates
- Consumer lag and offsets
- SQL write performance
- Batch sizes and durations

### Logging

Structured logging with configurable formats:

```json
{
  "timestamp": "2026-02-01T12:00:00Z",
  "level": "INFO",
  "logger": "financial_snapshot.service",
  "message": "Snapshot completed",
  "records_processed": 5000,
  "duration_seconds": 2.5
}
```

## Troubleshooting

### Connection Issues

```bash
# Test Kafka connectivity
python -m financial_snapshot --verify-only --config config.yaml
```

### Data Validation Errors

Check logs for validation failures:
- Incorrect data types
- Missing required fields
- Invalid market value calculations

### Performance Issues

- Monitor consumer lag
- Check SQL connection pool utilization
- Review batch sizes
- Enable DEBUG logging for detailed metrics

## Security Considerations

1. **Credentials**: Never commit passwords to version control
2. **SQL Injection**: Uses parameterized queries
3. **TLS**: Enable encryption for Kafka and SQL Server
4. **Least Privilege**: Use minimal database permissions

## Contributing

1. Follow PEP 8 style guide
2. Add tests for new features
3. Update documentation
4. Run test suite before committing
5. Use meaningful commit messages

## License

MIT License - See LICENSE file for details
