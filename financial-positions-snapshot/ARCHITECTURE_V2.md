# Financial Positions Snapshot Library - Architecture Update

## New Architecture Overview

The library has been refactored to support:

1. **Transactional Processing**: Data and Kafka offsets written in a single SQL transaction
2. **Multiple Consumers**: Parallel consumers (1 per partition) for higher throughput
3. **Continuous Buffering**: Messages buffered continuously, snapshots taken at intervals
4. **Message Handler Abstraction**: Pluggable handlers for different message formats
5. **SQL-Based Offset Management**: Offsets stored in SQL Server for recovery

## Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│                   Multiple Kafka Consumers                   │
│              (Continuously consuming in parallel)            │
└───────────────┬────────────────┬────────────────┬───────────┘
                │                │                │
                └────────────────┼────────────────┘
                                 │
                        ┌────────▼────────┐
                        │  Message Buffer │
                        │  (Thread-safe)  │
                        └────────┬────────┘
                                 │
                    ┌────────────▼────────────┐
                    │  Scheduler (Every 5min) │
                    └────────────┬────────────┘
                                 │
                        ┌────────▼────────┐
                        │ Drain Snapshot  │
                        └────────┬────────┘
                                 │
              ┌──────────────────┼──────────────────┐
              │                  │                  │
     ┌────────▼────────┐  ┌──────▼──────┐  ┌──────▼──────┐
     │ Write Data      │  │Write Offsets│  │   Commit    │
     │ to SQL Tables   │  │to KafkaOffsets│ │ (Single TX) │
     └────────┬────────┘  └──────┬──────┘  └──────┬──────┘
              └──────────────────┼──────────────────┘
                                 │
                        ┌────────▼────────┐
                        │ Commit Offsets  │
                        │   to Kafka      │
                        │ (Only on success)│
                        └─────────────────┘
```

## Key Components

### 1. Message Handlers (`handlers.py`)

Abstract handler system for different message formats:

**AbcMsgHandler**: Abstract base class defining the handler interface
- `parse_message()`: Parse and validate raw bytes
- `get_table_name()`: Get target SQL table
- `get_model_class()`: Get Pydantic model

**BaseMsgHandler**: Generic JSON message handler
- Parses JSON messages
- Validates with Pydantic models
- Supports key-to-table mapping

**AvroMsgHandler**: Avro schema registry handler
- Integrates with Confluent Schema Registry
- Deserializes Avro messages
- Falls back to JSON if needed

**FinancialPositionHandler**: Specialized handler for financial positions

### 2. Offset Manager (`offset_manager.py`)

Manages Kafka offsets in SQL Server:

**KafkaOffsets Table Schema**:
```sql
CREATE TABLE KafkaOffsets (
    topic VARCHAR(255) NOT NULL,
    partition INT NOT NULL,
    consumer_group VARCHAR(255) NOT NULL,
    offset_value BIGINT NOT NULL,
    updated_at DATETIME2 NOT NULL DEFAULT GETDATE(),
    PRIMARY KEY (topic, partition, consumer_group)
);
```

**Features**:
- Transactional offset saves
- Load offsets on startup
- Per-topic, per-partition, per-consumer-group tracking

### 3. Message Buffer (`message_buffer.py`)

Thread-safe buffer for continuous consumption:

**Features**:
- Concurrent access from multiple consumers
- Configurable max size (default: 100K messages)
- Snapshot draining (atomic operation)
- Statistics tracking

### 4. Multi-Consumer Manager (`multi_consumer.py`)

Manages multiple parallel consumers:

**Features**:
- Configurable number of consumers
- Each consumer runs in its own thread
- Shared message buffer
- Continuous background consumption
- Integrated validation and error handling

### 5. Transactional Writer (`transactional_writer.py`)

Atomic writes of data and offsets:

**Features**:
- Single transaction for data + offsets
- Automatic table creation
- Support for multiple target tables
- Connection pooling
- Batch inserts

### 6. Main Service (`service.py`)

Orchestrates all components:

**Lifecycle**:
1. Initialize buffer, writer, consumers
2. Load initial offsets from SQL (if configured)
3. Start consumers (continuous background consumption)
4. Start scheduler
5. On each snapshot interval:
   - Drain buffer
   - Write to SQL in transaction
   - Commit to Kafka on success
6. Graceful shutdown with final snapshot

## Configuration

### Topics and Consumers

```yaml
kafka:
  topics:  # List of topics
    - "financial-positions"
    - "trading-positions"
  num_consumers: 3  # Number of parallel consumers (1 per partition)
  load_offsets_from_sql: true  # Load offsets from SQL on startup
```

### Message Handlers

```yaml
handler:
  default_handler_type: "financial_position"
  
  # Map message keys to handler types
  key_to_handler:
    "com.example.Position": "avro"
    "com.example.Trade": "base"
  
  # Map message keys to SQL tables
  key_to_table:
    "com.example.Position": "FinanceDB.dbo.Positions"
  
  # Map topics to default tables
  topic_to_table:
    "financial-positions": "FinanceDB.dbo.FinancialPositions"
```

### Snapshot Configuration

```yaml
snapshot:
  interval_seconds: 300  # Snapshot every 5 minutes
  buffer_size: 100000   # Max messages to buffer
```

## Exactly-Once Semantics

The new architecture provides exactly-once semantics:

1. **Idempotent Writes**: Same offset range produces same result
2. **Transactional**: Data + offsets in single SQL transaction
3. **Conditional Commit**: Kafka commit only after SQL success
4. **Recovery**: Load offsets from SQL on startup

### Failure Scenarios

**Consumer Crash**:
- Consumers restart
- Load offsets from SQL
- Resume from last committed offset
- Buffered (uncommitted) messages are re-consumed

**SQL Transaction Failure**:
- Transaction rolled back
- Offsets NOT committed to Kafka
- Messages remain in buffer
- Retry on next snapshot cycle

**Kafka Commit Failure**:
- Offsets saved in SQL
- Kafka commit fails (non-fatal)
- On restart, load from SQL
- May see duplicate messages in Kafka (but SQL has offsets)

## Performance Tuning

### Multiple Consumers

Increase `num_consumers` to match partition count:
```yaml
kafka:
  num_consumers: 8  # For 8 partitions
```

### Buffer Size

Adjust based on message rate:
```yaml
snapshot:
  buffer_size: 200000  # For high-volume topics
```

### Snapshot Interval

Balance between latency and throughput:
```yaml
snapshot:
  interval_seconds: 60  # More frequent snapshots (lower latency)
  interval_seconds: 600  # Less frequent (higher throughput)
```

### Connection Pool

Increase for higher write throughput:
```yaml
sql_server:
  connection_pool_size: 10  # More concurrent connections
```

## Migration from Old Version

If migrating from the previous version:

1. **Configuration Changes**:
   - `kafka.topic` → `kafka.topics` (now a list)
   - Added `kafka.num_consumers`
   - Added `kafka.load_offsets_from_sql`
   - Added `kafka.offsets_table`
   - Added `handler` section
   - `snapshot.batch_size` → `snapshot.buffer_size`

2. **Database Changes**:
   - New `KafkaOffsets` table will be created automatically
   - Existing data tables remain unchanged

3. **Behavior Changes**:
   - Consumers run continuously (not per-snapshot)
   - Multiple consumers supported
   - Offsets stored in SQL (not just Kafka)
   - Single transaction for data + offsets

## Example Usage

```python
from financial_snapshot import FinancialSnapshotService, load_config

# Load configuration
config = load_config("config.yaml")

# Create and start service
service = FinancialSnapshotService(config)
service.start()  # Blocks until shutdown
```

## Monitoring

Check service statistics:

```python
stats = service.get_statistics()
print(f"Total snapshots: {stats['service']['total_snapshots']}")
print(f"Total records: {stats['service']['total_records_processed']}")
print(f"Buffer size: {stats['buffer']['total_messages']}")
print(f"Consumers: {stats['consumers']['num_consumers']}")
```

## Troubleshooting

### High Consumer Lag

- Increase `num_consumers`
- Increase `buffer_size`
- Decrease `snapshot_interval_seconds`

### Buffer Full

- Increase `buffer_size`
- Decrease `snapshot_interval_seconds`
- Check SQL write performance

### Slow Snapshots

- Increase SQL `connection_pool_size`
- Check SQL `batch_size`
- Optimize SQL indexes
- Check network latency

## See Also

- [ARCHITECTURE.md](ARCHITECTURE.md) - Detailed architecture documentation
- [DEVELOPMENT.md](DEVELOPMENT.md) - Development guide
- [config.example.yaml](config.example.yaml) - Example configuration
