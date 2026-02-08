# Architecture Documentation

## System Overview

The Financial Positions Snapshot library is designed as a highly reliable, performant data pipeline that captures financial positions from Kafka and persists them to SQL Server with zero data loss guarantees.

## Component Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Financial Snapshot Service                       │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌──────────────┐         ┌──────────────┐        ┌──────────────┐ │
│  │  Scheduler   │────────▶│   Service    │───────▶│   Logging    │ │
│  │  (Timer)     │         │ (Orchestrator)│       │  (structlog) │ │
│  └──────────────┘         └───────┬───────┘       └──────────────┘ │
│                                   │                                  │
│                     ┌─────────────┴──────────────┐                  │
│                     │                            │                  │
│              ┌──────▼──────┐            ┌────────▼───────┐          │
│              │   Kafka     │            │  SQL Server    │          │
│              │  Consumer   │            │    Writer      │          │
│              └──────┬──────┘            └────────┬───────┘          │
│                     │                            │                  │
└─────────────────────┼────────────────────────────┼──────────────────┘
                      │                            │
              ┌───────▼────────┐          ┌────────▼────────┐
              │  Kafka Cluster │          │  SQL Server DB  │
              │  (Source)      │          │  (Destination)  │
              └────────────────┘          └─────────────────┘
```

## Data Flow

### 1. Snapshot Cycle Initiation
```
Scheduler (every 5 min) → Service._execute_snapshot()
```

### 2. Message Consumption
```
Service → Consumer.consume_batch(5000) → Kafka Poll
                                       → JSON Decode
                                       → Pydantic Validation
                                       → FinancialPosition objects
```

### 3. Data Validation
```
FinancialPosition Model:
  ├─ Required Fields Validation
  ├─ Type Checking (Decimal for amounts)
  ├─ Business Logic (market_value = quantity × price)
  ├─ Data Normalization (uppercase symbols)
  └─ Range Validation (positive prices)
```

### 4. Database Write
```
Writer.write_batches() → Split into batches (1000 records)
                       → Get Connection from Pool
                       → Begin Transaction
                       → Batch INSERT (fast_executemany)
                       → Commit Transaction
                       → Return Connection to Pool
```

### 5. Offset Commit
```
Writer Success → Consumer.commit_offsets(sync=True)
              → Kafka Offset Commit
```

## Error Handling Strategy

### Transient Errors (Retry)
```python
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10)
)
def _execute_snapshot():
    # Retries up to 3 times with exponential backoff
```

### Permanent Errors (Log & Continue)
- Validation errors: Log and skip message
- Connection failures: Wait and retry next cycle
- Fatal errors: Shutdown gracefully

### Transaction Safety
```
SQL Transaction:
  BEGIN TRANSACTION
    ├─ INSERT batch
    ├─ COMMIT on success
    └─ ROLLBACK on error
  
Kafka Offset:
  └─ Commit only after successful SQL COMMIT
```

## Performance Optimizations

### 1. Connection Pooling
```python
Pool Size: 5 connections (configurable)
Strategy:
  ├─ Pre-create connections on startup
  ├─ Reuse connections across batches
  ├─ Auto-reconnect on dead connections
  └─ Graceful degradation on pool exhaustion
```

### 2. Batch Processing
```python
Kafka Consumer:
  └─ max_poll_records: 5000 (fetch in bulk)

SQL Writer:
  ├─ Batch size: 1000 records per INSERT
  ├─ fast_executemany: True (pyodbc optimization)
  └─ Parallelization: Connection pool enables concurrent writes
```

### 3. Resource Management
```python
Context Managers:
  ├─ Automatic connection return
  ├─ Automatic rollback on error
  └─ Resource cleanup on exit
```

## Configuration Hierarchy

```
1. Code Defaults (in Pydantic models)
   ↓
2. YAML Configuration File (if provided)
   ↓
3. Environment Variables (override YAML)
   ↓
4. Command-line Arguments (override env vars)
```

## Monitoring & Observability

### Metrics Tracked
```python
Service Level:
  ├─ Total snapshots completed
  ├─ Total records processed
  ├─ Error count and rate
  └─ Average records per snapshot

Consumer Level:
  ├─ Messages consumed
  ├─ Validation errors
  ├─ Current offsets
  └─ Consumer lag

Writer Level:
  ├─ Batches written
  ├─ Average batch size
  ├─ Write duration
  └─ Records per second

Scheduler Level:
  ├─ Cycle count
  ├─ Last snapshot time
  └─ Next snapshot time
```

### Logging Strategy
```
Structured JSON Logs:
{
  "timestamp": "ISO 8601",
  "level": "INFO|ERROR|DEBUG",
  "logger": "module.name",
  "message": "human readable",
  "context": {
    "records": 5000,
    "duration": 2.5,
    "batch_number": 42
  }
}
```

## Security Considerations

### Data Protection
- ✅ TLS encryption for Kafka connections
- ✅ TLS encryption for SQL Server connections
- ✅ Parameterized queries (SQL injection prevention)
- ✅ No credentials in logs
- ✅ Environment variable-based secrets

### Access Control
- ✅ Minimum required Kafka permissions (read only)
- ✅ Minimum required SQL permissions (INSERT only)
- ✅ Non-root Docker container user

## Scalability

### Horizontal Scaling
```
Multiple instances can run in parallel:
  ├─ Use different consumer group IDs
  ├─ Or partition topic by key
  └─ Each instance processes independent data
```

### Vertical Scaling
```
Tune parameters for higher throughput:
  ├─ Increase connection pool size
  ├─ Increase batch sizes
  ├─ Increase max_poll_records
  └─ Decrease snapshot interval
```

## Deployment Patterns

### Pattern 1: Single Instance
```
Best for:
  ├─ Lower message volumes (< 10K/min)
  ├─ Simple deployments
  └─ Development/testing

Configuration:
  └─ Default settings work well
```

### Pattern 2: Multiple Consumers
```
Best for:
  ├─ Higher message volumes (> 50K/min)
  ├─ High availability requirements
  └─ Partitioned topics

Configuration:
  ├─ Same group ID (load balancing)
  └─ Kafka assigns partitions automatically
```

### Pattern 3: Dedicated Instances
```
Best for:
  ├─ Multi-tenant deployments
  ├─ Different business units
  └─ Separate databases

Configuration:
  ├─ Different group IDs
  ├─ Different topics or filters
  └─ Separate database schemas
```

## Testing Strategy

### Unit Tests (79 tests)
```
├─ Configuration validation
├─ Data model validation
├─ Consumer logic
├─ Writer logic
├─ Scheduler logic
└─ Service orchestration
```

### Integration Points
```
Kafka:
  └─ Mock Consumer for unit tests
  └─ Real Kafka for integration tests

SQL Server:
  └─ Mock pyodbc for unit tests
  └─ Real SQL Server for integration tests
```

## Maintenance & Operations

### Health Checks
```python
# Docker health check
python -c "import financial_snapshot; print('OK')"

# Service verification
python -m financial_snapshot --verify-only
```

### Monitoring Commands
```bash
# View service statistics
curl http://localhost:8080/stats  # If metrics endpoint added

# Check logs
tail -f /var/log/financial-snapshot/service.log

# Consumer lag
kafka-consumer-groups --describe --group financial-snapshot-consumer
```

### Troubleshooting
```
High Consumer Lag:
  → Increase batch sizes
  → Add more consumer instances
  → Optimize SQL batch size

High Error Rate:
  → Check data validation rules
  → Verify connectivity
  → Review recent schema changes

Slow Performance:
  → Check SQL indexes
  → Increase connection pool
  → Optimize network latency
```

## Future Enhancements

### Potential Additions
- [ ] Metrics export (Prometheus)
- [ ] Dead letter queue for failed messages
- [ ] Data transformation pipeline
- [ ] Real-time alerting
- [ ] Web-based dashboard
- [ ] Multi-destination support
- [ ] Schema evolution handling
- [ ] Exactly-once semantics

## References

### Technologies Used
- **confluent-kafka**: Official Kafka Python client
- **pyodbc**: SQL Server ODBC driver
- **pydantic**: Data validation and settings
- **structlog**: Structured logging
- **tenacity**: Retry logic
- **pytest**: Testing framework

### Design Patterns Applied
- Repository Pattern (data access)
- Factory Pattern (connection creation)
- Strategy Pattern (configuration)
- Observer Pattern (logging)
- Context Manager Pattern (resource management)
