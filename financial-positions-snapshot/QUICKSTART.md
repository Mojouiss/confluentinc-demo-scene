# Financial Positions Snapshot Service

## Quick Start with Docker

### Using Docker Compose (Recommended)

1. Create a `.env` file with your configuration:

```bash
cp .env.example .env
# Edit .env with your settings
```

2. Start all services:

```bash
docker-compose up -d
```

3. View logs:

```bash
docker-compose logs -f financial-snapshot
```

4. Stop services:

```bash
docker-compose down
```

### Using Docker Only

Build the image:

```bash
docker build -t financial-snapshot:latest .
```

Run the container:

```bash
docker run -d \
  --name financial-snapshot \
  -e KAFKA_BOOTSTRAP_SERVERS=your-kafka:9092 \
  -e SQL_SERVER=your-sqlserver \
  -e SQL_PASSWORD=YourPassword \
  financial-snapshot:latest
```

## Development Setup

See [DEVELOPMENT.md](DEVELOPMENT.md) for detailed development instructions.

## Configuration

All configuration can be provided via:
1. Environment variables (recommended for Docker)
2. YAML configuration file
3. `.env` file

See `config.example.yaml` and `.env.example` for all options.

## Testing

Run the test suite:

```bash
pytest tests/ -v --cov=financial_snapshot
```

## Architecture

```
Kafka Topic → Consumer → Validator → Batch Writer → SQL Server
                ↓                        ↓
            Scheduler ← ← ← ← ← ← ← Commit Offsets
```

## Features

✅ **Zero Data Loss** - Manual offset commits after successful writes  
✅ **High Performance** - Connection pooling and batch processing  
✅ **Type Safety** - Full Pydantic validation  
✅ **Production Ready** - Comprehensive logging and error handling  
✅ **Well Tested** - 79 tests with 69% coverage  
✅ **Docker Support** - Ready for containerized deployment  

## Documentation

- [README.md](README.md) - Full documentation
- [DEVELOPMENT.md](DEVELOPMENT.md) - Development guide
- [examples.py](examples.py) - Usage examples

## License

MIT License - See [LICENSE](LICENSE) for details
