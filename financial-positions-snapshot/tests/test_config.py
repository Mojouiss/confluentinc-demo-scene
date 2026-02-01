"""Unit tests for configuration module."""

import os
import tempfile
from pathlib import Path

import pytest
import yaml

from financial_snapshot.config import (
    Config,
    KafkaConfig,
    SQLServerConfig,
    SnapshotConfig,
    LoggingConfig,
    load_config,
    get_default_config,
)


class TestKafkaConfig:
    """Tests for KafkaConfig."""
    
    def test_default_values(self):
        """Test default configuration values."""
        config = KafkaConfig()
        
        assert config.bootstrap_servers == "localhost:9092"
        assert config.topic == "financial-positions"
        assert config.group_id == "financial-snapshot-consumer"
        assert config.auto_offset_reset == "earliest"
        assert config.enable_auto_commit is False
    
    def test_custom_values(self):
        """Test custom configuration values."""
        config = KafkaConfig(
            bootstrap_servers="kafka1:9092,kafka2:9092",
            topic="custom-topic",
            group_id="custom-group"
        )
        
        assert config.bootstrap_servers == "kafka1:9092,kafka2:9092"
        assert config.topic == "custom-topic"
        assert config.group_id == "custom-group"
    
    def test_bootstrap_servers_validation(self):
        """Test bootstrap servers validation."""
        with pytest.raises(ValueError, match="cannot be empty"):
            KafkaConfig(bootstrap_servers="")
    
    def test_environment_variables(self):
        """Test loading from environment variables."""
        os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "env-kafka:9092"
        os.environ["KAFKA_TOPIC"] = "env-topic"
        
        config = KafkaConfig()
        
        assert config.bootstrap_servers == "env-kafka:9092"
        assert config.topic == "env-topic"
        
        # Cleanup
        del os.environ["KAFKA_BOOTSTRAP_SERVERS"]
        del os.environ["KAFKA_TOPIC"]


class TestSQLServerConfig:
    """Tests for SQLServerConfig."""
    
    def test_default_values(self):
        """Test default configuration values."""
        config = SQLServerConfig()
        
        assert config.server == "localhost"
        assert config.database == "FinanceDB"
        assert config.port == 1433
        assert config.connection_pool_size == 5
        assert config.batch_size == 1000
    
    def test_connection_string_generation(self):
        """Test ODBC connection string generation."""
        config = SQLServerConfig(
            server="testserver",
            database="testdb",
            username="testuser",
            password="testpass",
            port=1433
        )
        
        conn_str = config.get_connection_string()
        
        assert "DRIVER={ODBC Driver 17 for SQL Server}" in conn_str
        assert "SERVER=testserver,1433" in conn_str
        assert "DATABASE=testdb" in conn_str
        assert "UID=testuser" in conn_str
        assert "PWD=testpass" in conn_str
    
    def test_connection_pool_size_validation(self):
        """Test connection pool size validation."""
        # Valid range
        config = SQLServerConfig(connection_pool_size=10)
        assert config.connection_pool_size == 10
        
        # Test boundaries
        with pytest.raises(ValueError):
            SQLServerConfig(connection_pool_size=0)
        
        with pytest.raises(ValueError):
            SQLServerConfig(connection_pool_size=51)


class TestSnapshotConfig:
    """Tests for SnapshotConfig."""
    
    def test_default_values(self):
        """Test default configuration values."""
        config = SnapshotConfig()
        
        assert config.interval_seconds == 300  # 5 minutes
        assert config.batch_size == 5000
        assert config.commit_interval == 1000
        assert config.max_retries == 3
    
    def test_custom_values(self):
        """Test custom configuration values."""
        config = SnapshotConfig(
            interval_seconds=600,
            batch_size=10000,
            max_retries=5
        )
        
        assert config.interval_seconds == 600
        assert config.batch_size == 10000
        assert config.max_retries == 5
    
    def test_validation(self):
        """Test configuration validation."""
        # Interval must be positive
        with pytest.raises(ValueError):
            SnapshotConfig(interval_seconds=0)
        
        # Batch size must be positive
        with pytest.raises(ValueError):
            SnapshotConfig(batch_size=0)


class TestLoggingConfig:
    """Tests for LoggingConfig."""
    
    def test_default_values(self):
        """Test default configuration values."""
        config = LoggingConfig()
        
        assert config.level == "INFO"
        assert config.format == "json"
        assert config.log_file is None
    
    def test_log_level_validation(self):
        """Test log level validation."""
        # Valid levels
        for level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            config = LoggingConfig(level=level)
            assert config.level == level
        
        # Invalid level
        with pytest.raises(ValueError, match="must be one of"):
            LoggingConfig(level="INVALID")
    
    def test_case_insensitive_level(self):
        """Test case-insensitive log level."""
        config = LoggingConfig(level="debug")
        assert config.level == "DEBUG"


class TestConfig:
    """Tests for main Config class."""
    
    def test_default_config(self):
        """Test default configuration."""
        config = Config()
        
        assert isinstance(config.kafka, KafkaConfig)
        assert isinstance(config.sql_server, SQLServerConfig)
        assert isinstance(config.snapshot, SnapshotConfig)
        assert isinstance(config.logging, LoggingConfig)
    
    def test_nested_config(self):
        """Test nested configuration objects."""
        config = Config(
            kafka=KafkaConfig(bootstrap_servers="test:9092"),
            sql_server=SQLServerConfig(server="testdb"),
        )
        
        assert config.kafka.bootstrap_servers == "test:9092"
        assert config.sql_server.server == "testdb"


class TestLoadConfig:
    """Tests for config loading functions."""
    
    def test_load_from_yaml_file(self):
        """Test loading configuration from YAML file."""
        config_data = {
            "kafka": {
                "bootstrap_servers": "yaml-kafka:9092",
                "topic": "yaml-topic",
            },
            "sql_server": {
                "server": "yaml-server",
                "database": "yaml-db",
            },
            "snapshot": {
                "interval_seconds": 600,
            },
            "logging": {
                "level": "DEBUG",
            },
        }
        
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.yaml', delete=False
        ) as f:
            yaml.dump(config_data, f)
            config_file = f.name
        
        try:
            config = load_config(config_file)
            
            assert config.kafka.bootstrap_servers == "yaml-kafka:9092"
            assert config.kafka.topic == "yaml-topic"
            assert config.sql_server.server == "yaml-server"
            assert config.sql_server.database == "yaml-db"
            assert config.snapshot.interval_seconds == 600
            assert config.logging.level == "DEBUG"
        finally:
            Path(config_file).unlink()
    
    def test_load_nonexistent_file(self):
        """Test loading non-existent configuration file."""
        with pytest.raises(FileNotFoundError):
            load_config("/nonexistent/config.yaml")
    
    def test_get_default_config(self):
        """Test getting default configuration."""
        config = get_default_config()
        
        assert isinstance(config, Config)
        assert isinstance(config.kafka, KafkaConfig)
        assert isinstance(config.sql_server, SQLServerConfig)
