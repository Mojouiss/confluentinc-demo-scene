"""Configuration module for Financial Positions Snapshot library."""

import os
from pathlib import Path
from typing import Optional, Dict, Any

import yaml
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaConfig(BaseSettings):
    """Kafka consumer configuration."""
    
    bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Kafka bootstrap servers"
    )
    topic: str = Field(
        default="financial-positions",
        description="Kafka topic to consume from"
    )
    group_id: str = Field(
        default="financial-snapshot-consumer",
        description="Consumer group ID"
    )
    auto_offset_reset: str = Field(
        default="earliest",
        description="Auto offset reset policy"
    )
    enable_auto_commit: bool = Field(
        default=False,
        description="Whether to auto-commit offsets"
    )
    session_timeout_ms: int = Field(
        default=30000,
        description="Session timeout in milliseconds"
    )
    max_poll_records: int = Field(
        default=5000,
        description="Maximum records per poll"
    )
    
    model_config = SettingsConfigDict(env_prefix='KAFKA_')
    
    @field_validator('bootstrap_servers')
    @classmethod
    def validate_bootstrap_servers(cls, v: str) -> str:
        """Validate bootstrap servers format."""
        if not v or not v.strip():
            raise ValueError("Bootstrap servers cannot be empty")
        return v.strip()


class SQLServerConfig(BaseSettings):
    """SQL Server database configuration."""
    
    server: str = Field(
        default="localhost",
        description="SQL Server hostname"
    )
    database: str = Field(
        default="FinanceDB",
        description="Database name"
    )
    username: str = Field(
        default="sa",
        description="Database username"
    )
    password: str = Field(
        default="",
        description="Database password"
    )
    driver: str = Field(
        default="ODBC Driver 17 for SQL Server",
        description="ODBC driver name"
    )
    port: int = Field(
        default=1433,
        description="SQL Server port"
    )
    connection_pool_size: int = Field(
        default=5,
        ge=1,
        le=50,
        description="Connection pool size"
    )
    batch_size: int = Field(
        default=1000,
        ge=1,
        le=10000,
        description="Batch insert size"
    )
    connection_timeout: int = Field(
        default=30,
        description="Connection timeout in seconds"
    )
    table_name: str = Field(
        default="FinancialPositions",
        description="Target table name"
    )
    
    model_config = SettingsConfigDict(env_prefix='SQL_')
    
    def get_connection_string(self) -> str:
        """Generate ODBC connection string."""
        return (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.server},{self.port};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout={self.connection_timeout};"
        )


class SnapshotConfig(BaseSettings):
    """Snapshot process configuration."""
    
    interval_seconds: int = Field(
        default=300,  # 5 minutes
        ge=1,
        description="Snapshot interval in seconds"
    )
    batch_size: int = Field(
        default=5000,
        ge=1,
        description="Number of messages to process per batch"
    )
    commit_interval: int = Field(
        default=1000,
        ge=1,
        description="Commit offsets every N messages"
    )
    max_retries: int = Field(
        default=3,
        ge=0,
        description="Maximum retry attempts on failure"
    )
    retry_backoff_seconds: int = Field(
        default=5,
        ge=1,
        description="Seconds to wait between retries"
    )
    
    model_config = SettingsConfigDict(env_prefix='SNAPSHOT_')


class LoggingConfig(BaseSettings):
    """Logging configuration."""
    
    level: str = Field(
        default="INFO",
        description="Log level"
    )
    format: str = Field(
        default="json",
        description="Log format: json or text"
    )
    log_file: Optional[str] = Field(
        default=None,
        description="Optional log file path"
    )
    
    model_config = SettingsConfigDict(env_prefix='LOG_')
    
    @field_validator('level')
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level."""
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        v_upper = v.upper()
        if v_upper not in valid_levels:
            raise ValueError(f"Log level must be one of: {valid_levels}")
        return v_upper


class Config(BaseSettings):
    """Main configuration class."""
    
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    sql_server: SQLServerConfig = Field(default_factory=SQLServerConfig)
    snapshot: SnapshotConfig = Field(default_factory=SnapshotConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        env_nested_delimiter='__'
    )


def load_config(config_path: Optional[str] = None) -> Config:
    """
    Load configuration from file or environment variables.
    
    Args:
        config_path: Optional path to YAML configuration file
        
    Returns:
        Config: Configuration object
        
    Raises:
        FileNotFoundError: If config file not found
        ValueError: If configuration is invalid
    """
    if config_path:
        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_file, 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)
        
        # Create nested config objects
        return Config(
            kafka=KafkaConfig(**config_data.get('kafka', {})),
            sql_server=SQLServerConfig(**config_data.get('sql_server', {})),
            snapshot=SnapshotConfig(**config_data.get('snapshot', {})),
            logging=LoggingConfig(**config_data.get('logging', {}))
        )
    
    # Load from environment variables
    return Config()


def get_default_config() -> Config:
    """
    Get default configuration.
    
    Returns:
        Config: Default configuration object
    """
    return Config()
