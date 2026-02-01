"""Unit tests for Kafka consumer module."""

import json
from datetime import datetime
from decimal import Decimal
from unittest.mock import Mock, MagicMock, patch

import pytest
from confluent_kafka import KafkaError

from financial_snapshot.config import KafkaConfig
from financial_snapshot.kafka_consumer import (
    FinancialPositionConsumer,
    KafkaConsumerError,
    MessageValidationError,
)
from financial_snapshot.models import FinancialPosition


@pytest.fixture
def kafka_config():
    """Fixture for Kafka configuration."""
    return KafkaConfig(
        bootstrap_servers="test:9092",
        topic="test-topic",
        group_id="test-group",
    )


@pytest.fixture
def mock_consumer():
    """Fixture for mock Kafka consumer."""
    with patch('financial_snapshot.kafka_consumer.Consumer') as mock:
        yield mock


@pytest.fixture
def sample_position_data():
    """Fixture for sample position data."""
    return {
        "position_id": "POS-123",
        "account_id": "ACC-456",
        "symbol": "AAPL",
        "quantity": "100.0",
        "price": "150.25",
        "market_value": "15025.0",
        "timestamp": "2026-02-01T12:00:00",
        "currency": "USD",
        "position_type": "LONG",
    }


class TestFinancialPositionConsumer:
    """Tests for FinancialPositionConsumer."""
    
    def test_initialization(self, kafka_config):
        """Test consumer initialization."""
        consumer = FinancialPositionConsumer(kafka_config)
        
        assert consumer.config == kafka_config
        assert consumer._consumer is None
        assert consumer._message_count == 0
        assert consumer._error_count == 0
    
    def test_create_consumer(self, kafka_config, mock_consumer):
        """Test consumer creation."""
        consumer = FinancialPositionConsumer(kafka_config)
        kafka_consumer = consumer._create_consumer()
        
        assert kafka_consumer is not None
        mock_consumer.assert_called_once()
        
        # Verify consumer config
        call_args = mock_consumer.call_args[0][0]
        assert call_args['bootstrap.servers'] == "test:9092"
        assert call_args['group.id'] == "test-group"
        assert call_args['auto.offset.reset'] == "earliest"
    
    def test_connect(self, kafka_config, mock_consumer):
        """Test connecting to Kafka."""
        consumer = FinancialPositionConsumer(kafka_config)
        
        mock_kafka = MagicMock()
        mock_consumer.return_value = mock_kafka
        
        consumer.connect()
        
        assert consumer._consumer is not None
        mock_kafka.subscribe.assert_called_once_with(["test-topic"])
    
    def test_connect_failure(self, kafka_config, mock_consumer):
        """Test connection failure handling."""
        from confluent_kafka import KafkaException
        
        mock_consumer.side_effect = KafkaException("Connection failed")
        
        consumer = FinancialPositionConsumer(kafka_config)
        
        with pytest.raises(KafkaConsumerError):
            consumer.connect()
    
    def test_disconnect(self, kafka_config, mock_consumer):
        """Test disconnecting from Kafka."""
        consumer = FinancialPositionConsumer(kafka_config)
        
        mock_kafka = MagicMock()
        mock_consumer.return_value = mock_kafka
        
        consumer.connect()
        consumer.disconnect()
        
        mock_kafka.close.assert_called_once()
        assert consumer._consumer is None
    
    def test_parse_message_valid(self, kafka_config, sample_position_data):
        """Test parsing valid message."""
        consumer = FinancialPositionConsumer(kafka_config)
        
        message_bytes = json.dumps(sample_position_data).encode('utf-8')
        position = consumer._parse_message(message_bytes)
        
        assert isinstance(position, FinancialPosition)
        assert position.position_id == "POS-123"
        assert position.symbol == "AAPL"
    
    def test_parse_message_invalid_json(self, kafka_config):
        """Test parsing invalid JSON."""
        consumer = FinancialPositionConsumer(kafka_config)
        
        invalid_json = b"not valid json"
        
        with pytest.raises(MessageValidationError):
            consumer._parse_message(invalid_json)
    
    def test_parse_message_invalid_data(self, kafka_config):
        """Test parsing message with invalid data."""
        consumer = FinancialPositionConsumer(kafka_config)
        
        invalid_data = {
            "position_id": "POS-123",
            # Missing required fields
        }
        
        message_bytes = json.dumps(invalid_data).encode('utf-8')
        
        with pytest.raises(MessageValidationError):
            consumer._parse_message(message_bytes)
    
    def test_consume_batch_not_connected(self, kafka_config):
        """Test consuming batch without connection."""
        consumer = FinancialPositionConsumer(kafka_config)
        
        with pytest.raises(KafkaConsumerError):
            consumer.consume_batch(10)
    
    @patch('financial_snapshot.kafka_consumer.Consumer')
    def test_consume_batch_success(
        self,
        mock_consumer_cls,
        kafka_config,
        sample_position_data
    ):
        """Test successful batch consumption."""
        # Setup mock consumer
        mock_kafka = MagicMock()
        mock_consumer_cls.return_value = mock_kafka
        
        # Create mock messages
        mock_msg = MagicMock()
        mock_msg.error.return_value = None
        mock_msg.value.return_value = json.dumps(sample_position_data).encode('utf-8')
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 100
        
        # First poll returns message, second returns None
        mock_kafka.poll.side_effect = [mock_msg, None]
        
        consumer = FinancialPositionConsumer(kafka_config)
        consumer.connect()
        
        positions = consumer.consume_batch(batch_size=10, timeout=1.0)
        
        assert len(positions) == 1
        assert positions[0].position_id == "POS-123"
        assert consumer._message_count == 1
    
    @patch('financial_snapshot.kafka_consumer.Consumer')
    def test_consume_batch_with_errors(
        self,
        mock_consumer_cls,
        kafka_config
    ):
        """Test batch consumption with message errors."""
        # Setup mock consumer
        mock_kafka = MagicMock()
        mock_consumer_cls.return_value = mock_kafka
        
        # Create mock error message
        mock_msg = MagicMock()
        mock_error = MagicMock()
        mock_error.code.return_value = KafkaError.INVALID_MSG
        mock_msg.error.return_value = mock_error
        
        mock_kafka.poll.side_effect = [mock_msg, None]
        
        consumer = FinancialPositionConsumer(kafka_config)
        consumer.connect()
        
        positions = consumer.consume_batch(batch_size=10, timeout=1.0)
        
        assert len(positions) == 0
        assert consumer._error_count == 1
    
    @patch('financial_snapshot.kafka_consumer.Consumer')
    def test_commit_offsets(self, mock_consumer_cls, kafka_config):
        """Test committing offsets."""
        mock_kafka = MagicMock()
        mock_consumer_cls.return_value = mock_kafka
        
        consumer = FinancialPositionConsumer(kafka_config)
        consumer.connect()
        
        consumer.commit_offsets(asynchronous=True)
        
        mock_kafka.commit.assert_called_once_with(asynchronous=True)
    
    @patch('financial_snapshot.kafka_consumer.Consumer')
    def test_get_statistics(self, mock_consumer_cls, kafka_config):
        """Test getting consumer statistics."""
        consumer = FinancialPositionConsumer(kafka_config)
        consumer._message_count = 100
        consumer._error_count = 5
        
        stats = consumer.get_statistics()
        
        assert stats['messages_consumed'] == 100
        assert stats['errors'] == 5
        assert stats['error_rate'] == 0.05
    
    @patch('financial_snapshot.kafka_consumer.Consumer')
    def test_context_manager(self, mock_consumer_cls, kafka_config):
        """Test using consumer as context manager."""
        mock_kafka = MagicMock()
        mock_consumer_cls.return_value = mock_kafka
        
        with FinancialPositionConsumer(kafka_config) as consumer:
            assert consumer._consumer is not None
        
        mock_kafka.close.assert_called_once()
