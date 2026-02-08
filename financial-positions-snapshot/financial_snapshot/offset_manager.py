"""Kafka offset management in SQL Server."""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pyodbc

from financial_snapshot.config import SQLServerConfig


logger = logging.getLogger(__name__)


class KafkaOffsetManager:
    """
    Manages Kafka offsets in SQL Server.
    
    Stores and retrieves consumer offsets for each topic partition
    to enable exactly-once semantics and recovery.
    """
    
    def __init__(
        self,
        sql_config: SQLServerConfig,
        offsets_table: str = "KafkaOffsets"
    ):
        """
        Initialize offset manager.
        
        Args:
            sql_config: SQL Server configuration
            offsets_table: Name of the offsets table
        """
        self.sql_config = sql_config
        self.offsets_table = offsets_table
        
        # Build full table name
        if '.' in offsets_table:
            self.full_table_name = offsets_table
        else:
            self.full_table_name = f"{sql_config.database}.dbo.{offsets_table}"
        
        logger.info(
            "KafkaOffsetManager initialized",
            extra={"table": self.full_table_name}
        )
    
    def create_offsets_table_if_not_exists(self, connection: pyodbc.Connection) -> None:
        """
        Create the Kafka offsets table if it doesn't exist.
        
        Args:
            connection: Active database connection
        """
        cursor = connection.cursor()
        
        try:
            # Check if table exists
            check_query = """
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_CATALOG = ?
                AND TABLE_SCHEMA = ?
                AND TABLE_NAME = ?
            """
            
            parts = self.full_table_name.split('.')
            if len(parts) == 3:
                db, schema, table = parts
            else:
                db = self.sql_config.database
                schema = "dbo"
                table = parts[-1]
            
            cursor.execute(check_query, (db, schema, table))
            exists = cursor.fetchone()[0] > 0
            
            if not exists:
                create_query = f"""
                    CREATE TABLE {self.full_table_name} (
                        topic VARCHAR(255) NOT NULL,
                        partition INT NOT NULL,
                        consumer_group VARCHAR(255) NOT NULL,
                        offset_value BIGINT NOT NULL,
                        updated_at DATETIME2 NOT NULL DEFAULT GETDATE(),
                        PRIMARY KEY (topic, partition, consumer_group)
                    );
                    
                    CREATE INDEX idx_kafka_offsets_topic 
                        ON {self.full_table_name} (topic, consumer_group);
                """
                
                cursor.execute(create_query)
                connection.commit()
                
                logger.info(f"Created offsets table: {self.full_table_name}")
            else:
                logger.debug(f"Offsets table already exists: {self.full_table_name}")
                
        except Exception as e:
            logger.error(
                f"Failed to create offsets table: {e}",
                exc_info=True
            )
            raise
    
    def save_offsets(
        self,
        connection: pyodbc.Connection,
        topic: str,
        consumer_group: str,
        offsets: Dict[int, int]
    ) -> None:
        """
        Save offsets for all partitions in a single transaction.
        
        This method should be called within an active transaction.
        
        Args:
            connection: Active database connection (in transaction)
            topic: Kafka topic name
            consumer_group: Consumer group ID
            offsets: Dictionary mapping partition -> offset
        """
        if not offsets:
            logger.debug("No offsets to save")
            return
        
        cursor = connection.cursor()
        
        try:
            # Use MERGE for upsert
            merge_query = f"""
                MERGE {self.full_table_name} AS target
                USING (SELECT ? AS topic, ? AS partition, ? AS consumer_group, ? AS offset_value) AS source
                ON (target.topic = source.topic 
                    AND target.partition = source.partition 
                    AND target.consumer_group = source.consumer_group)
                WHEN MATCHED THEN
                    UPDATE SET 
                        offset_value = source.offset_value,
                        updated_at = GETDATE()
                WHEN NOT MATCHED THEN
                    INSERT (topic, partition, consumer_group, offset_value, updated_at)
                    VALUES (source.topic, source.partition, source.consumer_group, source.offset_value, GETDATE());
            """
            
            for partition, offset in offsets.items():
                cursor.execute(merge_query, (topic, partition, consumer_group, offset))
            
            logger.info(
                f"Saved offsets for {len(offsets)} partitions",
                extra={
                    "topic": topic,
                    "consumer_group": consumer_group,
                    "partitions": list(offsets.keys())
                }
            )
            
        except Exception as e:
            logger.error(
                f"Failed to save offsets: {e}",
                exc_info=True
            )
            raise
    
    def load_offsets(
        self,
        connection: pyodbc.Connection,
        topic: str,
        consumer_group: str
    ) -> Dict[int, int]:
        """
        Load offsets for all partitions of a topic.
        
        Args:
            connection: Active database connection
            topic: Kafka topic name
            consumer_group: Consumer group ID
            
        Returns:
            Dict[int, int]: Dictionary mapping partition -> offset
        """
        cursor = connection.cursor()
        
        try:
            query = f"""
                SELECT partition, offset_value
                FROM {self.full_table_name}
                WHERE topic = ? AND consumer_group = ?
            """
            
            cursor.execute(query, (topic, consumer_group))
            
            offsets = {}
            for row in cursor.fetchall():
                partition, offset = row
                offsets[partition] = offset
            
            logger.info(
                f"Loaded offsets for {len(offsets)} partitions",
                extra={
                    "topic": topic,
                    "consumer_group": consumer_group,
                    "partitions": list(offsets.keys())
                }
            )
            
            return offsets
            
        except Exception as e:
            logger.error(
                f"Failed to load offsets: {e}",
                exc_info=True
            )
            raise
    
    def get_latest_offset(
        self,
        connection: pyodbc.Connection,
        topic: str,
        partition: int,
        consumer_group: str
    ) -> Optional[int]:
        """
        Get the latest offset for a specific partition.
        
        Args:
            connection: Active database connection
            topic: Kafka topic name
            partition: Partition number
            consumer_group: Consumer group ID
            
        Returns:
            Optional[int]: Latest offset or None if not found
        """
        cursor = connection.cursor()
        
        try:
            query = f"""
                SELECT offset_value
                FROM {self.full_table_name}
                WHERE topic = ? AND partition = ? AND consumer_group = ?
            """
            
            cursor.execute(query, (topic, partition, consumer_group))
            row = cursor.fetchone()
            
            if row:
                return row[0]
            return None
            
        except Exception as e:
            logger.error(
                f"Failed to get latest offset: {e}",
                exc_info=True
            )
            return None
    
    def delete_offsets(
        self,
        connection: pyodbc.Connection,
        topic: str,
        consumer_group: str
    ) -> None:
        """
        Delete all offsets for a topic and consumer group.
        
        Args:
            connection: Active database connection
            topic: Kafka topic name
            consumer_group: Consumer group ID
        """
        cursor = connection.cursor()
        
        try:
            query = f"""
                DELETE FROM {self.full_table_name}
                WHERE topic = ? AND consumer_group = ?
            """
            
            cursor.execute(query, (topic, consumer_group))
            connection.commit()
            
            logger.info(
                f"Deleted offsets for topic {topic}",
                extra={"consumer_group": consumer_group}
            )
            
        except Exception as e:
            logger.error(
                f"Failed to delete offsets: {e}",
                exc_info=True
            )
            raise
