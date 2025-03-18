"""
Processor factory for CDC processing.

This module provides a factory class that creates the appropriate processor
for different database types.
"""

from typing import Optional
import logging

from pyspark.sql import SparkSession

from ..config.config_manager import ConfigManager
from ..schemas.schema_manager import SchemaManager
from .postgres_processor import PostgresProcessor
from .mongodb_processor import MongoDBProcessor


class ProcessorFactory:
    """
    Factory class to create appropriate processors based on database type.
    
    This class abstracts away the instantiation of the correct processor
    based on configuration settings.
    """
    
    @staticmethod
    def create_processor(
        spark: SparkSession, 
        config_manager: ConfigManager,
        logger: Optional[logging.Logger] = None
    ):
        """
        Create an appropriate processor based on database type.
        
        Args:
            spark: The SparkSession to use
            config_manager: Configuration manager instance
            logger: Optional logger instance
            
        Returns:
            A processor instance (PostgresProcessor or MongoDBProcessor)
            
        Raises:
            ValueError: If an unsupported database type is specified
        """
        # Set up logger if not provided
        if logger is None:
            logger = logging.getLogger("ProcessorFactory")
            if not logger.handlers:
                handler = logging.StreamHandler()
                formatter = logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                )
                handler.setFormatter(formatter)
                logger.addHandler(handler)
                logger.setLevel(logging.INFO)
        
        # Get configuration values
        db_type = config_manager.get("database_config", "type")
        output_path = config_manager.get("delta_config", "output_path")
        key_column = config_manager.get("processing_config", "key_column")
        schema_path = config_manager.get("cache_config", "schema_path")
        field_info_path = config_manager.get("cache_config", "field_info_path")
        
        # Create schema manager
        schema_manager = SchemaManager(schema_path, field_info_path)
        
        # Create and return the appropriate processor
        if db_type == "postgres":
            logger.info("Creating PostgreSQL processor")
            return PostgresProcessor(
                spark=spark,
                schema_manager=schema_manager,
                output_path=output_path,
                key_column=key_column,
                logger=logger
            )
        elif db_type == "mongo":
            logger.info("Creating MongoDB processor")
            # MongoDB typically uses _id as primary key if not specified
            if key_column is None or key_column == "":
                key_column = "_id"
            
            return MongoDBProcessor(
                spark=spark,
                schema_manager=schema_manager,
                output_path=output_path,
                key_column=key_column,
                logger=logger
            )
        else:
            error_msg = f"Unsupported database type: {db_type}"
            logger.error(error_msg)
            raise ValueError(error_msg) 