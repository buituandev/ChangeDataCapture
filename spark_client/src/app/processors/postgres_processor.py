"""
PostgreSQL CDC processor.

This module contains the batch processor for PostgreSQL CDC events from Debezium.
"""

from typing import Optional
import logging

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, max_by, struct, when, from_json
from delta.tables import DeltaTable

from ..schemas.schema_manager import SchemaManager


class PostgresProcessor:
    """
    Processes CDC events from PostgreSQL via Debezium.
    
    This class handles the batch processing of CDC events, including:
    - Schema discovery and validation
    - Event parsing and transformation
    - Delta table creation and updates (merge operations)
    """
    
    def __init__(
        self, 
        spark: SparkSession, 
        schema_manager: SchemaManager,
        output_path: str,
        key_column: str = "id",
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the PostgreSQL CDC processor.
        
        Args:
            spark: The SparkSession to use
            schema_manager: SchemaManager instance for schema operations
            output_path: Path to the Delta table
            key_column: Primary key column name
            logger: Optional logger instance
        """
        self.spark = spark
        self.schema_manager = schema_manager
        self.output_path = output_path
        self.key_column = key_column
        self.logger = logger or self._setup_logger()
        
        # Internal state
        self.select_cols = None
        self.ordered_fields = None
    
    def _setup_logger(self) -> logging.Logger:
        """Set up a logger for this class."""
        logger = logging.getLogger("PostgresProcessor")
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
    
    def process_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        """
        Process a batch of CDC events from Kafka.
        
        Args:
            batch_df: Batch of CDC events from Kafka
            batch_id: The batch identifier
        
        Raises:
            ValueError: If the key column is not found in the schema
        """
        if batch_df.isEmpty():
            return

        # Get or create schema
        if self.select_cols is None or self.ordered_fields is None:
            data_json = batch_df.first()["value"]
            schema, field_info = self.schema_manager.get_schema(data_json, "postgres")
            self.select_cols, self.ordered_fields = self.schema_manager.generate_select_statements(
                schema, field_info
            )
            
            if self.key_column not in self.ordered_fields:
                raise ValueError(
                    f"Key column '{self.key_column}' not found in schema fields: {self.ordered_fields}"
                )

        # Parse the CDC events
        parsed_batch = batch_df.select(
            from_json(col("value"), self.schema_manager.cached_schema).alias("data")
        )
        
        parsed_data = parsed_batch.select(self.select_cols)
        parsed_data = parsed_data.filter(col("operation").isNotNull())
        
        # Extract key value for grouping
        parsed_data = parsed_data.withColumn(
            "key_value", 
            when(col("operation") == "d", col(f"before_{self.key_column}"))
            .when(col(f"after_{self.key_column}").isNotNull(), col(f"after_{self.key_column}"))
        )
            
        # Group by key to get the latest operation for each record
        aggregated_batch = parsed_data.groupBy("key_value").agg(
            max_by(
                struct(
                    "operation", 
                    "timestamp",
                    *[col(f"after_{field}") for field in self.ordered_fields],
                    *[col(f"before_{field}") for field in self.ordered_fields]
                ), 
                "timestamp"
            ).alias("latest")
        )
        
        # Flatten the results
        final_df = aggregated_batch.select(
            col("key_value"),
            col("latest.operation").alias("operation"),
            col("latest.timestamp").alias("timestamp"),
            *[col(f"latest.after_{field}").alias(f"after_{field}") for field in self.ordered_fields],
            *[col(f"latest.before_{field}").alias(f"before_{field}") for field in self.ordered_fields]
        )
        
        # Handle the Delta table operations
        self._process_delta_operations(final_df)
    
    def _process_delta_operations(self, final_df: DataFrame) -> None:
        """
        Apply the CDC operations to the Delta table.
        
        Args:
            final_df: DataFrame with processed CDC events
        """
        # Check if the Delta table exists
        table_exists = False
        try:
            delta_table = DeltaTable.forPath(self.spark, self.output_path)
            table_exists = True
        except AnalysisException:
            table_exists = False
        
        # Create table if it doesn't exist
        if not table_exists:
            creates = final_df.filter(col("operation") == "c")
            if creates.isEmpty():
                self.logger.info("No create operations to initialize table")
                return
            
            # Prepare initial data
            initial_data = creates.select(
                *[col(f"after_{field}").alias(field) for field in self.ordered_fields],
                col("timestamp")
            )
            
            self.logger.info(f"Creating initial table with {initial_data.count()} records")
            initial_data.write.format("delta").mode("append").save(self.output_path)
            
            delta_table = DeltaTable.forPath(self.spark, self.output_path)
            table_exists = True
            
            # Remove creates from further processing
            final_df = final_df.filter(col("operation") != "c")
        
        # Process remaining operations
        if table_exists and not final_df.isEmpty():
            # Handle upserts (creates and updates)
            cu_ops = final_df.filter(col("operation").isin(["c", "u"]))
            if not cu_ops.isEmpty():
                update_df = cu_ops.select(
                    *[col(f"after_{field}").alias(field) for field in self.ordered_fields],
                    col("timestamp")
                )
                
                self.logger.info(f"Processing {update_df.count()} upserts in one merge")
                delta_table.alias("target").merge(
                    update_df.alias("source"),
                    f"target.{self.key_column} = source.{self.key_column}"
                ).whenMatchedUpdate(
                    condition=None,
                    set={**{field: f"source.{field}" for field in self.ordered_fields}, 
                         "timestamp": "source.timestamp"}
                ).whenNotMatchedInsertAll().execute()
            
            # Handle deletes
            d_ops = final_df.filter(col("operation") == "d")
            if not d_ops.isEmpty():
                delete_df = d_ops.select(
                    col(f"before_{self.key_column}").alias(self.key_column)
                )
                
                self.logger.info(f"Processing {delete_df.count()} deletes in one merge")
                delta_table.alias("target").merge(
                    delete_df.alias("source"),
                    f"target.{self.key_column} = source.{self.key_column}"
                ).whenMatchedDelete().execute()
        
        # Log final count for debugging
        if table_exists:
            final_count = self.spark.read.format("delta").load(self.output_path).count()
            self.logger.info(f"Final data count: {final_count}") 