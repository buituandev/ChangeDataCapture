"""
MongoDB CDC processor.

This module contains the batch processor for MongoDB CDC events from Debezium.
"""

from typing import Optional
import json
import logging

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, max_by, struct, when, udf, explode, map_keys
from pyspark.sql.types import MapType, StringType
from delta.tables import DeltaTable

from ..schemas.schema_manager import SchemaManager
from ..utils.mongodb_utils import process_mongodb_document


class MongoDBProcessor:
    """
    Processes CDC events from MongoDB via Debezium.
    
    This class handles the batch processing of CDC events, including:
    - Schema discovery and validation
    - Event parsing and transformation
    - Delta table creation and updates (merge operations)
    - Handling of MongoDB BSON format
    """
    
    def __init__(
        self, 
        spark: SparkSession, 
        schema_manager: SchemaManager,
        output_path: str,
        key_column: str = "_id",
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the MongoDB CDC processor.
        
        Args:
            spark: The SparkSession to use
            schema_manager: SchemaManager instance for schema operations
            output_path: Path to the Delta table
            key_column: Primary key column name (default: "_id")
            logger: Optional logger instance
        """
        self.spark = spark
        self.schema_manager = schema_manager
        self.output_path = output_path
        self.key_column = key_column
        self.logger = logger or self._setup_logger()
    
    def _setup_logger(self) -> logging.Logger:
        """Set up a logger for this class."""
        logger = logging.getLogger("MongoDBProcessor")
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
        Process a batch of MongoDB CDC events from Kafka.
        
        Args:
            batch_df: Batch of CDC events from Kafka
            batch_id: The batch identifier
        """
        if batch_df.isEmpty():
            return
        
        # Register UDF to process MongoDB JSON documents
        @udf(returnType=MapType(StringType(), StringType()))
        def process_document_udf(json_str):
            if not json_str:
                return None
            try:
                doc = json.loads(json_str)
                processed_doc = process_mongodb_document(doc)
                return {k: str(v) if v is not None else None for k, v in processed_doc.items()}
            except Exception as e:
                self.logger.error(f"Error processing document: {e}")
                return None
        
        # Extract basic fields from the Debezium envelope
        parsed_batch = batch_df.selectExpr(
            "CAST(value AS STRING) as event_json"
        )
        
        # Extract key fields using JSON path expressions
        parsed_data = parsed_batch.selectExpr(
            "get_json_object(event_json, '$.payload.op') as operation",
            "CAST(get_json_object(event_json, '$.payload.ts_ms') AS LONG) as timestamp",
            "get_json_object(event_json, '$.payload.before') as before_json",
            "get_json_object(event_json, '$.payload.after') as after_json"
        )
        
        # Apply UDF to process the documents
        processed_data = parsed_data.withColumn(
            "before_doc", process_document_udf(col("before_json"))
        ).withColumn(
            "after_doc", process_document_udf(col("after_json"))
        )
        
        # Extract key field for grouping
        with_key = processed_data.withColumn(
            "key_value", 
            when(col("operation") == "d", col(f"before_doc.{self.key_column}"))
            .otherwise(col(f"after_doc.{self.key_column}"))
        )
        
        # Group by key to get the latest state of each document
        aggregated = with_key.groupBy("key_value").agg(
            max_by(
                struct("operation", "timestamp", "before_doc", "after_doc"),
                "timestamp"
            ).alias("latest")
        )
        
        # Flatten the results
        final_df = aggregated.select(
            col("key_value"),
            col("latest.operation").alias("operation"),
            col("latest.timestamp").alias("timestamp"),
            col("latest.before_doc").alias("before_doc"),
            col("latest.after_doc").alias("after_doc")
        )
        
        # Process the Delta table operations
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
        
        # Handle table creation for initial data
        if not table_exists:
            creates = final_df.filter(col("operation") == "c")
            if creates.isEmpty():
                return
            
            # For table creation, we'll convert the map to a struct
            # First get all possible field names from all documents
            all_fields = creates.select(
                explode(map_keys(col("after_doc")))
            ).distinct().collect()
            
            field_names = [row[0] for row in all_fields]
            
            # Create a select expression for each field
            select_expr = ["timestamp"]
            for field_name in field_names:
                select_expr.append(f"after_doc['{field_name}'] as `{field_name}`")
            
            # Create the initial table
            initial_data = creates.selectExpr(*select_expr)
            initial_data.write.format("delta").mode("append").save(self.output_path)
            
            delta_table = DeltaTable.forPath(self.spark, self.output_path)
            table_exists = True
            
            # Remove creates from further processing
            final_df = final_df.filter(col("operation") != "c")
        
        # Process remaining operations if the table exists
        if table_exists and not final_df.isEmpty():
            # Handle upserts (creates and updates)
            cu_ops = final_df.filter(col("operation").isin(["c", "u"]))
            if not cu_ops.isEmpty():
                # Get the schema of the existing Delta table
                existing_schema = self.spark.read.format("delta").load(self.output_path).schema
                field_names = [field.name for field in existing_schema.fields if field.name != "timestamp"]
                
                # Create select expressions for all fields in the table
                select_expr = ["timestamp"]
                for field_name in field_names:
                    select_expr.append(f"after_doc['{field_name}'] as `{field_name}`")
                
                # Prepare update dataframe
                update_df = cu_ops.selectExpr(*select_expr)
                
                # Merge into Delta table
                delta_table.alias("target").merge(
                    update_df.alias("source"),
                    f"target.`{self.key_column}` = source.`{self.key_column}`"
                ).whenMatchedUpdate(
                    condition=None,
                    set={**{field_name: f"source.`{field_name}`" for field_name in field_names}, 
                         "timestamp": "source.timestamp"}
                ).whenNotMatchedInsertAll().execute()
            
            # Handle deletes
            d_ops = final_df.filter(col("operation") == "d")
            if not d_ops.isEmpty():
                # Create a dataframe with just the key field
                delete_expr = [f"before_doc['{self.key_column}'] as `{self.key_column}`"]
                delete_df = d_ops.selectExpr(*delete_expr)
                
                # Delete from Delta table
                delta_table.alias("target").merge(
                    delete_df.alias("source"),
                    f"target.`{self.key_column}` = source.`{self.key_column}`"
                ).whenMatchedDelete().execute()
        
        # Debug info
        if table_exists:
            final_count = self.spark.read.format("delta").load(self.output_path).count()
            self.logger.info(f"Final data count: {final_count}") 