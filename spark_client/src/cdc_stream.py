"""
Spark Structured Streaming Application for Change Data Capture (CDC) Processing

Overview:
This module implements a robust streaming Extract-Transform-Load (ETL) pipeline designed to
process Change Data Capture (CDC) events. It leverages Apache Spark Structured Streaming to
handle real-time data modifications from source systems and applies them to a target Delta table.

Author: Unknown
Version: 2.2
Last Updated: March 16, 2025

Features:
- Reads CDC events from Kafka in Debezium format
- Supports MongoDB Debezium format
- Dynamically discovers and caches schema from incoming events
- Handles upsert (insert/update) and delete operations
- Writes processed data to a Delta table in MinIO
- Manages schema evolution for adaptable data structures
- Provides resilient processing with checkpoint management
- Offers configurable processing intervals for performance tuning

Change Log:
- v2.2: Added improved error handling and logging
- v2.1: Raised exceptions for unsupported database types
- v2.0: Added support for MongoDB Debezium format
- v2.0: Implemented dynamic schema generation for MongoDB

Architecture:
1. Input: Consumes CDC events from Kafka topics
2. Processing: 
   - Discovers and caches schema dynamically
   - Interprets Debezium event payloads
   - Processes upsert and delete operations
3. Output: Applies transformed changes to Delta table in MinIO
4. Reliability: Uses checkpointing for fault tolerance and recovery

Usage:
This application is intended for semi-real-time data integration scenarios where source system changes need to be captured and applied to a target data lake or warehouse with minimal latency.

Note:
This script depends on the ConfigManager class from the config_manager module.
Ensure that the config_manager.py file is available in the same directory as this script.
"""

import os
import json
import argparse
import logging
import sys
import time
from datetime import datetime

from pyspark.errors import AnalysisException
from pyspark.sql.functions import from_json, col, max_by, struct, when, udf, explode, map_keys
from pyspark.sql.types import StructType, IntegerType, LongType, FloatType, DoubleType, StringType, StructField, \
    BinaryType, DecimalType, BooleanType, MapType
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from config_manager import ConfigManager

# Configure logging
def setup_logging(log_level, log_file=None):
    """
    Set up logging with the specified log level and optional file output.
    
    Args:
        log_level (str): The logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file (str, optional): Path to log file. If None, logs to console only.
    """
    log_format = '%(asctime)s [%(levelname)s] %(name)s - %(message)s'
    log_date_format = '%Y-%m-%d %H:%M:%S'
    
    # Convert string log level to logging constant
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        numeric_level = logging.INFO
    
    # Configure root logger
    logging.basicConfig(
        level=numeric_level,
        format=log_format,
        datefmt=log_date_format,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Add file handler if log_file is specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter(log_format, log_date_format))
        logging.getLogger().addHandler(file_handler)
    
    # Create logger for this module
    logger = logging.getLogger('cdc_stream')
    return logger

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Spark Structured Streaming CDC application')
    parser.add_argument('--config', type=str, default="/opt/src/config.json",
                        help='Path to configuration file')
    return parser.parse_args()

args = parse_args()

# region Spark Configuration
config_manager = ConfigManager(args.config)
accessKeyId = config_manager.get("s3_config", "access_key_id")
secretAccessKey = config_manager.get("s3_config", "secret_access_key")
minio_endpoint = config_manager.get("s3_config", "endpoint")
minio_output_path = config_manager.get("delta_config", "output_path")
checkpoint_dir = config_manager.get("delta_config", "checkpoint_dir")
table = config_manager.get("kafka_config", "topic")
cache_schema_path = config_manager.get("cache_config", "schema_path")
cache_field_info_path = config_manager.get("cache_config", "field_info_path")
process_time = config_manager.get("processing_config", "process_time")
database_type = config_manager.get("database_config", "type")
columns_to_save = config_manager.get("database_config", "columns_to_save", None)

# Set up logging configuration
log_level = config_manager.get("logging_config", "level", "INFO")
log_file = config_manager.get("logging_config", "file", None)
logger = setup_logging(log_level, log_file)

# Log startup information
logger.info(f"Starting CDC Stream Application - Version 2.2")
logger.info(f"Database Type: {database_type}")
logger.info(f"Kafka Topic: {table}")
logger.info(f"Output Path: {minio_output_path}")
logger.info(f"Process Time: {process_time}")
if columns_to_save:
    logger.info(f"Columns Filter: {columns_to_save}")

# Global variables
cached_schema = None
cached_field_info = None
select_cols = None
ordered_fields = None
future_data = None
is_halfway = False
existing_data = None

spark = SparkSession.builder \
    .appName(args.config) \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.0,io.delta:delta-core_2.12:2.4.0") \
    .config("spark.hadoop.fs.s3a.access.key", accessKeyId) \
    .config("spark.hadoop.fs.s3a.secret.key", secretAccessKey) \
    .config("spark.hadoop.fs.s3a.path.style.access", config_manager.get("s3_config", "path_style_access")) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(config_manager.get("s3_config", "ssl_enabled")).lower()) \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.cores.max", 1) \
    .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
# endregion

# region Dynamic Schema Generation
def get_spark_type(debezium_type):
    """
    Convert Debezium data type to corresponding Spark SQL type.
    
    Args:
        debezium_type (str): The Debezium data type name
        
    Returns:
        pyspark.sql.types.DataType: The corresponding Spark SQL data type
    """
    type_mapping = {
        "int32": IntegerType(),
        "int64": LongType(),
        "float": FloatType(),
        "double": DoubleType(),
        "boolean": BooleanType(),
        "string": StringType(),
        "bytes": BinaryType(),
        "decimal": DecimalType(38, 18),
    }
    return type_mapping.get(debezium_type, StringType())

def get_schema_info_from_debezium(json_str):
    """
    Convert Debezium data type to corresponding Spark SQL type.
    
    Args:
        debezium_type (str): The Debezium data type name
        
    Returns:
        pyspark.sql.types.DataType: The corresponding Spark SQL data type
    """
    data = json.loads(json_str)
    schema = data['schema']

    field_info = []
    for field in schema['fields']:
        if field['field'] in ['before', 'after']:
            field_defs = field['fields']
            for f in field_defs:
                field_info.append({
                    'name': f['field'],
                    'type': f['type'],
                    'optional': f.get('optional', True)
                })
            break

    return field_info

def create_struct_type_from_debezium(field_info):
    """
    Create a Spark StructType from Debezium field information.
    
    Args:
        field_info (list): List of dictionaries with field information
        
    Returns:
        pyspark.sql.types.StructType: Spark schema for record structure
    """
    fields = []
    for field in field_info:
        spark_type = get_spark_type(field['type'])
        fields.append(StructField(field['name'], spark_type, field['optional']))
    return StructType(fields)

def create_dynamic_schema(data_json):
    """
    Create a complete Spark schema from a Debezium JSON payload.
    
    Analyzes the Debezium format and constructs a nested schema that includes
    the 'before' and 'after' record states along with metadata fields.
    
    Args:
        data_json (str): JSON string containing a Debezium event
        
    Returns:
        tuple: (
            pyspark.sql.types.StructType: Complete schema for Debezium data,
            str: Original JSON string,
            list: List of field information dictionaries
        )
    """
    field_info = get_schema_info_from_debezium(data_json)
    record_schema = create_struct_type_from_debezium(field_info)
    schema = StructType([
        StructField("schema", StringType(), True),
        StructField("payload", StructType([
            StructField("before", record_schema, True),
            StructField("after", record_schema, True),
            StructField("source", StringType(), True),
            StructField("op", StringType(), True),
            StructField("ts_ms", LongType(), True),
            StructField("transaction", StringType(), True)
        ]), True)
    ])
    return schema, data_json, field_info

def generate_select_statements(schema, field_info):
    """
    Generate column selection expressions for processing CDC data.
    
    Creates a list of column selections that extract metadata and 'before'/'after'
    record states from the nested Debezium format.
    
    Args:
        schema (pyspark.sql.types.StructType): Spark schema for Debezium data
        field_info (list): List of dictionaries with field information
        
    Returns:
        tuple: (
            list: Column expressions for selecting data,
            list: Ordered list of field names
        )
    """
    select_columns = [
        col("data.payload.op").alias("operation"),
        col("data.payload.ts_ms").alias("timestamp")
    ]

    filtered_field_info = field_info
    if columns_to_save is not None and columns_to_save != "all":
        cols_list = columns_to_save
        if not isinstance(columns_to_save, list):
            if isinstance(columns_to_save, str):
                cols_list = [col.strip() for col in columns_to_save.split(',')]
            else:
                cols_list = [str(columns_to_save)]
        
        filtered_field_info = [f for f in field_info if f['name'] in cols_list]

    for field in filtered_field_info:
        field_name = field['name']
        select_columns.extend([
            col(f"data.payload.before.{field_name}").alias(f"before_{field_name}"),
            col(f"data.payload.after.{field_name}").alias(f"after_{field_name}")
        ])

    return select_columns, [f['name'] for f in filtered_field_info]

# endregion

# region MongoDB Debezium Schema Processing
def get_schema_info_from_mongodb_debezium(json_str):
    """
    Extract schema information from a MongoDB Debezium CDC event.
    
    Args:
        json_str (str): JSON string containing a MongoDB Debezium event
        
    Returns:
        list: List of dictionaries with field information
    """
    data = json.loads(json_str)
    
    # For MongoDB, we need to extract schema from the actual data
    # since the Debezium envelope doesn't contain the full schema
    field_info = []
    
    # Check if after/before fields exist and extract schema from them
    if 'payload' in data and 'after' in data['payload'] and data['payload']['after']:
        # Parse the "after" field which contains the actual document as a JSON string
        after_data = json.loads(data['payload']['after'])
        for field_name, field_value in after_data.items():
            field_type = get_mongodb_field_type(field_value)
            field_info.append({
                'name': field_name,
                'type': field_type,
                'optional': True
            })
    elif 'payload' in data and 'before' in data['payload'] and data['payload']['before']:
        # Parse the "before" field as a fallback
        before_data = json.loads(data['payload']['before'])
        for field_name, field_value in before_data.items():
            field_type = get_mongodb_field_type(field_value)
            field_info.append({
                'name': field_name,
                'type': field_type,
                'optional': True
            })
    
    return field_info

def get_mongodb_field_type(value):
    """
    Determine the Debezium data type based on a MongoDB field value.
    
    Args:
        value: The value to analyze
        
    Returns:
        str: The corresponding Debezium data type
    """
    if value is None:
        return "string"
    
    if isinstance(value, bool):
        return "boolean"
    elif isinstance(value, int):
        # Check if it's within 32-bit integer range
        if -2147483648 <= value <= 2147483647:
            return "int32"
        else:
            return "int64"
    elif isinstance(value, float):
        return "double"
    elif isinstance(value, dict):
        # Handle ObjectId and other BSON types that come as dictionaries
        if "$oid" in value:
            return "string"  # ObjectId is treated as string
        elif "$date" in value:
            return "int64"   # Timestamps are treated as int64
        elif "$numberDecimal" in value:
            return "decimal"
        else:
            return "string"  # Other objects are treated as JSON strings
    else:
        return "string"

def create_dynamic_schema_mongodb(data_json):
    """
    Create a complete Spark schema from a MongoDB Debezium JSON payload.
    
    Args:
        data_json (str): JSON string containing a MongoDB Debezium event
        
    Returns:
        tuple: (
            pyspark.sql.types.StructType: Complete schema for MongoDB Debezium data,
            str: Original JSON string,
            list: List of field information dictionaries
        )
    """
    field_info = get_schema_info_from_mongodb_debezium(data_json)
    
    # MongoDB Debezium format has before/after as JSON strings
    schema = StructType([
        StructField("schema", StringType(), True),
        StructField("payload", StructType([
            StructField("before", StringType(), True),
            StructField("after", StringType(), True),
            StructField("source", StructType([
                StructField("connector", StringType(), True),
                StructField("version", StringType(), True),
                StructField("name", StringType(), True),
                StructField("ts_ms", LongType(), True),
                StructField("snapshot", StringType(), True),
                StructField("db", StringType(), True),
                StructField("rs", StringType(), True),
                StructField("collection", StringType(), True),
                StructField("ord", IntegerType(), True),
                StructField("h", StringType(), True)
            ]), True),
            StructField("op", StringType(), True),
            StructField("ts_ms", LongType(), True),
            StructField("transaction", StringType(), True)
        ]), True)
    ])
    
    return schema, data_json, field_info

def extract_bson_value(value):
    """
    Extract the actual value from MongoDB extended JSON format.
    
    Args:
        value: Value potentially in MongoDB extended JSON format
        
    Returns:
        The extracted value in a format suitable for Delta Lake
    """
    if not isinstance(value, dict):
        return value
        
    # Handle MongoDB extended JSON types
    if "$oid" in value:
        return value["$oid"]  # Return ObjectId as string
    elif "$numberLong" in value:
        return int(value["$numberLong"])  # Convert to integer
    elif "$numberInt" in value:
        return int(value["$numberInt"])
    elif "$numberDouble" in value:
        return float(value["$numberDouble"])
    elif "$numberDecimal" in value:
        return float(value["$numberDecimal"])  # Convert to float as an approximation
    elif "$date" in value:
        # Could be timestamp or ISO string
        date_val = value["$date"]
        if isinstance(date_val, dict) and "$numberLong" in date_val:
            return int(date_val["$numberLong"])
        return date_val
    elif "$binary" in value:
        # For binary data, store as a JSON string representation
        return json.dumps(value)
    elif "$regex" in value:
        return json.dumps(value)
    else:
        # For other complex objects, serialize to JSON
        return json.dumps(value)

def process_mongodb_document(doc_dict):
    """
    Process a MongoDB document to handle extended JSON types.
    
    Args:
        doc_dict: Dictionary representing a MongoDB document
        
    Returns:
        dict: Processed document with normalized values
    """
    if not doc_dict:
        return None
        
    result = {}
    for field, value in doc_dict.items():
        if isinstance(value, dict):
            # Check if it's a MongoDB extended JSON type
            if any(key.startswith("$") for key in value.keys()):
                result[field] = extract_bson_value(value)
            else:
                # It's a nested document
                result[field] = process_mongodb_document(value)
        elif isinstance(value, list):
            # Handle arrays
            result[field] = [
                process_mongodb_document(item) if isinstance(item, dict) else item 
                for item in value
            ]
        else:
            result[field] = value
            
    return result
#endregion

# region Cache Schema
def save_cached_schema(schema, field_info):
    """
    Save schema and field information to disk for future use.
    
    Args:
        schema (pyspark.sql.types.StructType): Spark schema to save
        field_info (list): Field information dictionaries
    """
    schema_json = schema.json()
    with open(cache_schema_path, "w") as f:
        f.write(schema_json)
    with open(cache_field_info_path, "w") as f:
        f.write(json.dumps(field_info))


def load_cached_schema():
    """
    Load previously cached schema and field information from disk.
    
    Returns:
        tuple: (
            pyspark.sql.types.StructType: Loaded schema,
            list: Loaded field information
        )
    """
    with open(cache_schema_path, "r") as f:
        schema_json = f.read()
    schema = StructType.fromJson(json.loads(schema_json))
    with open(cache_field_info_path, "r") as f:
        field_info = json.loads(f.read())
    return schema, field_info


def is_cached_schema():
    """
    Check if cached schema files exist.
    
    Returns:
        bool: True if cached schema exists, False otherwise
    """
    return os.path.exists(cache_schema_path) and os.path.exists(cache_field_info_path)
# endregion

# region Batch Processing
def process_batch(batch_df, batch_id, key_column_name='id'):
    """
    Process a batch of CDC events from Kafka.
    
    This is the core processing function that:
    1. Initializes or loads the schema
    2. Parses the Debezium format
    3. Aggregates operations by key to get the latest state
    4. Applies changes to the target Delta table
    
    Args:
        batch_df (pyspark.sql.DataFrame): Batch of CDC events from Kafka
        batch_id (int): The batch identifier
        key_column_name (str, optional): Primary key column name. Defaults to 'id'.
    """
    global cached_schema, cached_field_info, select_cols, ordered_fields, process_time
    
    if batch_df.isEmpty():
        return

    logger.info(f"Processing batch {batch_id}")
    batch_start_time = time.time()
    
    try:
        # Schema initialization
        if is_cached_schema() and cached_schema is None:
            cached_schema, cached_field_info = load_cached_schema()

        if not cached_schema:
            logger.info("Schema not initialized, generating from data")
            data_json = batch_df.first()["value"]
            cached_schema, _, cached_field_info = create_dynamic_schema(data_json)
            save_cached_schema(cached_schema, cached_field_info)

        # Parse batch data
        parsed_batch = batch_df.select(from_json(col("value"), cached_schema).alias("data"))
        if not select_cols or not ordered_fields:
            select_cols, ordered_fields = generate_select_statements(cached_schema, cached_field_info)

        if key_column_name not in ordered_fields:
            err_msg = f"Key column '{key_column_name}' not found in schema fields: {ordered_fields}"
            logger.error(err_msg)
            raise ValueError(err_msg)

        parsed_data = parsed_batch.select(select_cols)
        parsed_data = parsed_data.filter(col("operation").isNotNull())
        
        parsed_data = parsed_data.withColumn(
            "key_value", 
            when(col("operation") == "d", col(f"before_{key_column_name}"))
            .when(col(f"after_{key_column_name}").isNotNull(), col(f"after_{key_column_name}"))
        )
        
        aggregated_batch = parsed_data.groupBy("key_value").agg(
            max_by(
                struct(
                    "operation", 
                    "timestamp",
                    *[col(f"after_{field}") for field in ordered_fields],
                    *[col(f"before_{field}") for field in ordered_fields]
                ), 
                "timestamp"
            ).alias("latest")
        )
        
        final_df = aggregated_batch.select(
            col("key_value"),
            col("latest.operation").alias("operation"),
            col("latest.timestamp").alias("timestamp"),
            *[col(f"latest.after_{field}").alias(f"after_{field}") for field in ordered_fields],
            *[col(f"latest.before_{field}").alias(f"before_{field}") for field in ordered_fields]
        )
        
        # Check if target Delta table exists    
        table_exists = False
        try:
            delta_table = DeltaTable.forPath(spark, minio_output_path)
            table_exists = True
            logger.debug(f"Target Delta table found at {minio_output_path}")
        except AnalysisException:
            logger.info(f"Target Delta table does not exist at {minio_output_path}, will create on first insert")
            table_exists = False
        
        # Handle table creation for initial data
        if not table_exists:
            creates = final_df.filter(col("operation") == "c")
            if creates.isEmpty():
                logger.info("No create operations to initialize table, skipping")
                return
            
            initial_data = creates.select(
                *[col(f"after_{field}").alias(field) for field in ordered_fields],
                col("timestamp")
            )
            
            count = initial_data.count()
            logger.info(f"Creating initial Delta table with {count} records")
            initial_data.write.format("delta").mode("append").save(minio_output_path)
            
            delta_table = DeltaTable.forPath(spark, minio_output_path)
            table_exists = True
            logger.info(f"Delta table created successfully at {minio_output_path}")
            
            final_df = final_df.filter(col("operation") != "c")
        
        # Process operations if the table exists
        if table_exists and not final_df.isEmpty():
            # Process upserts (creates and updates)
            cu_ops = final_df.filter(col("operation").isin(["c", "u"]))
            if not cu_ops.isEmpty():
                update_df = cu_ops.select(
                    *[col(f"after_{field}").alias(field) for field in ordered_fields],
                    col("timestamp")
                )
                
                upsert_count = update_df.count()
                logger.info(f"Processing {upsert_count} upserts in one merge operation")
                
                merge_start = time.time()
                delta_table.alias("target").merge(
                    update_df.alias("source"),
                    f"target.{key_column_name} = source.{key_column_name}"
                ).whenMatchedUpdate(
                    condition=None,
                    set={**{field: f"source.{field}" for field in ordered_fields}, 
                         "timestamp": "source.timestamp"}
                ).whenNotMatchedInsertAll().execute()
                
                logger.debug(f"Upsert merge completed in {time.time() - merge_start:.2f} seconds")
            
            # Process deletes
            d_ops = final_df.filter(col("operation") == "d")
            if not d_ops.isEmpty():
                delete_df = d_ops.select(
                    col(f"before_{key_column_name}").alias(key_column_name)
                )
                
                delete_count = delete_df.count()
                logger.info(f"Processing {delete_count} deletes in one merge operation")
                
                delete_start = time.time()
                delta_table.alias("target").merge(
                    delete_df.alias("source"),
                    f"target.{key_column_name} = source.{key_column_name}"
                ).whenMatchedDelete().execute()
                
                logger.debug(f"Delete merge completed in {time.time() - delete_start:.2f} seconds")
        
        # Log final table stats
        if table_exists:
            final_count = spark.read.format("delta").load(minio_output_path).count()
            logger.info(f"Batch {batch_id} processing complete. Final data count: {final_count}")
        
        new_process_time = config_manager.get("processing_config", "process_time")
        if new_process_time != process_time:
            logger.info(f"Process time config changed from {process_time} to {new_process_time}")
            process_time = new_process_time

    except Exception as e:
        logger.error(f"Error processing batch {batch_id}: {str(e)}", exc_info=True)
        raise
    finally:
        batch_duration = time.time() - batch_start_time
        logger.info(f"Batch {batch_id} processed in {batch_duration:.2f} seconds")

def process_mongodb_batch(batch_df, batch_id, key_column_name='_id'):
    """
    Process a batch of MongoDB CDC events from Kafka.
    
    Args:
        batch_df (pyspark.sql.DataFrame): Batch of CDC events from Kafka
        batch_id (int): The batch identifier
        key_column_name (str, optional): Primary key column name. Defaults to '_id'.
    """
    global cached_schema, cached_field_info, process_time
    
    if batch_df.isEmpty():
        return

    logger.info(f"Processing MongoDB batch {batch_id}")
    batch_start_time = time.time()
    
    try:
        # UDF to process MongoDB JSON documents
        @udf(returnType=MapType(StringType(), StringType()))
        def process_document_udf(json_str):
            if not json_str:
                return None
            try:
                doc = json.loads(json_str)
                processed_doc = process_mongodb_document(doc)
                return {k: str(v) if v is not None else None for k, v in processed_doc.items()}
            except Exception as e:
                logger.error(f"Error processing MongoDB document: {e}", exc_info=True)
                return None

        logger.debug("Parsing MongoDB CDC events")
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
        logger.debug("Processing MongoDB documents")
        processed_data = parsed_data.withColumn(
            "before_doc", process_document_udf(col("before_json"))
        ).withColumn(
            "after_doc", process_document_udf(col("after_json"))
        )
        
        # Extract key field for grouping
        with_key = processed_data.withColumn(
            "key_value", 
            when(col("operation") == "d", col(f"before_doc.{key_column_name}"))
            .otherwise(col(f"after_doc.{key_column_name}"))
        )
        
        # Group by key to get the latest state of each document
        logger.debug("Aggregating MongoDB documents by key")
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
        
        # Check if the Delta table exists
        table_exists = False
        try:
            delta_table = DeltaTable.forPath(spark, minio_output_path)
            table_exists = True
            logger.debug(f"MongoDB target Delta table found at {minio_output_path}")
        except AnalysisException:
            logger.info(f"MongoDB target Delta table does not exist at {minio_output_path}, will create on first insert")
            table_exists = False
        
        # Handle table creation for initial data
        if not table_exists:
            creates = final_df.filter(col("operation") == "c")
            if creates.isEmpty():
                logger.info("No MongoDB create operations to initialize table, skipping")
                return
            
            # For table creation, we'll convert the map to a struct
            # First get all possible field names from all documents
            logger.debug("Discovering fields from MongoDB documents")
            all_fields = creates.select(
                explode(map_keys(col("after_doc")))
            ).distinct().collect()
            
            field_names = [row[0] for row in all_fields]
            
            if columns_to_save is not None and columns_to_save != "all":
                cols_list = columns_to_save
                if not isinstance(columns_to_save, list):
                    if isinstance(columns_to_save, str):
                        cols_list = [col.strip() for col in columns_to_save.split(',')]
                    else:
                        cols_list = [str(columns_to_save)]
                
                if key_column_name not in cols_list:
                    cols_list.append(key_column_name)
                    
                field_names = [f for f in field_names if f in cols_list]
                logger.info(f"Filtered MongoDB fields to: {field_names}")
            
            # Create a select expression for each field
            select_expr = ["timestamp"]
            for field_name in field_names:
                select_expr.append(f"after_doc['{field_name}'] as `{field_name}`")
            
            # Create the initial table
            initial_data = creates.selectExpr(*select_expr)
            count = initial_data.count()
            logger.info(f"Creating initial MongoDB Delta table with {count} records and {len(field_names)} fields")
            initial_data.write.format("delta").mode("append").save(minio_output_path)
            
            delta_table = DeltaTable.forPath(spark, minio_output_path)
            table_exists = True
            logger.info(f"MongoDB Delta table created successfully at {minio_output_path}")
            
            # Remove creates from further processing
            final_df = final_df.filter(col("operation") != "c")
        
        # Process remaining operations if the table exists
        if table_exists and not final_df.isEmpty():
            # Handle upserts (creates and updates)
            cu_ops = final_df.filter(col("operation").isin(["c", "u"]))
            if not cu_ops.isEmpty():
                # Get the schema of the existing Delta table
                existing_schema = spark.read.format("delta").load(minio_output_path).schema
                field_names = [field.name for field in existing_schema.fields if field.name != "timestamp"]
                
                # Create select expressions for all fields in the table
                select_expr = ["timestamp"]
                for field_name in field_names:
                    select_expr.append(f"after_doc['{field_name}'] as `{field_name}`")
                
                # Prepare update dataframe
                update_df = cu_ops.selectExpr(*select_expr)
                upsert_count = update_df.count()
                logger.info(f"Processing {upsert_count} MongoDB upserts")
                
                merge_start = time.time()
                # Merge into Delta table
                delta_table.alias("target").merge(
                    update_df.alias("source"),
                    f"target.`{key_column_name}` = source.`{key_column_name}`"
                ).whenMatchedUpdate(
                    condition=None,
                    set={**{field_name: f"source.`{field_name}`" for field_name in field_names}, 
                         "timestamp": "source.timestamp"}
                ).whenNotMatchedInsertAll().execute()
                logger.debug(f"MongoDB upsert merge completed in {time.time() - merge_start:.2f} seconds")
            
            # Handle deletes
            d_ops = final_df.filter(col("operation") == "d")
            if not d_ops.isEmpty():
                # Create a dataframe with just the key field
                delete_expr = [f"before_doc['{key_column_name}'] as `{key_column_name}`"]
                delete_df = d_ops.selectExpr(*delete_expr)
                delete_count = delete_df.count()
                logger.info(f"Processing {delete_count} MongoDB deletes")
                
                delete_start = time.time()
                # Delete from Delta table
                delta_table.alias("target").merge(
                    delete_df.alias("source"),
                    f"target.`{key_column_name}` = source.`{key_column_name}`"
                ).whenMatchedDelete().execute()
                logger.debug(f"MongoDB delete merge completed in {time.time() - delete_start:.2f} seconds")
        
        # Debug info
        if table_exists:
            final_count = spark.read.format("delta").load(minio_output_path).count()
            logger.info(f"MongoDB batch {batch_id} processing complete. Final data count: {final_count}")
            
    except Exception as e:
        logger.error(f"Error processing MongoDB batch {batch_id}: {str(e)}", exc_info=True)
        raise
    finally:
        batch_duration = time.time() - batch_start_time
        logger.info(f"MongoDB batch {batch_id} processed in {batch_duration:.2f} seconds")

# endregion

# region Application
def run_stream():
    """
    Run the Spark Structured Streaming application.
    
    Sets up and starts the streaming query that:
    1. Reads CDC events from Kafka
    2. Processes batches using the process_batch function
    3. Handles configuration changes and graceful shutdown
    4. Manages checkpointing for fault tolerance
    
    The function runs indefinitely until interrupted.
    """
    global process_time
    
    # Get initial config values
    config = config_manager.get_config()
    kafka_servers = config["kafka_config"]["bootstrap_servers"]
    topic = config["kafka_config"]["topic"]
    fail_on_data_loss = config["kafka_config"]["fail_on_data_loss"]
    key_column = config["processing_config"]["key_column"]
    process_time = config["processing_config"]["process_time"]
    
    logger.info(f"Setting up Postgres CDC streaming from Kafka {kafka_servers}")
    logger.info(f"Using key column: {key_column}")
    
    restart_required = [False]
    batch_in_progress = [False]
    
    def create_query():
        """
        Create and return a streaming query.
        
        Returns:
            pyspark.sql.streaming.StreamingQuery: The running streaming query
        """
        logger.info(f"Creating streaming query with process time: {process_time}")
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", topic) \
            .option("failOnDataLoss", str(fail_on_data_loss).lower()) \
            .load() \
            .selectExpr("CAST(value AS STRING) as value")
        
        def managed_batch_processing(dataframe, b_id):
            current_process_time = process_time
            
            batch_in_progress[0] = True
            
            try:
                process_batch(dataframe, b_id, key_column_name=key_column)
                
                if process_time != current_process_time:
                    logger.info(f"Process time config changed from {current_process_time} to {process_time}")
                    restart_required[0] = True
            finally:
                batch_in_progress[0] = False
        
        query = df.writeStream \
            .foreachBatch(managed_batch_processing) \
            .option("checkpointLocation", config["delta_config"]["checkpoint_dir"]) \
            .trigger(processingTime=process_time) \
            .start()
            
        logger.info(f"Streaming query started with checkpoint at {config['delta_config']['checkpoint_dir']}")
        return query
    
    query = create_query()
    
    try:
        logger.info("CDC stream processing started, awaiting data...")
        while True:
            import time
            time.sleep(1)
            
            if restart_required[0] and not batch_in_progress[0]:
                status = query.status
                if status["isTriggerActive"] == False:
                    logger.info("Restarting query with new process time configuration")
                    query.stop()
                    query = create_query()
                    restart_required[0] = False
                    logger.info(f"Query restarted with process time: {process_time}")
    except KeyboardInterrupt:
        logger.info("Stopping stream processing due to user interrupt...")
        logger.info("Waiting for any in-progress batch to complete before stopping...")
        while batch_in_progress[0]:
            time.sleep(1)
        try:
            if query.isActive:
                query.stop()
                logger.info("Streaming query stopped successfully")
        except Exception as e:
            logger.error(f"Error stopping query: {str(e)}")
    except Exception as e:
        logger.error(f"Error in streaming application: {str(e)}", exc_info=True)
    finally:
        logger.info("CDC stream processing terminated")

def run_mongodb_stream():
    """
    Run the Spark Structured Streaming application for MongoDB CDC.
    """
    global process_time
    
    config = config_manager.get_config()
    kafka_servers = config["kafka_config"]["bootstrap_servers"]
    topic = config["kafka_config"]["topic"]
    fail_on_data_loss = config["kafka_config"]["fail_on_data_loss"]
    key_column = config["processing_config"].get("key_column", "_id")  # Default to _id for MongoDB
    process_time = config["processing_config"]["process_time"]
    
    logger.info(f"Setting up MongoDB CDC streaming from Kafka {kafka_servers}")
    logger.info(f"Using MongoDB key column: {key_column}")
    
    restart_required = [False]
    batch_in_progress = [False]
    
    def create_query():
        """
        Create and return a streaming query for MongoDB CDC.
        """
        logger.info(f"Creating MongoDB streaming query with process time: {process_time}")
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", topic) \
            .option("failOnDataLoss", str(fail_on_data_loss).lower()) \
            .load() \
            .selectExpr("CAST(value AS STRING) as value")
        
        def managed_batch_processing(dataframe, b_id):
            current_process_time = process_time
            
            batch_in_progress[0] = True
            
            try:
                process_mongodb_batch(dataframe, b_id, key_column_name=key_column)
                
                if process_time != current_process_time:
                    logger.info(f"MongoDB process time config changed from {current_process_time} to {process_time}")
                    restart_required[0] = True
            finally:
                batch_in_progress[0] = False
        
        query = df.writeStream \
            .foreachBatch(managed_batch_processing) \
            .option("checkpointLocation", checkpoint_dir) \
            .trigger(processingTime=process_time) \
            .start()
            
        logger.info(f"MongoDB streaming query started with checkpoint at {checkpoint_dir}")
        return query
    
    query = create_query()
    
    try:
        logger.info("MongoDB CDC stream processing started, awaiting data...")
        while True:
            import time
            time.sleep(1)
            
            if restart_required[0] and not batch_in_progress[0]:
                status = query.status
                if status["isTriggerActive"] == False:
                    logger.info("Restarting MongoDB query with new process time configuration")
                    query.stop()
                    query = create_query()
                    restart_required[0] = False
                    logger.info(f"MongoDB query restarted with process time: {process_time}")
    except KeyboardInterrupt:
        logger.info("Stopping MongoDB stream processing due to user interrupt...")
        logger.info("Waiting for any in-progress MongoDB batch to complete before stopping...")
        while batch_in_progress[0]:
            time.sleep(1)
        try:
            if query.isActive:
                query.stop()
                logger.info("MongoDB streaming query stopped successfully")
        except Exception as e:
            logger.error(f"Error stopping MongoDB query: {str(e)}")
    except Exception as e:
        logger.error(f"Error in MongoDB streaming application: {str(e)}", exc_info=True)
    finally:
        logger.info("MongoDB CDC stream processing terminated")

if __name__ == "__main__":
    try:
        logger.info(f"Starting CDC streaming application with database type: {database_type}")
        if database_type == "postgres":
            run_stream()
        elif database_type == "mongo":
            run_mongodb_stream()
        else:
            error_msg = f"Unsupported database type: {database_type}"
            logger.error(error_msg)
            raise ValueError(error_msg)
    except Exception as e:
        logger.error(f"Fatal error in CDC application: {str(e)}", exc_info=True)
        raise
# endregion